import errno
import fnmatch
import os.path
import shutil
import struct

import six
import sys

from scandir import scandir


class Path(str):

    def join(self, path):
        return Path(os.path.join(self, path))

    def reset(self):
        shutil.rmtree(self, ignore_errors=True)
        os.makedirs(self)

    def read(self):
        with open(self, 'rb') as f:
            result = f.read()
        return result

    def write(self, content):
        with open(self, 'wb') as f:
            f.write(content)

    def delete(self):
        if os.path.isfile(self):
            os.remove(self)
        else:
            shutil.rmtree(self)

    def append(self, line):
        try:
            encoder = ZSetEncoder(open(self, 'rb+'))
        except IOError:
            encoder = ZSetEncoder(open(self, 'wb'))
            old_size = 0
        else:
            old_size = encoder.read_header()

        encoder.write_header(old_size + 1)
        encoder.write_element_to_eof(line)

    def readlines(self):
        with open(self, 'rb') as f:
            return ZSetEncoder(f).read_elements()

    def exists(self):
        return os.path.exists(self)

    def makedirs(self, ignore_if_exists=False):
        try:
            return os.makedirs(self)
        except OSError as exc:
            if ignore_if_exists and exc.errno == errno.EEXIST:
                pass
            else:
                six.reraise(*sys.exc_info())

    def listdir(self, pattern=None):
        all_files = os.listdir(self)
        if pattern is None:
            return all_files
        else:
            return list(fnmatch.filter(all_files, pattern))

    def remove_line(self, line_to_remove):
        with open(self, 'rb+') as f:
            num_elems = ZSetEncoder(f).remove_element(line_to_remove)
        if num_elems == 0:
            os.remove(self)

    def empty_directory(self):
        if self.exists():
            try:
                next(scandir(self))
            except StopIteration:
                return True
            else:
                return False
        else:
            return False

    def read_zset_header(self):
        with open(self, 'rb') as f:
            return ZSetEncoder(f).read_header()


class ZSetEncoder(object):

    HEADER_FMT = ">Q"
    HEADER_BYTES = 8
    ELEMENT_FMT = ">I"
    SIZE_BYTES = 4
    ACTIVE_FMT = ">c"
    ACTIVE_BYTES = 1
    IS_ACTIVE = 'y'
    IS_INACTIVE = 'n'

    def __init__(self, file_):
        self._file = file_

    def write_header(self, size, seek_to_start=True):
        if seek_to_start:
            self._file.seek(0, os.SEEK_SET)
        self._file.write(struct.pack(self.HEADER_FMT, size))

    def read_header(self):
        return struct.unpack(self.HEADER_FMT, self._file.read(self.HEADER_BYTES))[0]

    def write_element(self, element):
        self._file.write(struct.pack(self.ACTIVE_FMT, self.IS_ACTIVE))
        self._file.write(struct.pack(self.ELEMENT_FMT, len(element)))
        self._file.write(element)

    def write_element_to_eof(self, element):
        self.move_to_eof()
        self.write_element(element)

    def read_element(self):
        active_string = self._file.read(self.ACTIVE_BYTES)
        active = struct.unpack(self.ACTIVE_FMT, active_string)[0]
        size_string = self._file.read(self.SIZE_BYTES)
        size = struct.unpack(self.ELEMENT_FMT, size_string)[0]
        return active, self._file.read(size)

    def rewrite_content(self, lines):
        self.write_header(len(lines), seek_to_start=False)
        for line in lines:
            self.write_element(line)

    def read_elements(self):
        count = self.read_header()
        result = []
        while len(result) < count:
            active, element = self.read_element()
            if active == self.IS_ACTIVE:
                result.append(element)
        return result

    def remove_element(self, element_to_remove):
        count = self.read_header()
        for _ in range(count):
            active, element = self.read_element()
            if element == element_to_remove:
                self._file.seek(-(self.ACTIVE_BYTES + self.SIZE_BYTES + len(element)), os.SEEK_CUR)
                self._file.write(struct.pack(self.ACTIVE_FMT, self.IS_INACTIVE))
                count -= 1
                break
        self.write_header(count)
        return count

    def move_to_eof(self):
        self._file.seek(0, os.SEEK_END)
