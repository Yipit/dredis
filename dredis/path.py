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
            f = open(self, 'rb+')
        except IOError:
            f = open(self, 'wb')
            old_size = 0
        else:
            old_size = ZSetEncoder.read_header(f)

        ZSetEncoder.write_header(f, old_size + 1)
        ZSetEncoder.write_element_to_eof(f, line)

    def readlines(self):
        with open(self, 'rb') as f:
            return ZSetEncoder.read_elements(f)

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
        lines = self.readlines()
        if line_to_remove in lines:
            lines.remove(line_to_remove)
            if len(lines) == 0:
                os.remove(self)
            else:
                with open(self, 'wb') as f:
                    ZSetEncoder.rewrite_content(f, lines)

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
            return ZSetEncoder.read_header(f)


class ZSetEncoder(object):

    HEADER_FMT = ">Q"
    HEADER_BYTES = 8
    ELEMENT_FMT = ">I"
    SIZE_BYTES = 4

    @classmethod
    def write_header(cls, f, size, seek_to_start=True):
        if seek_to_start:
            f.seek(0, os.SEEK_SET)
        f.write(struct.pack(cls.HEADER_FMT, size))

    @classmethod
    def read_header(cls, f):
        return struct.unpack(cls.HEADER_FMT, f.read(cls.HEADER_BYTES))[0]

    @classmethod
    def write_element(cls, f, element):
        f.write(struct.pack(cls.ELEMENT_FMT, len(element)))
        f.write(element)

    @classmethod
    def write_element_to_eof(cls, f, element):
        cls.move_to_eof(f)
        cls.write_element(f, element)

    @classmethod
    def read_element(cls, f):
        size_string = f.read(cls.SIZE_BYTES)
        if size_string:
            size = struct.unpack(cls.ELEMENT_FMT, size_string)[0]
            return f.read(size)
        else:
            return None

    @classmethod
    def rewrite_content(cls, f, lines):
        cls.write_header(f, len(lines), seek_to_start=False)
        for line in lines:
            cls.write_element(f, line)

    @classmethod
    def read_elements(cls, f):
        elements = []
        f.seek(cls.HEADER_BYTES)  # skip header
        element = cls.read_element(f)
        while element:
            elements.append(element)
            element = cls.read_element(f)
        return elements

    @classmethod
    def move_to_eof(cls, f):
        f.seek(0, os.SEEK_END)
