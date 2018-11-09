import errno
import fnmatch
import json
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
        with open(self, 'r') as f:
            result = f.read()
        return self._deserialize(result)

    def write(self, content):
        with open(self, 'w') as f:
            f.write(self._serialize(content))

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
            old_size = struct.unpack(">Q", f.read(8))[0]

        # rewrite header
        f.seek(0, os.SEEK_SET)
        f.write(struct.pack(">Q", old_size + 1))

        # add new (size,element) to the end
        f.seek(0, os.SEEK_END)
        f.write(struct.pack(">I", len(line)))
        f.write(line)

    def readlines(self):
        lines = []
        with open(self, 'rb') as f:
            f.seek(8)
            string_size = f.read(4)
            while string_size:
                size = struct.unpack(">I", string_size)[0]
                line = f.read(size)
                lines.append(line)
                string_size = f.read(4)
        return lines

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
                    f.write(struct.pack(">Q", len(lines)))
                    for line in lines:
                        f.write(struct.pack(">I", len(line)))
                        f.write(line)

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

    def _deserialize(self, value):
        return json.loads(value)

    def _serialize(self, value):
        return json.dumps(value)

    def read_zset_header(self):
        with open(self, 'rb') as f:
            return struct.unpack(">Q", f.read(8))[0]
