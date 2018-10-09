import errno
import fnmatch
import json
import os.path
import shutil
import six
import sys


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
        with open(self, 'a') as f:
            f.write(self._serialize(line) + '\n')

    def readlines(self):
        with open(self) as f:
            lines = f.readlines()
        return map(self._deserialize, lines)

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
                new_content = "".join(self._serialize(line) + "\n" for line in lines)
                with open(self, 'w') as f:
                    f.write(new_content)

    def empty_directory(self):
        return self.exists() and not self.listdir()

    def _deserialize(self, value):
        return json.loads(value)

    def _serialize(self, value):
        return json.dumps(value)
