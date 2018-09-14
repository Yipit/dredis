import errno
import fnmatch
import json
import os.path
import shutil
import six
import sys
import tempfile

from typing import Any, List


class Path(str):

    def __init__(self, path):
        # type: (Any[str, Path]) -> None
        self._path = path

    def join(self, path):
        # type: (Any[str, Path]) -> Path
        return Path(os.path.join(self._path, path))

    def reset(self):
        # type: () -> None
        shutil.rmtree(self._path, ignore_errors=True)
        os.makedirs(self._path)

    def read(self):
        # type: () -> Any
        with open(self._path, 'r') as f:
            result = f.read()
        return self._deserialize(result)

    def write(self, content):
        # type: (str) -> None
        with open(self._path, 'w') as f:
            f.write(self._serialize(content))

    def delete(self):
        # type: () -> None
        if os.path.isfile(self._path):
            os.remove(self._path)
        else:
            shutil.rmtree(self._path)

    def append(self, line):
        # type: (str) -> None
        with open(self._path, 'a') as f:
            f.write(self._serialize(line) + '\n')

    def readlines(self):
        # type: () -> List[str]
        with open(self._path) as f:
            lines = f.readlines()
        return list(map(self._deserialize, lines))

    def exists(self):
        # type: () -> bool
        return os.path.exists(self._path)

    def makedirs(self, ignore_if_exists=False):
        # type: (bool) -> None
        try:
            return os.makedirs(self._path)
        except OSError as exc:
            if ignore_if_exists and exc.errno == errno.EEXIST:
                pass
            else:
                six.reraise(*sys.exc_info())

    def listdir(self, pattern=None):
        # type: (Any[str, None]) -> List[str]
        all_files = os.listdir(self._path)
        if pattern is None:
            return all_files
        else:
            return list(fnmatch.filter(all_files, pattern))

    def remove_line(self, line_to_remove):
        # type: (str) -> None
        with tempfile.NamedTemporaryFile('w', delete=False) as tfile:
            for line in self.readlines():
                if line != line_to_remove:
                    tfile.write(self._serialize(line) + "\n")
            tfile.close()
        os.rename(tfile.name, self._path)

    def empty_directory(self):
        # type: () -> bool
        return self.exists() and not self.listdir()

    def _deserialize(self, value):
        # type: (str) -> Any
        return json.loads(value)

    def _serialize(self, value):
        # type: (str) -> str
        return json.dumps(value)
