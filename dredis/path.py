import fnmatch
import os.path
import shutil
import tempfile


class Path(str):

    def __init__(self, path):
        self._path = path

    def join(self, path):
        return Path(os.path.join(self._path, path))

    def reset(self):
        try:
            shutil.rmtree(self._path)
        except Exception:
            pass
        try:
            os.makedirs(self._path)
        except Exception:
            pass

    def read(self):
        with open(self._path, 'r') as f:
            result = f.read()
        return result

    def write(self, content):
        with open(self._path, 'w') as f:
            f.write(content)

    def delete(self):
        if os.path.isfile(self._path):
            os.remove(self._path)
        else:
            shutil.rmtree(self._path)

    def append(self, line):
        with open(self._path, 'a') as f:
            f.write(line + '\n')

    def readlines(self):
        with open(self._path) as f:
            lines = f.readlines()
        return [line.strip() for line in lines]

    def exists(self):
        return os.path.exists(self._path)

    def makedirs(self):
        return os.makedirs(self._path)

    def listdir(self, pattern=None):
        all_files = os.listdir(self._path)
        if pattern is None:
            return all_files
        else:
            return list(fnmatch.filter(all_files, pattern))

    def remove_line(self, line_to_remove):
        with tempfile.NamedTemporaryFile('w', delete=False) as tfile:
            for line in self.readlines():
                if line != line_to_remove:
                    tfile.write(line + "\n")
            tfile.close()
        os.rename(tfile.name, self._path)

    def empty_directory(self):
        return self.exists() and not self.listdir()
