import os.path
import shutil


class Path(str):

    def __init__(self, path):
        self._path = path

    def join(self, path):
        return Path(os.path.join(self._path, path))

    def reset(self):
        try:
            shutil.rmtree(self._path)
        except:
            pass
        try:
            os.makedirs(self._path)
        except:
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
        os.makedirs(self._path)
