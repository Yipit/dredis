import os.path
import shutil


class Path(object):

    def __init__(self, path):
        self._path = path

    def join(self, path):
        return os.path.join(self._path, path)

    def reset(self):
        try:
            shutil.rmtree(self._path)
        except:
            pass
        try:
            os.makedirs(self._path)
        except:
            pass
