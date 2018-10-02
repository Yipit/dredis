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
        import shelve
        f = shelve.open(self)
        result = f.get('value', '')
        f.close()
        return result
        # with open(self, 'r') as f:
        #     result = f.read()
        # return self._deserialize(result)

    def write(self, content):
        import shelve
        f = shelve.open(self)
        f['value'] = content
        f.close()

        # with open(self, 'w') as f:
        #     f.write(self._serialize(content))

    def delete(self):
        if os.path.isfile(self):
            os.remove(self)
        elif os.path.isfile(self + ".db"):
            os.remove(self + ".db")
        else:
            shutil.rmtree(self)

    def append(self, line):
        import shelve
        f = shelve.open(self)
        if 'value' not in f:
            f['value'] = set()
        new_value = f['value']
        new_value.add(line)
        f['value'] = new_value
        f.close()
        # with open(self, 'a') as f:
        #     f.write(self._serialize(line) + '\n')

    def readlines(self):
        import shelve
        f = shelve.open(self)
        result = f.get('value', [])
        f.close()
        return result

        # with open(self) as f:
        #     lines = f.readlines()
        # return map(self._deserialize, lines)

    def exists(self):
        return os.path.exists(self) or os.path.exists(self + ".db")

    def makedirs(self, ignore_if_exists=False):
        try:
            return os.makedirs(self)
        except OSError as exc:
            if ignore_if_exists and exc.errno == errno.EEXIST:
                pass
            else:
                six.reraise(*sys.exc_info())

    def listdir(self, pattern=None):
        all_files = map(lambda x: x.replace('.db', ''), os.listdir(self))
        if pattern is None:
            return all_files
        else:
            return list(fnmatch.filter(all_files, pattern))

    def remove_line(self, line_to_remove):
        import shelve
        f = shelve.open(self)
        if 'value' in f and line_to_remove in f['value']:
            new_value = f['value']
            new_value.remove(line_to_remove)
            f['value'] = new_value
        f.close()
        #
        # lines = self.readlines()
        # if line_to_remove not in lines:
        #     return
        # with open(self, 'w') as f:
        #     for line in lines:
        #         if line != line_to_remove:
        #             f.write(self._serialize(line) + "\n")

    def empty_directory(self):
        return self.exists() and not self.listdir()

    # def _deserialize(self, value):
    #     return json.loads(value)
    #
    # def _serialize(self, value):
    #     return json.dumps(value)
