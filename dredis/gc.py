import threading
import time

from dredis.db import NUMBER_OF_REDIS_DATABASES, DB_MANAGER, KEY_CODEC


DEFAULT_GC_INTERVAL = 500  # milliseconds
DEFAULT_GC_BATCH_SIZE = 10000  # number of storage keys to delete in a batch


class KeyGarbageCollector(threading.Thread):

    def __init__(self, gc_interval=DEFAULT_GC_INTERVAL, batch_size=DEFAULT_GC_BATCH_SIZE):
        threading.Thread.__init__(self, name="Key Garbage Collector")
        self._gc_interval_in_secs = gc_interval / 1000.0  # convert to seconds
        self._batch_size = batch_size

    def run(self):
        while True:
            self.collect()
            time.sleep(self._gc_interval_in_secs)

    def collect(self):
        for db_id in range(NUMBER_OF_REDIS_DATABASES):
            with DB_MANAGER.thread_lock:
                self._collect(DB_MANAGER.get_db(db_id))

    def _collect(self, db):
        deleted = 0
        with db.write_batch() as batch:
            for deleted_db_key, _ in db.iterator(prefix=KEY_CODEC.MIN_DELETED_VALUE):
                _, _, deleted_key_value = KEY_CODEC.decode_key(deleted_db_key)
                for db_key, _ in db.iterator(prefix=deleted_key_value):
                    deleted += 1
                    batch.delete(db_key)
                    if deleted == self._batch_size:
                        return
                batch.delete(deleted_db_key)
