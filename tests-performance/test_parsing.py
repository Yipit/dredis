"""
The following results should serve as reference
------

Results from 2020-01-09 on @htlbra's Macbook and LARGE_NUMBER = 50 * 1024 * 1024

commit 63abbfd5df83eb5f0fa705487afc64f660cee82b:
large SET time = 6.34038s

commit a33e223ccfe413064b8656cee629ebbaa73f0dce:
large SET time = 0.89820s
"""

import time

from tests.helpers import fresh_redis


PROFILE_PORT = 6376
LARGE_NUMBER = 50 * 1024 * 1024  # 50MiB


def test_very_large_command_to_parse():
    r = fresh_redis(port=PROFILE_PORT)
    before = time.time()
    assert r.set("test", 'x' * LARGE_NUMBER)
    after = time.time()
    print '\nlarge SET time = {:.5f}s'.format(after - before)
