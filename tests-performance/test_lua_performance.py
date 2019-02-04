"""
The following results should serve as reference
------

Results from 2019-02-04 on @htlbra's Macbook (LARGE_NUMBER == 1000):
Lua EVAL time = 0.12649s
"""

import time

from tests.helpers import fresh_redis


PROFILE_PORT = 6376
LARGE_NUMBER = 1000


def test_lua_evaluation():
    r = fresh_redis(port=PROFILE_PORT)
    before_eval = time.time()
    for score in range(LARGE_NUMBER):
        assert r.eval("return 1".format(score), 0) == 1
    after_eval = time.time()
    print '\nLua EVAL time = {:.5f}s'.format(after_eval - before_eval)
