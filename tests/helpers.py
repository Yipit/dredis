import redis


HOST = 'localhost'
PORT = 6377


def fresh_redis():
    r = redis.StrictRedis(host=HOST, port=PORT)
    r.flushall()
    return r
