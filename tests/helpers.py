import redis


HOST = 'localhost'
PORT = 6377


def fresh_redis(db=0):
    r = redis.StrictRedis(host=HOST, port=PORT, db=db)
    r.flushall()
    return r
