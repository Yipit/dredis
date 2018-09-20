import redis


HOST = 'localhost'
PORT = 6377
DB = 0


def fresh_redis(db=DB, host=HOST, port=PORT):
    r = redis.StrictRedis(host=host, port=port, db=db)
    r.flushall()
    return r
