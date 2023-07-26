import redis

import ziki_helpers.redis.read as read


def delete_keys(r: redis.Redis, keys: list[str]):
    r.delete(*keys)


def delete_keys_by_prefix(r: redis.Redis, prefix: str):
    keys = read.get_keys_by_prefix(r, prefix)
    delete_keys(r, keys)


def delete_all(r: redis.Redis):
    r.flushall()
