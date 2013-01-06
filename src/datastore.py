from os.path import exists
from cPickle import loads, dumps

from redis import StrictRedis

from config import *


redis = StrictRedis(REDIS_HOST, REDIS_PORT, REDIS_DATABASE)


def make_key(*args):
    return ':'.join(args)


def load_user():
    key = make_key('user')
    user = redis.hgetall(key)

    if user:
        return user

    return {
        'blog_title': 'My blog'
    }


def save_user(user):
    key = make_key('user')
    redis.hmset(key, user)


def load_request_token(oauth_token):
    key = make_key('request-token', oauth_token)
    raw = redis.get(key)
    if not raw:
        return raw
    return loads(raw)


def save_request_token(oauth_token, request_token):
    key = make_key('request-token', oauth_token)
    redis.set(key, dumps(request_token))
    redis.expire(key, 60)


def load_session(session_token):
    key = make_key('session', session_token)
    return redis.get(key)


def set_session(session_token):
    key = make_key('session', session_token)
    redis.set(key, 'OK')
    redis.expire(key, 3600)


def load_resource_headers(folder, resource):
    key = make_key('header', folder, resource)
    return redis.hgetall(key)


def set_resource_headers(folder, resource, headers):
    key = make_key('header', folder, resource)
    redis.hmset(key, headers)
    if folder.lower() == 'draft':
        redis.expire(key, 10)
    else:
        redis.expire(key, 60)


def delete_resource_headers(folder, resource):
    key = make_key('header', folder, resource)
    redis.delete(key)


def load_resource_content(folder, resource):
    key = make_key('content', folder, resource)
    return redis.get(key)


def set_resource_content(folder, resource, content):
    key = make_key('content', folder, resource)
    redis.set(key, content)
    if folder.lower() == 'draft':
        redis.expire(key, 10)
    else:
        redis.expire(key, 60)


