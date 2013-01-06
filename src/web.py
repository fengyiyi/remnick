from gevent import monkey; monkey.patch_all()

from sys import argv
from os import urandom, walk
from os.path import join, split
from base64 import b64encode
from datetime import datetime, timedelta
from time import mktime
from urllib import urlencode
from bcrypt import gensalt, hashpw
from simplejson import loads as j_loads
from requests import get as r_get
from requests.auth import HTTPBasicAuth
from dropbox import client, session
from dropbox.rest import ErrorResponse
from gevent import spawn, sleep
from bottle import get, post, request, response, run, \
    abort, redirect, static_file, debug
from bottle import mako_view as view
from boto import connect_s3

from datastore import *
from s3proxy import serve_s3
from config import *


#-- S3 helpers

s3_conn = connect_s3(AMAZON_ACCESS_KEY, AMAZON_PRIVATE_KEY)

def rebuild():
    meta_bucket = s3_conn.create_bucket(S3_BUCKET)
    for folder in ['draft', 'live']:
        meta_bucket.delete_key(folder)


#-- Auth helpers

def salt_password(password):
    return ACCESS_TOKEN_SECRET + password


def make_access_token(password):
    return hashpw(salt_password(password), gensalt(12))


def make_session_token():
    return b64encode(urandom(32))


def get_user():
    token = request.get_cookie('session_token')

    if not token:
        redirect('/login')

    return load_user()


#-- Auth

@get('/login')
@view('tmpl/site/login')
def login():
    error = request.query['error'] if 'error' in request.query else ''

    return {
        'error': error 
    }


@post('/login')
def login():
    password = request.forms['password']

    if not password:
        abort(400, "Malformed request.")

    user = load_user()
    salted = salt_password(password)

    if 'access_token' in user:
        access_token = user['access_token']
        hashed = hashpw(salted, access_token) 

        if hashed != access_token:
            redirect('/login?error=invalid_login')
    else:
        access_token = make_access_token(password)
        user['access_token'] = access_token
        save_user(user)

    session_token = make_session_token()
    set_session(session_token)

    response.set_cookie('session_token', session_token)
    redirect('/admin')


#-- Account settings

@get('/admin')
@view('tmpl/site/admin')
def settings():
    user = get_user()

    print user
    return {
        'blog_title': user['blog_title'],
        'ga_account': user.get('ga_account', ''),
        'disqus_account': user.get('disqus_account', '')
    }


@post('/admin')
def settings():
    user = get_user()
    
    for key in ['blog_title', 'ga_account', 'disqus_account']:
        value = request.forms[key]
        if not value:
            if key in user:
                del user[key]
        else:
            user[key] = value

    save_user(user)
    rebuild()

    redirect('/admin')


#-- Link dropbox account.

@get('/link')
@view('tmpl/site/link')
def link():
    user = get_user()
    oauth_token = request.query.oauth_token
    if oauth_token:
        sess = session.DropboxSession(
            DROPBOX_APP_KEY, 
            DROPBOX_APP_SECRET, 
            'app_folder')

        request_token = load_request_token(oauth_token)
        if request is None:
            redirect('/login')
        access_token = sess.obtain_access_token(request_token)

        dbc = client.DropboxClient(sess)
        info = dbc.account_info()

        user['dropbox_email'] = info['email']
        user['dropbox_token'] = access_token.key
        user['dropbox_secret'] = access_token.secret
        save_user(user)

        setup_dropbox_folder(user, dbc)
        redirect('/admin')
    return {}


def setup_dropbox_folder(user, client):
    def metadata(path):
        try:
            return client.metadata(path)
        except ErrorResponse as e:
            if e.status == 404:
                return None
            raise e

    template_path = 'tmpl/dropbox/'

    for dirpath, dirnames, filenames in walk(template_path):
        dirpath = dirpath.replace(template_path, '')
        meta = metadata(dirpath)
        if meta:
            for item in meta['contents']:
                name = split(item['path'])[1]
                if name in dirnames:
                    if not item['is_dir']:
                        #TODO: Error for user
                        pass
                    dirnames.remove(name)
                if name in filenames:
                    filenames.remove(name)

        for filename in filenames:
            path = join(dirpath, filename)
            client.put_file(path, join(dirpath, filename))


@post('/link')
def link():
    user = get_user()
    sess = session.DropboxSession(
        DROPBOX_APP_KEY, 
        DROPBOX_APP_SECRET, 
        'app_folder'
    )

    request_token = sess.obtain_request_token()
    save_request_token(request_token.key, request_token)

    return_url = 'http://%s%s' % (request.headers['host'], request.path)
    url = sess.build_authorize_url(request_token, return_url)
    redirect(url)


#-- Static files

@get('/static/<filepath:path>')
def static_files(filepath):
    return static_file(filepath, root='./static')


#-- Actual blog content

@get('/draft')
@view('tmpl/site/draft')
def draft():
    return {}


@get('/draft/')
@get('/draft/<resource:path>')
def draft_server(resource="index"):
    user = get_user()

    return serve_s3('draft', resource)

@get('/')
@get('/<resource:path>')
def live_server(resource="index"):
    return serve_s3('live', resource)


if __name__ == '__main__':
    host = argv[1] if len(argv) > 1 else "0.0.0.0"
    port = argv[2] if len(argv) > 2 else 8080

    run(host=host, port=port, server='gevent')


