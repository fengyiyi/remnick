from gevent import monkey; monkey.patch_all()

from os.path import join
from cPickle import dumps, loads
from hashlib import sha224
from bottle import request, response, abort, HTTPResponse
from boto import connect_s3

from datastore import *
from config import *


s3_conn = connect_s3(AMAZON_ACCESS_KEY, AMAZON_PRIVATE_KEY)
bucket = s3_conn.get_bucket(S3_BUCKET) 


def serve_s3(folder, resource):
    headers = load_resource_headers(folder, resource)
    if headers:
        if request.headers.get('If-None-Match') == headers['ETag']:
            raise HTTPResponse(status=304)
        content = load_resource_content(folder, resource)
        if content:
            response.content_type = headers['Content-Type']
            response.add_header('ETag', headers['ETag'])
            return content[0]

    s3_key = bucket.get_key(join(folder, resource))
    if not s3_key:
        abort(404, "Resource not found")

    headers = {
        'Content-Type': s3_key.content_type,
        'ETag': s3_key.etag
    }
    content = s3_key.read()

    set_resource_headers(folder, resource, headers)
    set_resource_content(folder, resource, content)

    response.content_type = headers['Content-Type']
    response.add_header('ETag', headers['ETag'])
    return content


