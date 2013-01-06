from gevent import monkey; monkey.patch_all()

from os.path import basename, splitext, exists, join
from os import mkdir, remove, makedirs
from functools import partial
from datetime import datetime
from hashlib import sha224
from cgi import escape

from gevent import sleep
from dropbox import client, rest, session
from boto import connect_s3
from boto.s3.key import Key
from boto.dynamodb.exceptions import DynamoDBKeyNotFoundError
from simplejson import dumps, loads
from markdown import Markdown
from mako.template import Template
from mako.lookup import TemplateLookup
from PyRSS2Gen import RSS2, RSSItem

from datastore import load_user
from config import *


s3_conn = connect_s3(AMAZON_ACCESS_KEY, AMAZON_PRIVATE_KEY)
s3_bucket = s3_conn.create_bucket(S3_BUCKET)

lookup = TemplateLookup(['./tmpl/blog/'])
post_tmpl = lookup.get_template('post.html')
list_tmpl = lookup.get_template('list.html')
archive_tmpl = lookup.get_template('archive.html')

md = Markdown(extensions=['extra', 'meta'])
meta_cache = {}


def main():
    while 1:
        for folder in ['Draft', 'Live']:
            process_folder(folder)
        sleep(5)


def process_folder(folder):
    user = load_user()
    cache_path = join('cache', folder)

    if not exists(cache_path):
        makedirs(cache_path)

    if folder in meta_cache:
        old_meta = meta_cache[folder]
    else:
        meta_key = s3_bucket.get_key(folder.lower())
        if not meta_key:
            old_meta = []
        else:
            old_meta = loads(meta_key.get_contents_as_string())

    dbc = connect_to_dropbox(user)
    if not dbc:
        return

    cur_meta = dbc.metadata('/'+join(folder))['contents']

    def matches_extension(extensions, filename):
        return splitext(filename)[1].lower().replace('.', '') in extensions

    content_extensions = [
        'md', 'css', 'jpg', 'jpeg', 'png', 'gif']

    new, deleted, modified = diff_metadata(
        old_meta, 
        cur_meta, 
        file_filter=partial(matches_extension, content_extensions))

    all_files = [
        file for file in cur_meta
        if 
            not file['is_dir'] and 
            matches_extension(content_extensions, file['path'])]

    if not (new or deleted or modified):
        return

    for file in modified:
        if '/navigation.md' in file['path']:
            modified = [f for f in all_files if matches_extension(['md'], file['path'])]
            break

    # Read any new or changed files to the cache
    for new_file in new + modified:
        cached_name = join(cache_path, basename(new_file['path']))
        with open(cached_name, 'w') as cached_file:
            db_file = dbc.get_file(new_file['path'])
            cached_file.write(db_file.read())


    # Make sure all files are in the cache
    all_paths = [file['path'] for file in all_files]
    for path in all_paths:
        cached_name = join(cache_path, basename(path))
        if not exists(cached_name):
            with open(cached_name, 'w') as cached_file:
                db_file = dbc.get_file(new_file['path'])
                cached_file.write(db_file.read())


    # Write out any new style sheets
    css_files = [item 
        for item in new + modified 
        if not item['is_dir'] and
           matches_extension(['css'], item['path'])]


    user_css = []
    for css_file in css_files:
        name = basename(css_file['path'])
        user_css.append(name)

        s3_key = Key(s3_bucket)
        s3_key.key = join(folder.lower(), name)
        s3_key.set_metadata('Content-Type', 'text/css')
        with open(join(cache_path, name)) as f:
            s3_key.set_contents_from_file(f)


    # Generate the nav
    navigation = ''
    if '/navigation.md' in all_paths:
        with open(join(cache_path, basename('navigation.md'))) as f:
            navigation = md.convert(f.read())


    def render(tmpl, **kwargs):
        return tmpl.render(
            state=folder.lower(),
            blog_title=user['blog_title'],
            user_css=user_css,
            navigation=navigation,
            common_bucket=BASE_URL,
            ga_account=user.get('ga_account', None),
            disqus_account=user.get('disqus_account', None),
            **kwargs)


    # Write out any new media
    media_files = [item 
        for item in new + modified 
        if not item['is_dir'] and
           matches_extension(
               ['jpg', 'jpeg', 'gif', 'png'], 
               item['path'])]


    for media_file in media_files:
        name = basename(media_file['path'])
        user_css.append(name)
        
        s3_key = Key(s3_bucket)
        s3_key.key = join(folder.lower(), name)
        with open(join(cache_path, name)) as f:
            s3_key.set_contents_from_file(f)
        

    # Collect all the posts
    post_files = [item 
        for item in cur_meta
        if not item['is_dir'] and
           matches_extension(['md'], item['path']) and
           item['path'] != '/navigation.md']

    
    def make_posts(file_infos):
        for file_info in file_infos:
            short_title = splitext(basename(file_info['path']))[0]
            with open(join(cache_path, basename(file_info['path']))) as f:
                content = md.convert(f.read())
            title = md.Meta.get('Title', short_title)

            yield {
                'title': title,
                'short_title': short_title,
                'content': content,
                'modified': datetime.strptime(
                    file_info['modified'], 
                    '%a, %d %b %Y %H:%M:%S +0000')
            }


    # Write out the new or modified pages
    for post in make_posts(new + modified):
        html = render(post_tmpl, 
            post=post)

        s3_key = Key(s3_bucket)
        s3_key.key = join(folder.lower(), post['short_title'])
        s3_key.set_metadata('Content-Type', 'text/html')
        s3_key.set_contents_from_string(html)


    # Collect all of the posts
    next_posts = sorted(
        make_posts(post_files),
        key=lambda info: info['modified'])
    next_posts.reverse()


    # Write out the RSS
    rss_items = [
        RSSItem(
            title = post['title'],
            link = BASE_URL + post['short_title'],
            description = post['content'],
            pubDate = post['modified'])
        for post in next_posts[:10]]

    rss = RSS2(
        title=user['blog_title'],
        link=BASE_URL,
        description='',
        lastBuildDate=datetime.utcnow(),
        items=rss_items)

    s3_key = Key(s3_bucket)
    s3_key.key = join(folder.lower(), 'rss.xml')
    s3_key.set_metadata('Content-Type', 'application/rss+xml')
    s3_key.set_contents_from_string(rss.to_xml())
    
    
    # Write out the archive page
    year_sets = []
    cur_year = cur_month = None

    for post in next_posts:
        if cur_year != post['modified'].year:
            cur_year = post['modified'].year
            year_sets.append((cur_year, []))
        if cur_month != post['modified'].month:
            cur_month = post['modified'].month
            year_sets[-1][1].append((cur_month, []))
        year_sets[-1][1][-1][1].append(post)

    html = render(archive_tmpl,
        year_sets=year_sets)

    s3_key = Key(s3_bucket)
    s3_key.key = join(folder.lower(), 'archive')
    s3_key.set_metadata('Content-Type', 'text/html')
    s3_key.set_contents_from_string(html)


    # Write out the listings
    i = 1
    while(next_posts): 
        cur_posts = next_posts[:5]
        next_posts = next_posts[5:]
        html = render(list_tmpl, 
            posts=cur_posts,
            has_next=len(next_posts) > 0,
            next_page='page_%i' % (i+1))

        s3_key = Key(s3_bucket)
        key_name = 'index' if i == 1 else 'page_%i' % i
        s3_key.key = join(folder.lower(), key_name)
        s3_key.set_metadata('Content-Type', 'text/html')
        s3_key.set_contents_from_string(html)
        i += 1


    # Delete any old pages
    for file_info in deleted:
        filename = basename(file_info['path'])
        remove(join(folder.lower(), cache_path, filename))

        if filename.endswith('.md'):
            s3_name = splitext(filename)[0]
        else:
            s3_name = filename
        s3_bucket.delete_key(s3_name)


    # Save the current folder state back to S3
    meta_cache[folder] = cur_meta

    meta_key = Key(s3_bucket)
    meta_key.name = folder.lower()
    meta_key.set_contents_from_string(dumps(cur_meta))


def connect_to_dropbox(user):
    try:
        token = user['dropbox_token']
        secret = user['dropbox_secret']

        sess = session.DropboxSession(
            DROPBOX_APP_KEY, 
            DROPBOX_APP_SECRET, 
            'app_folder')
        sess.set_token(token, secret)
        return client.DropboxClient(sess)
    except:
        return None


def diff_metadata(old_meta, cur_meta, file_filter=None):
    cur_files = dict([(basename(item['path']), item)
        for item in cur_meta
        if not item['is_dir'] and 
           (not file_filter or file_filter(item['path']))])
    old_files = dict([(basename(item['path']), item)
        for item in old_meta
        if not item['is_dir'] and 
           (not file_filter or file_filter(item['path']))])

    new_files = [
        cur_files[file] for file in cur_files
        if file not in old_files]
    deleted_files = [
        old_files[file] for file in old_files
        if file not in cur_files]
    modified_files = [
        cur_files[file] for file in cur_files
        if file in old_files and
           datetime.strptime(
               cur_files[file]['modified'], 
               '%a, %d %b %Y %H:%M:%S +0000') > 
           datetime.strptime(
               old_files[file]['modified'], 
               '%a, %d %b %Y %H:%M:%S +0000')]
    
    return (new_files, deleted_files, modified_files)


if __name__ == '__main__':
    main()


