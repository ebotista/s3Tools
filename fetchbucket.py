import errno

import boto3
import os

from multiprocessing.dummy import Pool as ThreadPool

s3_resource = boto3.resource('s3')
client = boto3.client('s3')
path_prefix = '/Volumes/Elements/aws/'
blacklist = ['.DS_Store']

def download_files():
    for bucket in s3_resource.buckets.all():
        bucket_name_traversal(bucket)

def bucket_name_traversal(bucket):
    print('Bucket -> %s' % bucket.name)
    pool = ThreadPool(10)
    objects = bucket.objects.all()
    list_bucket_object = []
    for object in objects:
        list_bucket_object.append((object, bucket))

    pool.map(process_bucket_object, list_bucket_object)

def process_bucket_object(object_bucket):
    object, bucket = object_bucket
    if is_glacier(object):
        should_restore = should_restore_glacier(bucket.name, object.key)
        if should_restore == 0:
            print('Restoring -> %s' % object.key)
            return
        elif should_restore == 1:
            print('Restore in process -> %s' % object.key)
            return
    process_object(object, bucket)


def process_object(object, bucket):
    full_path = path_prefix + object.key
    if not check_if_dir_exits(full_path):
        print('Creating Directory -> %s' % full_path)
        create_dir(full_path)

    if not check_if_file_exist(full_path):
        write_file(bucket, object)
    else:
        print('File Already Exist -> %s' % full_path)

    print('Done With -> %s' % full_path)

def write_file(bucket, object):
    path = path_prefix + object.key
    obj = bucket.Object(object.key)
    skip = obj.key.endswith('/')
    for item in blacklist:
        if obj.key.endswith(item):
            skip = True
    if skip:
        print('Will Skip Download')
        return
    with open(path, 'wb') as data:
        print('Downloading File -> %s' % path)
        obj.download_fileobj(data)

def should_restore_glacier(bucket, key):
    object = s3_resource.Object(bucket, key)
    if object.restore is None:
        object.restore_object(
            Bucket=bucket,
            Key=key,
            RestoreRequest={'Days': 10}
        )
        return 0
    elif 'ongoing-request="true"' in object.restore:
        return 1

    return 2

def is_glacier(key):
    return key.storage_class == 'GLACIER'


def create_dir(filename):
    try:
        os.makedirs(os.path.dirname(filename))
    except OSError as exc:
        if exc.errno != errno.EEXIST:
            raise

def check_if_dir_exits(filename):
    return os.path.exists(os.path.dirname(filename))

def check_if_file_exist(filename):
    return os.path.isfile(filename)


download_files()

