import errno

import boto3
import os

s3_resource = boto3.resource('s3')
client = boto3.client('s3')
path_prefix = '/Volumes/Elements/aws/'

def download_files():
    for bucket in s3_resource.buckets.all():
        bucket_name_traversal(bucket)

def bucket_name_traversal(bucket):
    print('Bucket -> ' + bucket.name)
    objects = bucket.objects.all()
    for object in objects:
        process_object(object, bucket)

def process_object(object, bucket):
    full_path = path_prefix + object.key
    if not check_if_dir_exits(full_path):
        print('Creating Directory -> ' + full_path)
        create_dir(full_path)

    if not check_if_file_exist(full_path):
        write_file(bucket, object)
    else:
        print('File Already Exist -> ' + full_path)

    print('Done With -> ' + full_path)

def write_file(bucket, object):
    path = path_prefix + object.key
    obj = bucket.Object(object.key)
    if obj.key.endswith('/'):
        print('Folder Will Skip Download')
        return
    with open(path, 'wb') as data:
        print('Downloading File -> ' + path)
        obj.download_fileobj(data)


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

