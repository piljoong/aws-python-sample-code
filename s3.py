import os
import sys
import time
import string, random

import boto
from boto.s3.connection import Location
from boto.s3.key import Key

class S3Sample():
    def __init__(self):
        self.s3 = boto.connect_s3()
        self.bucket = None

    def exists(self, name):
        self.bucket = self.s3.lookup(name)
        if self.bucket == None:
            return False
        return True

    def create_bucket(self, name, region):
        try:
            self.bucket = self.s3.create_bucket(name, location=region)
            print 'Bucket {name} has been created'.format(name=name)
        except Exception as e:
            print 'Failed to create bucket: {err}'.format(err=e)
            self.bucket = None

    def delete_bucket(self, name):
        try:
            self.s3.delete_bucket(name)
            print 'Bucket {name} has been deleted'.format(name=name)
        except Exception as e:
            print 'Failed to delete bucket: {err}'.format(err=e)

    def create_object_with_text(self, key, text):
        try:
            obj = Key(self.bucket)
            obj.key = key
            obj.set_contents_from_string(text)
            return obj
        except Exception as e:
            print 'Failed to create object: {err}'.format(err=e)

    def generate_object_url(self, obj, expires_in_seconds=1800):
        try:
            return obj.generate_url(expires_in_seconds)
        except Exception as e:
            return None

    def delete_object(self, obj):
        try:
            obj.delete()
        except Exception as e:
            print 'Failed to delete object {err}'.format(err=e)

if __name__ == '__main__':
    s3 = S3Sample()

    bucket_name = 'pj-test-bucket-for-joycity'
    region = boto.s3.connection.Location.APNortheast

    if not s3.exists(bucket_name):
        s3.create_bucket(bucket_name, region)

    obj = s3.create_object_with_text('text-key', 'ok!')

    raw_input('> Press enter to generate url for the generated object...')
    obj_url = s3.generate_object_url(obj)
    print 'URL: ' + obj_url

    raw_input('> Press enter to delete the generated object...')
    s3.delete_object(obj)

    raw_input('> Press enter to delete the test bucket...')
    s3.delete_bucket(bucket_name)


