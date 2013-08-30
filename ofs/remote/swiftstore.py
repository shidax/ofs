'''This implements OFS backends for remote storage systems supported by the
`python-swiftclient <https://github.com/openstack/python-swiftclient>`_ .

'''
import os
try:
    import json
except ImportError:
    import simplejson as json
from datetime import datetime
from tempfile import mkstemp
from ofs.base import OFSInterface, OFSException

import swiftclient
from swiftclient import client

SWIFT_AUTH_VERSION=2
CHUNK_SIZE=1024

class SwiftOFS(OFSInterface):
    '''swift backend for OFS.
    
    This is a simple implementation of OFS for controll OpenStack Swift.
    There are some difference in term of storage.
    1. bucket = container in swift
    2. label = object in swift
    '''
    
    def __init__(self, auth_url=None, user=None, passwd=None, tenant=None):
        # Currently support keystone authentication.
        self.connection = client.Connection(authurl=auth_url,
                                            user=user,
                                            key=passwd,
                                            tenant_name=tenant,
                                            auth_version=SWIFT_AUTH_VERSION)

    def _get_object(self, container, obj, chunk_size=0):
        try:
            return self.connection.get_object(container, obj, resp_chunk_size=chunk_size)
        except swiftclient.ClientException as e:
            return None

    def _get_container(self, container):
        try: 
            return self.connection.get_container(container)
        except swiftclient.ClientException as e:
            return None

    def _head_container(self, container):
        try:
            return self.connection.head_container(container)
        except swiftclient.ClientException as e:
            return None

    def _head_object(self, container, obj):
        try:        
            return self.connection.head_object(container, obj)
        except swiftclient.ClientException as e:
            return None

    def exists(self, bucket, label=None):
        container = self._head_container(bucket)
        if container is None: 
            return False
        return (label is None) or (self._head_object(bucket, label) is not None)
    
    def claim_bucket(self, bucket):
        try:
            if not self._get_container(bucket):
                return False
            self.connection.put_container(bucket)
            return True
        except swiftclient.ClientException as e:
            return False
    
    def list_labels(self, bucket):
        _, labels = self._get_container(bucket)
        for label in labels:
            yield label['name']

    def list_buckets(self):
        # blank string to container name means list buckets
        _, buckets = self._get_container('')
        for bucket in buckets:
            yield bucket['name']

    def get_stream(self, bucket, label, as_stream=True):
        if not as_stream:
            _, body = self._get_object(bucket, label)
            return body
        _, body = self._get_object(bucket, label, chunk_size=CHUNK_SIZE)
        return body
    
    def get_url(self, bucket, label):
        container = self._head_container(bucket)
        obj = self._head_object(bucket, label)
        return "%s/%s/%s" % (self.connection.url, bucket, label)

    def put_stream(self, bucket, label, stream_object, params={}):
        ''' Create a new file to swift object storage. '''
        self.claim_bucket(bucket) 
        self.connection.put_object(bucket, label, stream_object)

    def del_stream(self, bucket, label):
        self.connection.delete_object(bucket, label)

    def get_metadata(self, bucket, label):
        container = self._head_container(bucket)
        obj = self._head_object(bucket, label)
        
        meta = dict()
        meta.update({
            '_bucket': bucket,
            '_label': label,
            '_owner': bucket,
            '_last_modified': obj['last-modified'],
            '_format': obj['content-type'],
            '_content_length': obj['content-length'],
            # Content-MD5 header is not made available from boto it seems but
            # etag is and it corresponds to MD5. See
            # http://code.google.com/apis/storage/docs/reference-headers.html#etag
            # https://github.com/boto/boto/blob/master/boto/s3/key.py#L531
            '_checksum': obj['etag']
        })
        return meta
    
    def update_metadata(self, bucket, label, params):
        key = self._require_key(self._require_bucket(bucket), label)
        self._update_key_metadata(key, params)
        # cannot update metadata on its own. way round this is to copy file
        key.copy(key.bucket, key.name, dict(key.metadata), preserve_acl=True)
        key.close()
    
    def del_metadata_keys(self, bucket, label, keys):
        key = self._require_key(self._require_bucket(bucket), label)
        for _key, value in key.metadata.items():
            if _key in keys:
                del key.metadata[_key] 
        key.close()

