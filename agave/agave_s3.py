import boto3
import os
from agave.agave_uri import AgaveURI
from botocore.client import Config
from uri import URI


class AgaveS3:
    """
    Wraps an S3 client and bucket allowing objects to be pushed
    with key based on the Agave URI.
    Getting objects is not implemented.
    """

    def __init__(self, **params):
        """
        Creates an AgaveS3 object using the params object.
        """
        self.client = boto3.client(
            's3',
            endpoint_url="{}://{}".format(params['protocol'],
                                          params['uri']),
            aws_access_key_id=params['key'],
            aws_secret_access_key=params['secret'],
            config=Config(signature_version=params['signature']),
            region_name=params['region']
        )

    def put_object(self, object, bucket_path, agave_uri,
                   content_type='text/plain'):
        """
        Puts an object to the s3 bucket.
        """
        (bucket, dir_path) = reverse_split(bucket_path)
        dest_path = AgaveS3.agave_key(dir_path, agave_uri)
        self.client.put_object(
            Body=object,
            Bucket=bucket,
            ContentType=content_type,
            Key=dest_path
        )

    @staticmethod
    def agave_key(path_prefix, agave_uri):
        """
        Creates the S3 key using the path_prefix and the path of the agave_uri
        by stripping the first two directories from the agave_uri path.
        For SD2 the first two directories are something like
        `data-sd2e-community/biofab` and the prefix will be `ingest/biofab`.
        """
        agave_uri = AgaveURI.from_URI(URI(agave_uri))
        return os.path.join(path_prefix, os.path.join(
            *(str.split(str(agave_uri.path), os.sep))[1:]))


def reverse_split(path):
    """
    Splits a path into a pair (head, tail) where head is the top level
    directory and tail is the remaining path from that directory to the
    base directory.
    This is opposite of `os.path.split`, for which the tail is base directory
    and head is the path for the parent of that directory.
    """
    bucket_dirs = str.split(path, os.sep)
    return (bucket_dirs[0], os.path.join(*bucket_dirs[1:]))
