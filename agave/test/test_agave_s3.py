import os
import pytest
from agave.agave_s3 import AgaveS3, reverse_split


@pytest.fixture(scope="module")
def example_uri():
    bucket = "sd2e-community"
    path = "biofab/yeast-gates/aq_1/1/instrument_output/od.csv"
    key = os.path.join("ingest", path)
    s3_path_prefix = "ingest/biofab"
    return {
        "uri": "agave://data-sd2e-community/" + path,
        "bucket_path": os.path.join(bucket, s3_path_prefix),
        "path_prefix": s3_path_prefix,
        "key": os.path.join("ingest", path),
        "file_path": os.path.join(bucket, key),
        "text": "well,od\nA01,0.325"
    }


@pytest.fixture(scope="module")
def example_client():
    return AgaveS3(
        protocol='http',
        uri='s3:10001',
        key='dummykey',
        secret='dummysecret',
        signature='s3v4',
        region='us-east-1'
    )


def test_reverse_split(example_uri):
    prefix, suffix = reverse_split(example_uri['bucket_path'])
    assert prefix == 'sd2e-community'
    assert suffix == 'ingest/biofab'


class TestAgaveS3:

    def test_key(self, example_uri):
        key = AgaveS3.agave_key(example_uri['path_prefix'], example_uri['uri'])
        assert key == example_uri['key']

    def test_put_object(self, example_uri, example_client):
        assert os.path.exists('/mnt/s3')
        client = example_client
        client.put_object(
            example_uri['text'],
            example_uri['bucket_path'],
            example_uri['uri']
        )
        key_path = os.path.join('/mnt/s3/s3', example_uri['file_path'])
        assert os.path.exists(key_path)
        file_path = fakes3_path(key_path)
        with open(file_path) as f:
            contents = f.read()
        f.close()
        assert contents == example_uri['text']


def fakes3_path(key_path):
    """
    Maps the path consisting of bucket+key to the file system path where
    fakes3 places the file.
    """
    return os.path.join(key_path, '.fakes3_metadataFFF/content')
