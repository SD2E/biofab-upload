import os
import pytest
from agave.agave_uri import AgaveURI, replace_prefix
from uri import URI


@pytest.fixture(scope="module")
def example_uri():
    return {
        "uri": "agave://data-sd2e-community/biofab/yeast-gates/aq_1/1/instrument_output/od.csv",
        "host": "data-sd2e-community",
        "path": "biofab/yeast-gates/aq_1/1/instrument_output/od.csv",
        "basename": "od.csv"
    }


class TestAgaveURI:

    def test_from_uri(self, example_uri):
        uri_string = example_uri['uri']
        uri = AgaveURI.from_URI(URI(uri_string))
        assert uri.host == example_uri['host']
        assert uri.path == example_uri['path']
        assert str(uri) == uri_string
        assert str(uri.basename()) == example_uri['basename']

    def test_extend(self, example_uri):
        path_list = example_uri['path'].split(os.sep)
        assert len(path_list) >= 3
        uri = AgaveURI(host=example_uri['host'], path=path_list[0])
        suffix = os.path.join(*(path_list[1:3]))
        uri1 = uri.extend(suffix)
        assert uri != uri1
        assert uri1.path == os.path.join(*(path_list[0:3]))
        assert uri1.basename() == path_list[2]
        assert str(uri1) == "agave://{}/{}".format(
            example_uri['host'],
            os.path.join(*(path_list[0:3])))


def test_replace_prefix(example_uri):
    path = example_uri['path']
    path_list = path.split(os.sep)
    prefix = os.path.join(*(path_list[0:3]))
    new_prefix = "blah"
    new_path = replace_prefix(path, prefix, new_prefix)
    assert new_path == os.path.join('blah', os.path.join(*(path_list[3:])))
