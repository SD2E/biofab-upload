import pytest

from labs.lab_proxy import LabProxy
from agave.agave_s3 import AgaveS3, reverse_split
from upload.xplan_operator import Operator


class MockS3:
    def __init__(self):
        self.data = list()

    def put_object(self, *, object, bucket_path, agave_uri, content_type):
        (bucket, dir_path) = reverse_split(bucket_path)
        dest_path = AgaveS3.agave_key(dir_path, agave_uri)
        self.data.append({
            'bucket': bucket,
            'key': dest_path,
            'object': object,
            'agave_uri': agave_uri,
            'content_type': content_type
        })


@pytest.fixture(scope='module')
def mock_s3():
    return MockS3()


class MockLab(LabProxy):
    def get_bucket_path(self):
        return 'sd2e-community/ingest/biofab'

    def get_fcs(self, sample_uri):
        return b'should-be-data'

    def get_spectrophotometry(self, sample_uri):
        return 'id,od,gfp\nblah,0,0\n'


@pytest.fixture(scope='function')
def mock_lab():
    return MockLab()


@pytest.fixture(scope='module')
def example_plan():
    return {
        'plan_id': 'https://hub.sd2e.org/user/sd2e/experiment/biofab_yeast_gates_q0_aq_dummy/1'
    }


@pytest.fixture(scope='function')
def example_flow_operator(example_plan):
    example = example_plan
    op_json = {
        'type': 'flow_cytometry',
        'instrument_configuration': 'agave://data-sd2e-community/biofab/instruments/accuri/5539/11272017/cytometer_configuration.json',
        'manifest': 'agave://data-sd2e-community/biofab/yeast-gates_q0/aq_dummy/3/manifest/manifest.json',
        'measurements': [{
            'file': [
                'agave://data-sd2e-community/biofab/yeast-gates_q0/aq_dummy/3/instrument_output/s84_R90_R117_R205.fcs'
            ],
            'source': [{
                'sample': 'https://hub.sd2e.org/user/sd2e/biofab_yeast_gates_q0_aq_dummy/s84_R90_R117_R205/1'
            }]
        }]
    }
    return {
        'operator': Operator.create_operator(
            plan_id=example['plan_id'],
            operator_json=op_json
        ),
        'key_list': [
            'ingest/biofab/yeast-gates_q0/aq_dummy/3/instrument_output/s84_R90_R117_R205.fcs',
            'ingest/biofab/yeast-gates_q0/aq_dummy/3/manifest/manifest.json'
        ]
    }


@pytest.fixture(scope='function')
def example_plate_operator(example_plan):
    example = example_plan
    op_json = {
        'id': 5,
        'instrument_configuration': 'agave://data-sd2e-community/biofab/instruments/synergy_ht/216503/03132018/platereader_configuration.json',
        'manifest': 'agave://data-sd2e-community/biofab/yeast-gates_q0/aq_dummy/1/manifest/manifest.json',
        'measurements': [{
            'file': [
                'agave://data-sd2e-community/biofab/yeast-gates_q0/aq_dummy/1/instrument_output/od.csv'
            ],
            'source': [
                {
                    'sample': 'https://hub.sd2e.org/user/sd2e/biofab_yeast_gates_q0_aq_dummy/s84_R90/1'
                },
                {
                    'sample': 'https://hub.sd2e.org/user/sd2e/biofab_yeast_gates_q0_aq_dummy/s84_R89/1'
                },
                {
                    'sample': 'https://hub.sd2e.org/user/sd2e/biofab_yeast_gates_q0_aq_dummy/s84_R88/1'
                }
            ]
        }],
        'type': 'spectrophotometry'
    }
    return Operator.create_operator(
        plan_id=example['plan_id'],
        operator_json=op_json
    )


class TestOperator:

    def test_flow(self, example_flow_operator, mock_lab, mock_s3):
        s3 = mock_s3
        operator = example_flow_operator['operator']
        operator.upload(lab=mock_lab, s3=s3)
        assert len(s3.data) == 2
        for item in s3.data:
            assert item['key'] in example_flow_operator['key_list']
