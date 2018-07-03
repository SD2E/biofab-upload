import json
import pytest
from upload.upload_manifest import UploadManifest


@pytest.fixture(scope='module')
def manifest_version():
    return "https://github.com/SD2E/reactors-etl/releases/tag/2"


@pytest.fixture(scope='module')
def example_plan():
    return {
        "manifest_uri": "agave://data-sd2e-community/biofab/yeast-gates_q0/aq_12548_4/1/manifest/manifest.json",
        "plan_uri": "https://hub.sd2e.org/user/sd2e/experiment/biofab_yeast_gates_q0_aq_12548_4/1",
        "instrument_configuration": "agave://data-sd2e-community/biofab/instruments/accuri/5539/11272017/cytometer_configuration.json"
    }


@pytest.fixture(scope='function')
def empty_manifest(manifest_version, example_plan):
    return UploadManifest(
        manifest_uri=example_plan['manifest_uri'],
        plan_uri=example_plan['plan_uri'],
        manifest_version=manifest_version,
        config_uri=example_plan["instrument_configuration"]
    )


class TestUploadManifest:

    def test_empty_manifest(self, empty_manifest, example_plan):
        manifest = empty_manifest
        assert manifest.uri == example_plan['manifest_uri']
        assert not bool(manifest.sample_list)
        manifest_json = json.loads(str(manifest))
        assert manifest_json is not None
        assert not bool(manifest_json['samples'])
        assert manifest_json['rdf:about'] == manifest.uri

    def test_add_sample(self, empty_manifest, example_plan):
        manifest = empty_manifest
        manifest.add_sample(
            samples=[
                "https://hub.sd2e.org/user/sd2e/biofab_yeast_gates_q0_aq_12548_4/s906/1"],
            files=[
                "agave://data-sd2e-community/biofab/yeast-gates_q0/aq_12548_4/3/instrument_output/s906.fcs"]
        )
        assert len(manifest.sample_list) == 1
        for sample in manifest.sample_list:
            assert sample['collected']
            assert len(sample['files']) == 1

    def test_add_uncollected(self, empty_manifest):
        manifest = empty_manifest
        manifest.add_sample(
            samples=[
                "https://hub.sd2e.org/user/sd2e/biofab_yeast_gates_q0_aq_12548_4/s906/1"],
            files=[]
        )
        manifest.add_sample(
            samples=[
                "https://hub.sd2e.org/user/sd2e/biofab_yeast_gates_q0_aq_12548_4/s906/1"],
            files=[
                "agave://data-sd2e-community/biofab/yeast-gates_q0/aq_12548_4/3/instrument_output/s906.fcs"],
            collected=False
        )
        assert len(manifest.sample_list) == 2
        for sample in manifest.sample_list:
            assert not sample['collected']
            assert len(sample['files']) == 0
        # TODO: validate manifest
