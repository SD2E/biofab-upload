
import abc
from agave.agave_s3 import AgaveS3
from labs.lab_proxy import LabProxy
from upload.upload_manifest import UploadManifest, object_checksum
from typing import Any, Dict, List, Union

OperatorJSON = Dict[str, Any]
MeasurementJSON = List[Dict[str, Any]]


class Operator(abc.ABC):
    """
    Abstract class for upload for XPlan measurement operators.
    """

    def __init__(self, *, plan_id: str, operator_json: OperatorJSON):
        self._plan_id = plan_id
        self._operator = operator_json
        self.manifest_uri = operator_json['manifest']

    @abc.abstractmethod
    def upload(self, *, lab: LabProxy, s3: AgaveS3):
        """
        Upload the files for this operator from the lab to the AgaveS3 server.
        """

    def get_manifest(self) -> UploadManifest:
        """
        Returns the manifest for this measurement operator.
        """
        return UploadManifest(
            manifest_uri=self.manifest_uri,
            plan_uri=self._plan_id,
            config_uri=self._operator['instrument_configuration']
        )

    @staticmethod
    def create_operator(*, plan_id: str, operator_json: OperatorJSON):
        """
        Creates the appropriate measurements subclass for the given operator
        JSON.
        Uses the operator type.
        Treats the protstab_round operator as a flow cytometry operator.
        Throws NotImplementedError for sequencing and non-measurement operator.
        """
        operator_type = operator_json['type']
        if operator_type == 'flow_cytometry':
            return FlowCytometryOperator(
                plan_id=plan_id,
                operator_json=operator_json)
        elif operator_type == 'spectrophotometry':
            return PlateReaderOperator(
                plan_id=plan_id,
                operator_json=operator_json)
        elif operator_type == 'protstab_round':
            # uses FACS to generate fcs files so pretend is a flow operator
            return FlowCytometryOperator(
                plan_id=plan_id,
                operator_json=operator_json
            )
        elif operator_type in ['dna_seq', 'rna_seq']:
            # TODO: this is only true for biofab. Anyone else using this?
            raise NotImplementedError(
                "Sequencing operator data transfer implemented elsewhere")
        else:
            raise NotImplementedError(
                "Operator type {} is not supported".format(operator_type))


class FlowCytometryOperator(Operator):
    """
    Defines upload for a flow cytometry operator.
    """

    def __init__(self, *, plan_id: str, operator_json: OperatorJSON):
        self._measurements = FlowCytometryOperator._get_measurements(
            operator_json['measurements'])
        super().__init__(plan_id=plan_id, operator_json=operator_json)

    @staticmethod
    def _get_measurements(
            measurements_json: MeasurementJSON) -> List[Dict[str, str]]:
        """
        Transforms measurements from the JSON to a convenient form for
        uploading flow cytometry data set.
        should have type (sample, file) list
        """
        measurement_list = list()
        for measurement in measurements_json:
            file_uri = next(iter(measurement['file'], None))
            source = next(iter(measurement['source']), None)
            sample_uri = source['sample']
            measurement_list.append({
                'file_uri': file_uri,
                'sample_uri': sample_uri
            })
        return measurement_list

    def upload(self, *, lab: LabProxy, s3: AgaveS3):
        """
        Uploads the files for a flow cytometry
        """
        manifest = self.get_manifest()
        for measurement in self._measurements:
            file_uri = measurement['file_uri']
            sample_uri = measurement['sample_uri']
            file_object = lab.get(sample_uri)
            if file_object is not None:
                s3.put_object(
                    object=file_object,
                    bucket_path=lab.bucket_path,
                    agave_uri=file_uri,
                    content_type='application/octet-stream'
                )
                checksum = object_checksum(file_object)
                files = [{'file': file_uri, 'checksum': str(checksum)}]
                collected = True
            else:
                files = []
                collected = False
            manifest.add_sample(
                samples=sample_uri,
                files=files,
                collected=collected
            )
        s3.put_object(
            object=manifest,
            bucket_path=lab.bucket_path,
            agave_uri=self.manifest_uri,
            content_type='application/json'
        )


class PlateReaderOperator(Operator):

    def __init__(self, *, plan_id: str, operator_json: OperatorJSON):
        self._measurements = FlowCytometryOperator._get_measurements(
            operator_json['measurements'])
        super().__init__(plan_id=plan_id, operator_json=operator_json)

    @staticmethod
    def _get_measurements(
            measurements_json: MeasurementJSON
    ) -> Dict[str, Union[str, List[str]]]:
        measurement = next(iter(measurements_json), None)
        file_uri = next(iter(measurement['file']), None)
        sample_list = list()
        for source in measurement['source']:
            sample_list.append(source['sample'])
        return {
            'file_uri': file_uri,
            'samples': sample_list
        }

    def upload(self, *, lab: LabProxy, s3: AgaveS3):
        manifest = self.get_manifest()
        file_uri = self._measurements['file_uri']
        # other stuff here

        s3.put_object(
            object=manifest,
            bucket_path=lab.bucket_path,
            agave_uri=self.manifest_uri,
            content_type='application/json'
        )
