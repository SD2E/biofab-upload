
import abc
from agave.agave_s3 import AgaveS3
from labs.lab_proxy import LabProxy
from upload.upload_manifest import UploadManifest, object_checksum
from typing import Any, Dict, List, Tuple, Union

OperatorJSON = Dict[str, Any]
MeasurementJSON = List[Dict[str, Any]]


class UploadVisitor:

    def __init__(self, *,
                 lab: LabProxy, s3: AgaveS3, manifest: UploadManifest):
        self.lab = lab
        self._s3 = s3
        self._manifest = manifest

    def upload_files(self, *, file_objects: List[Tuple[Any, str]],
                     samples: List[str],
                     content_type):
        """
        Uploads the list of file objects for the samples from the lab to the
        s3 service
        """
        files = []
        for file_object, file_uri in file_objects:
            if file_object is None:
                self._manifest.add_sample(samples=samples, collected=False)
                return

            self._s3.put_object(
                object=file_object,
                bucket_path=self.lab.get_bucket_path(),
                agave_uri=file_uri,
                content_type=content_type
            )
            checksum = object_checksum(file_object)
            files.append({'file': file_uri, 'checksum': str(checksum)})
        self._manifest.add_sample(samples=samples, files=files)


class Operator(abc.ABC):
    """
    Abstract class for upload for XPlan measurement operators.
    """

    def __init__(self, *, plan_id: str, operator_json: OperatorJSON):
        self._plan_id = plan_id
        self._operator = operator_json
        self._manifest_uri = operator_json['manifest']

    @abc.abstractmethod
    def accept(self, visitor: UploadVisitor):
        pass

    def upload(self, *, lab: LabProxy, s3: AgaveS3):
        """
        Upload the files for this operator from the lab to the AgaveS3 server.
        """
        manifest = UploadManifest(
            manifest_uri=self._manifest_uri,
            plan_uri=self._plan_id,
            config_uri=self._operator['instrument_configuration']
        )
        self.accept(UploadVisitor(lab=lab, s3=s3, manifest=manifest))
        s3.put_object(
            object=str(manifest),
            bucket_path=lab.get_bucket_path(),
            agave_uri=self._manifest_uri,
            content_type='application/json'
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
            # TODO: not clear whether to implement these here for biofab
            raise NotImplementedError(
                "Sequencing operator data transfer not implemented")
        else:
            msg = "either not a measurement or not supported"
            raise NotImplementedError(
                "Operator type {} is {}".format(operator_type, msg))


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
            file_uri = next(iter(measurement['file']), None)
            source = next(iter(measurement['source']), None)
            sample_uri = source['sample']
            measurement_list.append({
                'file_uri': file_uri,
                'sample_uri': sample_uri
            })
        return measurement_list

    def accept(self, visitor: UploadVisitor):
        """
        Uploads the files for a flow cytometry
        """
        for measurement in self._measurements:
            file_uri = measurement['file_uri']
            sample_uri = measurement['sample_uri']
            file_object = visitor.lab.get_fcs(sample_uri)
            sample_list = [sample_uri]
            visitor.upload_files(
                file_objects=[(file_object, file_uri)],
                samples=sample_list,
                content_type='application/octet-stream'
            )


class PlateReaderOperator(Operator):

    def __init__(self, *, plan_id: str, operator_json: OperatorJSON):
        self._measurements = PlateReaderOperator._get_measurements(
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

    def accept(self, visitor: UploadVisitor):
        measurement = self._measurements
        file_uri = measurement['file_uri']
        sample_list = measurement['samples']
        file_object = visitor.lab.get_spectrophotometry(sample_list)
        visitor.upload_files(
            file_objects=[(file_object, file_uri)],
            samples=sample_list,
            content_type='text/csv'
        )
