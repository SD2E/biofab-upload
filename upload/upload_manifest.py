"""
Provides the UploadManifest class.
"""
import json
import hashlib
import os


class UploadManifest:
    """
    Represents the manifest for an upload in a measurement operation.
    """

    def __init__(self, **params):
        self.uri = params['manifest_uri']
        self.plan_uri = params['plan_uri']
        self.manifest_version = params['manifest_version']
        self.instrument_configuration = params['config_uri']
        self.sample_list = []

    def add_sample(self, **params):
        entry = dict()
        entry['sample'] = params['sample']
        if 'files' in params:
            entry['files'] = params['files']
        else:
            entry['files'] = []
        if 'collected' in params:
            entry['collected'] = params['collected']
        else:
            entry['collected'] = bool(entry['files'])
        self.sample_list.append(entry)

    # TODO: deal with missing components
    def __str__(self):
        """
        Returns the string containing the JSON for this manifest.
        """
        return json.dumps(
            {
                "rdf:about": str(self.uri),
                "plan": str(self.plan_uri),
                "manifest_version": str(self.manifest_version),
                "instrument_configuration": str(self.instrument_configuration),
                "samples": self.sample_list
            },
            indent=2)

    def add_samples(self, association, exp_uri):
        """
        Add samples from uri_map to manifest
        """
        visitor = ManifestAssociationVisitor(manifest=self, exp_uri=exp_uri)
        association.accept(visitor)


class ManifestAssociationVisitor:
    def __init__(self, **params):
        self._manifest = params['manifest']
        self._exp_uri = params['exp_uri']

    def visit(self, well_id, uri):
        dirpath = os.path.join('data', well_id)
        file = os.listdir(dirpath)[0]
        self._manifest.add_sample(
            sample_uri=str(self._exp_uri.extend(well_id)),
            file_list=[{
                'file': str(uri),
                'checksum': file_checksum(os.path.join(dirpath, file))
            }]
        )


def object_checksum(object):
    """
    Computes the sha1 hash of an object to be uploaded.  The object should be
    the internal representation of a file (as in from upload.data).
    """
    hash_sha = hashlib.sha1()
    hash_sha.update(object)
    return hash_sha.hexdigest()


def file_checksum(filePath):
    """
    Computes the SHA1 checksum for the file at `filePath`.
    """
    hash_sha = hashlib.sha1()
    with open(filePath, "rb") as file:
        for chunk in iter(lambda: file.read(4096), b""):
            hash_sha.update(chunk)
    return hash_sha.hexdigest()
