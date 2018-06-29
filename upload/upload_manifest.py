"""
Provides the UploadManifest class.
"""
import json
import hashlib


class UploadManifest:
    """
    Represents the manifest for an upload in a measurement operation.
    """

    def __init__(self, *,
                 manifest_uri, plan_uri, manifest_version, config_uri):
        self.uri = manifest_uri
        self.plan_uri = plan_uri
        self.manifest_version = manifest_version
        self.instrument_configuration = config_uri
        self.sample_list = []

    def add_sample(self, *, sample, files=[], collected=True):
        """
        Adds a sample to the manifest.
        If `collected` is false or `files` is empty, ensures both are true.
        """
        entry = dict()
        entry['sample'] = sample
        entry['files'] = []
        if collected:
            entry['files'] = files
        entry['collected'] = collected and bool(entry['files'])
        self.sample_list.append(entry)

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
