import json
import hashlib
from typing import Any, List


class UploadManifest:
    """
    Represents the manifest for an upload in a measurement operation.
    Collects sample and file relationships, and then generates the manifest file
    when dumped to a string.
    """

    def __init__(self, *,
                 manifest_uri: str,
                 plan_uri: str,
                 manifest_version: str="https://gitlab.sd2e.org/sd2program/ta3-api/tags/ta3-api-0.0.2",
                 config_uri: str):
        self.uri = manifest_uri
        self.plan_uri = plan_uri
        self.manifest_version = manifest_version
        self.instrument_configuration = config_uri
        self.sample_list = []

    def add_sample(self, *,
                   samples: List[str],
                   files: List[str]=[],
                   collected: bool=True):
        """
        Adds a sample to the manifest.
        If `collected` is false or `files` is empty, ensures both are true.
        """
        entry = dict()
        entry['sample'] = samples
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


def object_checksum(object: Any):
    """
    Computes the sha1 hash of an object to be uploaded.  The object should be
    the internal representation of a file (as in from upload.data).
    """
    hash_sha = hashlib.sha1()
    hash_sha.update(object)
    return hash_sha.hexdigest()


def file_checksum(filePath: str):
    """
    Computes the SHA1 checksum for the file at `filePath`.
    """
    hash_sha = hashlib.sha1()
    with open(filePath, "rb") as file:
        for chunk in iter(lambda: file.read(4096), b""):
            hash_sha.update(chunk)
    return hash_sha.hexdigest()
