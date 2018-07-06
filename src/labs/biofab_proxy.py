from labs.lab_proxy import LabProxy
from typing import List


class BIOFABProxy(LabProxy):

    def __init__(self, *, username, password, uri):
        pass

    def get_bucket_path(self):
        """
        Return lab specific bucket path.
        """
        return 'sd2e-community/ingest/biofab'

    def get_fcs(self, sample_uri: str):
        """
        Return the file for sample.
        Returns `None` if there is none.
        """
        pass

    def get_spectrophotometry(self, sample_list: List[str]):
        """
        Return a file which is a table of plate reader measurements for the file.
        Returns the empty list if there are none.
        """
        pass
