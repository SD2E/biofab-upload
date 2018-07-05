import abc
from typing import List


class LabProxy(abc.ABC):

    @abc.abstractmethod
    def get_bucket_path(self):
        """
        Return lab specific bucket path.
        """

    @abc.abstractmethod
    def get_fcs(self, sample_uri: str):
        """
        Return the file for sample.
        Returns `None` if there is none.
        """

    @abc.abstractmethod
    def get_spectrophotometry(self, sample_list: List[str]):
        """
        Return a file which is a table of plate reader measurements for the file.
        Returns the empty list if there are none.
        """
