"""
Provides the util.AgaveURI class, which represents a URI of the form
`agave://host/path`.
"""
import os


class AgaveURI:
    """
    Class to represent a URI on an agave resource.
    """

    def __init__(self, *, host, path):
        self._scheme = 'agave'
        self.host = host
        self.path = path

    @classmethod
    def from_URI(cls, uri):
        return cls(
            host=uri.host,
            path=os.path.join(*(str.split(str(uri.path), os.sep)))
        )

    def extend(self, ext_path):
        """
        Extends this URI with the given path.
        """
        return AgaveURI(
            host=self.host,
            path=os.path.join(self.path, ext_path)
        )

    def basename(self):
        return os.path.basename(self.path)

    def __dict__(self):
        return str(self)

    def __str__(self):
        return "{}://{}/{}".format(self._scheme, self.host, str(self.path))


def replace_prefix(path, old_prefix, new_prefix):
    if (path.startswith(old_prefix)):
        return path.replace(old_prefix, new_prefix, 1)
    else:
        return path
