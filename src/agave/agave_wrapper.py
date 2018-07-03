import os
from agavepy.agave import Agave


class AgaveWrapper:
    """
    An adapter for writing files to TA3 file space using SD2e Agave.

    Methods shadow the S3 interface in boto3, if you swap "directory" for
    "bucket"
    """

    def __init__(self, *, rootpath, system_id, server, username, password,
                 client, api_key, api_secret):
        """
        Initialize a DataManager object.

        Arguments:
          params (string) rootpath  the root path on the Agave server
          params (string) system_id  the system id for the Agave client
          params (string) username  the username for connecting to the server
          params (string) password  the password for connecting to the server
          params (string) server  the server name
          params (string) client the Agave client
          params (string) api_key the Agave API key
          params (string) api_secret the Agave API secret
        """
        self._rootpath = rootpath
        self._system_id = system_id

        self._agave = Agave(
            api_server='https://' + server,
            username=username,
            password=password,
            client_name=client,
            api_key=api_key,
            api_secret=api_secret
        )

    def make_directory(self, path):
        """
        Creates a subdirectory from the root path.
        (not necessary to include the root path in the argument)

        Argument:
          path (string) the path from the root path to be created
        """
        self._agave.files.manage(
            body={'action': 'mkdir', 'path': path},
            systemId=self._system_id,
            filePath=self._rootpath
        )

    def delete_directory(self, path):
        """
        Deletes a subdirectory of the root path.
        (Do not include the root path in the argument.)

        Argument:
          path (string) the path from the root path to be deleted
        """
        self._agave.files.manage(
            body={'action': 'delete', 'path': path},
            systemId=self._system_id,
            filePath=self._rootpath
        )

    def upload_file(self, file_path, dest_path, filename):
        """
        Uploads the file to the given destination.

        Arguments:
          file_path (string)  the path to the file to upload
          dest_path (string)  the destination path on the server
          filename (string)  the name of the file to upload
        """
        with open(file_path, 'rb') as file:
            self.upload_fileobj(file, dest_path, filename)

    def upload_fileobj(self, file_obj, dest_path, filename):
        """
        Uploads the file object to the given destination.

        Arguments:
          file_object (file object)  the object for the file to upload
          dest_path (string)  the destination path on the server
          filename (string)  the name of the file to upload
        """
        qualified_path = os.path.join(self._rootpath, dest_path)
        self._agave.files.importData(
            systemId=self._system_id,
            filePath=qualified_path,
            fileToUpload=file_obj,
            fileName=filename
        )


class AgaveInstanceFactory:
    """
    Factory object for Agave instances.
    """

    def __init__(self, *, username, password, server, client, api_key,
                 api_secret):
        self._map = dict()
        self._username = username
        self._password = password
        self._server = server
        self._client = client
        self._api_key = api_key
        self._api_secret = api_secret

    def get_instance(self, agave_path):
        """
        Returns the AgaveWrapper instance with the system_id and rootpath of
        the given AgavePath object.
        """
        agave = None
        instance_key = "[" + agave_path.system_id + \
            "," + agave_path.rootpath + "]"
        if instance_key not in self._map:
            agave = AgaveWrapper(
                rootpath=agave_path.rootpath,
                system_id=agave_path.system_id,
                username=self._username,
                password=self._password,
                server=self._client,
                client=self._client,
                api_key=self._api_key,
                api_secret=self._api_secret
            )
            self._map[instance_key] = agave
        else:
            agave = self._map[instance_key]
        return agave
