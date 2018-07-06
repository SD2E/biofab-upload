from synbiohub_adapter.upload_sbol import SynBioHub

class SynBioHubProxy:

    def __init__(self, *, uri, username, password, api):
        self._sbh = SynBioHub(uri, username, password, api)

    def get_operator(self, *, plan_uri, step_id):
        pass
