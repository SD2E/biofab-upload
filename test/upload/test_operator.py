import pytest

from upload.xplan_operator import Operator



class TestOperator:
    pass

class MockS3:
    def put_object(self, *, object, bucket_path, agave_uri, content_type):
        pass