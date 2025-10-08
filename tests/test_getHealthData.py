import importlib.util
from pathlib import Path
import sys

def load_module(path):
    # Insert light-weight dummy aws modules to avoid import errors at module import time
    import types
    if 'boto3' not in sys.modules:
        sys.modules['boto3'] = types.SimpleNamespace(session=types.SimpleNamespace(Session=lambda: None), client=lambda *a, **k: None)
    if 'botocore.exceptions' not in sys.modules:
        sys.modules['botocore'] = types.SimpleNamespace(exceptions=types.SimpleNamespace(ClientError=Exception))
    if 'awswrangler' not in sys.modules:
        class DummyS3:
            @staticmethod
            def to_parquet(*a, **k):
                return {'ok': True}
        sys.modules['awswrangler'] = types.SimpleNamespace(s3=DummyS3())

    spec = importlib.util.spec_from_file_location("mod", str(path))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_getHealthData_happy(monkeypatch):
    path = Path(__file__).resolve().parent.parent / "lambda_functions" / "getHealthData" / "getHealthData.py"
    mod = load_module(path)

    monkeypatch.setenv('bucket_name', 's3://test-bucket')
    monkeypatch.setattr(mod, 'get_secret', lambda name: {'inclasns': 'DUMMY'})

    class Resp:
        def __init__(self, status, payload):
            self.status_code = status
            self._payload = payload
            self.headers = {}
        def json(self):
            return self._payload
        @property
        def text(self):
            return str(self._payload)

    indicadores = [{"nombre": "EPOC indicador", "codigo": "C1"}]
    datos_for_C1 = [{"datos": [{"year": 2023, "month": 1, "day": 1, "value": 10}], "codigo": "C1", "nombre": "EPOC indicador"}]
    responses = [Resp(200, indicadores), Resp(200, datos_for_C1)]

    def fake_get(url, params=None, headers=None):
        return responses.pop(0)

    monkeypatch.setattr(mod.requests, 'get', fake_get)

    res = mod.lambda_handler({}, {})
    assert res is not None
    assert res.get('statusCode') == 200
