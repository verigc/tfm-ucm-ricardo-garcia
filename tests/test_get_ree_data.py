import importlib.util
from pathlib import Path
import sys

def load_module(path):
    import types
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

def test_get_ree_minimal(monkeypatch):
    path = Path(__file__).resolve().parent.parent / "lambda_functions" / "get_ree_data" / "get_ree_data.py"
    mod = load_module(path)

    # Patch environment so it doesn't crash on parsing
    monkeypatch.setenv('fecha_ini', '2024-01-01T00:00:00')
    monkeypatch.setenv('fecha_fin', '2024-12-31T23:59:59')
    monkeypatch.setenv('time_trunc', 'day')
    monkeypatch.setenv('uri_definition', '/es/datos/demanda/evolucion')
    monkeypatch.setenv('bucket_name', 'test-bucket')

    # Patch requests.get to return a minimal expected structure
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

    data = {'included': [{ 'attributes': {'values': [{'value': 1}]}}]}
    monkeypatch.setattr('requests.get', lambda url, headers=None: Resp(200, data))

    res = mod.lambda_handler({}, {})
    assert isinstance(res, dict)
    assert res.get('statusCode') == 200
