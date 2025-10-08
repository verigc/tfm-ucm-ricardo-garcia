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

def test_get_openaq_sensors_minimal(monkeypatch):
    path = Path(__file__).resolve().parent.parent / "lambda_functions" / "get_openaq_sensors" / "get_openaq_sensors.py"
    mod = load_module(path)

    monkeypatch.setenv('bucket_name', 'test-bucket')
    # Fake parameters and locations
    monkeypatch.setattr('requests.get', lambda url, params=None, headers=None: type('R', (), {'status_code': 200, 'json': lambda: {'results': [], 'meta': {'found': 0, 'limit': 1000}}, 'headers': {}})())

    res = mod.lambda_handler({}, {})
    assert isinstance(res, list)
