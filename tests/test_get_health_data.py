import os
import json
import importlib.util
from pathlib import Path

import pytest


def load_module():
	"""Load the lambda module directly from the file path."""
	path = Path(__file__).resolve().parent.parent / "lambda_function.py"
	spec = importlib.util.spec_from_file_location("gethealth_lambda", str(path))
	mod = importlib.util.module_from_spec(spec)
	spec.loader.exec_module(mod)
	return mod


def test_lambda_handler_success(monkeypatch):
	mod = load_module()

	# Set environment expected by the lambda
	monkeypatch.setenv("bucket_name", "s3://test-bucket")

	# Patch get_secret to avoid calling AWS Secrets Manager
	monkeypatch.setattr(mod, "get_secret", lambda name: {"inclasns": "DUMMY_KEY"})

	# Prepare fake responses for requests.get: first call = indicadores, second = datos for indicador
	class Resp:
		def __init__(self, status, payload, headers=None):
			self.status_code = status
			self._payload = payload
			self.headers = headers or {}

		def json(self):
			return self._payload

		@property
		def text(self):
			try:
				return json.dumps(self._payload)
			except Exception:
				return str(self._payload)

	indicadores = [
		{"nombre": "EPOC indicador", "codigo": "C1"},
		{"nombre": "otro indicador", "codigo": "C2"},
	]

	datos_for_C1 = [{"datos": [{"year": 2023, "month": 1, "day": 1, "value": 10}], "codigo": "C1"}]

	responses = [Resp(200, indicadores), Resp(200, datos_for_C1)]

	def fake_requests_get(url, params=None, headers=None):
		# pop responses in order to simulate sequential calls
		return responses.pop(0)

	monkeypatch.setattr(mod.requests, "get", fake_requests_get)

	# Patch awswrangler.s3.to_parquet to avoid S3 interactions and capture calls
	calls = []

	def fake_to_parquet(df=None, path=None, **kwargs):
		calls.append({"path": path, "df_shape": getattr(df, 'shape', None), "kwargs": kwargs})
		return {"result": "ok"}

	# Ensure wr and wr.s3 exist and patch
	if not hasattr(mod, "wr"):
		class DummyS3:
			pass

		class DummyWr:
			s3 = DummyS3()

		mod.wr = DummyWr()

	if not hasattr(mod.wr, "s3"):
		mod.wr.s3 = type("S3", (), {})()

	monkeypatch.setattr(mod.wr.s3, "to_parquet", fake_to_parquet)

	# Execute lambda handler
	result = mod.lambda_handler({}, {})

	assert result is not None
	assert result.get("statusCode") == 200
	# One indicador (EPOC) debe haberse procesado
	assert result.get("size") == 1
	# Comprobamos que to_parquet fue llamado y que el path contiene el bucket
	assert len(calls) == 1
	assert "s3://test-bucket/staging/inclasns" in calls[0]["path"]


def test_lambda_handler_indicators_api_error(monkeypatch):
	mod = load_module()

	monkeypatch.setenv("bucket_name", "s3://test-bucket")
	monkeypatch.setattr(mod, "get_secret", lambda name: {"inclasns": "DUMMY_KEY"})

	class Resp:
		def __init__(self, status, payload=None, headers=None):
			self.status_code = status
			self._payload = payload or {}
			self.headers = headers or {}

		def json(self):
			return self._payload

		@property
		def text(self):
			try:
				return json.dumps(self._payload)
			except Exception:
				return str(self._payload)

	# First call returns 500 -> lambda should not return the success dict
	monkeypatch.setattr(mod.requests, "get", lambda url, params=None, headers=None: Resp(500, {"error": "fail"}))

	result = mod.lambda_handler({}, {})
	# In the code path if initial API call != 200, lambda_handler does not return the success dict
	assert result is None

