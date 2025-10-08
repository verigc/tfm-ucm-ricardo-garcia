import sys
import types


def _ensure_dummy_aws_modules():
    # boto3 dummy
    if 'boto3' not in sys.modules:
        boto3_mod = types.SimpleNamespace()
        # add minimal Session callable used by some modules
        boto3_mod.session = types.SimpleNamespace(Session=lambda: types.SimpleNamespace(client=lambda *a, **k: None))
        sys.modules['boto3'] = boto3_mod

    # botocore.exceptions dummy
    if 'botocore' not in sys.modules:
        botocore = types.ModuleType('botocore')
        botocore.exceptions = types.SimpleNamespace(ClientError=Exception)
        sys.modules['botocore'] = botocore
        sys.modules['botocore.exceptions'] = botocore.exceptions

    # awswrangler dummy with s3.to_parquet
    if 'awswrangler' not in sys.modules:
        class DummyS3:
            @staticmethod
            def to_parquet(*args, **kwargs):
                return {'ok': True}

        awswrangler = types.SimpleNamespace(s3=DummyS3())
        sys.modules['awswrangler'] = awswrangler


_ensure_dummy_aws_modules()
