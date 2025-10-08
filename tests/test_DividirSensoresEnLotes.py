import importlib.util
from pathlib import Path


def load_module(path):
    spec = importlib.util.spec_from_file_location("mod", str(path))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_divide_batches():
    path = Path(__file__).resolve().parent.parent / "lambda_functions" / "DividirSensoresEnLotes" / "DividirSensoresEnLotes.py"
    mod = load_module(path)

    all_ids = list(range(1200))
    batches = mod.lambda_handler(all_ids, None)

    # Should create ceil(1200/500) = 3 batches
    assert isinstance(batches, list)
    assert len(batches) == 3
    # Check sizes: first two 500, last 200
    assert len(batches[0]) == 500
    assert len(batches[1]) == 500
    assert len(batches[2]) == 200
