# env_test.py  (pytest will collect *_test.py)
import os, sys, importlib
import importlib.util

# pytest might not exist if you run this as a plain script
try:
    import pytest
except Exception:
    pytest = None

def _skip(msg: str):
    if pytest:
        pytest.skip(msg, allow_module_level=True)
    else:
        print(f"SKIP: {msg}")
        return True
    return False

# skip the whole test if dotenv isn't installed
if importlib.util.find_spec("dotenv") is None:
    _skip("python-dotenv not installed in this environment")

print("👋 running env_test.py")
print("python:", sys.version)
print("cwd:", os.getcwd())

from pvvp.common.env_setup import load_env, get_openai_config, mask

try:
    loaded = load_env(override=False)
    print("dotenv imported:", True, "| load_env() loaded:", [str(p) for p in loaded])
except Exception as e:
    if pytest:
        pytest.fail(f"❌ env load failed: {e!r}")
    else:
        raise

try:
    cfg = get_openai_config()
    key = cfg["api_key"]
    print("OPENAI_API_KEY present:", bool(key))
    if key:
        print("key length:", len(key), "| preview:", mask(key))
except Exception as e:
    print("❌", e)
