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

try:
    from dotenv import load_dotenv, find_dotenv
    # search upwards for a .env; don't override existing env vars
    loaded = load_dotenv(find_dotenv(), override=False)
    print("dotenv imported:", True, "| load_dotenv() returned:", loaded)
except Exception as e:
    if pytest:
        pytest.fail(f"❌ python-dotenv import/load failed: {e!r}")
    else:
        raise

key = os.getenv("OPENAI_API_KEY")
print("OPENAI_API_KEY present:", bool(key))
if key:
    print("key length:", len(key))
