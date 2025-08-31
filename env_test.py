import os, sys

print("👋 running env_test.py")
print("python:", sys.version)
print("cwd:", os.getcwd())

try:
    from dotenv import load_dotenv
    loaded = load_dotenv()
    print("dotenv imported:", True, "| load_dotenv() returned:", loaded)
except Exception as e:
    print("❌ python-dotenv not installed or import failed:", repr(e))
    raise SystemExit(1)

key = os.getenv("OPENAI_API_KEY")
print("OPENAI_API_KEY present:", bool(key))
if key:
    print("key length:", len(key))
