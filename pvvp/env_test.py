from pathlib import Path
from pvvp.common.env_setup import load_env, get_openai_config, mask

print("cwd:", Path().resolve())
loaded = load_env(override=False)
print("Loaded .env files:", [str(p) for p in loaded])
try:
    cfg = get_openai_config()
    print(f"✅ OPENAI_API_KEY loaded: {mask(cfg['api_key'])}")
except Exception as e:
    print("❌", e)
