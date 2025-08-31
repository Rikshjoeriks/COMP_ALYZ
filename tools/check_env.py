from pathlib import Path
from pvvp.common.env_setup import load_env, get_openai_config, mask

print("CWD:", Path().resolve())
loaded = load_env(override=False)
print("Loaded .env files:", [str(p) for p in loaded])
cfg = get_openai_config()
print("OPENAI_API_KEY:", mask(cfg["api_key"]))
print("OPENAI_BASE_URL:", cfg["base_url"])
print("OPENAI_MODEL:", cfg["model"])
