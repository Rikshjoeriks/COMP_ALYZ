from pvvp.common.env_setup import load_env, get_openai_config, mask
from openai import OpenAI

load_env()
cfg = get_openai_config()
print("OPENAI_API_KEY:", mask(cfg["api_key"]))
client = OpenAI(api_key=cfg["api_key"])  # or just OpenAI() if .env is loaded

models = client.models.list()
for m in models.data:
    print(m.id)
