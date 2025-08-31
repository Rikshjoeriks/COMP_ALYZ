import requests
from pvvp.common.env_setup import load_env, get_openai_config, mask

load_env()
cfg = get_openai_config()
api_key = cfg["api_key"]
print("OPENAI_API_KEY:", mask(api_key))
url = cfg["base_url"].rstrip("/") + "/models"
headers = {"Authorization": f"Bearer {api_key}"}

resp = requests.get(url, headers=headers)
if resp.status_code == 200:
    print("API is working. Available models:")
    for m in resp.json().get("data", []):
        print("-", m["id"])
else:
    print(f"API error {resp.status_code}: {resp.text}")