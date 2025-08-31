import os
import requests

api_key = os.getenv("OPENAI_API_KEY")
if not api_key:
    print("OPENAI_API_KEY not set.")
    exit(1)

url = "https://api.openai.com/v1/models"
headers = {"Authorization": f"Bearer {api_key}"}

resp = requests.get(url, headers=headers)
if resp.status_code == 200:
    print("API is working. Available models:")
    for m in resp.json().get("data", []):
        print("-", m["id"])
else:
    print(f"API error {resp.status_code}: {resp.text}")