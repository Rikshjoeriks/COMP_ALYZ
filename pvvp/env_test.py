# env_test.py
import os
from dotenv import load_dotenv

load_dotenv()  # loads .env from the current working directory (or parents)
key = os.getenv("OPENAI_API_KEY")

print("cwd:", os.getcwd())
if key and key.startswith("sk-"):
    print(f"✅ OPENAI_API_KEY loaded: length={len(key)} (value hidden)")
else:
    print("❌ OPENAI_API_KEY not found. Make sure you created a .env file next to env_test.py.")
