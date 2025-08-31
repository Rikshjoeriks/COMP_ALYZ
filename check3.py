import os
from dotenv import load_dotenv

load_dotenv()  # reads .env from cwd
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    raise RuntimeError("OPENAI_API_KEY is not set. Put it in your .env file.")

from openai import OpenAI

client = OpenAI(api_key=OPENAI_API_KEY)  # or just OpenAI() if .env is loaded

models = client.models.list()
for m in models.data:
    print(m.id)
