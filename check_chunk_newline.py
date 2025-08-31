import json, os

root = "pvvp"
car = "MCAFHEV"
MIN = 800
MAX = 2500

p = os.path.join(root, "sessions", car, "chunks.jsonl")
t = os.path.join(root, "sessions", car, "text_normalized.txt")

with open(p, "r", encoding="utf-8") as f:
    chunks = [json.loads(line) for line in f]

with open(t, "r", encoding="utf-8") as f:
    text = f.read()

for c in chunks:
    length = c["end"] - c["start"] + 1
    if MIN <= length <= MAX:
        if text[c["end"]] == "\n":
            print(f"Chunk {c['id']} ends on a newline at position {c['end']}")
        else:
            print(f"Chunk {c['id']} does NOT end on a newline at position {c['end']}")