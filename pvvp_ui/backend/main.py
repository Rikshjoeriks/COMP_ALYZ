from fastapi import FastAPI, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from pathlib import Path
import subprocess, json

from settings import REPO_ROOT, PVVP_DIR, SESSIONS_DIR, FRONTEND_ORIGIN, python_exec

app = FastAPI(title="PVVP UI Backend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[FRONTEND_ORIGIN],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

SESSIONS = ["MCAICE", "MCAFHEV", "MCAPHEV", "EV", "BEV"]

class InitBody(BaseModel):
    sessionId: str
    carId: str

class InputBody(BaseModel):
    sessionId: str
    text: str

class RunBody(BaseModel):
    sessionId: str

class ChunkBody(BaseModel):
    sessionId: str
    min: int | None = None
    max: int | None = None

def ensure_session_folder(session_id: str) -> Path:
    (SESSIONS_DIR / session_id).mkdir(parents=True, exist_ok=True)
    return SESSIONS_DIR / session_id

def run_script(args: list[str]) -> dict:
    try:
        proc = subprocess.run(args, capture_output=True, text=True, cwd=REPO_ROOT)
        return {"exit": proc.returncode, "stdout": proc.stdout, "stderr": proc.stderr}
    except Exception as e:
        return {"exit": -1, "stdout": "", "stderr": str(e)}

@app.get("/api/hello")
def hello():
    return {"msg": "ok"}

@app.get("/api/sessions")
def sessions():
    return {"items": SESSIONS}

@app.post("/api/session/init")
def session_init(body: InitBody):
    if body.sessionId not in SESSIONS:
        return JSONResponse({"error": "Unknown sessionId"}, status_code=400)
    if not PVVP_DIR.exists():
        return JSONResponse({"error": f"Missing pvvp folder at {PVVP_DIR}"}, status_code=400)
    ensure_session_folder(body.sessionId)
    return {"ok": True}

@app.post("/api/input")
def save_input(body: InputBody):
    session_dir = ensure_session_folder(body.sessionId)
    target = session_dir / "input_raw.txt"
    target.write_text(body.text or "", encoding="utf-8")
    return {"ok": True, "path": str(target)}

@app.post("/api/input/upload")
async def upload_input(sessionId: str = Form(...), file: UploadFile = File(...)):
    session_dir = ensure_session_folder(sessionId)
    target = session_dir / "input_raw.txt"
    data = await file.read()
    target.write_bytes(data)
    return {"ok": True, "path": str(target)}

@app.post("/api/run/normalize")
def run_normalize(body: RunBody):
    if not (PVVP_DIR / "L03_normalize.py").exists():
        return JSONResponse({"error": "L03_normalize.py not found"}, status_code=400)
    cmd = [
        python_exec(), "pvvp/L03_normalize.py",
        "--session", body.sessionId,
        "--project-root", "pvvp"
    ]
    result = run_script(cmd)
    return result

@app.get("/api/review/candidates")
def review_candidates(sessionId: str):
    path = ensure_session_folder(sessionId) / "review_candidates.json"
    if not path.exists():
        sample = {
            "items": [
                {"variable":"Apsildāms stūres rats","evidence":"3-spieķu multifunkcionāla ādas sporta stūre ar apsildi"},
                {"variable":"Kruīza kontrole – adaptīvā","evidence":"Adaptīvā kruīza kontrole"}
            ]
        }
        return sample
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, list):
            return {"items": data}
        if "items" not in data and "review_candidates" in data:
            return {"items": data["review_candidates"]}
        return data
    except Exception as e:
        return JSONResponse({"error": f"Failed to parse: {e}"}, status_code=500)

