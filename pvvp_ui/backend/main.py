from fastapi import FastAPI, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from pathlib import Path
import subprocess, json, os

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
    if PVVP_DIR is None:
        raise FileNotFoundError("PVVP_DIR is not resolved")
    (PVVP_DIR / "sessions" / session_id).mkdir(parents=True, exist_ok=True)
    return PVVP_DIR / "sessions" / session_id

def run_script(args: list[str]) -> dict:
    if REPO_ROOT is None:
        return {"exit": -1, "stdout": "", "stderr": "REPO_ROOT not resolved"}
    try:
        proc = subprocess.run(args, capture_output=True, text=True, cwd=REPO_ROOT)
        return {"exit": proc.returncode, "stdout": proc.stdout, "stderr": proc.stderr}
    except Exception as e:
        return {"exit": -1, "stdout": "", "stderr": str(e)}

@app.get("/api/hello")
def hello():
    return {"msg": "ok"}

@app.get("/api/debug/paths")
def debug_paths():
    return {
        "cwd": os.getcwd(),
        "this_file": str(Path(__file__).resolve()),
        "REPO_ROOT": str(REPO_ROOT) if REPO_ROOT else None,
        "PVVP_DIR": str(PVVP_DIR) if PVVP_DIR else None,
        "PVVP_DIR_exists": bool(PVVP_DIR and Path(PVVP_DIR).exists()),
        "L03_exists": bool(PVVP_DIR and (Path(PVVP_DIR) / "L03_normalize.py").exists()),
        "SESSIONS_DIR": str(SESSIONS_DIR) if SESSIONS_DIR else None,
    }

@app.get("/api/sessions")
def sessions():
    return {"items": SESSIONS}

@app.post("/api/session/init")
def session_init(body: InitBody):
    if body.sessionId not in SESSIONS:
        return JSONResponse({"error": "Unknown sessionId"}, status_code=400)
    if PVVP_DIR is None or not (PVVP_DIR / "L03_normalize.py").exists():
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
    if PVVP_DIR is None or not (PVVP_DIR / "L03_normalize.py").exists():
        return JSONResponse({"error": f"L03_normalize.py not found under {PVVP_DIR}"}, status_code=400)
    session_dir = ensure_session_folder(body.sessionId)
    input_file = session_dir / "input_raw.txt"
    if not input_file.exists():
        return JSONResponse({"error": f"Missing input file at {input_file}"}, status_code=400)
    cmd = [
        python_exec(),
        "-m",
        "pvvp.L03_normalize",
        "--session",
        body.sessionId,
        "--project-root",
        "pvvp",
    ]
    result = run_script(cmd)
    if result.get("exit") != 0:
        debug_file = session_dir / "normalize_debug.txt"
        if debug_file.exists():
            result["debug"] = debug_file.read_text(encoding="utf-8", errors="ignore")
        return JSONResponse(result, status_code=500)
    return result
