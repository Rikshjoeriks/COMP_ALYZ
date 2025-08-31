from __future__ import annotations

import asyncio
import json
import uuid
from pathlib import Path
from typing import Dict, List, Optional

from fastapi import BackgroundTasks, FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

# Base directories
BASE_DIR = Path(__file__).resolve().parents[1]  # pvvp package directory
REPO_DIR = BASE_DIR.parent
SESSIONS_DIR = BASE_DIR / "sessions"

app = FastAPI(title="PVVP Web API")


class SessionInit(BaseModel):
    sessionId: str
    carId: Optional[str] = None


class SessionRequest(BaseModel):
    sessionId: str


class InputText(SessionRequest):
    text: str


class ChunkParams(SessionRequest):
    min: Optional[int] = None
    max: Optional[int] = None


class MergeRequest(SessionRequest):
    approvals: List[Dict[str, Optional[str]]] = []


class ExportRequest(SessionRequest):
    carId: Optional[str] = None


# -------------------- utility helpers --------------------

class ProcessRun:
    def __init__(self) -> None:
        self.id = uuid.uuid4().hex
        self.queue: asyncio.Queue[str | None] = asyncio.Queue()
        self.returncode: Optional[int] = None


RUNS: Dict[str, ProcessRun] = {}


async def _spawn(cmd: List[str]) -> str:
    run = ProcessRun()
    RUNS[run.id] = run
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        cwd=str(REPO_DIR),
    )

    async def _read_stream() -> None:
        assert proc.stdout
        async for line in proc.stdout:
            await run.queue.put(line.decode())
        await proc.wait()
        run.returncode = proc.returncode
        await run.queue.put(None)

    asyncio.create_task(_read_stream())
    return run.id


async def _stream(run: ProcessRun):
    while True:
        line = await run.queue.get()
        if line is None:
            yield f"event: end\ndata: {run.returncode}\n\n"
            break
        yield f"data: {line}\n\n"


def _session_path(session_id: str) -> Path:
    path = SESSIONS_DIR / session_id
    path.mkdir(parents=True, exist_ok=True)
    return path


def _load_json(path: Path, fallback):
    if not path.exists():
        return fallback
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return fallback


# ----------------------- API endpoints -----------------------

@app.get("/api/sessions")
async def list_sessions() -> List[Dict[str, object]]:
    sessions = []
    if SESSIONS_DIR.exists():
        for p in SESSIONS_DIR.iterdir():
            if p.is_dir():
                sessions.append({"id": p.name, "lastModified": int(p.stat().st_mtime)})
    return sessions


@app.post("/api/session/init")
async def init_session(req: SessionInit):
    path = _session_path(req.sessionId)
    if req.carId:
        (path / "car_id.txt").write_text(req.carId, encoding="utf-8")
    return {"status": "ok"}


@app.post("/api/input")
async def save_input(req: InputText):
    path = _session_path(req.sessionId)
    (path / "input_raw.txt").write_text(req.text, encoding="utf-8")
    return {"status": "ok"}


@app.post("/api/input/upload")
async def upload_input(sessionId: str = Form(...), file: UploadFile = File(...)):
    path = _session_path(sessionId)
    content = await file.read()
    (path / "input_raw.txt").write_bytes(content)
    return {"status": "ok"}


async def _run_script(script: str, *extra: str) -> str:
    cmd = ["python", str(BASE_DIR / script), "--session", extra[0], "--project-root", "pvvp", *extra[1:]]
    return await _spawn(cmd)


@app.post("/api/run/normalize")
async def run_normalize(req: SessionRequest):
    run_id = await _run_script("L03_normalize.py", req.sessionId)
    return {"runId": run_id}


@app.post("/api/run/chunk")
async def run_chunk(req: ChunkParams):
    args: List[str] = [req.sessionId]
    if req.min is not None:
        args.extend(["--min", str(req.min)])
    if req.max is not None:
        args.extend(["--max", str(req.max)])
    cmd = ["python", str(BASE_DIR / "L04_chunker.py"), "--session", req.sessionId, "--project-root", "pvvp"]
    if req.min is not None:
        cmd += ["--min", str(req.min)]
    if req.max is not None:
        cmd += ["--max", str(req.max)]
    run_id = await _spawn(cmd)
    return {"runId": run_id}


@app.post("/api/run/mapper")
async def run_mapper(req: SessionRequest):
    run_id = await _run_script("L06_mapper.py" if (BASE_DIR / "L05_mapper.py").exists() else "L05_mapper.py", req.sessionId)
    # Note: some repos may name mapper script L05_mapper.py
    return {"runId": run_id}


@app.post("/api/run/review/prepare")
async def run_review_prepare(req: SessionRequest):
    cmd = [
        "python",
        str(BASE_DIR / "L06_mapper_review.py"),
        "--session",
        req.sessionId,
        "--project-root",
        "pvvp",
        "--prepare",
    ]
    run_id = await _spawn(cmd)
    return {"runId": run_id}


@app.get("/api/review/candidates")
async def get_review_candidates(sessionId: str):
    path = _session_path(sessionId) / "review_candidates.json"
    return _load_json(path, {"items": []})


@app.post("/api/review/merge")
async def run_review_merge(req: MergeRequest):
    path = _session_path(req.sessionId)
    decisions_file = path / "review_decisions.json"
    decisions_file.write_text(
        json.dumps({"approved": req.approvals}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    cmd = [
        "python",
        str(BASE_DIR / "L06_mapper_review.py"),
        "--session",
        req.sessionId,
        "--project-root",
        "pvvp",
        "--merge",
    ]
    run_id = await _spawn(cmd)
    return {"runId": run_id}


@app.get("/api/merge/summary")
async def get_merge_summary(sessionId: str):
    path = _session_path(sessionId)
    merged = _load_json(path / "mapper_all_merged.json", {})
    result = _load_json(path / "merge_result.json", {})
    return {"mapper_all_merged": merged, "merge_result": result}


@app.post("/api/table/preview")
async def table_preview(req: SessionRequest):
    path = _session_path(req.sessionId)
    master_file = path / "pvvp_master.csv"
    merge_file = path / "merge_result.json"
    merge = _load_json(merge_file, {})
    mentioned = set(merge.get("mentioned_vars", []))
    evidence_map = merge.get("evidence", {})
    items: List[Dict[str, object]] = []
    if master_file.exists():
        import csv

        with master_file.open(encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                var = row.get("Variable Name", "")
                is_tt = row.get("Section TT", "").strip().upper() == "TT"
                mentioned_flag = var in mentioned
                items.append(
                    {
                        "nr_code": row.get("Nr Code"),
                        "variable_name_lv": var,
                        "is_tt": is_tt,
                        "mentioned_YN": "Y" if mentioned_flag else "N",
                        "evidence": evidence_map.get(var, "") if mentioned_flag else "",
                    }
                )
    return {"items": items}


@app.get("/api/budget")
async def get_budget(sessionId: str):
    path = _session_path(sessionId) / "budget_report.json"
    return _load_json(path, {})


@app.post("/api/export/csv")
async def export_csv(req: ExportRequest):
    return {"status": "stub"}


@app.post("/api/export/positives")
async def export_positives(req: ExportRequest):
    return {"status": "stub"}


@app.post("/api/export/summary")
async def export_summary(req: ExportRequest):
    return {"status": "stub"}


@app.get("/api/stream/{run_id}")
async def stream_logs(run_id: str):
    run = RUNS.get(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="run not found")
    return StreamingResponse(_stream(run), media_type="text/event-stream")
