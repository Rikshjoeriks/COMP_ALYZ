from pathlib import Path
import os, sys

# Allow override via env var if you ever want to set it manually:
#   setx PVVP_PATH "C:\\Users\\vanag\\Documents\\INCHCAPE\\pvvp_app\\pvvp"
ENV_PVVP = os.environ.get("PVVP_PATH")

_here = Path(__file__).resolve()
# Start with cwd (when uvicorn launches) and walk up to find a folder that contains "pvvp"
candidates = []

# 1) CWD
try:
    from os import getcwd
    candidates.append(Path(getcwd()))
except Exception:
    pass

# 2) This file's parents
candidates.extend(list(_here.parents))  # backend, pvvp_ui, pvvp_app, ...

# 3) Optional env override's parent
if ENV_PVVP:
    env_p = Path(ENV_PVVP).resolve()
    candidates.append(env_p.parent)

REPO_ROOT = None
PVVP_DIR = None

# Prefer explicit env override if valid
if ENV_PVVP and (Path(ENV_PVVP) / "L03_normalize.py").exists():
    PVVP_DIR = Path(ENV_PVVP)
    REPO_ROOT = PVVP_DIR.parent
else:
    # otherwise scan candidates for a folder that has pvvp/L03_normalize.py
    for base in candidates:
        pvvp_try = base / "pvvp"
        if (pvvp_try / "L03_normalize.py").exists():
            PVVP_DIR = pvvp_try
            REPO_ROOT = base
            break

# Final fallback: two levels up (pvvp_app) + "pvvp"
if PVVP_DIR is None:
    fallback = _here.parents[2] / "pvvp"
    if (fallback / "L03_normalize.py").exists():
        PVVP_DIR = fallback
        REPO_ROOT = _here.parents[2]

# Sessions dir (created if missing later)
SESSIONS_DIR = PVVP_DIR / "sessions" if PVVP_DIR else None

FRONTEND_ORIGIN = "http://localhost:5173"

def python_exec() -> str:
    # use the current interpreter (your activated venv)
    return sys.executable or "python"
