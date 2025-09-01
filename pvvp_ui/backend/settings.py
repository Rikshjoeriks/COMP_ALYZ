from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
PVVP_DIR = REPO_ROOT / "pvvp"          # expects sibling 'pvvp' inside pvvp_app
SESSIONS_DIR = PVVP_DIR / "sessions"

FRONTEND_ORIGIN = "http://localhost:5173"

def python_exec() -> str:
    return sys.executable or "python"
