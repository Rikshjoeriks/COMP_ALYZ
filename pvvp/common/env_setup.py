from __future__ import annotations
import os
from pathlib import Path
from typing import Dict, List, Optional

try:
    from dotenv import load_dotenv, find_dotenv
except Exception:  # pragma: no cover
    load_dotenv = None
    find_dotenv = None

_LOADED_PATHS: List[Path] = []


def _safe_load(path: Path, override: bool) -> None:
    if load_dotenv is None:
        return
    if path and path.exists():
        load_dotenv(dotenv_path=str(path), override=override, encoding="utf-8")
        _LOADED_PATHS.append(path)


def load_env(override: bool = False) -> List[Path]:
    """Load .env files with defined precedence.

    Precedence (later overrides earlier if override=True):
      1) repo root .env
      2) pvvp/.env
      3) nearest .env discovered from CWD
    Returns list of loaded paths in order.
    """
    _LOADED_PATHS.clear()
    root = Path(__file__).resolve().parents[2]
    pvvp_dir = root / "pvvp"

    _safe_load(root / ".env", override=False)
    _safe_load(pvvp_dir / ".env", override=True if not override else True)

    if find_dotenv is not None:
        discovered = find_dotenv(filename=".env", usecwd=True)
        if discovered:
            p = Path(discovered).resolve()
            _safe_load(p, override=True if override else False)

    return list(_LOADED_PATHS)


def mask(s: Optional[str]) -> str:
    if not s:
        return "<empty>"
    s = str(s)
    if len(s) <= 12:
        return s[0:2] + "****" + s[-2:]
    return s[0:6] + "****" + s[-4:]


def get_openai_config() -> Dict[str, str]:
    """Return OpenAI config from environment variables.

    Env names supported for API key:
      OPENAI_API_KEY, OPENAI_APIKEY, OPENAI_TOKEN
    Optional env vars:
      OPENAI_BASE_URL (default https://api.openai.com/v1)
      OPENAI_MODEL (default gpt-4o-mini)
    """
    api_key = (
        os.getenv("OPENAI_API_KEY")
        or os.getenv("OPENAI_APIKEY")
        or os.getenv("OPENAI_TOKEN")
        or ""
    ).strip()

    base_url = (os.getenv("OPENAI_BASE_URL") or "https://api.openai.com/v1").strip()
    model = (os.getenv("OPENAI_MODEL") or "gpt-4o-mini").strip()

    if not api_key:
        loaded = " | ".join(str(p) for p in _LOADED_PATHS) or "(no .env loaded)"
        raise EnvironmentError(
            "OPENAI_API_KEY not found.\n"
            "Add it to one of these locations and retry:\n"
            f" - {Path.cwd() / '.env'} (current CWD)\n"
            f" - {Path(__file__).resolve().parents[2] / '.env'} (repo root)\n"
            f" - {Path(__file__).resolve().parents[2] / 'pvvp' / '.env'} (pvvp)\n"
            f"Loaded .env files: {loaded}"
        )

    return {"api_key": api_key, "base_url": base_url, "model": model}
