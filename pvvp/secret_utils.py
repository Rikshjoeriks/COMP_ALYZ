import os
from pathlib import Path
from typing import Tuple, Optional

def mask_secret(s: str, keep: int = 4) -> str:
    if not s:
        return "<empty>"
    if len(s) <= keep * 2:
        return s[0] + "***" + s[-1]
    return f"{s[:keep]}…{s[-keep:]}"

def load_api_key(
    cli_key: Optional[str] = None,
    key_file: Optional[Path] = None,
    env_name: str = "OPENAI_API_KEY",
    dot_env_paths: Optional[list[Path]] = None,
) -> Tuple[Optional[str], str]:
    """Returns (api_key, source_label)."""
    if cli_key:
        return cli_key.strip(), "cli"
    if key_file and key_file.exists():
        k = key_file.read_text(encoding="utf-8").strip().splitlines()[0].strip()
        if k:
            return k, f"file:{key_file}"
    k = os.environ.get(env_name, "").strip()
    if k:
        return k, f"env:{env_name}"
    dot_env_paths = dot_env_paths or [
        Path.cwd() / ".env",
        Path(__file__).resolve().parent / ".env",
        Path(__file__).resolve().parents[1] / ".env",
    ]
    for p in dot_env_paths:
        if p.exists():
            for line in p.read_text(encoding="utf-8").splitlines():
                if line.strip().startswith(f"{env_name}="):
                    v = line.split("=", 1)[1].strip().strip('"').strip("'")
                    if v:
                        return v, f".env:{p}"
    return None, "not_found"
