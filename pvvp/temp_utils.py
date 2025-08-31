from __future__ import annotations
from pathlib import Path
import os
import shutil
import tempfile
import time


def make_temp_root(prefix: str = "pvvp_") -> Path:
    """Create the root temp directory with required subfolders."""
    base = os.environ.get("LOCALAPPDATA") or None
    temp_root = Path(tempfile.mkdtemp(prefix=prefix, dir=base))
    for sub in ("input", "work", "out", "logs"):
        (temp_root / sub).mkdir(parents=True, exist_ok=True)
    return temp_root


def atomic_publish(src: Path, dest: Path) -> None:
    """Atomically publish ``src`` to ``dest`` using a .partial temp and best-effort lock."""
    dest = dest.resolve()
    dest.parent.mkdir(parents=True, exist_ok=True)
    partial = dest.with_suffix(dest.suffix + ".partial")
    if src != partial:
        shutil.copy2(src, partial)
    lock = dest.with_suffix(dest.suffix + ".lock")
    for _ in range(5):
        try:
            fd = os.open(str(lock), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.close(fd)
            break
        except FileExistsError:
            time.sleep(0.2)
    os.replace(partial, dest)
    try:
        os.remove(lock)
    except FileNotFoundError:
        pass
