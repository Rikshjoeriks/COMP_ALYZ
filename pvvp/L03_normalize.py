# program/L03_normalize.py
"""
L03.Normalize — level-0
- Input : pvvp/sessions/<car_id>/input_raw.txt
- Output: pvvp/sessions/<car_id>/text_normalized.txt

Level-0 normalization steps (in strict order):
  1) Unicode NFKC normalization
  2) Normalize newlines to '\n' (preserve them)
  3) Strip BOM/zero-width and all control chars except '\n' and '\t'
  4) Replace exotic/Unicode space separators with a normal space ' '
  5) Per-line:
       - collapse runs of spaces/tabs to a single space (do NOT collapse newlines)
       - trim leading/trailing spaces/tabs
  6) Remove "obvious junk" symbols: replacement char U+FFFD and soft hyphen U+00AD
No translation, casing changes or rephrasing. Idempotent by construction.
"""

from __future__ import annotations
import argparse
import os
import sys
import traceback
import unicodedata
import re
from typing import Iterable
from pathlib import Path
import shutil
from temp_utils import make_temp_root, atomic_publish

# --- regex precompilations ---
# collapse runs of spaces/tabs (NOT newlines)
_RE_SPACES_TABS_RUN = re.compile(r"[ \t]+")
# strip leading/trailing spaces/tabs per physical line
_RE_TRIM_LINE = re.compile(r"^[ \t]+|[ \t]+$", flags=re.MULTILINE)

# Characters to remove as "obvious junk"
_OBVIOUS_JUNK = {
    "\uFFFD",  # Unicode Replacement Character
    "\u00AD",  # Soft hyphen (SHY)
    "\ufeff",  # BOM (if it appears anywhere)
}

def _normalize_newlines(s: str) -> str:
    # Convert CRLF / CR to LF, preserve LF
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    return s

def _is_control_keep_ok(ch: str) -> bool:
    """Return True if ch is a control/format char we should KEEP."""
    # We always keep '\n' and '\t' explicitly.
    return ch in ("\n", "\t")

def _filter_controls_and_formats(s: str) -> str:
    """Remove:
       - All 'Cc' (control) chars except '\n' and '\t'
       - All 'Cf' (format) chars (covers zero-width joiners, LRM/RLM, etc.)
       - BOM if present
    """
    out_chars = []
    for ch in s:
        if ch in _OBVIOUS_JUNK:
            # handled later too, but skip now to be safe
            continue
        cat = unicodedata.category(ch)
        if cat == "Cc":
            if _is_control_keep_ok(ch):
                out_chars.append(ch)
            # else: drop control
        elif cat == "Cf":
            # drop all format (includes zero-width)
            continue
        else:
            out_chars.append(ch)
    return "".join(out_chars)

def _replace_exotic_spaces(s: str) -> str:
    """Replace all Unicode 'Zs' (space separators) and NBSP variants with a plain space."""
    out_chars = []
    for ch in s:
        if ch == "\xa0":  # NBSP fast-path
            out_chars.append(" ")
            continue
        cat = unicodedata.category(ch)
        if cat == "Zs":
            out_chars.append(" ")
        else:
            out_chars.append(ch)
    return "".join(out_chars)

def _remove_obvious_junk(s: str) -> str:
    if not _OBVIOUS_JUNK.intersection(s):
        return s
    return "".join(ch for ch in s if ch not in _OBVIOUS_JUNK)

def normalize_level0(text: str) -> str:
    # 1) NFKC normalization
    text = unicodedata.normalize("NFKC", text)

    # 2) Normalize newlines (preserve them)
    text = _normalize_newlines(text)

    # 3) Remove BOM/zero-width/control (keep \n and \t)
    text = _filter_controls_and_formats(text)

    # 4) Replace exotic spaces with normal space
    text = _replace_exotic_spaces(text)

    # 5) Per-line collapse spaces/tabs -> single space; trim per line
    #    Do not collapse or remove newlines. Work line-by-line but keepends.
    lines = text.splitlines(keepends=True)
    normalized_lines = []
    for line in lines:
        if line.endswith("\n"):
            body, nl = line[:-1], "\n"
        else:
            body, nl = line, ""

        # collapse runs of spaces/tabs
        body = _RE_SPACES_TABS_RUN.sub(" ", body)
        # trim leading/trailing spaces/tabs
        # (use simple strip since we only target spaces/tabs, not other chars)
        body = body.strip(" \t")

        normalized_lines.append(body + nl)

    text = "".join(normalized_lines)

    # 6) Remove obvious junk symbols
    text = _remove_obvious_junk(text)

    return text

def _read_text(path: str) -> str:
    # Try UTF-8 first (expected), but fall back to 'utf-8-sig' to swallow BOM if present.
    # If file contains CP-1252-like, user should convert upstream; here we target UTF-8 pipeline.
    with open(path, "r", encoding="utf-8", errors="strict") as f:
        return f.read()

def _write_text(path: str, content: str) -> None:
    # Always write UTF-8 without BOM
    with open(path, "w", encoding="utf-8", newline="") as f:
        f.write(content)



def run(session: str, project_root: Path, workdir: Path | None, keep_workdir: bool, readonly: bool) -> int:
    session_dir = project_root / "sessions" / session
    input_src = session_dir / "input_raw.txt"
    output_dest = session_dir / "text_normalized.txt"
    debug_dest = session_dir / "normalize_debug.txt"

    temp_root = Path(workdir).resolve() if workdir else make_temp_root()
    log_path = temp_root / "logs" / "L03_normalize.log"
    log_path.write_text(f"Temp workdir: {temp_root}\n", encoding="utf-8")
    print(f"Temp workdir: {temp_root}")

    try:
        tmp_input = temp_root / "input" / "input_raw.txt"
        shutil.copy2(input_src, tmp_input)
        if readonly:
            try:
                os.chmod(tmp_input, 0o444)
            except Exception:
                pass

        raw = _read_text(str(tmp_input))
        normalized = normalize_level0(raw)
        if normalize_level0(normalized) != normalized:
            raise AssertionError("Normalization is not idempotent. Please report this input.")

        tmp_out = temp_root / "out" / "text_normalized.txt.partial"
        _write_text(str(tmp_out), normalized)
        atomic_publish(tmp_out, output_dest)
        if not keep_workdir:
            shutil.rmtree(temp_root, ignore_errors=True)
        return 0
    except Exception as e:
        tb = traceback.format_exc()
        payload = [
            "L03.Normalize level-0 ERROR",
            f"Session: {session}",
            f"Error: {repr(e)}",
            "",
            "Traceback:",
            tb,
        ]
        tmp_debug = temp_root / "out" / "normalize_debug.txt.partial"
        _write_text(str(tmp_debug), "\n".join(payload))
        try:
            atomic_publish(tmp_debug, debug_dest)
        except Exception:
            pass
        print(f"Workdir preserved at: {temp_root}", file=sys.stderr)
        return 1

from typing import Sequence


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "L03.Normalize — level-0 (NFKC, exotic spaces->space, strip BOM/zero-width/control "
            "(keep \n/\t), collapse spaces/tabs per line, trim per line)"
        )
    )
    parser.add_argument("--session", required=True, help="Car/session id (folder under pvvp/sessions)")
    parser.add_argument("--project-root", required=True, help="Project root directory")
    parser.add_argument("--workdir", help="Use existing workdir instead of creating temp")
    parser.add_argument("--keep-workdir", action="store_true", help="Preserve temp workdir for debugging")
    parser.add_argument("--readonly", action="store_true", help="Disallow writes outside temp until publish")
    args = parser.parse_args(argv)

    project_root = Path(args.project_root).resolve()
    workdir = Path(args.workdir).resolve() if args.workdir else None
    return run(args.session, project_root, workdir, args.keep_workdir, args.readonly)


if __name__ == "__main__":
    sys.exit(main())
