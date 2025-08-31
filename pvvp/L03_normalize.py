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
import subprocess

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

def run(session_id: str) -> int:
    base_dir = os.path.join("pvvp", "sessions", session_id)  # <-- change "program" to "pvvp"
    input_path = os.path.join(base_dir, "input_raw.txt")
    output_path = os.path.join(base_dir, "text_normalized.txt")
    debug_path = os.path.join(base_dir, "normalize_debug.txt")

    try:
        if not os.path.isdir(base_dir):
            os.makedirs(base_dir, exist_ok=True)

        if not os.path.isfile(input_path):
            raise FileNotFoundError(
                f"Missing input file: {input_path}. "
                "Provide pvvp/sessions/<car_id>/input_raw.txt"
            )

        raw = _read_text(input_path)
        normalized = normalize_level0(raw)

        # Idempotency check (optional safety): normalizing again should be identical.
        if normalize_level0(normalized) != normalized:
            # This should never happen; raise for visibility.
            raise AssertionError("Normalization is not idempotent. Please report this input.")

        _write_text(output_path, normalized)
        return 0

    except Exception as e:
        # Write debug info
        try:
            tb = traceback.format_exc()
            payload = [
                "L03.Normalize level-0 ERROR",
                f"Session: {session_id}",
                f"Input Path: {input_path}",
                f"Output Path: {output_path}",
                "",
                f"Error: {repr(e)}",
                "",
                "Traceback:",
                tb,
            ]
            os.makedirs(base_dir, exist_ok=True)
            _write_text(debug_path, "\n".join(payload))
        except Exception:
            # As a last resort, print to stderr
            print("Failed to write normalize_debug.txt", file=sys.stderr)
            print(traceback.format_exc(), file=sys.stderr)
        return 1

from typing import Sequence

def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="L03.Normalize — level-0 (NFKC, exotic spaces->space, strip BOM/zero-width/control "
                    "(keep \\n/\\t), collapse spaces/tabs per line, trim per line)"
    )
    parser.add_argument("--session", required=True, help="Car/session id (folder under pvvp/sessions)")
    parser.add_argument("--project-root", default=".", help="Project root directory")
    args = parser.parse_args(argv)

    # Change working directory to project root
    project_root = os.path.abspath(args.project_root)
    os.chdir(project_root)

    return run(args.session)

if __name__ == "__main__":
    sys.exit(main())
