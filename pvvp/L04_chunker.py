#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
L04.Chunker — size-based, newline-friendly

CLI:
  python L04_chunker.py --session <car_id> --project-root <project_root> --target 1800 --min 800 --max 2500

Defaults:
  --target 1800 --min 800 --max 2500

Behavior:
  - Reads <project_root>/sessions/<car_id>/text_normalized.txt (UTF-8)
  - Writes <project_root>/sessions/<car_id>/chunks.jsonl (UTF-8, \n endings)
  - Each line is {"id":<int>, "start":<int>, "end":<int>, "text":<str>}
  - start/end are 0-based, inclusive indices into the original normalized text
  - Non-overlapping, gapless, deterministic, idempotent
  - Prefers to split at a newline nearest the target within [min, max]; otherwise hard-cut at max
  - Final tail may be shorter than min (but ≥1 char)
  - On missing input, writes chunker_debug.txt and exits non-zero
"""

import argparse
import json
import os
import sys
from pathlib import Path
import shutil
from pvvp.temp_utils import make_temp_root, atomic_publish

def write_debug(debug_path: str, message: str) -> None:
    try:
        with open(debug_path, "w", encoding="utf-8", newline="\n") as f:
            f.write(message.strip() + "\n")
    except Exception:
        # As a last resort, print to stderr; but still exit non-zero.
        sys.stderr.write((message.strip() + "\n"))

def choose_split_pos(text: str, start: int, min_len: int, target_len: int, max_len: int) -> int:
    """
    Choose the end index (inclusive) for the current chunk starting at `start`.

    Rules:
      - Prefer a newline split with chunk length in [min_len, max_len].
      - Among candidate newlines in that window, pick the one whose resulting length
        is closest to target_len (ties resolved by the later newline to keep chunks larger).
      - If no newline within window, hard-cut at start + max_len - 1 (or end of text).

    Returns the inclusive end index for the chunk.
    """
    n = len(text)
    # If remaining is less than or equal to max_len, return the tail.
    remaining = n - start
    if remaining <= max_len:
        # Entire remainder is one chunk (even if < min_len, it's the final tail).
        return n - 1

    # Define search window for newline preference.
    win_start = start + min_len
    win_end = min(start + max_len, n)  # exclusive upper bound for scanning indices
    if win_start < n:
        # Collect newline positions within [win_start, win_end)
        # We will search efficiently by scanning segments.
        best_pos = None
        best_delta = None

        # We'll scan by finding '\n' occurrences using str.find in a loop.
        search_from = win_start
        while True:
            nl = text.find("\n", search_from, win_end)
            if nl == -1:
                break
            # resulting length if we include newline in this chunk
            length = (nl - start) + 1  # inclusive end at nl
            delta = abs(length - target_len)
            # Prefer closer to target; if tie, prefer the later newline (bigger chunk)
            if (best_delta is None) or (delta < best_delta) or (delta == best_delta and best_pos is not None and nl > best_pos):
                best_pos = nl
                best_delta = delta
            search_from = nl + 1

        if best_pos is not None:
            return best_pos  # inclusive end at newline

    # No newline candidate; hard-cut at max.
    return (start + max_len - 1)

def chunk_text(text: str, min_len: int, target_len: int, max_len: int):
    """
    Yield dicts: {"id": int, "start": int, "end": int, "text": str}
    Cover the full text contiguously, non-overlapping.
    """
    chunks = []
    start = 0
    cid = 1
    n = len(text)

    while start < n:
        end = choose_split_pos(text, start, min_len, target_len, max_len)
        # Safety: ensure indices are sensible
        if end < start:
            end = start  # at least 1 char

        entry = {
            "id": cid,
            "start": start,
            "end": end,
            "text": text[start:end+1],
        }
        chunks.append(entry)
        cid += 1
        start = end + 1

    return chunks



def run(session: str, project_root: Path, min_len: int, target_len: int, max_len: int, workdir: Path | None, keep_workdir: bool, readonly: bool) -> int:
    session_dir = project_root / "sessions" / session
    input_src = session_dir / "text_normalized.txt"
    output_dest = session_dir / "chunks.jsonl"
    debug_dest = session_dir / "chunker_debug.txt"

    temp_root = Path(workdir).resolve() if workdir else make_temp_root()
    log_path = temp_root / "logs" / "L04_chunker.log"
    log_path.write_text(f"Temp workdir: {temp_root}\n", encoding="utf-8")
    print(f"Temp workdir: {temp_root}")

    try:
        tmp_input = temp_root / "input" / "text_normalized.txt"
        shutil.copy2(input_src, tmp_input)
        if readonly:
            try:
                os.chmod(tmp_input, 0o444)
            except Exception:
                pass

        if min_len <= 0 or max_len <= 0 or target_len <= 0:
            raise ValueError("Invalid lengths: --min, --target, --max must be >0")
        if not (min_len <= target_len <= max_len):
            raise ValueError(f"Invalid relation among lengths: require --min <= --target <= --max (got min={min_len}, target={target_len}, max={max_len}).")

        with open(tmp_input, "r", encoding="utf-8") as f:
            text = f.read()

        chunks = chunk_text(text, min_len=min_len, target_len=target_len, max_len=max_len)
        tmp_out = temp_root / "out" / "chunks.jsonl.partial"
        with open(tmp_out, "w", encoding="utf-8", newline="\n") as out:
            for entry in chunks:
                line = json.dumps({"id": entry["id"], "start": entry["start"], "end": entry["end"], "text": entry["text"]}, ensure_ascii=False, separators=(",", ":"), sort_keys=False)
                out.write(line + "\n")
        atomic_publish(tmp_out, output_dest)
        if debug_dest.exists():
            try:
                debug_dest.unlink()
            except Exception:
                pass
        if not keep_workdir:
            shutil.rmtree(temp_root, ignore_errors=True)
        return 0
    except Exception as e:
        tmp_debug = temp_root / "out" / "chunker_debug.txt.partial"
        write_debug(str(tmp_debug), f"{e}")
        try:
            atomic_publish(tmp_debug, debug_dest)
        except Exception:
            pass
        print(f"Workdir preserved at: {temp_root}", file=sys.stderr)
        return 2


def main() -> None:
    parser = argparse.ArgumentParser(description="L04.Chunker — size-based, newline-friendly")
    parser.add_argument("--session", required=True, help="Car ID / session folder name (e.g., MCAFHEV)")
    parser.add_argument("--project-root", required=True, help="Project root path")
    parser.add_argument("--target", type=int, default=1800, help="Target chunk length (default 1800)")
    parser.add_argument("--min", dest="min_len", type=int, default=800, help="Minimum chunk length (default 800)")
    parser.add_argument("--max", dest="max_len", type=int, default=2500, help="Maximum chunk length (default 2500)")
    parser.add_argument("--workdir", help="Use existing workdir instead of creating temp")
    parser.add_argument("--keep-workdir", action="store_true", help="Preserve temp workdir for debugging")
    parser.add_argument("--readonly", action="store_true", help="Disallow writes outside temp until publish")
    args = parser.parse_args()

    project_root = Path(args.project_root).resolve()
    workdir = Path(args.workdir).resolve() if args.workdir else None
    rc = run(args.session, project_root, args.min_len, args.target, args.max_len, workdir, args.keep_workdir, args.readonly)
    sys.exit(rc)


if __name__ == "__main__":
    main()
