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

def main():
    parser = argparse.ArgumentParser(description="L04.Chunker — size-based, newline-friendly")
    parser.add_argument("--session", required=True, help="Car ID / session folder name (e.g., MCAFHEV)")
    parser.add_argument("--project-root", required=True, help="Project root path")
    parser.add_argument("--target", type=int, default=1800, help="Target chunk length (default 1800)")
    parser.add_argument("--min", dest="min_len", type=int, default=800, help="Minimum chunk length (default 800)")
    parser.add_argument("--max", dest="max_len", type=int, default=2500, help="Maximum chunk length (default 2500)")
    args = parser.parse_args()

    # Paths
    session_dir = os.path.join(args.project_root, "sessions", args.session)
    input_path = os.path.join(session_dir, "text_normalized.txt")
    output_path = os.path.join(session_dir, "chunks.jsonl")
    debug_path = os.path.join(session_dir, "chunker_debug.txt")

    # Validate input existence
    if not os.path.isfile(input_path):
        write_debug(debug_path, f"Missing input file: {input_path}")
        sys.exit(2)

    # Validate length parameters
    min_len = args.min_len
    target_len = args.target
    max_len = args.max_len

    if min_len <= 0 or max_len <= 0 or target_len <= 0:
        write_debug(debug_path, "Invalid lengths: all of --min, --target, --max must be positive integers.")
        sys.exit(2)

    if not (min_len <= target_len <= max_len):
        # Strict but helpful: fail fast with a clear message.
        write_debug(
            debug_path,
            f"Invalid relation among lengths: require --min <= --target <= --max (got min={min_len}, target={target_len}, max={max_len})."
        )
        sys.exit(2)

    # Read input (UTF-8). text_normalized.txt is expected to already use \n line endings.
    try:
        with open(input_path, "r", encoding="utf-8") as f:
            text = f.read()
    except Exception as e:
        write_debug(debug_path, f"Failed to read input: {e}")
        sys.exit(2)

    # Chunking (single pass over text, O(n))
    chunks = chunk_text(text, min_len=min_len, target_len=target_len, max_len=max_len)

    # Idempotently write output (overwrite)
    try:
        with open(output_path, "w", encoding="utf-8", newline="\n") as out:
            for entry in chunks:
                # Deterministic key order
                line = json.dumps(
                    {"id": entry["id"], "start": entry["start"], "end": entry["end"], "text": entry["text"]},
                    ensure_ascii=False,
                    separators=(",", ":"),
                    sort_keys=False,
                )
                out.write(line + "\n")
    except Exception as e:
        write_debug(debug_path, f"Failed to write output: {e}")
        sys.exit(2)

    # Success: ensure no stale debug file claims an error (optional—don’t fail if we can’t remove).
    try:
        if os.path.isfile(debug_path):
            os.remove(debug_path)
    except Exception:
        pass

if __name__ == "__main__":
    main()
