# File: L07_merge.py
# Usage:
#   python L07_merge.py --session <car_id> --project-root <project_root>
#
# Behavior:
# - Reads allowed chunk ids from budget_report.json
# - Reads chunks.jsonl to map chunk_id -> text and preserve processing order
# - For each allowed chunk, loads mapper_chunk_<id>.json
# - Filters mentioned_vars to the allow-list (pvvp_list_lv.txt; exact match after trim)
# - Evidence guard: evidence must be a case-insensitive literal substring of the chunk text
# - First-win across chunks: first evidence kept, later duplicates counted & ignored
# - Writes merge_result.json, merge_report.json, evidence_failures.jsonl
# - On any blocking error, writes merge_debug.txt and exits non-zero
#
# Invariants:
# - Deterministic & idempotent (re-runs overwrite outputs identically)
#
# Out of scope: Y/N expansion, numbering/TT, CSV, any GPT/model calls.

import argparse
import json
import os
import sys
from typing import Dict, List, Set, Tuple

DEBUG_FILE = "merge_debug.txt"
MERGE_RESULT = "merge_result.json"
MERGE_REPORT = "merge_report.json"
EVIDENCE_FAIL_JSONL = "evidence_failures.jsonl"

def write_debug(session_dir: str, msg: str) -> None:
    try:
        with open(os.path.join(session_dir, DEBUG_FILE), "w", encoding="utf-8") as f:
            f.write(msg.strip() + "\n")
    except Exception:
        # Last-ditch: print to stderr if even debug write fails
        sys.stderr.write((msg.strip() + "\n"))

def load_json(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def load_jsonl(path: str) -> List[dict]:
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows

def normalize_line(s: str) -> str:
    # Only trimming; no case-folding for allow-list (exact match after trim)
    return s.strip()

def read_allow_list(path: str) -> Set[str]:
    allow: Set[str] = set()
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = normalize_line(line)
            if line:
                allow.add(line)
    return allow

def parse_allowed_chunk_ids(budget: dict) -> Set[int]:
    """
    Accept a few likely shapes, e.g.:
      {"chunks":[{"chunk_id":1,"allowed":true}, ...]}
      {"allowed_chunks":[1,2,3], ...}
      [{"chunk_id":1,"allowed":true}, ...]
    Returns a set of allowed chunk ids.
    """
    allowed: Set[int] = set()

    # Case A: dict with list under "chunks"
    if isinstance(budget, dict) and "chunks" in budget and isinstance(budget["chunks"], list):
        for item in budget["chunks"]:
            try:
                if item.get("allowed") and "chunk_id" in item:
                    allowed.add(int(item["chunk_id"]))
            except Exception:
                continue

    # Case B: dict with explicit allowed list
    if isinstance(budget, dict) and "allowed_chunks" in budget and isinstance(budget["allowed_chunks"], list):
        for cid in budget["allowed_chunks"]:
            try:
                allowed.add(int(cid))
            except Exception:
                continue

    # Case C: the whole thing is a list of per-chunk dicts
    if isinstance(budget, list):
        for item in budget:
            if isinstance(item, dict):
                try:
                    if item.get("allowed") and "chunk_id" in item:
                        allowed.add(int(item["chunk_id"]))
                except Exception:
                    continue

    return allowed

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--session", required=True, help="Session / car_id, e.g., MCAFHEV")
    parser.add_argument("--project-root", required=True, help="Project root directory")
    args = parser.parse_args()

    session = args.session
    project_root = os.path.abspath(args.project_root)

    # Session directory convention
    session_dir = os.path.join(project_root, "sessions", session)

    try:
        # Paths (inputs)
        chunks_path = os.path.join(session_dir, "chunks.jsonl")
        budget_path = os.path.join(session_dir, "budget_report.json")
        allow_list_path = os.path.join(session_dir, f"LV_{session}PVVP.txt")

        # Validate required inputs
        for p in [chunks_path, budget_path, allow_list_path]:
            if not os.path.isfile(p):
                write_debug(session_dir, f"Missing required input: {p}")
                sys.exit(1)

        # Load inputs
        chunks_rows = load_jsonl(chunks_path)
        budget_data = load_json(budget_path)
        allow_set = read_allow_list(allow_list_path)

        # Build chunk_id -> text mapping & ordered list of ids as they appear in chunks.jsonl
        chunk_text_by_id: Dict[int, str] = {}
        ordered_chunk_ids: List[int] = []
        for r in chunks_rows:
            if "id" not in r or "text" not in r:
                write_debug(session_dir, "Invalid record in chunks.jsonl: missing 'id' or 'text'.")
                sys.exit(1)
            try:
                cid = int(r["id"])
            except Exception:
                write_debug(session_dir, f"Invalid chunk id in chunks.jsonl: {r.get('id')}")
                sys.exit(1)
            chunk_text_by_id[cid] = r["text"]
            ordered_chunk_ids.append(cid)

        allowed_ids = parse_allowed_chunk_ids(budget_data)
        if not allowed_ids:
            # Not strictly an error, but nothing to do — still produce empty outputs deterministically
            pass

        # Process chunks in the order they appear in chunks.jsonl, filtered to allowed
        process_ids: List[int] = [cid for cid in ordered_chunk_ids if cid in allowed_ids]

        # Outputs to produce / collect
        merged_vars: List[str] = []                      # first-seen order
        evidence_map: Dict[str, str] = {}               # var -> kept evidence
        processed_chunk_ids: List[int] = []
        evidence_failures: List[dict] = []
        total_mentions_in_chunks = 0
        duplicates_dropped = 0

        for cid in process_ids:
            processed_chunk_ids.append(cid)

            # Load per-chunk mapper
            mapper_path = os.path.join(session_dir, f"mapper_chunk_{cid}.json")
            if not os.path.isfile(mapper_path):
                # If a mapper is missing for an allowed chunk, treat as empty (non-blocking)
                continue

            try:
                mapper = load_json(mapper_path)
            except Exception as e:
                write_debug(session_dir, f"Failed to read {mapper_path}: {e}")
                sys.exit(1)

            # Validate minimal schema
            if "mentioned_vars" not in mapper or "evidence" not in mapper:
                write_debug(session_dir, f"Invalid mapper schema in {mapper_path}")
                sys.exit(1)

            mentioned_vars = mapper.get("mentioned_vars") or []
            evidence = mapper.get("evidence") or {}
            chunk_text = chunk_text_by_id.get(cid, "")

            # Iterate mentions
            for raw_var in mentioned_vars:
                var = normalize_line(raw_var)
                # Closed world: only allow-list exact matches after trim
                if var not in allow_set:
                    continue

                total_mentions_in_chunks += 1

                ev_raw = evidence.get(raw_var)
                if ev_raw is None:
                    # Treat missing evidence as failure
                    evidence_failures.append({
                        "chunk_id": cid,
                        "var": var,
                        "evidence": "",
                        "reason": "evidence_missing"
                    })
                    continue

                ev = ev_raw.strip()

                # Evidence guard: case-insensitive literal substring of chunk text
                if ev.lower() not in chunk_text.lower():
                    evidence_failures.append({
                        "chunk_id": cid,
                        "var": var,
                        "evidence": ev,
                        "reason": "substring_not_found"
                    })
                    continue

                # First-win: keep first occurrence only
                if var in evidence_map:
                    duplicates_dropped += 1
                    continue

                merged_vars.append(var)
                evidence_map[var] = ev

        # Prepare outputs
        merge_result = {
            "mentioned_vars": merged_vars,
            "evidence": evidence_map,
        }

        merge_report = {
            "processed_chunk_ids": processed_chunk_ids,
            "total_mentions_in_chunks": total_mentions_in_chunks,
            "duplicates_dropped": duplicates_dropped,
            "evidence_failures": evidence_failures,
            "final_mentioned_count": len(merged_vars),
        }

        # Write outputs (idempotent: overwrite)
        with open(os.path.join(session_dir, MERGE_RESULT), "w", encoding="utf-8") as f:
            json.dump(merge_result, f, ensure_ascii=False, indent=2)

        with open(os.path.join(session_dir, MERGE_REPORT), "w", encoding="utf-8") as f:
            json.dump(merge_report, f, ensure_ascii=False, indent=2)

        # JSONL failures
        fail_path = os.path.join(session_dir, EVIDENCE_FAIL_JSONL)
        with open(fail_path, "w", encoding="utf-8") as f:
            for rec in evidence_failures:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")

        # Success: clear prior debug if any (keep things tidy & deterministic)
        dbg_path = os.path.join(session_dir, DEBUG_FILE)
        if os.path.exists(dbg_path):
            try:
                os.remove(dbg_path)
            except Exception:
                # Non-fatal
                pass

    except SystemExit:
        raise
    except Exception as e:
        write_debug(session_dir if os.path.isdir(session_dir) else ".", f"Unhandled error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
