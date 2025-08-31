#!/usr/bin/env python3
"""
L10_export_csv.py — Milestone 8 (L10.Export.CSV + L09.Positives + L13.Summary finalize)

CLI:
    python L10_export_csv.py --session <car_id> --project-root <project_root>

Behavior (strict, deterministic, idempotent):
- Reads authoritative per-row data from sessions/<car_id>/master_aligned.jsonl
- Emits CSV: exports/<car_id>/detections_<car_id>.csv (UTF-8, LF)
- Emits positives JSONL: sessions/<car_id>/positives_explanations.jsonl
- Appends summary line to: sessions/<car_id>/summary.txt
- Writes non-blocking warnings & any error reasons to: sessions/<car_id>/export_debug.txt
- No model calls. No mutation of session sources.

Accepted Inputs (must exist):
- sessions/<car_id>/pvvp_master.csv               (existence check only)
- sessions/<car_id>/master_aligned.jsonl          (authoritative for CSV rows)
- sessions/<car_id>/merge_result.json             (for positives & counts)
- sessions/<car_id>/final_decisions.json          (sanity reference; existence check only)
- sessions/<car_id>/pvvp_list_lv.txt              (sanity reference; existence check only)

Outputs:
- exports/<car_id>/detections_<car_id>.csv
  Columns (exact, comma-separated, UTF-8):
    nr_code,variable_name_lv,is_tt,mentioned_YN,evidence
  Order exactly as master_aligned.jsonl (which mirrors pvvp_master.csv)

 - sessions/<car_id>/positives_explanations.jsonl
  One line per positive from merge_result.json (first-win order):
    {"nr": "<NR code>", "evidence": "<literal LV snippet or empty>", "reason": "<evidence_reason>"}

- sessions/<car_id>/summary.txt (append-only)
  Appends one line like:
    EXPORT: detections_<car_id>.csv (rows N, positives P)

Sanity Checks (non-blocking warnings -> export_debug.txt):
- Count of is_tt=="Y" rows
- Count of mentioned_YN=="Y" rows
- If positives_count (from merge_result) != Y_count (from CSV) -> warning

Encoding: UTF-8; line endings: \n.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from typing import Dict, List, Any

# -----------------------------
# Helpers
# -----------------------------

def pjoin(*parts: str) -> str:
    return os.path.join(*parts)


def read_json(path: str) -> Any:
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def write_text(path: str, text: str) -> None:
    with open(path, 'w', encoding='utf-8', newline='') as f:
        f.write(text)


def append_text(path: str, text: str) -> None:
    # Ensure parent directory exists
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'a', encoding='utf-8', newline='') as f:
        f.write(text)


def file_exists(path: str) -> bool:
    return os.path.isfile(path)


def dir_exists(path: str) -> bool:
    return os.path.isdir(path)


# -----------------------------
# Core logic
# -----------------------------

def load_master_aligned(jsonl_path: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with open(jsonl_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip('\n')
            if not line:
                continue
            try:
                obj = json.loads(line)
                rows.append(obj)
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSONL line in {jsonl_path}: {e}")
    return rows


def write_csv_from_master(rows: List[Dict[str, Any]], out_csv_path: str) -> Dict[str, int]:
    # Ensure exports dir exists
    os.makedirs(os.path.dirname(out_csv_path), exist_ok=True)

    # Write exact columns & order
    fieldnames = ["nr_code", "variable_name_lv", "is_tt", "mentioned_YN", "maybe_flag", "evidence"]

    with open(out_csv_path, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        for r in rows:
            # Ensure missing fields default to empty strings (strict columns only)
            safe_row = {k: (str(r.get(k, "")) if r.get(k, "") is not None else "") for k in fieldnames}
            writer.writerow(safe_row)

    # Compute counts for sanity
    num_tt = sum(1 for r in rows if str(r.get("is_tt", "")).upper() == "Y")
    num_y = sum(1 for r in rows if str(r.get("mentioned_YN", "")).upper() == "Y")
    num_maybe = sum(1 for r in rows if str(r.get("maybe_flag", "")).upper() == "Y")

    return {"rows_total": len(rows), "num_tt": num_tt, "num_y": num_y, "num_maybe": num_maybe}


def write_positives_jsonl(merge_result: Dict[str, Any], out_jsonl_path: str) -> int:
    if "mentioned_vars" in merge_result:
        items = [
            (nr, {
                "evidence": merge_result.get("evidence", {}).get(nr, ""),
                "reason": merge_result.get("evidence_reason", {}).get(nr, ""),
            })
            for nr in merge_result.get("mentioned_vars", [])
        ]
    else:
        items = [
            (nr, {
                "evidence": (v.get("evidence", "") if isinstance(v, dict) else ""),
                "reason": (v.get("evidence_reason", "") if isinstance(v, dict) else ""),
            })
            for nr, v in merge_result.items()
        ]

    with open(out_jsonl_path, 'w', encoding='utf-8', newline='') as f:
        for nr, meta in items:
            line = json.dumps({"nr": nr, "evidence": meta["evidence"], "reason": meta["reason"]}, ensure_ascii=False)
            f.write(line + "\n")

    return len(items)


def append_summary_line(summary_path: str, car_id: str, rows_total: int, positives_count: int) -> None:
    line = f"EXPORT: detections_{car_id}.csv (rows {rows_total}, positives {positives_count})\n"
    append_text(summary_path, line)


def warn(debug_path: str, message: str) -> None:
    append_text(debug_path, message.rstrip('\n') + "\n")


# -----------------------------
# Main
# -----------------------------

def main(argv: List[str]) -> int:
    parser = argparse.ArgumentParser(description="L10 Export CSV + Positives + Summary")
    parser.add_argument("--session", required=True, help="Car/session id (e.g., MCAFHEV)")
    parser.add_argument("--project-root", required=True, help="Project root path")
    args = parser.parse_args(argv)

    car_id = args.session
    root = os.path.abspath(args.project_root)

    # Layout
    sessions_dir = pjoin(root, "sessions", car_id)
    exports_dir = pjoin(root, "exports", car_id)

    # Inputs
    pvvp_master_csv = pjoin(sessions_dir, "pvvp_master.csv")
    master_aligned_jsonl = pjoin(sessions_dir, "master_aligned.jsonl")
    merge_result_json = pjoin(sessions_dir, "merge_result.json")
    final_decisions_json = pjoin(sessions_dir, "final_decisions.json")
    pvvp_list_lv_txt = pjoin(sessions_dir, f"LV_{car_id}PVVP.txt")  # <-- changed here

    # Outputs
    detections_csv = pjoin(exports_dir, f"detections_{car_id}.csv")
    positives_jsonl = pjoin(sessions_dir, "positives_explanations.jsonl")
    summary_txt = pjoin(sessions_dir, "summary.txt")
    debug_txt = pjoin(sessions_dir, "export_debug.txt")

    # Ensure sessions dir exists
    if not dir_exists(sessions_dir):
        warn(debug_txt, f"ERROR: Missing sessions directory: {sessions_dir}")
        return 1

    # Required inputs existence check (write a SHORT reason and exit non-zero if any missing)
    missing = []
    for required in [pvvp_master_csv, master_aligned_jsonl, merge_result_json, final_decisions_json, pvvp_list_lv_txt]:
        if not file_exists(required):
            missing.append(required)

    if missing:
        for m in missing:
            warn(debug_txt, f"ERROR: Missing required input: {m}")
        return 1

    # Load authoritative master_aligned rows
    try:
        rows = load_master_aligned(master_aligned_jsonl)
    except Exception as e:
        warn(debug_txt, f"ERROR: Failed to read master_aligned.jsonl: {e}")
        return 1

    # Write CSV exactly as specified
    try:
        counts = write_csv_from_master(rows, detections_csv)
    except Exception as e:
        warn(debug_txt, f"ERROR: Failed to write CSV: {e}")
        return 1

    # Load merge_result for positives
    try:
        merge_result = read_json(merge_result_json)
    except Exception as e:
        warn(debug_txt, f"ERROR: Failed to read merge_result.json: {e}")
        return 1

    # Write positives JSONL (overwrite)
    try:
        positives_count = write_positives_jsonl(merge_result, positives_jsonl)
    except Exception as e:
        warn(debug_txt, f"ERROR: Failed to write positives_explanations.jsonl: {e}")
        return 1

    # Append summary line
    try:
        append_summary_line(summary_txt, car_id, counts["rows_total"], positives_count)
    except Exception as e:
        warn(debug_txt, f"ERROR: Failed to append summary: {e}")
        return 1

    # Sanity warnings (non-blocking)
    try:
        num_tt = counts["num_tt"]
        num_y = counts["num_y"]
        num_maybe = counts.get("num_maybe", 0)
        csv_pos = num_y + num_maybe
        if positives_count != csv_pos:
            warn(debug_txt, (
                "WARNING: Positives count from merge_result ({} ) != Y+Maybe count in CSV ({}). "
                "Investigate upstream alignment/merging."
            ).format(positives_count, csv_pos))
        warn(
            debug_txt,
            f"INFO: Rows total: {counts['rows_total']}, is_tt==Y: {num_tt}, mentioned_YN==Y: {num_y}, maybe_flag==Y: {num_maybe}, positives: {positives_count}",
        )
    except Exception as e:
        # Never fail the export because a warning couldn't be written
        warn(debug_txt, f"WARNING: Failed to write sanity stats: {e}")

    return 0


if __name__ == "__main__":
    try:
        exit_code = main(sys.argv[1:])
    except Exception as e:
        # Last-resort error capture; try to write somewhere predictable if possible
        # (Without args we cannot know session path; just print to stderr.)
        sys.stderr.write(f"FATAL: {e}\n")
        exit_code = 1
    sys.exit(exit_code)
