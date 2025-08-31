# File: L08_validate_yn.py
# CLI:  python L08_validate_yn.py --session <car_id> --project-root <project_root>

import argparse
import csv
import json
import os
import re
import sys
from collections import Counter
from typing import Dict, List, Tuple, Optional

from pvvp.textnorm import norm_basic

MASTER_CSV = "pvvp_master.csv"
ALLOW_LIST = "LV_{args.session}PVVP.txt"
MERGE_RESULT = "merge_result.json"

FINAL_DECISIONS = "final_decisions.json"
MASTER_ALIGNED = "master_aligned.jsonl"
VALIDATE_REPORT = "validate_report.json"
DEBUG_LOG = "validate_debug.txt"

def die(session_dir: str, msg: str, exc: Optional[Exception] = None, code: int = 1):
    try:
        with open(os.path.join(session_dir, DEBUG_LOG), "w", encoding="utf-8") as f:
            f.write(msg.strip() + ("\n" + repr(exc) if exc else "") + "\n")
    finally:
        sys.stderr.write(msg + "\n")
        sys.exit(code)

def load_allow_list(path: str) -> List[str]:
    names = []
    with open(path, "r", encoding="utf-8-sig") as f:
        for line in f:
            name = line.strip()
            if name != "":
                names.append(name)
    return names

def load_merge_result(path: str) -> Tuple[List[str], Dict[str, str], Dict[str, str]]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    mentioned = data.get("mentioned_vars", []) or []
    evidence = data.get("evidence", {}) or {}
    reasons = data.get("evidence_reason", {}) or {}
    mentioned = [m.strip() for m in mentioned]
    evidence = { (k.strip()): (v if isinstance(v, str) else "") for k, v in evidence.items() }
    reasons = { (k.strip()): (v if isinstance(v, str) else "") for k, v in reasons.items() }
    return mentioned, evidence, reasons

def load_master_csv(path: str) -> List[dict]:
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        expected = {"Nr Code", "Variable Name", "Section TT"}
        missing = expected - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"pvvp_master.csv missing columns: {', '.join(sorted(missing))}")
        rows = []
        for row in reader:
            # Normalize fields we use; preserve exact Nr Code as-is (verbatim copy)
            row["Nr Code"] = (row.get("Nr Code") or "").strip()
            # TT rule: empty/blank Variable Name means TT
            row["Variable Name"] = (row.get("Variable Name") or "").strip()
            row["Section TT"] = (row.get("Section TT") or "").strip()
            rows.append(row)
    return rows

def main():
    ap = argparse.ArgumentParser(description="Expand merged positives into a full Y/N table aligned to master CSV.")
    ap.add_argument("--session", required=True, help="Car/session id (folder under sessions)")
    ap.add_argument("--project-root", required=True, help="Project root")
    args = ap.parse_args()

    session_dir = os.path.join(args.project_root, "sessions", args.session)
    try:
        os.makedirs(session_dir, exist_ok=True)
    except Exception as e:
        die(session_dir=args.project_root, msg=f"Failed to ensure session directory: {session_dir}", exc=e)

    master_csv_path = os.path.join(session_dir, MASTER_CSV)
    allow_list_path = os.path.join(session_dir, f"LV_{args.session}PVVP.txt")
    merge_result_path = os.path.join(session_dir, MERGE_RESULT)

    # Output paths
    final_decisions_path = os.path.join(session_dir, FINAL_DECISIONS)
    master_aligned_path = os.path.join(session_dir, MASTER_ALIGNED)
    validate_report_path = os.path.join(session_dir, VALIDATE_REPORT)

    # Validate inputs presence
    for p in (master_csv_path, allow_list_path, merge_result_path):
        if not os.path.isfile(p):
            die(session_dir, f"Missing required input: {os.path.basename(p)} at {p}")

    # Load inputs
    try:
        allow_raw = load_allow_list(allow_list_path)
        mentioned_vars, evidence_map, reason_map = load_merge_result(merge_result_path)
        mentioned_set = set(mentioned_vars)
        master_rows = load_master_csv(master_csv_path)
    except Exception as e:
        die(session_dir, f"Failed to load inputs: {e}", exc=e)

    name_to_nr = {
        norm_basic(r["Variable Name"]).lower(): r["Nr Code"]
        for r in master_rows
        if r["Variable Name"]
    }
    allow_nr_list: List[str] = []
    for item in allow_raw:
        if re.match(r"^NR\d+$", item, re.I):
            allow_nr_list.append(item.upper())
        else:
            nr = name_to_nr.get(norm_basic(item).lower())
            if nr:
                allow_nr_list.append(nr)
    allow_nr_set = set(allow_nr_list)

    # Diagnostics containers
    rows_total = len(master_rows)
    tt_rows = 0
    feature_rows = 0
    positives_after_merge = 0
    duplicate_variable_names: List[str] = []
    notes: List[str] = []

    # Unknowns in merge (not present in allow-list)
    unknown_in_merge = sorted([nr for nr in mentioned_set if nr not in allow_nr_set])
    unknown_in_merge_dropped = len(unknown_in_merge)

    if unknown_in_merge_dropped > 0:
        notes.append(f"merge_result contains {unknown_in_merge_dropped} NR codes not in allow-list; dropped from decisions.")
        notes.append(
            "Unknown (not in allow-list): " + "; ".join(unknown_in_merge[:50]) + (" ..." if len(unknown_in_merge) > 50 else "")
        )

    # Drift detection: non-TT master NR codes that are not in allow-list
    drift_missing = []

    # Detect duplicate non-TT names inside master
    non_tt_names = [r["Variable Name"] for r in master_rows if (r["Variable Name"].strip() != "")]
    name_counts = Counter(non_tt_names)
    duplicate_variable_names = sorted([n for n, c in name_counts.items() if c > 1])

    # Build master_aligned.jsonl in master order
    # Also collect final decisions for non-TT rows whose NR codes are in allow-list
    final_decisions: Dict[str, str] = {}
    aligned_lines: List[str] = []

    for row in master_rows:
        nr_code = row["Nr Code"]
        var_name = row["Variable Name"]
        is_tt = "Y" if var_name == "" else "N"

        if is_tt == "Y":
            tt_rows += 1
            mentioned_YN = "N"
            maybe_flag = "N"
            ev = ""
        else:
            feature_rows += 1
            if nr_code not in allow_nr_set:
                drift_missing.append(var_name)

            if nr_code in mentioned_set:
                reason = reason_map.get(nr_code, "")
                maybe_flag = "Y" if reason.startswith("fuzzy") else "N"
                mentioned_YN = "Y" if reason.split("_")[0] in ("exact", "normalized") else "N"
                if mentioned_YN == "Y":
                    positives_after_merge += 1
                ev = evidence_map.get(nr_code, "") or ""
            else:
                mentioned_YN = "N"
                maybe_flag = "N"
                ev = ""

            if nr_code in allow_nr_set:
                if mentioned_YN == "Y" and maybe_flag == "Y":
                    final_decisions[nr_code] = "M"
                elif mentioned_YN == "Y":
                    final_decisions[nr_code] = "Y"
                else:
                    final_decisions[nr_code] = "N"

        aligned_obj = {
            "nr_code": nr_code,
            "variable_name_lv": var_name,
            "is_tt": is_tt,
            "mentioned_YN": mentioned_YN,
            "maybe_flag": maybe_flag,
            "evidence": ev,
        }
        aligned_lines.append(json.dumps(aligned_obj, ensure_ascii=False))

    # Suspicion note if everything is N but there were merge mentions or feature rows>0
    if positives_after_merge == 0 and feature_rows > 0:
        if len(mentioned_vars) == 0:
            notes.append("No positives after merge and merge_result.mentioned_vars is empty.")
        else:
            notes.append("No positives after merge despite non-empty merge_result.mentioned_vars; check allow-list and exact name matching.")

    # Write outputs deterministically (overwrite)
    try:
        # master_aligned.jsonl
        with open(master_aligned_path, "w", encoding="utf-8") as f:
            for line in aligned_lines:
                f.write(line + "\n")

        # final_decisions.json (keys limited to names that appear in BOTH master (non-TT) and allow-list)
        # Ensure keys are emitted in the allow-list order for human diff stability (even though JSON objects are unordered)
        ordered_final = {nr: final_decisions.get(nr, "N") for nr in allow_nr_list if nr in final_decisions}
        with open(final_decisions_path, "w", encoding="utf-8") as f:
            json.dump(ordered_final, f, ensure_ascii=False, indent=2)

        # validate_report.json
        validate_report = {
            "rows_total": rows_total,
            "tt_rows": tt_rows,
            "feature_rows": feature_rows,
            "positives_after_merge": positives_after_merge,
            "unknown_in_merge_dropped": unknown_in_merge_dropped,
            "duplicate_variable_names": duplicate_variable_names,
            # Added explicit drift field per acceptance test wording
            "drift": sorted(drift_missing),
            "notes": notes,
        }
        with open(validate_report_path, "w", encoding="utf-8") as f:
            json.dump(validate_report, f, ensure_ascii=False, indent=2)

        # Write drift_report.json if drift detected
        if drift_missing:
            drift_report_path = os.path.join(session_dir, "drift_report.json")
            with open(drift_report_path, "w", encoding="utf-8") as f:
                json.dump({"drift": sorted(drift_missing)}, f, ensure_ascii=False, indent=2)

    except Exception as e:
        die(session_dir, f"Failed to write outputs: {e}", exc=e)

if __name__ == "__main__":
    main()
