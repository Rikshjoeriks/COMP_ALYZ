#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
L06 — Unified mapper review (no LLM). Prepares review candidates from strict mapper
and merges approved selections back into a merged mapper file.

Inputs (session folder):
  - mapper_all.json                # produced by strict mapper (list of {chunk_id, mentioned_vars, evidence{}})
  - pvvp_master.csv                # for ordering & TT rows
  - (optional) review_decisions.json  # from UI; shape: {"approved":[{chunk_id, name, evidence, note?}, ...]}

Outputs (session folder):
  - review_candidates.json         # {"candidates":[{chunk_id,status,name,evidence,note?}, ...]}
  - review_decisions.json          # {"approved":[...]} (if updated by UI)
  - mapper_all_merged.json         # merged strict + approved
  - merge_result.json              # compact audit: {"mentioned_vars":[...], "evidence":{...}}
"""

import os, json, argparse, csv
from typing import Any, Dict, List

def read(path): 
    with open(path,'r',encoding='utf-8') as f: return f.read()

def write(path, s):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path,'w',encoding='utf-8') as f: f.write(s)

def read_json(path):
    with open(path,'r',encoding='utf-8') as f: return json.load(f)

def write_json(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path,'w',encoding='utf-8') as f: json.dump(obj,f,ensure_ascii=False,indent=2)

def load_master(master_csv: str):
    rows=[]
    with open(master_csv,'r',encoding='utf-8-sig',newline='') as f:
        for i,row in enumerate(csv.DictReader(f)):
            rows.append(row)
    return rows

def build_candidates(strict_all_path: str) -> List[Dict[str,Any]]:
    if not os.path.exists(strict_all_path):
        return []
    base = read_json(strict_all_path)  # list of {chunk_id, mentioned_vars, evidence{}}
    cands=[]
    for it in base:
        cid = int(it.get("chunk_id", 0) or 0)
        evmap = it.get("evidence", {}) or {}
        for name in it.get("mentioned_vars", []) or []:
            ev = evmap.get(name, "")
            if not ev: 
                # still list it; user may approve based on context
                pass
            cands.append({
                "chunk_id": cid,
                "status": "Yes",          # initial status from strict mapper = Yes
                "name": name,
                "evidence": ev
            })
    return cands

def merge_approved(strict_all_path: str, approved: List[Dict[str,Any]], out_path: str, audit_path: str):
    base = read_json(strict_all_path) if os.path.exists(strict_all_path) else []
    by_id: Dict[int, Dict[str,Any]] = {}
    for obj in base:
        obj.setdefault("mentioned_vars", [])
        obj.setdefault("evidence", {})
        by_id[int(obj["chunk_id"])] = obj

    for it in approved:
        cid = int(it["chunk_id"])
        name = it["name"]
        ev   = it.get("evidence","")
        rec = by_id.setdefault(cid, {"chunk_id": cid, "mentioned_vars": [], "evidence": {}})
        if name not in rec["mentioned_vars"]:
            rec["mentioned_vars"].append(name)
        if ev and name not in rec["evidence"]:
            rec["evidence"][name] = ev

    merged = [by_id[k] for k in sorted(by_id.keys())]
    write_json(out_path, merged)

    # compact audit
    mentioned=[]
    ev={}
    for obj in merged:
        for v in obj.get("mentioned_vars", []):
            mentioned.append(v)
            if v in obj.get("evidence", {}):
                ev[v]=obj["evidence"][v]
    write_json(audit_path, {"mentioned_vars": mentioned, "evidence": ev})

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--session", required=True)
    ap.add_argument("--project-root", required=True)
    ap.add_argument("--prepare", action="store_true", help="Build review_candidates.json from strict mapper")
    ap.add_argument("--merge", action="store_true", help="Merge review_decisions.json into mapper_all_merged.json")
    args = ap.parse_args()

    root = os.path.abspath(args.project_root)
    sdir = os.path.join(root, "sessions", args.session)

    strict_all  = os.path.join(sdir, "mapper_all.json")
    candidates  = os.path.join(sdir, "review_candidates.json")
    decisions   = os.path.join(sdir, "review_decisions.json")
    merged_out  = os.path.join(sdir, "mapper_all_merged.json")
    audit_out   = os.path.join(sdir, "merge_result.json")

    if args.prepare:
        cands = build_candidates(strict_all)
        write_json(candidates, {"candidates": cands})
        print(f"[L06] Prepared {len(cands)} candidates → {candidates}")

    if args.merge:
        if not os.path.exists(decisions):
            print(f"[L06] No decisions found at {decisions}; nothing to merge.")
            return 0
        dec = read_json(decisions)
        approved = dec.get("approved", [])
        merge_approved(strict_all, approved, merged_out, audit_out)
        print(f"[L06] Merged {len(approved)} approvals → {merged_out}")
        print(f"[L06] Audit → {audit_out}")
    return 0

if __name__=="__main__":
    main()
