from __future__ import annotations

# Robust merge and evidence guard
# CLI: python L07_merge.py --session <car_id> --project-root <project_root> [--diag-merge]

import argparse
import json
import sys
from pathlib import Path
from typing import Dict, List, Tuple, Set
import traceback

from pvvp.temp_utils import make_temp_root, atomic_publish
from pvvp.textnorm import norm_basic

# --------------------- helpers ---------------------

DASH_PRIORITY = {"exact": 3, "normalized": 2}


def load_json(path: Path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def load_jsonl(path: Path) -> List[dict]:
    rows: List[dict] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def read_allow_list(path: Path) -> Set[str]:
    allow: Set[str] = set()
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                allow.add(line)
    return allow


def parse_allowed_chunk_ids(budget: object) -> Set[int]:
    allowed: Set[int] = set()
    if isinstance(budget, dict) and "chunks" in budget and isinstance(budget["chunks"], list):
        for item in budget["chunks"]:
            try:
                if item.get("allowed") and "chunk_id" in item:
                    allowed.add(int(item["chunk_id"]))
            except Exception:
                continue
    if isinstance(budget, dict) and "allowed_chunks" in budget and isinstance(budget["allowed_chunks"], list):
        for cid in budget["allowed_chunks"]:
            try:
                allowed.add(int(cid))
            except Exception:
                continue
    if isinstance(budget, list):
        for item in budget:
            if isinstance(item, dict):
                try:
                    if item.get("allowed") and "chunk_id" in item:
                        allowed.add(int(item["chunk_id"]))
                except Exception:
                    continue
    return allowed


def evidence_passes(evidence: str, chunk_text: str) -> Tuple[bool, str]:
    if not evidence:
        return False, "empty_evidence"
    if evidence in chunk_text:
        return True, "exact"
    if norm_basic(evidence) and norm_basic(evidence) in norm_basic(chunk_text):
        return True, "normalized"
    try:
        from rapidfuzz.fuzz import partial_ratio, token_set_ratio  # type: ignore

        score = max(partial_ratio(evidence, chunk_text), token_set_ratio(evidence, chunk_text))
        if score >= 92:
            return True, f"fuzzy_{int(score)}"
    except Exception:
        pass
    return False, "not_found"


# --------------------- main ---------------------


def main(argv: List[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Merge mapper chunks with robust evidence guard")
    ap.add_argument("--session", required=True)
    ap.add_argument("--project-root", required=True)
    ap.add_argument("--diag-merge", action="store_true")
    args = ap.parse_args(argv)

    session_id = args.session
    project_root = Path(args.project_root).resolve()
    session_dir = project_root / "sessions" / session_id

    tmp_root = make_temp_root("merge_")
    try:
        # Inputs
        chunks_path = session_dir / "chunks.jsonl"
        budget_path = session_dir / "budget_report.json"
        allow_list_path = session_dir / f"LV_{session_id}PVVP.txt"

        for p in (chunks_path, budget_path, allow_list_path):
            if not p.is_file():
                raise FileNotFoundError(f"Missing required input: {p}")

        chunk_rows = load_jsonl(chunks_path)
        budget_data = load_json(budget_path)
        allow_set = read_allow_list(allow_list_path)

        chunk_text_by_id: Dict[int, str] = {}
        for r in chunk_rows:
            if "id" in r and "text" in r:
                try:
                    cid = int(r["id"])
                    chunk_text_by_id[cid] = r["text"]
                except Exception:
                    continue

        allowed_ids = parse_allowed_chunk_ids(budget_data)

        mapper_files = sorted(session_dir.glob("mapper_chunk_*.json"))
        if args.diag_merge:
            print(f"[diag] mapper files found: {len(mapper_files)}")
            if not mapper_files:
                print(f"[diag] cwd: {Path.cwd()}")
                print(f"[diag] files in session_dir: {[p.name for p in session_dir.iterdir()]}")
            print(f"[diag] allowed_chunks: {sorted(allowed_ids)}")

        # Accumulators
        processed_chunk_ids: List[int] = []
        total_mentions = 0
        drops: List[Dict[str, object]] = []

        ordered_vars: List[str] = []
        evidence_map: Dict[str, str] = {}
        reason_map: Dict[str, str] = {}
        reason_priority = {"exact": 3, "normalized": 2, "fuzzy": 1}

        for mf in mapper_files:
            try:
                mapper = load_json(mf)
            except Exception:
                if args.diag_merge:
                    print(f"[diag] failed to read {mf.name}")
                continue
            cid = mapper.get("chunk_id")
            try:
                cid = int(cid)
            except Exception:
                continue
            if allowed_ids and cid not in allowed_ids:
                continue
            processed_chunk_ids.append(cid)
            chunk_text = chunk_text_by_id.get(cid, "")
            mentioned_vars = mapper.get("mentioned_vars") or []
            ev_map = mapper.get("evidence") or {}
            for raw_var in mentioned_vars:
                var = raw_var.strip()
                if var not in allow_set:
                    continue
                total_mentions += 1
                ev = ev_map.get(raw_var)
                ok, reason = evidence_passes(ev, chunk_text)
                if not ok:
                    drops.append({"chunk_id": cid, "var": var, "reason": reason})
                    continue
                canon = norm_basic(var)
                pr_key = reason.split("_")[0]
                pr_val = reason_priority.get(pr_key, 0)
                if canon not in evidence_map:
                    ordered_vars.append(var)
                    evidence_map[canon] = ev
                    reason_map[canon] = reason
                else:
                    existing_reason = reason_map[canon]
                    ex_key = existing_reason.split("_")[0]
                    ex_pr = reason_priority.get(ex_key, 0)
                    if pr_val > ex_pr:
                        evidence_map[canon] = ev
                        reason_map[canon] = reason

        # Prepare outputs (convert canonical keys back to stored original names)
        final_vars = []
        final_evidence: Dict[str, str] = {}
        final_reason: Dict[str, str] = {}
        for var in ordered_vars:
            canon = norm_basic(var)
            if canon in evidence_map:
                final_vars.append(var)
                final_evidence[var] = evidence_map[canon]
                final_reason[var] = reason_map[canon]

        merge_result = {
            "mentioned_vars": final_vars,
            "evidence": final_evidence,
            "evidence_reason": final_reason,
        }
        merge_report = {
            "processed_chunk_ids": processed_chunk_ids,
            "total_mentions_in_chunks": total_mentions,
            "deduped_vars": len(final_vars),
            "drops": drops,
        }

        out_res_tmp = tmp_root / "out" / "merge_result.json"
        out_rep_tmp = tmp_root / "out" / "merge_report.json"
        with out_res_tmp.open("w", encoding="utf-8") as f:
            json.dump(merge_result, f, ensure_ascii=False, indent=2)
        with out_rep_tmp.open("w", encoding="utf-8") as f:
            json.dump(merge_report, f, ensure_ascii=False, indent=2)

        atomic_publish(out_res_tmp, session_dir / "merge_result.json")
        atomic_publish(out_rep_tmp, session_dir / "merge_report.json")

        if args.diag_merge:
            accepted = len(final_vars)
            print(f"[diag] processed_chunks: {processed_chunk_ids}")
            print(f"[diag] accepted: {accepted}; drops: {len(drops)}")

        return 0
    except Exception:
        tb = traceback.format_exc()
        try:
            (tmp_root / "logs").mkdir(parents=True, exist_ok=True)
            (tmp_root / "logs" / "merge.err.log").write_text(tb, encoding="utf-8")
        except Exception:
            pass
        return 1
    finally:
        # best effort cleanup
        try:
            import shutil

            shutil.rmtree(tmp_root, ignore_errors=True)
        except Exception:
            pass


if __name__ == "__main__":
    sys.exit(main())
