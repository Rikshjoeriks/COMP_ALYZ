from __future__ import annotations

"""Merge mapper chunks with NR-keyed aggregation and evidence guard."""

import argparse
import csv
import json
import re
import sys
import traceback
from pathlib import Path
from typing import Any, Dict, List, Tuple

from pvvp.temp_utils import make_temp_root, atomic_publish
from pvvp.textnorm import norm_lv


NR_RE = re.compile(r"^NR\d+$", re.I)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def load_json(path: Path) -> Any:
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


def parse_allowed_chunk_ids(budget: object) -> set[int]:
    allowed: set[int] = set()
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


def load_master(path: Path) -> Tuple[List[dict], Dict[str, str]]:
    """Load master CSV with tolerant headers.

    Returns (rows, header_info) where rows are raw master rows and header_info
    records which column names were used for nr/name/tt/en.
    """

    NR_COLS = ["nr_code", "nr code", "nr"]
    NAME_COLS = [
        "variable_name_lv",
        "variable name lv",
        "variable name",
        "variable name lv",
        "variable name",
    ]
    TT_COLS = ["is_tt", "section tt", "tt"]
    EN_COLS = ["variable_name_en", "variable name en", "variable name en"]

    rows: List[dict] = []
    header_info: Dict[str, str] = {}
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        fields = {c.lower(): c for c in reader.fieldnames or []}

        def _find(aliases: List[str]) -> str | None:
            for a in aliases:
                if a in fields:
                    return fields[a]
            return None

        nr_col = _find(NR_COLS)
        name_col = _find(NAME_COLS)
        tt_col = _find(TT_COLS)
        en_col = _find(EN_COLS)
        header_info = {
            "nr": nr_col or "",
            "name": name_col or "",
            "tt": tt_col or "",
            "en": en_col or "",
        }

        for row in reader:
            nr = (row.get(nr_col or "") or "").strip()
            name_lv = (row.get(name_col or "") or "").strip()
            name_en = (row.get(en_col or "") or "").strip() if en_col else ""
            val_tt = (row.get(tt_col or "") or "").strip().upper() if tt_col else ""
            is_tt = val_tt in {"Y", "YES", "TRUE", "1"}
            rows.append({"nr": nr, "lv": name_lv, "en": name_en, "is_tt": is_tt})

    return rows, header_info


def evidence_passes(ev: str, txt: str) -> Tuple[bool, str]:
    if not ev:
        return False, "empty"
    if ev in txt:
        return True, "exact"
    nev, ntx = norm_lv(ev), norm_lv(txt)
    if nev and nev in ntx:
        return True, "normalized"
    try:
        from rapidfuzz.fuzz import partial_ratio, token_set_ratio  # type: ignore

        score = max(partial_ratio(nev, ntx), token_set_ratio(nev, ntx))
        if score >= 92:
            return True, f"fuzzy_{int(score)}"
    except Exception:
        pass
    return False, "miss"


# ---------------------------------------------------------------------------
# legacy merge fallback
# ---------------------------------------------------------------------------

def legacy_merge(session_dir: Path, chunk_rows: List[dict], budget_data: Any, tmp_root: Path) -> int:
    allowed_ids = parse_allowed_chunk_ids(budget_data)
    chunk_text_by_id = {int(r["id"]): r.get("text", "") for r in chunk_rows if "id" in r}
    mapper_files = sorted(session_dir.glob("mapper_chunk_*.json"))

    mentioned: List[str] = []
    evidence: Dict[str, str] = {}
    reason_map: Dict[str, str] = {}

    for mf in mapper_files:
        mapper = load_json(mf)
        cid = mapper.get("chunk_id")
        try:
            cid = int(cid)
        except Exception:
            continue
        if allowed_ids and cid not in allowed_ids:
            continue
        chunk_text = chunk_text_by_id.get(cid, "")
        res = mapper.get("results")
        if isinstance(res, list):
            items = res
        else:
            mv = mapper.get("mentioned_vars") or []
            evid = mapper.get("evidence") or {}
            items = [{"nr": m, "match": evid.get(m, "")} for m in mv]
        for it in items:
            nr = str(it.get("nr", "")).strip()
            match = str(it.get("match", ""))
            if not nr:
                continue
            ok, reason = evidence_passes(match, chunk_text)
            if not ok:
                continue
            if nr not in mentioned:
                mentioned.append(nr)
            evidence[nr] = match
            reason_map[nr] = reason

    merge_result = {
        "mentioned_vars": mentioned,
        "evidence": evidence,
        "evidence_reason": reason_map,
    }

    out_res = tmp_root / "out" / "merge_result.json"
    out_rep = tmp_root / "out" / "merge_report.json"
    out_dbg = tmp_root / "out" / "merge_debug.json"
    out_res.parent.mkdir(parents=True, exist_ok=True)
    json.dump(merge_result, out_res.open("w", encoding="utf-8"), ensure_ascii=False, indent=2)
    json.dump({}, out_rep.open("w", encoding="utf-8"), ensure_ascii=False, indent=2)
    json.dump({}, out_dbg.open("w", encoding="utf-8"), ensure_ascii=False, indent=2)
    atomic_publish(out_res, session_dir / "merge_result.json")
    atomic_publish(out_rep, session_dir / "merge_report.json")
    atomic_publish(out_dbg, session_dir / "merge_debug.json")
    return 0


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def main(argv: List[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Merge mapper chunks with evidence guard")
    ap.add_argument("--session", required=True)
    ap.add_argument("--project-root", required=True)
    ap.add_argument("--master-csv")
    ap.add_argument("--diag-merge", action="store_true")
    args = ap.parse_args(argv)

    session_id = args.session
    project_root = Path(args.project_root).resolve()
    session_dir = project_root / "sessions" / session_id

    master_path = Path(args.master_csv) if args.master_csv else session_dir / "pvvp_master.csv"

    tmp_root = make_temp_root("merge_")
    try:
        chunks_path = session_dir / "chunks.jsonl"
        budget_path = session_dir / "budget_report.json"

        for p in (chunks_path, budget_path, master_path):
            if not p.is_file():
                raise FileNotFoundError(f"Missing required input: {p}")

        chunk_rows = load_jsonl(chunks_path)
        budget_data = load_json(budget_path)
        master_rows, header_info = load_master(master_path)
        master_index = [r for r in master_rows if r.get("nr") and not r.get("is_tt")]
        name_to_nr = {r["lv"]: r["nr"] for r in master_rows if r.get("nr") and r.get("lv")}
        if args.diag_merge:
            print(
                f"[diag] master headers: {header_info}; non_tt_count={len(master_index)}"
            )

        if not master_index:
            print(
                "[merge] Warning: no NR-coded records found; falling back to legacy merge",
                file=sys.stderr,
            )
            return legacy_merge(session_dir, chunk_rows, budget_data, tmp_root)

        valid_nrs = {r["nr"] for r in master_index}
        chunk_text_by_id = {
            int(r["id"]): r.get("text", "") for r in chunk_rows if "id" in r
        }
        allowed_ids = parse_allowed_chunk_ids(budget_data)

        mapper_files = sorted(session_dir.glob("mapper_chunk_*.json"))

        processed_chunk_ids: List[int] = []
        total_mentions = 0
        drops: List[Dict[str, Any]] = []
        unresolved: List[Dict[str, Any]] = []
        mapping_stats = {"nr_hits": 0, "nr_unresolved": 0}
        hits: List[Dict[str, Any]] = []

        for mf in mapper_files:
            mapper = load_json(mf)
            cid = mapper.get("chunk_id")
            try:
                cid = int(cid)
            except Exception:
                continue
            if allowed_ids and cid not in allowed_ids:
                continue
            processed_chunk_ids.append(cid)
            chunk_text = chunk_text_by_id.get(cid, "")
            results = mapper.get("results")
            if not isinstance(results, list):
                mv = mapper.get("mentioned_vars") or []
                ev_map = mapper.get("evidence") or {}
                results = [
                    {
                        "nr": name_to_nr.get(m, m),
                        "verdict": "Jā",
                        "match": ev_map.get(m, ""),
                    }
                    for m in mv
                ]
            for item in results:
                if not isinstance(item, dict):
                    continue
                nr = str(item.get("nr", "")).strip().upper()
                verdict = str(item.get("verdict", "")).strip()
                match = str(item.get("match", ""))
                total_mentions += 1
                if verdict not in ("Jā", "Varbūt"):
                    continue
                if nr not in valid_nrs:
                    unresolved.append({"nr": nr})
                    mapping_stats["nr_unresolved"] += 1
                    continue
                ok, reason = evidence_passes(match, chunk_text)
                if not ok:
                    drops.append({"nr": nr, "chunk": cid, "reason": reason, "ev": match})
                    continue
                if reason.startswith("fuzzy"):
                    verdict = "Varbūt"
                hits.append(
                    {
                        "nr": nr,
                        "chunk_id": cid,
                        "evidence": match,
                        "reason": reason,
                        "verdict": verdict,
                    }
                )
                mapping_stats["nr_hits"] += 1

        reason_priority = {"exact": 3, "normalized": 2, "fuzzy": 1}
        best_by_nr: Dict[str, Dict[str, Any]] = {}
        order: List[str] = []
        for h in hits:
            nr = h["nr"]
            pr = reason_priority.get(h["reason"].split("_")[0], 0)
            existing = best_by_nr.get(nr)
            if not existing:
                best_by_nr[nr] = h
                order.append(nr)
                continue
            epr = reason_priority.get(existing["reason"].split("_")[0], 0)
            if pr > epr or (pr == epr and len(h["evidence"]) > len(existing["evidence"])):
                best_by_nr[nr] = h

        merge_result = {
            nr: {
                "verdict": best_by_nr[nr]["verdict"],
                "evidence": best_by_nr[nr]["evidence"],
                "evidence_reason": best_by_nr[nr]["reason"],
            }
            for nr in order
        }

        sample_master = [{"raw": r["lv"], "norm": norm_lv(r["lv"])} for r in master_index[:5]]
        merge_report = {
            "processed_chunks": len(processed_chunk_ids),
            "total_mapper_hits": total_mentions,
            "deduped_nrs": len(order),
            "header_info": header_info,
            "mapping_stats": mapping_stats,
            "drops": drops,
            "unresolved": unresolved,
            "master_samples": sample_master,
        }

        merge_debug = {
            "header_info": header_info,
            "accepted_by_nr": {
                nr: {
                    "reason": best_by_nr[nr]["reason"],
                    "chunk": best_by_nr[nr]["chunk_id"],
                    "verdict": best_by_nr[nr]["verdict"],
                }
                for nr in order
            },
            "drops": drops,
            "unresolved": unresolved,
        }

        out_res = tmp_root / "out" / "merge_result.json"
        out_rep = tmp_root / "out" / "merge_report.json"
        out_dbg = tmp_root / "out" / "merge_debug.json"
        out_res.parent.mkdir(parents=True, exist_ok=True)
        json.dump(merge_result, out_res.open("w", encoding="utf-8"), ensure_ascii=False, indent=2)
        json.dump(merge_report, out_rep.open("w", encoding="utf-8"), ensure_ascii=False, indent=2)
        json.dump(merge_debug, out_dbg.open("w", encoding="utf-8"), ensure_ascii=False, indent=2)

        atomic_publish(out_res, session_dir / "merge_result.json")
        atomic_publish(out_rep, session_dir / "merge_report.json")
        atomic_publish(out_dbg, session_dir / "merge_debug.json")

        if args.diag_merge:
            print(f"[diag] processed_chunks: {processed_chunk_ids}")
            print(f"[diag] header_info: {header_info}")
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
        try:
            import shutil

            shutil.rmtree(tmp_root, ignore_errors=True)
        except Exception:
            pass


if __name__ == "__main__":
    sys.exit(main())

