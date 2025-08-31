from __future__ import annotations

"""Merge mapper chunks with NR-keyed aggregation and evidence guard."""

import argparse
import csv
import json
import re
import sys
import traceback
from pathlib import Path
from typing import Dict, List, Tuple, Set, Any

from pvvp.temp_utils import make_temp_root, atomic_publish
from pvvp.textnorm import norm_basic


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

NR_RE = re.compile(r"^NR\d+$", re.I)


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


def read_allow_list(path: Path) -> List[str]:
    names: List[str] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                names.append(line)
    return names


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


def load_master_map(path: Path) -> Tuple[Dict[str, str], Dict[str, str]]:
    """Return mappings: normalized name -> NR, NR -> original name."""
    name_to_nr: Dict[str, str] = {}
    nr_to_name: Dict[str, str] = {}
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            nr = (row.get("Nr Code") or "").strip()
            name = (row.get("Variable Name") or "").strip()
            if not nr:
                continue
            if name:
                key = norm_basic(name).lower()
                if key not in name_to_nr:
                    name_to_nr[key] = nr
                nr_to_name[nr] = name
    return name_to_nr, nr_to_name


def load_alias_map(path: Path) -> Dict[str, str]:
    try:
        data = load_json(path)
    except Exception:
        return {}
    out: Dict[str, str] = {}
    if isinstance(data, dict):
        for k, v in data.items():
            if isinstance(k, str) and isinstance(v, str):
                out[norm_basic(k).lower()] = v.strip()
    return out


def evidence_passes(ev: str, txt: str) -> Tuple[bool, str]:
    if not ev:
        return False, "empty"
    if ev in txt:
        return True, "exact"
    nev, ntx = norm_basic(ev), norm_basic(txt)
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


def sanity_checks(best: Dict[str, Dict[str, Any]], nr_to_name: Dict[str, str]) -> List[Dict[str, str]]:
    warnings: List[Dict[str, str]] = []
    for nr, hit in best.items():
        name = nr_to_name.get(nr, "").lower()
        ev_norm = norm_basic(hit.get("evidence", "")).lower()
        if not ev_norm:
            continue
        if "pārnesum" in name or "automatic transmission" in name:
            if not any(k in ev_norm for k in ["automātisk", "at", "pārnesumkār"]):
                warnings.append({"nr": nr, "evidence": hit.get("evidence", "")})
        if "stūres" in name and "apsild" in name:
            if not ("stūr" in ev_norm and "apsild" in ev_norm):
                warnings.append({"nr": nr, "evidence": hit.get("evidence", "")})
        if "spoguļ" in name:
            if "spoguļ" not in ev_norm:
                warnings.append({"nr": nr, "evidence": hit.get("evidence", "")})
        if "digit" in name or "klaster" in name or "ekrān" in name:
            if not ("ekrān" in ev_norm and any(ch.isdigit() for ch in ev_norm)):
                warnings.append({"nr": nr, "evidence": hit.get("evidence", "")})
    return warnings


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


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
        chunks_path = session_dir / "chunks.jsonl"
        budget_path = session_dir / "budget_report.json"
        allow_list_path = session_dir / f"LV_{session_id}PVVP.txt"
        master_csv_path = session_dir / "pvvp_master.csv"
        alias_path = session_dir / "alias_map.json"

        for p in (chunks_path, budget_path, allow_list_path, master_csv_path):
            if not p.is_file():
                raise FileNotFoundError(f"Missing required input: {p}")

        chunk_rows = load_jsonl(chunks_path)
        budget_data = load_json(budget_path)
        allow_raw = read_allow_list(allow_list_path)
        name_to_nr, nr_to_name = load_master_map(master_csv_path)
        alias_map = load_alias_map(alias_path)
        alias_map.update(name_to_nr)

        allow_nr_list: List[str] = []
        for item in allow_raw:
            if NR_RE.match(item):
                allow_nr_list.append(item.upper())
            else:
                nr = alias_map.get(norm_basic(item).lower())
                if nr:
                    allow_nr_list.append(nr)
        allow_nr_set = set(allow_nr_list)

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

        processed_chunk_ids: List[int] = []
        total_mentions = 0
        drops: List[Dict[str, Any]] = []
        unresolved: List[Dict[str, Any]] = []

        hits: List[Dict[str, Any]] = []
        best_by_nr: Dict[str, Dict[str, Any]] = {}
        order: List[str] = []
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
                total_mentions += 1
                raw = str(raw_var).strip()
                nr: str | None = None
                if NR_RE.match(raw):
                    nr = raw.upper()
                else:
                    nr = alias_map.get(norm_basic(raw).lower())
                if not nr:
                    unresolved.append({"mention": raw})
                    continue
                ev = ev_map.get(raw_var)
                ev_str = ev if isinstance(ev, str) else ""
                ok, reason = evidence_passes(ev_str, chunk_text)
                if not ok:
                    drops.append({"nr": nr, "reason": reason, "chunk": cid, "ev": ev})
                    continue
                # Ensure nr is not None before adding to hits
                if nr is not None:
                    hits.append({
                        "nr": nr,
                        "var_name_src": raw,
                        "chunk_id": cid,
                        "evidence": ev_str,
                        "reason": reason,
                    })

        reason_priority = {"exact": 3, "normalized": 2, "fuzzy": 1}
        for h in hits:
            nr = h["nr"]
            if nr is None:
                continue
            rkey = h["reason"].split("_")[0]
            pr = reason_priority.get(rkey, 0)
            existing = best_by_nr.get(nr)
            if not existing:
                best_by_nr[nr] = h
                order.append(nr)
                continue
            ekey = existing["reason"].split("_")[0]
            epr = reason_priority.get(ekey, 0)
            if pr > epr or (pr == epr and len(h.get("evidence", "")) > len(existing.get("evidence", ""))):
                best_by_nr[nr] = h
            if pr > epr or (pr == epr and len(h.get("evidence", "")) > len(existing.get("evidence", ""))):
                best_by_nr[nr] = h

        warnings = sanity_checks(best_by_nr, nr_to_name)

        merge_result = {
            "mentioned_vars": order,
            "evidence": {nr: best_by_nr[nr]["evidence"] for nr in order},
            "evidence_reason": {nr: best_by_nr[nr]["reason"] for nr in order},
        }
        merge_report = {
            "processed_chunk_ids": processed_chunk_ids,
            "total_mentions_in_chunks": total_mentions,
            "deduped_vars": len(order),
            "drops": drops,
            "unresolved": unresolved,
            "warnings": warnings,
        }
        merge_debug = {
            "found_mapper_files": [p.name for p in mapper_files],
            "accepted_by_nr": {nr: {"reason": hit["reason"], "chunk": hit["chunk_id"]} for nr, hit in best_by_nr.items()},
            "drops": drops,
            "unresolved_mentions": unresolved,
        }

        out_res_tmp = tmp_root / "out" / "merge_result.json"
        out_rep_tmp = tmp_root / "out" / "merge_report.json"
        out_dbg_tmp = tmp_root / "out" / "merge_debug.json"
        out_res_tmp.parent.mkdir(parents=True, exist_ok=True)
        with out_res_tmp.open("w", encoding="utf-8") as f:
            json.dump(merge_result, f, ensure_ascii=False, indent=2)
        with out_rep_tmp.open("w", encoding="utf-8") as f:
            json.dump(merge_report, f, ensure_ascii=False, indent=2)
        with out_dbg_tmp.open("w", encoding="utf-8") as f:
            json.dump(merge_debug, f, ensure_ascii=False, indent=2)

        atomic_publish(out_res_tmp, session_dir / "merge_result.json")
        atomic_publish(out_rep_tmp, session_dir / "merge_report.json")
        atomic_publish(out_dbg_tmp, session_dir / "merge_debug.json")

        if args.diag_merge:
            print(f"[diag] processed_chunks: {processed_chunk_ids}")
            print(f"[diag] accepted: {len(order)}; drops: {len(drops)}; unresolved: {len(unresolved)}")
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

