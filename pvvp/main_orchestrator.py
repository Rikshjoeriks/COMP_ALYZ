#!/usr/bin/env python3
"""
Main Orchestrator & CLI — end-to-end session run (Milestone 9)
Project: PVVP (COMPET_ALYZ_FORD)

This orchestrates existing legos (M1–M8) to run a full session for one car.
No new business logic; sequencing + error handling + budget-aware short-circuiting only.

Inputs (CLI):
  --project-root <path> (required)
  Exactly one source for CSCOPIED text:
    --input-file <path> | --stdin
  Masterlist: one of
    --master-csv <path> (recommended)
    --vehicle-type <name> (optional; uses config mapping if available)
  Optional:
    --session-id <id> (auto-generated if omitted)
    --enable-cs-unmatched (flag; default ON)

Outputs: whatever legos write under /sessions/<car_id>/ and /exports/<car_id>/.
Appends RUN: ... status line to /sessions/<car_id>/summary.txt at the end.

Exit codes:
  0 = success (including capped but graceful)
  2 = partial (some steps failed, but CSV exported)
  1 = fatal before export (normalization/chunking/master load failed)

Assumed lego entrypoints (CLI or modules) under <project_root>/program:
  L03_normalize.py      --session <id>
  L04_chunker.py        --session <id>
  L12_budget_guard.py   --session <id>
  L06_mapper.py         --session <id> --chunk-id <int>
  L07_merge.py          --session <id>
  L08_validate_align.py --session <id>
  L10_export_csv.py     --session <id>
  L09_export_positives.py --session <id>
  L13_summary_finalize.py --session <id>

If your filenames differ, adjust LEGO_PATHS below.
"""
from __future__ import annotations
import argparse
import csv
import json
import os
import shutil
import sys
import textwrap
from datetime import datetime, UTC
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import subprocess

# ---------- Configuration of lego script paths (relative to project root) ----------
LEGO_PATHS = {
    "normalize": "program/L03_normalize.py",
    "chunker": "program/L04_chunker.py",
    "budget": "program/L12_budget_guard.py",
    "mapper": "program/L06_mapper.py",
    "merge": "program/L07_merge.py",
    "validate": "program/L08_validate_align.py",
    "export_csv": "program/L10_export_csv.py",
    "export_positives": "program/L09_export_positives.py",
    "summary_finalize": "program/L13_summary_finalize.py",
}

# ---------- Helpers ----------

def now_utc_ts() -> str:
    # Use timezone-aware UTC to avoid deprecation warnings
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def auto_session_id() -> str:
    return datetime.now().strftime("%Y%m%d-%H%M%S-%04d") % 0


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def write_text(p: Path, s: str) -> None:
    p.write_text(s, encoding="utf-8")


def append_line(p: Path, s: str) -> None:
    with p.open("a", encoding="utf-8") as f:
        f.write(s.rstrip("\n") + "\n")


def run_py(script_path: Path, *args: str, cwd: Optional[Path] = None) -> Tuple[int, str, str]:
    """Run a python script with the current interpreter, capture output.
    Returns (returncode, stdout, stderr).
    """
    cmd = [sys.executable, str(script_path), *args]
    proc = subprocess.run(cmd, cwd=str(cwd or script_path.parent), capture_output=True, text=True)
    return proc.returncode, proc.stdout, proc.stderr


def load_json(path: Path, default):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


# ---------- Core orchestrator ----------

class Orchestrator:
    def __init__(self, project_root: Path, session_id: str, enable_cs_unmatched: bool = True):
        self.project_root = project_root.resolve()
        self.session_id = session_id
        self.enable_cs_unmatched = enable_cs_unmatched
        self.sessions_dir = self.project_root / "sessions" / session_id
        self.exports_dir = self.project_root / "exports" / session_id
        ensure_dir(self.sessions_dir)
        ensure_dir(self.exports_dir)
        self.summary_path = self.sessions_dir / "summary.txt"

        # Predeclare expected key files per lego
        self.paths = {
            "input_raw": self.sessions_dir / "input_raw.txt",
            "pvvp_master": self.sessions_dir / "pvvp_master.csv",
            "pvvp_allow": self.sessions_dir / "pvvp_list_lv.txt",
            "normalized": self.sessions_dir / "text_normalized.txt",
            "normalize_debug": self.sessions_dir / "normalize_debug.txt",
            "chunks": self.sessions_dir / "chunks.jsonl",
            "chunker_debug": self.sessions_dir / "chunker_debug.txt",
            "budget": self.sessions_dir / "budget_report.json",
            "mapper_all": self.sessions_dir / "mapper_all.json",
            "merge_result": self.sessions_dir / "merge_result.json",
            "merge_report": self.sessions_dir / "merge_report.json",
            "final_decisions": self.sessions_dir / "final_decisions.json",
            "master_aligned": self.sessions_dir / "master_aligned.jsonl",
            "validate_report": self.sessions_dir / "validate_report.json",
            "export_csv": self.exports_dir / f"detections_{self.session_id}.csv",
            "positives_jsonl": self.sessions_dir / "positives_explanations.jsonl",
            "summary_final": self.sessions_dir / "summary.txt",  # same as summary_path
        }

        append_line(self.summary_path, f"{now_utc_ts()} | RUN START session={self.session_id}")

    # ----- Session init -----
    def save_input(self, source_text: str) -> None:
        write_text(self.paths["input_raw"], source_text)
        append_line(self.summary_path, f"{now_utc_ts()} | INPUT saved bytes={len(source_text.encode('utf-8'))}")

    def copy_master_csv(self, master_csv_path: Path) -> None:
        """Safely copy master CSV into the session, skipping if src==dst and
        retrying on transient Windows file locks."""
        src = Path(master_csv_path).resolve()
        dst = self.paths["pvvp_master"].resolve()
        # Skip copy if source and destination are the same file
        if src == dst:
            append_line(self.summary_path, f"{now_utc_ts()} | MASTER already in place at {dst}")
            return
        # Retry copy to work around WinError 32 from other processes (e.g., Excel/AV)
        import time
        for attempt in range(5):
            try:
                ensure_dir(dst.parent)
                shutil.copy2(str(src), str(dst))
                append_line(self.summary_path, f"{now_utc_ts()} | MASTER copied from={src}")
                return
            except PermissionError as e:
                if attempt == 4:
                    raise
                time.sleep(0.25 * (attempt + 1))

    def master_from_vehicle_type(self, vehicle_type: str) -> Path:
        """Optional mapping: config/masterlists/<vehicle_type>.csv.
        Raise FileNotFoundError if missing.
        """
        candidate = self.project_root / "config" / "masterlists" / f"pvvp_master_{vehicle_type}.csv"
        if not candidate.exists():
            raise FileNotFoundError(f"vehicle type '{vehicle_type}' not mapped to master CSV at {candidate}")
        return candidate

    def derive_allow_list(self) -> None:
        """Derive non-TT Variable Name list preserving master order to pvvp_list_lv.txt.
        Convention: TT rows have empty/blank Variable Name; all others included verbatim.
        Columns expected: 'Nr Code', 'Variable Name', 'Section TT' (header names are flexible by index position too).
        """
        master_csv = self.paths["pvvp_master"]
        if not master_csv.exists():
            raise FileNotFoundError("pvvp_master.csv not found in session")

        allow_lines: List[str] = []
        with master_csv.open("r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            rows = list(reader)
            if not rows:
                raise ValueError("pvvp_master.csv is empty")
            header = rows[0]
            # Try to detect column indices by header label; fallback to default positions
            try:
                idx_var = header.index("Variable Name")
            except ValueError:
                idx_var = 1  # default fallback
            for i, row in enumerate(rows[1:], start=2):
                try:
                    var = (row[idx_var] or "").strip()
                except IndexError:
                    var = ""
                if var:
                    allow_lines.append(var)
        write_text(self.paths["pvvp_allow"], "\n".join(allow_lines) + ("\n" if allow_lines else ""))
        append_line(self.summary_path, f"{now_utc_ts()} | ALLOW-LIST derived count={len(allow_lines)}")

    # ----- Lego runners -----
    def run_lego(self, key: str, *args: str) -> Tuple[bool, str]:
        script_rel = LEGO_PATHS[key]
        script_path = (self.project_root / script_rel).resolve()
        if not script_path.exists():
            return False, f"lego script missing: {script_path}"
        rc, out, err = run_py(script_path, *args)
        ok = rc == 0
        log = out.strip() + ("\n" + err.strip() if err.strip() else "")
        append_line(self.summary_path, f"{now_utc_ts()} | LEGO {key} rc={rc}")
        if log:
            append_line(self.summary_path, textwrap.shorten(log, width=800, placeholder=" …"))
        return ok, log

    def run_normalize(self) -> bool:
        ok, _ = self.run_lego("normalize", "--session", self.session_id)
        if not ok:
            append_line(self.summary_path, f"{now_utc_ts()} | FAIL normalize")
        # require normalized file
        return self.paths["normalized"].exists()

    def run_chunker(self) -> bool:
        ok, _ = self.run_lego("chunker", "--session", self.session_id)
        if not ok:
            append_line(self.summary_path, f"{now_utc_ts()} | FAIL chunker")
        return self.paths["chunks"].exists()

    def run_budget(self) -> Dict:
        ok, _ = self.run_lego("budget", "--session", self.session_id)
        report = load_json(self.paths["budget"], default={})
        allowed_ids = []
        per_chunk = report.get("per_chunk", [])
        for c in per_chunk:
            if c.get("allowed"):
                cid = c.get("chunk_id")
                if cid is not None:
                    allowed_ids.append(int(cid))
        append_line(self.summary_path, f"{now_utc_ts()} | BUDGET allowed_chunks={allowed_ids}")
        return report

    def run_mapper_for_chunk(self, chunk_id: int) -> bool:
        ok, log = self.run_lego("mapper", "--session", self.session_id, "--chunk-id", str(chunk_id))
        if not ok:
            err_path = self.sessions_dir / f"mapper_chunk_{chunk_id}_error.txt"
            write_text(err_path, log or "mapper error")
        # success is not required to continue
        return ok

    def run_merge(self) -> bool:
        ok, _ = self.run_lego("merge", "--session", self.session_id)
        return self.paths["merge_result"].exists()

    def run_validate(self) -> bool:
        ok, _ = self.run_lego("validate", "--session", self.session_id)
        return self.paths["final_decisions"].exists() and self.paths["master_aligned"].exists()

    def run_export(self) -> bool:
        ok_csv, _ = self.run_lego("export_csv", "--session", self.session_id)
        ok_pos, _ = self.run_lego("export_positives", "--session", self.session_id)
        ok_sum, _ = self.run_lego("summary_finalize", "--session", self.session_id)
        return self.paths["export_csv"].exists() and self.paths["positives_jsonl"].exists()

    # ----- CS unmatched (simple substring heuristic) -----
    def run_cs_unmatched(self) -> Optional[Path]:
        if not self.enable_cs_unmatched:
            return None
        try:
            text = self.paths["normalized"].read_text(encoding="utf-8")
            allow = self.paths["pvvp_allow"].read_text(encoding="utf-8").splitlines()
        except FileNotFoundError:
            return None
        text_lower = text.lower()
        unmatched = []
        for v in allow:
            v_clean = v.strip()
            if not v_clean:
                continue
            if v_clean.lower() not in text_lower:
                unmatched.append(v_clean)
        out_path = self.exports_dir / f"cs_unmatched_{self.session_id}.txt"
        write_text(out_path, "\n".join(unmatched) + ("\n" if unmatched else ""))
        append_line(self.summary_path, f"{now_utc_ts()} | CS_UNMATCHED count={len(unmatched)}")
        return out_path

    # ----- Summaries/Counters -----
    def count_chunks(self) -> Tuple[int, int]:
        total = 0
        allowed = 0
        report = load_json(self.paths["budget"], default={})
        if report:
            per = report.get("per_chunk", [])
            total = len(per)
            allowed = sum(1 for c in per if c.get("allowed"))
        else:
            # fallback: count chunks.jsonl lines
            try:
                with self.paths["chunks"].open("r", encoding="utf-8") as f:
                    total = sum(1 for _ in f)
            except Exception:
                total = 0
        return total, allowed

    def count_positives(self) -> int:
        merge = load_json(self.paths["merge_result"], default={})
        mv = merge.get("mentioned_vars")
        if isinstance(mv, list):
            return len(mv)
        return 0

    # ----- Final RUN line -----
    def final_run_line(self, capped: bool, partial: bool) -> str:
        chunks_total, chunks_allowed = self.count_chunks()
        positives = self.count_positives()
        status = "OK"
        if partial:
            status = "PARTIAL"
        # Fatal handled by main control flow before export
        return (
            f"RUN: {status} (chunks processed {chunks_allowed}/{chunks_total}; "
            f"positives {positives}; capped={'true' if capped else 'false'})"
        )


# ---------- CLI ----------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="PVVP Main Orchestrator (Milestone 9)")
    p.add_argument("--project-root", required=True, help="Project root path")
    src = p.add_mutually_exclusive_group(required=True)
    src.add_argument("--input-file", help="Path to input text file")
    src.add_argument("--stdin", action="store_true", help="Read input text from STDIN")
    master = p.add_mutually_exclusive_group(required=True)
    master.add_argument("--master-csv", help="Path to master CSV")
    master.add_argument("--vehicle-type", help="Vehicle type name for preconfigured CSV")
    p.add_argument("--session-id", help="Optional session id; auto if omitted")
    p.add_argument("--enable-cs-unmatched", action="store_true", default=False,
                   help="Emit cs_unmatched_<id>.txt (simple substring heuristic)")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    project_root = Path(args.project_root).resolve()
    if not project_root.exists():
        print(f"Project root not found: {project_root}", file=sys.stderr)
        return 1

    session_id = args.session_id or auto_session_id()
    orch = Orchestrator(project_root, session_id, enable_cs_unmatched=args.enable_cs_unmatched)

    # 1) Save CSCOPIED input
    if args.input_file:
        try:
            text = Path(args.input_file).read_text(encoding="utf-8")
        except Exception as e:
            append_line(orch.summary_path, f"{now_utc_ts()} | FATAL cannot read input file: {e}")
            print(f"Cannot read input file: {e}", file=sys.stderr)
            return 1
    else:  # stdin
        text = sys.stdin.read()
        if not text:
            append_line(orch.summary_path, f"{now_utc_ts()} | FATAL no stdin text received")
            print("No stdin text received", file=sys.stderr)
            return 1
    orch.save_input(text)

    # 2) Master CSV selection & copy
    try:
        master_csv_path = Path(args.master_csv) if args.master_csv else orch.master_from_vehicle_type(args.vehicle_type)
    except Exception as e:
        append_line(orch.summary_path, f"{now_utc_ts()} | FATAL master selection: {e}")
        print(f"Master selection failed: {e}", file=sys.stderr)
        return 1

    if not master_csv_path.exists():
        append_line(orch.summary_path, f"{now_utc_ts()} | FATAL master csv missing at {master_csv_path}")
        print(f"Master CSV not found at {master_csv_path}", file=sys.stderr)
        return 1
    orch.copy_master_csv(master_csv_path)

    # 3) Derive allow-list
    try:
        orch.derive_allow_list()
    except Exception as e:
        append_line(orch.summary_path, f"{now_utc_ts()} | FATAL allow-list: {e}")
        print(f"Allow-list derivation failed: {e}", file=sys.stderr)
        return 1

    # 4) Normalize
    if not orch.run_normalize():
        print("Normalization failed", file=sys.stderr)
        append_line(orch.summary_path, orch.final_run_line(capped=False, partial=False))
        return 1

    # 5) Chunk
    if not orch.run_chunker():
        print("Chunker failed", file=sys.stderr)
        append_line(orch.summary_path, orch.final_run_line(capped=False, partial=False))
        return 1

    # 6) Budget
    budget_report = orch.run_budget()
    per_chunk = budget_report.get("per_chunk", [])
    allowed_ids = [int(c.get("chunk_id")) for c in per_chunk if c.get("allowed")]
    capped = bool(budget_report.get("capped", False))

    # If no chunks allowed, continue (all N path)

    # 7) Mapper for allowed chunks (resilient per-chunk)
    any_mapper_error = False
    for cid in allowed_ids:
        ok = orch.run_mapper_for_chunk(cid)
        if not ok:
            any_mapper_error = True

    # 8) Merge (always attempt)
    merge_ok = orch.run_merge()

    # 9) Validate & align (always attempt)
    validate_ok = orch.run_validate()

    # 10) Export steps
    export_ok = orch.run_export()

    # 11) Optional cs_unmatched
    orch.run_cs_unmatched()

    partial = any_mapper_error or (not merge_ok) or (not validate_ok) or (not export_ok)

    # Final RUN line
    final_line = orch.final_run_line(capped=capped, partial=partial)
    append_line(orch.summary_path, final_line)
    print(final_line)

    # Exit code policy
    if not export_ok:
        return 2 if (merge_ok or validate_ok) else 1
    return 0 if not partial else 2


if __name__ == "__main__":
    sys.exit(main())
