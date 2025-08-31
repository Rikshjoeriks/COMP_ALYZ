#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
L12.BudgetGuard — estimate & enforce €1/car (or configured cap)
Inputs:
  sessions/<car_id>/chunks.jsonl
  sessions/<car_id>/pvvp_list_lv.txt
  config/cost_guard.json              (optional; has pricing & caps; may also override constants)
  config/mapper_preset.json           (optional; model/constant defaults)
Outputs in session folder:
  budget_report.json
  summary.txt                         (append one line)
On failure:
  budget_debug.txt (short reason) and non-zero exit
"""

import argparse
import json
import math
import sys
from pathlib import Path
from decimal import Decimal, ROUND_HALF_UP, getcontext
from datetime import datetime, timezone

# Use stable decimal precision for currency math
getcontext().prec = 28  # plenty for our needs

DEFAULTS = {
    "model": "gpt-4.0-mini",
    "input_cost_per_1k": 0.15,      # € per 1K input tokens
    "output_cost_per_1k": 0.60,     # € per 1K output tokens
    "chars_per_token": 4,
    "fixed_overhead_tokens": 300,
    "assumed_output_tokens": 200,
    "max_calls_per_car": 8,
    "euro_cap_per_car": 2.00
}

CONFIG_FILES = {
    "cost_guard": "config/cost_guard.json",
    "mapper_preset": "config/mapper_preset.json",
}

def d_eur(x) -> Decimal:
    """Coerce to Decimal with two extra guard digits (we'll round for display later)."""
    return Decimal(str(x))

def read_jsonl(path: Path):
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)

def write_json(path: Path, data: dict):
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=False) + "\n", encoding="utf-8")

def append_summary_line(path: Path, line: str):
    with path.open("a", encoding="utf-8") as f:
        f.write(line.rstrip() + "\n")

def write_debug(path: Path, msg: str):
    path.write_text(msg.strip() + "\n", encoding="utf-8")

def ceil_div_chars(chars: int, chars_per_token: int) -> int:
    if chars <= 0:
        return 0
    return int(math.ceil(chars / max(1, chars_per_token)))

def load_config(project_root: Path):
    """Load defaults, overlay mapper_preset, then overlay cost_guard."""
    cfg = DEFAULTS.copy()

    # mapper_preset first (model / constants)
    mp_path = project_root / CONFIG_FILES["mapper_preset"]
    if mp_path.exists():
        try:
            mp = json.loads(mp_path.read_text(encoding="utf-8"))
            for k in ("model", "fixed_overhead_tokens", "assumed_output_tokens", "chars_per_token"):
                if k in mp:
                    cfg[k] = mp[k]
        except Exception:
            # Non-fatal: ignore malformed file to keep deterministic behavior
            pass

    # cost_guard last (pricing & caps; may override any field)
    cg_path = project_root / CONFIG_FILES["cost_guard"]
    if cg_path.exists():
        try:
            cg = json.loads(cg_path.read_text(encoding="utf-8"))
            for k, v in cg.items():
                cfg[k] = v
        except Exception:
            # Non-fatal: ignore malformed file to keep deterministic behavior
            pass

    # Normalize numeric types
    cfg["input_cost_per_1k"] = d_eur(cfg["input_cost_per_1k"])
    cfg["output_cost_per_1k"] = d_eur(cfg["output_cost_per_1k"])
    cfg["euro_cap_per_car"] = d_eur(cfg["euro_cap_per_car"])
    cfg["chars_per_token"] = int(cfg["chars_per_token"])
    cfg["fixed_overhead_tokens"] = int(cfg["fixed_overhead_tokens"])
    cfg["assumed_output_tokens"] = int(cfg["assumed_output_tokens"])
    cfg["max_calls_per_car"] = int(cfg["max_calls_per_car"])
    return cfg

def format_eur(x: Decimal) -> str:
    return f"€{x.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)}"

def run(session: str, project_root: Path, calls_already_made: int) -> int:
    session_dir = project_root / "sessions" / session
    chunks_path = session_dir / "chunks.jsonl"
    pvvp_path = next(session_dir.glob("*PVVP.txt"))
    debug_path = session_dir / "budget_debug.txt"
    report_path = session_dir / "budget_report.json"
    summary_path = session_dir / "summary.txt"

    # Validate inputs exist
    missing = []
    if not chunks_path.exists():
        missing.append(str(chunks_path))
    if not pvvp_path.exists():
        missing.append(str(pvvp_path))
    if missing:
        write_debug(debug_path, f"Missing required input(s): {', '.join(missing)}")
        return 1

    # Load config (with defaults)
    cfg = load_config(project_root)

    if calls_already_made < 0:
        calls_already_made = 0
    remaining_call_cap = max(0, cfg["max_calls_per_car"] - calls_already_made)

    # Read chunks (preserve file order for determinism)
    chunks = list(read_jsonl(chunks_path))
    # Minimal shape validation / augment missing fields safely
    normalized_chunks = []
    for row in chunks:
        cid = row.get("id")
        text = row.get("text", "")
        # Ensure deterministic typing
        try:
            cid = int(cid)
        except Exception:
            # If missing/invalid, fall back to sequential index based on current length
            cid = len(normalized_chunks) + 1
        text_chars = len(text)
        normalized_chunks.append({"chunk_id": cid, "text_chars": int(text_chars)})

    # pvvp list chars (if empty, that's fine)
    try:
        pvvp_text = pvvp_path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        # Fallback if file was saved in a different encoding
        pvvp_text = pvvp_path.read_text(encoding="latin-1")
    pvvp_chars = len(pvvp_text)

    # Token estimates shared params
    chars_per_token = max(1, int(cfg["chars_per_token"]))
    fixed_overhead_tokens = int(cfg["fixed_overhead_tokens"])
    assumed_output_tokens = int(cfg["assumed_output_tokens"])
    input_cost_per_1k = cfg["input_cost_per_1k"]
    output_cost_per_1k = cfg["output_cost_per_1k"]
    euro_cap = cfg["euro_cap_per_car"]

    pvvp_tokens = ceil_div_chars(pvvp_chars, chars_per_token)

    # Walk chunks in order; accumulate cost until cap or call limit
    total_cost = d_eur(0)
    allowed_calls = 0
    per_chunk = []

    # Sort by chunk_id for stability if file order isn't guaranteed
    normalized_chunks.sort(key=lambda r: r["chunk_id"])

    for row in normalized_chunks:
        text_tokens = ceil_div_chars(row["text_chars"], chars_per_token)
        est_input_tokens = fixed_overhead_tokens + text_tokens + pvvp_tokens
        est_output_tokens = assumed_output_tokens

        # Cost = (in_tok/1000 * input_cost) + (out_tok/1000 * output_cost)
        in_cost = (Decimal(est_input_tokens) / Decimal(1000)) * input_cost_per_1k
        out_cost = (Decimal(est_output_tokens) / Decimal(1000)) * output_cost_per_1k
        est_cost_eur = (in_cost + out_cost)

        # Would allowing this chunk exceed caps?
        would_exceed_euro = (total_cost + est_cost_eur) > euro_cap
        would_exceed_calls = allowed_calls >= remaining_call_cap if remaining_call_cap >= 0 else False

        allow = (not would_exceed_euro) and (not would_exceed_calls)

        if allow:
            allowed_calls += 1
            total_cost += est_cost_eur

        per_chunk.append({
            "chunk_id": row["chunk_id"],
            "text_chars": row["text_chars"],
            "pvvp_chars": pvvp_chars,
            "est_input_tokens": est_input_tokens,
            "est_output_tokens": est_output_tokens,
            # Keep monetary precision but also store a rounded display value for readability
            "est_cost_eur": float(est_cost_eur.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)),
            "allowed": allow
        })

    status = "OK"
    note = "Under caps."
    if any(not c["allowed"] for c in per_chunk):
        status = "CAPPED"
        # Determine reason priority for clarity
        if remaining_call_cap == 0:
            note = "Max calls already reached before processing."
        else:
            # Check the first denied chunk to infer reason
            first_denied = next(c for c in per_chunk if not c["allowed"])
            # Recompute what blocked it (deterministically)
            # (We can infer from counts and euro)
            if allowed_calls >= remaining_call_cap:
                note = f"Capped by max_calls_per_car ({cfg['max_calls_per_car']}, {calls_already_made} already made)."
            elif total_cost >= euro_cap:
                note = f"Capped by euro_cap_per_car ({format_eur(euro_cap)})."
            else:
                note = "Capped by configured limits."

    report = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "session": session,
        "config_effective": {
            "model": cfg["model"],
            "input_cost_per_1k": float(input_cost_per_1k),
            "output_cost_per_1k": float(output_cost_per_1k),
            "chars_per_token": chars_per_token,
            "fixed_overhead_tokens": fixed_overhead_tokens,
            "assumed_output_tokens": assumed_output_tokens,
            "max_calls_per_car": cfg["max_calls_per_car"],
            "euro_cap_per_car": float(euro_cap),
            "calls_already_made": calls_already_made
        },
        "per_chunk": per_chunk,
        "sum_est_cost_eur": float(total_cost.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)),
        "allowed_calls_count": allowed_calls,
        "status": status,
        "note": note
    }

    # Idempotent overwrite
    write_json(report_path, report)

    # Append one human line to summary.txt
    if status == "OK":
        line = f"BUDGET: OK (est {format_eur(total_cost)} / cap {format_eur(euro_cap)})"
    else:
        line = f"BUDGET: CAPPED after {allowed_calls} call(s) (est {format_eur(total_cost)})"
    append_summary_line(summary_path, line)

    return 0

def main():
    parser = argparse.ArgumentParser(description="L12.BudgetGuard — estimate & enforce € cap per car without calling the model.")
    parser.add_argument("--session", required=True, help="Car/session ID (e.g., MCAFHEV)")
    parser.add_argument("--project-root", required=True, help="Project root folder")
    parser.add_argument("--calls-already-made", type=int, default=0, help="Subtract from max_calls_per_car")
    args = parser.parse_args()

    try:
        exit_code = run(args.session, Path(args.project_root), args.calls_already_made)
        sys.exit(exit_code)
    except Exception as e:
        # Best-effort debug file if session folder exists
        session_dir = Path(args.project_root) / "sessions" / args.session
        try:
            write_debug(session_dir / "budget_debug.txt", f"Unhandled error: {type(e).__name__}: {e}")
        except Exception:
            pass
        raise

if __name__ == "__main__":
    main()
