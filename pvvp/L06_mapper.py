#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
L06.Mapper.StrictGPT — positives-only, TT-aware (via allow-list)
CLI:
  python L06_mapper.py --session <car_id> --project-root <project_root>

Inputs  (under <project_root>/sessions/<car_id>/):
  - chunks.jsonl                : JSONL [{"id", "start", "end", "text"}, ...]
  - LV_<car_id>PVVP.txt         : allow-list (LV, non-TT, one per line)
  - budget_report.json          : per-chunk allowed true|false
  - config/mapper_preset.json   : model and params (created with defaults if missing)
  - prompts/strict_mapper_system_lv.md (created if missing)
  - prompts/strict_mapper_user_lv.md   (created if missing)

Outputs (under session folder):
  - mapper_chunk_<id>.json      : {"chunk_id", "mentioned_vars", "evidence"}
  - mapper_all.json             : [ per-chunk objects ... ]
  - mapper_response.json        : raw last model response text (for debug)
  - mapper_chunk_<id>_error.txt : short reason on per-chunk error
  - mapper_debug.txt            : fatal run-level errors

Env:
  - OPENAI_API_KEY must be set.

Determinism:
  - temperature = 0, top_p = 1, closed-world filtering, idempotent overwrites.

Out of scope (L07+):
  - cross-chunk merges; evidence substring verification; Y/N expansion; numbering; CSV exports.
"""

import argparse, sys, traceback
import json
import os
import re
from pathlib import Path
from typing import Dict, List, Any, Tuple, Set
import shutil

from pvvp.temp_utils import make_temp_root, atomic_publish

tmp: Path | None = None


def write_err(tmp: Path, name: str, tb: str):
    try:
        (tmp / "logs").mkdir(parents=True, exist_ok=True)
        (tmp / "logs" / f"{name}.err.log").write_text(tb, encoding="utf-8")
    except Exception:
        pass

# Use requests to minimize dependency/version drift.
try:
    import requests  # type: ignore
except ImportError:
    requests = None


# ----------- Constants & Defaults -----------

DEFAULT_MAPPER_PRESET = {
    "model": "gpt-4.0-mini",
    "temperature": 0,
    "top_p": 1,
    "max_tokens": 600,
    "timeout_seconds": 45,
    "repair_retry": 1,
    "evidence_max_chars": 120
}

SYSTEM_PROMPT_DEFAULT = """Tu esi stingrs PVVP kartētājs slēgtā pasaulē (tikai no dotā saraksta).
Mērķis: identificēt TEKSTĀ skaidrus pieminējumus PVVP mainīgajiem no dotā saraksta, atļaujot viennozīmīgus sinonīmus/saīsinājumus/virspusējas variācijas, bet IZVADĒ atgriezt tikai precīzus saraksta nosaukumus.

Noteikumi:

SLĒGTĀ PASAULE: Atzīmē tikai tos mainīgos, kas IR dotajā PVVP sarakstā. Nekādus jaunus nosaukumus.

SADERĪGUMS:
• Pieņem locījumu, rakstzīmju (diakritiku), atstarpju/defišu un lielo/mazo burtu variācijas.
• Pieņem nozarē tipiskus sinonīmus/saīsinājumus, ja tie VIENNOZĪMĪGI atbilst tieši vienam saraksta nosaukumam (piem., “AT”, “automātiskā kārba” → “Automātiskā pārnesumkārba”; “A/C”, “kondicionieris” → attiecīgais kondicionēšanas mainīgais; “ABS” → “ABS bremžu sistēma”). Ja ir vairāku iespējamu atbilsmi — IZLAID.
• Daudzskaitlis var nozīmēt vairākus mainīgos (piem., “Apsildāmi priekšējie sēdekļi” → vadītājs + pasažieris), ja teksts NE norāda pretējo.

NEGĀCIJU/IZŅĒMUMU FILTRS: ja formulējums ir noliegums/izslēgšana (“nav”, “bez”, “nepieejams”, “–”), NEATZĪMĒ to kā pozitīvu pieminējumu.

EVIDENCE OBLIGĀTI:
• Katrā “mentioned_vars” vienumam jābūt atbilstošai NE-TUKŠAI “evidence” vērtībai.
• “evidence” ir īss burtisks citāts no TEKSTA (nepārfrāzēts), līdz {EVIDENCE_MAX_CHARS} rakstzīmēm.
• Ja vajag, vari iekļaut 2 īsus citātus vienā “evidence”, atdalot ar “ … ”, lai aptvertu pilnu nozīmi.
• Ja burtisku citātu atrast nevar, šo mainīgo NEIEKĻAUJ.

IZVADES LĪGUMS (tikai JSON, nekā cita):
{"mentioned_vars": ["<precīzs nosaukums>", "..."], "evidence": {"<precīzs nosaukums>": "<burtisks LV citāts(i)>", "...": "..."}}

Ja nav skaidru, viennozīmīgu pieminējumu: atgriez {"mentioned_vars": [], "evidence": {}}.
"""

USER_PROMPT_TEMPLATE = """Uzdevums: No DOTĀ PVVP saraksta atlasīt tos mainīgos, kas ŠAJĀ TEKSTA FRAGMENTĀ ir skaidri un tieši pieminēti.
Atļauti viennozīmīgi sinonīmi/saīsinājumi un rakstības variācijas (locījumi, atstarpes/defises, diakritikas).
IZVADĒ lieto TIKAI precīzus saraksta nosaukumus. Ja pieminējums ir neskaidrs vai var attiekties uz vairākiem nosaukumiem — izlaižam.
Negatīvas formas (“nav”, “bez”, “nepieejams”) neatzīmējam kā pozitīvas.
KATRAM iekļautajam mainīgajam obligāti pievieno NE-TUKŠU burtisku “evidence” citātu no teksta (līdz {EVIDENCE_MAX_CHARS}).

Atgriez tikai JSON pēc līguma.

PVVP saraksts (precīzi nosaukumi):
{PVVP_ARRAY}

Teksts analizēšanai (nepārfrāzē, citē burtiski):
<<<
{TEXT}

Atceries:

slēgtā pasaule (tikai no saraksta),

sinonīmi/saīsinājumi tikai ja VIENNOZĪMĪGI atbilst tieši vienam nosaukumam,

negatīvas formas neatzīmē,

katram minētajam mainīgajam ir NE-TUKŠA “evidence” vērtība,

izeja – tikai JSON ar "mentioned_vars" un "evidence".
"""

REPAIR_SYSTEM_PROMPT = """Tu esi JSON formāta remontētājs.
ATGRIEZ tikai derīgu JSON, kas precīzi atbilst šim līgumam:
{"mentioned_vars": ["<exact name>", "..."], "evidence": {"<same exact name>": "<literal LV snippet(s)>", "...": "..."}}
NEMAINI saturisko nozīmi; NEPIEVIENO jaunus mainīgos; nepārfrāzē “evidence”; tukšas “evidence” jāpaliek tukšām (ja tādas ir).
Tava loma ir tikai salabot formātu/atslēgas/tipus, lai JSON būtu derīgs. Nekādu citu tekstu.
"""

# ----------- Helpers -----------

def read_text(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

def write_text(path: str, text: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)

def read_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def write_json(path: str, obj: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def ensure_file(path: str, content: str) -> None:
    if not os.path.exists(path):
        write_text(path, content)

def load_or_create_preset(config_dir: str) -> Dict[str, Any]:
    os.makedirs(config_dir, exist_ok=True)
    preset_path = os.path.join(config_dir, "mapper_preset.json")
    if not os.path.exists(preset_path):
        write_json(preset_path, DEFAULT_MAPPER_PRESET)
        return DEFAULT_MAPPER_PRESET.copy()
    preset = read_json(preset_path)
    # Fill any missing defaults to be robust
    merged = DEFAULT_MAPPER_PRESET.copy()
    merged.update({k: v for k, v in preset.items() if v is not None})
    return merged

def ensure_prompts(project_root: str, evidence_max: int) -> Tuple[str, str]:
    prompts_dir = os.path.join(project_root, "prompts")
    os.makedirs(prompts_dir, exist_ok=True)
    system_path = os.path.join(prompts_dir, "strict_mapper_system_lv.md")
    user_path = os.path.join(prompts_dir, "strict_mapper_user_lv.md")

    ensure_file(system_path, SYSTEM_PROMPT_DEFAULT)
    ensure_file(user_path, USER_PROMPT_TEMPLATE)

    # Inject evidence limit at runtime (placeholder replacement)
    sys_prompt = read_text(system_path).replace("{EVIDENCE_MAX_CHARS}", str(evidence_max))
    usr_prompt = read_text(user_path)
    return sys_prompt, usr_prompt

def find_allow_list_path(session_dir: str, car_id: str) -> str:
    preferred = os.path.join(session_dir, f"LV_{car_id}PVVP.txt")
    if os.path.exists(preferred):
        return preferred
    # Fallback: first *PVVP.txt in session folder
    for name in os.listdir(session_dir):
        if name.endswith("PVVP.txt"):
            return os.path.join(session_dir, name)
    raise FileNotFoundError("Allow-list file not found (expected LV_<car_id>PVVP.txt).")

def load_allow_list(path: str) -> List[str]:
    lines = []
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            s = raw.strip()
            if s:
                lines.append(s)
    # Preserve order but also expose a set for filtering
    return lines

def load_chunks_jsonl(path: str) -> List[Dict[str, Any]]:
    chunks: List[Dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as f:
        for ln in f:
            s = ln.strip()
            if not s:
                continue
            try:
                obj = json.loads(s)
                # minimal validation
                if not all(k in obj for k in ("id", "text")):
                    continue
                chunks.append(obj)
            except json.JSONDecodeError:
                continue
    if not chunks:
        raise ValueError("No valid chunks found in chunks.jsonl")
    return chunks

def derive_allowed_set(budget_obj: Any) -> Set[int]:
    allowed: Set[int] = set()

    # Common schema 1: {"per_chunk":[{"chunk_id":1,"allowed":true}, ...]}
    if isinstance(budget_obj, dict) and "per_chunk" in budget_obj and isinstance(budget_obj["per_chunk"], list):
        for item in budget_obj["per_chunk"]:
            try:
                cid = int(item.get("chunk_id"))
                if bool(item.get("allowed", False)):
                    allowed.add(cid)
            except Exception:
                continue
        return allowed

    # Common schema 2: {"chunks":{"1":{"allowed":true}, "2":{"allowed":false}}}
    if isinstance(budget_obj, dict) and "chunks" in budget_obj and isinstance(budget_obj["chunks"], dict):
        for k, v in budget_obj["chunks"].items():
            try:
                cid = int(k)
                if isinstance(v, dict) and bool(v.get("allowed", False)):
                    allowed.add(cid)
            except Exception:
                continue
        return allowed

    # Common schema 3: {"allowed_chunks":[1,3,5]}
    if isinstance(budget_obj, dict) and isinstance(budget_obj.get("allowed_chunks"), list):
        for cid in budget_obj["allowed_chunks"]:
            try:
                allowed.add(int(cid))
            except Exception:
                continue
        return allowed

    # Fallback: try a flat dict int->bool
    if isinstance(budget_obj, dict):
        ok_found = False
        for k, v in budget_obj.items():
            try:
                cid = int(k)
                if isinstance(v, bool):
                    ok_found = True
                    if v:
                        allowed.add(cid)
            except Exception:
                continue
        if ok_found:
            return allowed

    raise ValueError("Could not derive allowed chunk set from budget_report.json")

def http_chat_completion(
    api_key: str,
    model: str,
    system_prompt: str,
    user_prompt: str,
    temperature: float,
    top_p: float,
    max_tokens: int,
    timeout_seconds: int,
) -> str:
    if requests is None:
        raise RuntimeError("The 'requests' package is required. Install it via: pip install requests")

    url = "https://api.openai.com/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": temperature,
        "top_p": top_p,
        "max_tokens": max_tokens,
        # Avoid extra knobs for portability. JSON-only behavior is enforced via prompts.
    }
    resp = requests.post(url, headers=headers, json=payload, timeout=timeout_seconds)
    if resp.status_code != 200:
        raise RuntimeError(f"OpenAI API error {resp.status_code}: {resp.text}")
    data = resp.json()
    try:
        return data["choices"][0]["message"]["content"]
    except Exception:
        raise RuntimeError(f"Unexpected API response: {json.dumps(data)[:800]}")

def normalize_output_against_allowlist(
    raw_obj: Any,
    allow_ordered: List[str],
    evidence_max_chars: int,
) -> Dict[str, Any]:
    allow_set = set(allow_ordered)
    if not isinstance(raw_obj, dict):
        raise ValueError("Model output is not a JSON object.")

    mv = raw_obj.get("mentioned_vars", [])
    ev = raw_obj.get("evidence", {})

    # Coerce/validate shapes
    if not isinstance(mv, list):
        mv = []
    mv = [str(x).strip() for x in mv if isinstance(x, (str, int, float))]
    # Preserve original model order but filter by allow-list (exact, trimmed)
    filtered_mv: List[str] = []
    seen: Set[str] = set()
    for name in mv:
        if name in allow_set and name not in seen:
            filtered_mv.append(name)
            seen.add(name)

    # Evidence: keep only for kept vars, coerce str and trim
    if not isinstance(ev, dict):
        ev = {}
    filtered_ev: Dict[str, str] = {}
    for k, v in ev.items():
        k_s = str(k).strip()
        if k_s in seen:
            val = "" if v is None else str(v)
            if evidence_max_chars >= 0:
                val = val[:evidence_max_chars]
            filtered_ev[k_s] = val

    return {"mentioned_vars": filtered_mv, "evidence": filtered_ev}

def build_parser():
    p = argparse.ArgumentParser(description="L06 Mapper", allow_abbrev=False)
    p.add_argument("--project-root", default=None)
    p.add_argument("--session", "--session-id", dest="session", default=None)
    p.add_argument("--workdir", default=None)
    p.add_argument("--keep-workdir", action="store_true")
    p.add_argument("--readonly", action="store_true")
    p.add_argument("--diag", action="store_true")
    return p


def run(
    session: str,
    project_root: Path,
    workdir: Path | None,
    keep_workdir: bool,
    readonly: bool,
    diag: bool = False,
) -> int:
    session_dir = project_root / "sessions" / session
    debug_dest = session_dir / "mapper_debug.txt"
    response_dest = session_dir / "mapper_response.json"
    mapper_all_dest = session_dir / "mapper_all.json"

    temp_root = workdir.resolve() if workdir else make_temp_root()
    if diag:
        print(f"cwd={Path.cwd()}")
        print(f"sys.executable={sys.executable}")
        print(f"sys.path[0:5]={sys.path[:5]}")
        print(f"project_root={project_root.resolve()}")
        print(f"session_dir={session_dir.resolve()}")
        print(f"temp_root={temp_root}")
    try:
        chunks_src = session_dir / "chunks.jsonl"
        if not chunks_src.exists():
            raise FileNotFoundError(f"Missing chunks.jsonl at {chunks_src}. Run L04 first.")
        budget_src = session_dir / "budget_report.json"
        if not budget_src.exists():
            raise FileNotFoundError(f"Missing budget_report.json at {budget_src}")
        allow_src = Path(find_allow_list_path(str(session_dir), session))

        tmp_chunks = temp_root / "input" / "chunks.jsonl"
        tmp_budget = temp_root / "input" / "budget_report.json"
        tmp_allow = temp_root / "input" / allow_src.name
        shutil.copy2(chunks_src, tmp_chunks)
        shutil.copy2(budget_src, tmp_budget)
        shutil.copy2(allow_src, tmp_allow)
        if readonly:
            for p in (tmp_chunks, tmp_budget, tmp_allow):
                try:
                    os.chmod(p, 0o444)
                except Exception:
                    pass

        allow_list = load_allow_list(str(tmp_allow))
        if not allow_list:
            raise ValueError("Allow-list is empty.")
        cfg_dir = project_root / "config"
        preset = load_or_create_preset(str(cfg_dir))
        sys_prompt, user_template = ensure_prompts(str(project_root), preset.get("evidence_max_chars", 120))
        api_key = os.environ.get("OPENAI_API_KEY", "").strip()
        if not api_key:
            raise EnvironmentError("OPENAI_API_KEY is not set.")
        chunks = load_chunks_jsonl(str(tmp_chunks))
        budget_obj = read_json(str(tmp_budget))
        allowed_set = derive_allowed_set(budget_obj)
        pvvparr_json = json.dumps(allow_list, ensure_ascii=False, indent=0)

        processed: List[Dict[str, Any]] = []
        tmp_all = temp_root / "out" / "mapper_all.json.partial"
        write_json(str(tmp_all), processed)
        atomic_publish(tmp_all, mapper_all_dest)

        for ch in chunks:
            cid = int(ch["id"])
            if cid not in allowed_set:
                dest = session_dir / f"mapper_chunk_{cid}.json"
                if dest.exists():
                    try:
                        dest.unlink()
                    except Exception:
                        pass
                continue

            text = ch.get("text", "")
            user_prompt = user_template.replace("{PVVP_ARRAY}", pvvparr_json).replace("{TEXT}", text)

            raw = http_chat_completion(
                api_key=api_key,
                model=str(preset.get("model", DEFAULT_MAPPER_PRESET["model"])),
                system_prompt=sys_prompt,
                user_prompt=user_prompt,
                temperature=float(preset.get("temperature", 0)),
                top_p=float(preset.get("top_p", 1)),
                max_tokens=int(preset.get("max_tokens", 600)),
                timeout_seconds=int(preset.get("timeout_seconds", 45)),
            )

            tmp_resp = temp_root / "out" / "mapper_response.json.partial"
            write_text(str(tmp_resp), raw)
            atomic_publish(tmp_resp, response_dest)

            parsed = None
            try:
                parsed = json.loads(raw.strip())
            except Exception:
                pass

            if parsed is None and int(preset.get("repair_retry", 1)) > 0:
                repair_user = raw.strip()
                raw2 = http_chat_completion(
                    api_key=api_key,
                    model=str(preset.get("model", DEFAULT_MAPPER_PRESET["model"])),
                    system_prompt=REPAIR_SYSTEM_PROMPT,
                    user_prompt=repair_user,
                    temperature=0,
                    top_p=1,
                    max_tokens=int(preset.get("max_tokens", 600)),
                    timeout_seconds=int(preset.get("timeout_seconds", 45)),
                )
                write_text(str(tmp_resp), raw2)
                atomic_publish(tmp_resp, response_dest)
                try:
                    parsed = json.loads(raw2.strip())
                except Exception:
                    parsed = None

            out_err_tmp = temp_root / "out" / f"mapper_chunk_{cid}_error.txt.partial"
            out_chunk_tmp = temp_root / "out" / f"mapper_chunk_{cid}.json.partial"

            if parsed is None:
                write_text(str(out_err_tmp), "Invalid JSON after one repair attempt.")
                atomic_publish(out_err_tmp, session_dir / f"mapper_chunk_{cid}_error.txt")
                dest_chunk = session_dir / f"mapper_chunk_{cid}.json"
                if dest_chunk.exists():
                    try:
                        dest_chunk.unlink()
                    except Exception:
                        pass
                continue

            normalized = normalize_output_against_allowlist(
                parsed, allow_list, int(preset.get("evidence_max_chars", 120))
            )
            result_obj = {
                "chunk_id": cid,
                "mentioned_vars": normalized["mentioned_vars"],
                "evidence": normalized["evidence"],
            }
            write_json(str(out_chunk_tmp), result_obj)
            atomic_publish(out_chunk_tmp, session_dir / f"mapper_chunk_{cid}.json")
            processed.append(result_obj)
            err_dest = session_dir / f"mapper_chunk_{cid}_error.txt"
            if err_dest.exists():
                try:
                    err_dest.unlink()
                except Exception:
                    pass

        tmp_all = temp_root / "out" / "mapper_all.json.partial"
        write_json(str(tmp_all), processed)
        atomic_publish(tmp_all, mapper_all_dest)
        if debug_dest.exists():
            try:
                debug_dest.unlink()
            except Exception:
                pass
        if not keep_workdir:
            shutil.rmtree(temp_root, ignore_errors=True)
        return 0
    except Exception:
        tb = traceback.format_exc()
        write_err(temp_root, "L06", tb)
        tmp_debug = temp_root / "out" / "mapper_debug.txt.partial"
        try:
            write_text(str(tmp_debug), tb.splitlines()[-1])
            atomic_publish(tmp_debug, debug_dest)
        except Exception:
            pass
        print(tb, file=sys.stderr)
        print(f"Workdir preserved at: {temp_root}", file=sys.stderr)
        return 1


def main() -> int:
    global tmp
    parser = build_parser()
    args, unknown = parser.parse_known_args()
    if unknown:
        print(f"[L06] Ignoring unknown args: {unknown}")
    if not args.project_root or not args.session:
        parser.error("--project-root and --session are required")
    project_root = Path(args.project_root).resolve()
    tmp = Path(args.workdir).resolve() if args.workdir else None
    return run(args.session, project_root, tmp, args.keep_workdir, args.readonly, args.diag)


if __name__ == "__main__":
    try:
        sys.exit(main() or 0)
    except Exception:
        tb = traceback.format_exc()
        try:
            write_err(locals().get("tmp", Path(".")), "L06", tb)
        except Exception:
            pass
        print(tb, file=sys.stderr)
        sys.exit(1)
