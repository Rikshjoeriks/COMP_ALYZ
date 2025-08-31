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

import argparse
import json
import os
import re
import sys
import traceback
from typing import Dict, List, Any, Tuple, Set

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

def main() -> int:
    parser = argparse.ArgumentParser(description="L06.Mapper.StrictGPT")
    parser.add_argument("--session", required=True, help="Car/session id, e.g., MCAFHEV")
    parser.add_argument("--project-root", required=True, help="Project root folder")
    args = parser.parse_args()

    project_root = os.path.abspath(args.project_root)
    session_id = args.session
    session_dir = os.path.join(project_root, "sessions", session_id)

    debug_path = os.path.join(session_dir, "mapper_debug.txt")
    last_response_path = os.path.join(session_dir, "mapper_response.json")
    mapper_all_path = os.path.join(session_dir, "mapper_all.json")

    try:
        # Basic existence checks
        chunks_path = os.path.join(session_dir, "chunks.jsonl")
        if not os.path.exists(chunks_path):
            raise FileNotFoundError(f"Missing chunks.jsonl at {chunks_path}")

        budget_path = os.path.join(session_dir, "budget_report.json")
        if not os.path.exists(budget_path):
            raise FileNotFoundError(f"Missing budget_report.json at {budget_path}")

        allow_list_path = find_allow_list_path(session_dir, session_id)
        allow_list = load_allow_list(allow_list_path)

        if not allow_list:
            raise ValueError("Allow-list is empty.")

        # Config & prompts
        cfg_dir = os.path.join(project_root, "config")
        preset = load_or_create_preset(cfg_dir)
        sys_prompt, user_template = ensure_prompts(project_root, preset.get("evidence_max_chars", 120))

        # API key
        api_key = os.environ.get("OPENAI_API_KEY", "").strip()
        if not api_key:
            raise EnvironmentError("OPENAI_API_KEY is not set.")

        # Load inputs
        chunks = load_chunks_jsonl(chunks_path)
        budget_obj = read_json(budget_path)
        allowed_set = derive_allowed_set(budget_obj)

        # Build PVVP array for the user prompt
        pvvparr_json = json.dumps(allow_list, ensure_ascii=False, indent=0)

        processed: List[Dict[str, Any]] = []
        # Overwrite mapper_all.json every run for idempotency (even if empty)
        write_json(mapper_all_path, processed)

        # Iterate over chunks in file order, process only allowed=true
        for ch in chunks:
            cid = int(ch["id"])
            if cid not in allowed_set:
                # Respect budget: skip and ensure any old per-chunk output is gone
                out_chunk_path = os.path.join(session_dir, f"mapper_chunk_{cid}.json")
                if os.path.exists(out_chunk_path):
                    try:
                        os.remove(out_chunk_path)
                    except Exception:
                        pass
                continue

            text = ch.get("text", "")
            # Compose user prompt
            user_prompt = user_template.replace("{PVVP_ARRAY}", pvvparr_json).replace("{TEXT}", text)

            # Call model
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

            # Always write last raw response (debug)
            write_text(last_response_path, raw)

            # Try parse
            parsed = None
            try:
                parsed = json.loads(raw.strip())
            except Exception:
                pass

            # Repair retry if needed
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
                write_text(last_response_path, raw2)  # update to last raw
                try:
                    parsed = json.loads(raw2.strip())
                except Exception:
                    parsed = None

            out_err_path = os.path.join(session_dir, f"mapper_chunk_{cid}_error.txt")
            out_chunk_path = os.path.join(session_dir, f"mapper_chunk_{cid}.json")

            if parsed is None:
                # Write short error and skip this chunk
                write_text(out_err_path, "Invalid JSON after one repair attempt.")
                # Ensure no stale success file remains
                if os.path.exists(out_chunk_path):
                    try:
                        os.remove(out_chunk_path)
                    except Exception:
                        pass
                continue
            else:
                # Clean/Filter against allow-list
                normalized = normalize_output_against_allowlist(
                    parsed, allow_list, int(preset.get("evidence_max_chars", 120))
                )
                result_obj = {
                    "chunk_id": cid,
                    "mentioned_vars": normalized["mentioned_vars"],
                    "evidence": normalized["evidence"],
                }
                write_json(out_chunk_path, result_obj)
                processed.append(result_obj)
                # Remove any old error file on success
                if os.path.exists(out_err_path):
                    try:
                        os.remove(out_err_path)
                    except Exception:
                        pass

        # Write combined (in order of processing)
        write_json(mapper_all_path, processed)

        return 0

    except Exception as e:
        tb = traceback.format_exc(limit=5)
        write_text(debug_path, f"{str(e)}\n\n{tb}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
