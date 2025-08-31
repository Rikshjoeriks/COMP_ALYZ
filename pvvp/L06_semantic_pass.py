#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Second pass (semantic mode) for PVVP mapping — with optional interactive review & merge.

New flags:
  --interactive            : terminal Y/N/W review loop (no HTML needed)
  --approve-positives      : auto-approve 'Yes' items; only ask for 'Warning'
  --temperature 0.65       : override config temperature at runtime
  --write-merged           : write mapper_all_merged.json (strict + approved)

Usage examples:
  python L06_semantic_pass.py --session MCAFHEV --project-root .
  python L06_semantic_pass.py --session MCAFHEV --project-root . --interactive --approve-positives --write-merged --temperature 0.6
"""
import os, json, argparse, traceback, sys
from typing import Any, Dict, List, Set

try:
    import requests
except Exception:
    requests = None

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

def http_chat(api_key, model, system, user, max_tokens=1500, temperature=0.9, top_p=1.0, timeout=60):
    if requests is None:
        raise RuntimeError("pip install requests")
    url = "https://api.openai.com/v1/chat/completions"
    payload = {
        "model": model,
        "messages":[{"role":"system","content":system},{"role":"user","content":user}],
        "temperature": float(temperature),
        "top_p": float(top_p),
        "max_tokens": int(max_tokens)
    }
    r = requests.post(
        url,
        headers={"Authorization":f"Bearer {api_key}","Content-Type":"application/json"},
        json=payload, timeout=int(timeout)
    )
    if r.status_code!=200:
        raise RuntimeError(f"API {r.status_code}: {r.text[:300]}")
    data = r.json()
    return data["choices"][0]["message"]["content"]

def load_allow_list(session_dir: str) -> List[str]:
    allow_path = None
    for name in os.listdir(session_dir):
        if name.endswith("PVVP.txt"):
            allow_path = os.path.join(session_dir, name); break
    if not allow_path:
        raise FileNotFoundError("Allow-list *PVVP.txt not found in session dir")
    return [ln.strip() for ln in read(allow_path).splitlines() if ln.strip()]

def load_chunks(chunks_path: str) -> List[Dict[str,Any]]:
    out=[]
    with open(chunks_path,'r',encoding='utf-8') as f:
        for ln in f:
            s=ln.strip()
            if not s: continue
            try:
                obj=json.loads(s)
                if "id" in obj: out.append(obj)
            except: pass
    if not out: raise ValueError("No chunks in chunks.jsonl")
    return out

def derive_allowed_ids(budget_path: str, chunks: List[Dict[str,Any]]) -> Set[int]:
    if not os.path.exists(budget_path):
        return {int(ch["id"]) for ch in chunks}
    try:
        bud = read_json(budget_path)
        allowed=set()
        if isinstance(bud, dict) and "per_chunk" in bud:
            for it in bud["per_chunk"]:
                if it.get("allowed"): allowed.add(int(it["chunk_id"]))
        elif isinstance(bud, dict) and "chunks" in bud:
            for k,v in bud["chunks"].items():
                if v.get("allowed"): allowed.add(int(k))
        if not allowed:
            allowed = {int(ch["id"]) for ch in chunks}
        return allowed
    except:
        return {int(ch["id"]) for ch in chunks}

def interactive_review(candidates: List[Dict[str,Any]], approve_positives: bool) -> List[Dict[str,Any]]:
    """
    Terminal loop:
      For each candidate: show chunk_id, status, name, evidence, optional model note.
      Input: [y]es / [n]o / [w]arning-with-note / [s]kip / [q]uit
      Returns: list with approved flag + optional review_note.
    """
    print("\n=== Interaktīvais pārskats ===")
    print("Komandas: y=apstiprināt  n=noraidīt  w=apstipr.+piezīme  s=izlaist  q=iziet\n")
    reviewed=[]
    for i,c in enumerate(candidates, start=1):
        auto = False  # Always require manual review
        if auto:
            print(f"[AUTO] {i}/{len(candidates)} Yes  #{c['chunk_id']}  {c['name']}")
            c2 = dict(c)
            c2["approved"]=True
            c2["review_note"]=""
            reviewed.append(c2)
            continue

        print(f"{i}/{len(candidates)}  status={c.get('status')}  chunk={c.get('chunk_id')}  name={c.get('name')}")
        print(f"Evidence: \"{c.get('evidence','')}\"")
        if c.get("note"):
            print(f"Model note: {c['note']}")
        while True:
            choice = input("[y/n/w/s/q] > ").strip().lower()
            if choice in ("y","n","w","s","q"):
                break
        if choice=="q":
            print("Pārskatīšana pārtraukta pēc lietotāja pieprasījuma.")
            break
        if choice=="s":
            continue
        c2 = dict(c)
        if choice=="y":
            c2["approved"]=True; c2["review_note"]=""
        elif choice=="n":
            c2["approved"]=False; c2["review_note"]=""
        elif choice=="w":
            c2["approved"]=True
            c2["review_note"]=input("Ievadi īsu piezīmi: ").strip()
        reviewed.append(c2)
    print("=== Gatavs ===\n")
    return reviewed

def merge_into_mapper_all(strict_all_path: str, approved: List[Dict[str,Any]], out_path: str):
    """
    Merge approved candidates back into strict structure:
      mapper_all.json is a list of {chunk_id, mentioned_vars, evidence:{}}.
      We append names/evidence to their chunks (avoid duplicates).
    """
    base = read_json(strict_all_path) if os.path.exists(strict_all_path) else []
    # index by chunk_id
    by_id: Dict[int, Dict[str,Any]] = {}
    for obj in base:
        by_id[int(obj["chunk_id"])] = obj
        obj.setdefault("mentioned_vars", [])
        obj.setdefault("evidence", {})

    for it in approved:
        if not it.get("approved"): continue
        cid = int(it["chunk_id"]); name = it["name"]; ev = it.get("evidence","")
        rec = by_id.setdefault(cid, {"chunk_id": cid, "mentioned_vars": [], "evidence": {}})
        if name not in rec["mentioned_vars"]:
            rec["mentioned_vars"].append(name)
        # if name already in evidence, keep existing; else add
        if name not in rec["evidence"] and ev:
            rec["evidence"][name] = ev

    # Sort by chunk_id to keep determinism
    merged = [by_id[k] for k in sorted(by_id.keys())]
    write_json(out_path, merged)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--session", required=True)
    ap.add_argument("--project-root", required=True)
    ap.add_argument("--interactive", action="store_true", help="Terminal review (no HTML)")
    ap.add_argument("--approve-positives", action="store_true", help="Auto-approve status=Yes")
    ap.add_argument("--write-merged", action="store_true", help="Write mapper_all_merged.json")
    ap.add_argument("--temperature", type=float, default=None, help="Override temperature for this run")
    ap.add_argument("--retry-if-empty", type=int, default=0, help="Retry N times if a chunk returns no results")
    args = ap.parse_args()

    root = os.path.abspath(args.project_root)
    ses  = args.session
    sdir = os.path.join(root, "sessions", ses)
    debug = os.path.join(sdir, "semantic_debug.txt")

    try:
        chunks_path = os.path.join(sdir, "chunks.jsonl")
        budget_path = os.path.join(sdir, "budget_report.json")
        strict_all  = os.path.join(sdir, "mapper_all.json")

        allow_list = load_allow_list(sdir)
        allow_set  = set(allow_list)

        strict = read_json(strict_all) if os.path.exists(strict_all) else []
        confirmed: Set[str] = set()
        by_chunk_confirmed: Dict[int, Set[str]] = {}
        for itm in strict:
            cid = int(itm["chunk_id"])
            mvs = itm.get("mentioned_vars", [])
            by_chunk_confirmed.setdefault(cid, set()).update(mvs)
            confirmed.update(mvs)

        # config
        cfg = os.path.join(root, "config", "mapper_preset.json")
        preset = {"model":"gpt-4o-mini","temperature":0.6,"top_p":1,"max_tokens":1500,"timeout_seconds":60,"evidence_max_chars":400}
        if os.path.exists(cfg):
            try: preset.update(read_json(cfg) or {})
            except: pass
        if args.temperature is not None:
            preset["temperature"] = float(args.temperature)

        sys_path = os.path.join(root, "prompts", "semantic_mapper_system_lv.md")
        usr_path = os.path.join(root, "prompts", "semantic_mapper_user_lv.md")
        sys_prompt = read(sys_path).replace("{EVIDENCE_MAX_CHARS}", str(preset.get("evidence_max_chars", 400)))
        user_tpl   = read(usr_path)

        # optional hints block
        hints_path = os.path.join(root, "config", "synonyms_juke.json")
        hints_lines=[]
        if os.path.exists(hints_path):
            try:
                h = read_json(hints_path)
                for k,v in h.items():
                    if v in allow_set:
                        hints_lines.append(f'- "{k}" → "{v}"')
            except: pass
        hints_block = "\n".join(hints_lines) if hints_lines else "(nav norāžu)"

        # chunks + budget
        chunks = load_chunks(chunks_path)
        allowed_ids = derive_allowed_ids(budget_path, chunks)

        api_key = os.environ.get("OPENAI_API_KEY","").strip()
        if not api_key: 
            raise EnvironmentError("OPENAI_API_KEY is not set")

        review_items: List[Dict[str,Any]] = []
        pvvp_json = json.dumps(allow_list, ensure_ascii=False, indent=0)

        for ch in chunks:
            cid = int(ch["id"])
            if cid not in allowed_ids: 
                continue
            already = by_chunk_confirmed.get(cid, set())
            txt = ch.get("text","")

            user_prompt = (user_tpl
                .replace("{PVVP_ARRAY}", pvvp_json)
                .replace("{HINTS}", hints_block)
                .replace("{TEXT}", txt)
            )

            tries = 1 + args.retry_if_empty
            for attempt in range(tries):
                raw = http_chat(api_key, 
                                model=str(preset.get("model","gpt-4o-mini")),
                                system=sys_prompt,
                                user=user_prompt,
                                max_tokens=int(preset.get("max_tokens",1500)),
                                temperature=float(preset.get("temperature",0.6)),
                                top_p=float(preset.get("top_p",1)),
                                timeout=int(preset.get("timeout_seconds",60)))
                write(os.path.join(sdir, f"semantic_response_{cid}.txt"), raw)
                try:
                    obj = json.loads(raw.strip())
                except:
                    obj = None

                pos = obj.get("positives", []) if obj else []
                warn = obj.get("warnings", []) if obj else []

                # If results found or last attempt, break
                if pos or warn or attempt == tries - 1:
                    break

            def clean(lst):
                out=[]
                for it in lst:
                    name = (it.get("name") or "").strip()
                    ev   = (it.get("evidence") or "").strip()
                    note = (it.get("note") or "").strip()
                    if (name in allow_set) and ev and (name not in already):
                        out.append({"chunk_id": cid, "status": "Yes", "name": name, "evidence": ev}) if it is lst[0] else None
                        out.append({"chunk_id": cid, "name": name, "evidence": ev, **({"note":note} if note else {})})
                return out

            # Keep separate tags:
            def pack(lst, status):
                out=[]
                for it in lst:
                    name = (it.get("name") or "").strip()
                    ev   = (it.get("evidence") or "").strip()
                    note = (it.get("note") or "").strip()
                    if (name in allow_set) and ev and (name not in already):
                        item = {"chunk_id": cid, "status": status, "name": name, "evidence": ev}
                        if note: item["note"]=note
                        out.append(item)
                return out

            review_items.extend(pack(pos, "Yes"))
            review_items.extend(pack(warn, "Warning"))

        # Save raw candidates (even if empty)
        review_json = os.path.join(sdir, "review_candidates.json")
        write_json(review_json, {"candidates": review_items})

        # Optional interactive review
        approved = []
        if args.interactive:
            approved = interactive_review(review_items, approve_positives=args.approve_positives)
            write_json(os.path.join(sdir, "review_decisions.json"), {"approved": approved})
            print(f"Zapīpēts: {len([x for x in approved if x.get('approved')])} apstiprināti, {len(approved)} izskatīti.")
        else:
            # Non-interactive: mark nothing approved; you can edit review_candidates.json manually if desired
            approved = []

        # Optional merge back into a merged file
        if args.write_merged:
            out_merged = os.path.join(sdir, "mapper_all_merged.json")
            merge_into_mapper_all(strict_all, approved, out_merged)
            print(f"Saglabāts: {out_merged}")

        return 0
    except Exception as e:
        write(os.path.join(sdir, "semantic_debug.txt"), f"{e}\n\n{traceback.format_exc(limit=4)}")
        return 1

if __name__=="__main__":
    sys.exit(main())
