# pvvp/ui_review.py
import os, json, argparse, sys
from pathlib import Path

try:
    import streamlit as st
except Exception as e:
    print("streamlit is not installed. pip install streamlit", file=sys.stderr)
    raise

def read_json(path):
    with open(path,'r',encoding='utf-8') as f: return json.load(f)
def write_json(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path,'w',encoding='utf-8') as f: json.dump(obj,f,ensure_ascii=False,indent=2)

def main(session: str, project_root: str):
    root = os.path.abspath(project_root)
    sdir = os.path.join(root, "sessions", session)
    cand_path = os.path.join(sdir, "review_candidates.json")
    dec_path  = os.path.join(sdir, "review_decisions.json")

    st.set_page_config(page_title=f"PVVP Review — {session}", layout="wide")
    st.title(f"PVVP Mapper Review — {session}")

    if not os.path.exists(cand_path):
        st.error(f"Candidates not found: {cand_path}. Run L06 with --prepare first.")
        st.stop()

    data = read_json(cand_path)
    items = data.get("candidates", [])

    groups = {}
    for it in items:
        groups.setdefault(int(it.get("chunk_id",0)), []).append(it)

    approved = []
    with st.form("review_form", clear_on_submit=False):
        st.write(f"Total candidates: {len(items)}")
        approve_all = st.checkbox("Approve all visible")

        for cid in sorted(groups.keys()):
            st.subheader(f"Chunk {cid}")
            for it in groups[cid]:
                key = f"{cid}:{it['name']}"
                cols = st.columns([0.6, 0.25, 0.15])
                with cols[0]:
                    st.markdown(f"**{it['name']}**")
                    ev = (it.get("evidence") or "").strip()
                    if ev:
                        st.code(ev, language="text")
                with cols[1]:
                    note_default = it.get("note","")
                    note = st.text_input("Note (optional)", value=note_default, key=f"note:{key}")
                with cols[2]:
                    ok = st.checkbox("Approve", value=approve_all, key=f"ok:{key}")
                if ok:
                    rec = {"chunk_id": cid, "name": it["name"], "evidence": it.get("evidence","")}
                    if note:
                        rec["note"] = note
                    approved.append(rec)

        saved = st.form_submit_button("💾 Save decisions")
        if saved:
            write_json(dec_path, {"approved": approved})
            st.success(f"Saved {len(approved)} approvals → {dec_path}")

    st.caption("Re-open and re-save anytime. Merge with L06 --merge afterwards.")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--session", required=True)
    ap.add_argument("--project-root", required=True)
    args = ap.parse_args()
    main(args.session, args.project_root)
