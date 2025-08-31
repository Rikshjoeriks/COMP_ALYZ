import csv
import json
from pathlib import Path

from pvvp import L07_merge


def make_session(tmp_path: Path) -> tuple[Path, list[str]]:
    session = "TEST"
    session_dir = tmp_path / "sessions" / session
    session_dir.mkdir(parents=True)
    text = (
        "Adaptīvie LED priekšējie lukturi ar Matrix un Glare Free. "
        "LED priekšējie un aizmugurējie lukturi. "
        "Adaptīvā kruīza kontrole. "
        "Stāvvietā novietošanas sensori priekšā un aizmugurē + atpakaļskata kamera. "
        "3-spieķu sporta stūre ar apsildi. "
        "Elektriski regulējami, nolokāmi, apsildāmi sānu spoguļi."
    )
    with (session_dir / "chunks.jsonl").open("w", encoding="utf-8") as f:
        f.write(json.dumps({"id": 1, "text": text}, ensure_ascii=False) + "\n")
    with (session_dir / "budget_report.json").open("w", encoding="utf-8") as f:
        json.dump({"allowed_chunks": [1]}, f)
    allow_names = [
        "Priekšējie lukturi – LED",
        "Priekšējie lukturi – adaptīvie LED ar Matrix vai Glare Free",
        "Kruīza kontrole – adaptīvā",
        "Stāvvietas palīgs – priekšējie sensori",
        "Stāvvietas palīgs – aizmugurējie sensori",
        "Apsildāms stūres rats",
        "Durvju spoguļi - elektriski/sildāmi/salokāmi",
    ]
    allow_nrs: list[str] = []
    with (session_dir / "pvvp_master.csv").open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["nr_code", "variable name", "is_tt"])
        for i, name in enumerate(allow_names, start=1):
            nr = f"NR{i}"
            allow_nrs.append(nr)
            writer.writerow([nr, name, ""])
    with (session_dir / f"LV_{session}PVVP.txt").open("w", encoding="utf-8") as f:
        for nr in allow_nrs:
            f.write(nr + "\n")
    evidence_map = {
        "NR1": "LED priekšējie un aizmugurējie lukturi",
        "NR2": "Adaptīvie LED priekšējie lukturi ar Matrix un Glare Free",
        "NR3": "Adaptīvā kruīza kontrole",
        "NR4": "Stāvvietā novietošanas sensori priekšā un aizmugurē + atpakaļskata kamera",
        "NR5": "Stāvvietā novietošanas sensori priekšā un aizmugurē + atpakaļskata kamera",
        "NR6": "3-spieķu sporta stūre ar apsildi",
        "NR7": "Elektriski regulējami, nolokāmi, apsildāmi sānu spoguļi",
    }
    results = [{"nr": nr, "verdict": "Jā", "match": ev} for nr, ev in evidence_map.items()]
    with (session_dir / "mapper_chunk_1.json").open("w", encoding="utf-8") as f:
        json.dump({"chunk_id": 1, "results": results}, f, ensure_ascii=False)
    return session_dir, allow_nrs


def test_merge_produces_mentions(tmp_path):
    session_dir, allow_nrs = make_session(tmp_path)
    session = session_dir.name
    root = tmp_path
    exit_code = L07_merge.main(["--session", session, "--project-root", str(root)])
    assert exit_code == 0
    result_path = session_dir / "merge_result.json"
    with result_path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    for nr in allow_nrs:
        assert nr in data
        assert data[nr]["evidence"]
