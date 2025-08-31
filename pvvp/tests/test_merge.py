import json
from pathlib import Path

from pvvp import L07_merge


def make_session(tmp_path: Path) -> Path:
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
    with (session_dir / f"LV_{session}PVVP.txt").open("w", encoding="utf-8") as f:
        for name in allow_names:
            f.write(name + "\n")
    evidence = {
        "Priekšējie lukturi – LED": "LED priekšējie un aizmugurējie lukturi",
        "Priekšējie lukturi – adaptīvie LED ar Matrix vai Glare Free": "Adaptīvie LED priekšējie lukturi ar Matrix un Glare Free",
        "Kruīza kontrole – adaptīvā": "Adaptīvā kruīza kontrole",
        "Stāvvietas palīgs – priekšējie sensori": "Stāvvietā novietošanas sensori priekšā un aizmugurē + atpakaļskata kamera",
        "Stāvvietas palīgs – aizmugurējie sensori": "Stāvvietā novietošanas sensori priekšā un aizmugurē + atpakaļskata kamera",
        "Apsildāms stūres rats": "3-spieķu sporta stūre ar apsildi",
        "Durvju spoguļi - elektriski/sildāmi/salokāmi": "Elektriski regulējami, nolokāmi, apsildāmi sānu spoguļi",
    }
    with (session_dir / "mapper_chunk_1.json").open("w", encoding="utf-8") as f:
        json.dump({"chunk_id": 1, "mentioned_vars": list(evidence.keys()), "evidence": evidence}, f, ensure_ascii=False)
    return session_dir


def test_merge_produces_mentions(tmp_path):
    session_dir = make_session(tmp_path)
    session = session_dir.name
    root = tmp_path
    exit_code = L07_merge.main(["--session", session, "--project-root", str(root)])
    assert exit_code == 0
    result_path = session_dir / "merge_result.json"
    with result_path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    for name in [
        "Priekšējie lukturi – LED",
        "Priekšējie lukturi – adaptīvie LED ar Matrix vai Glare Free",
        "Kruīza kontrole – adaptīvā",
        "Stāvvietas palīgs – priekšējie sensori",
        "Stāvvietas palīgs – aizmugurējie sensori",
        "Apsildāms stūres rats",
        "Durvju spoguļi - elektriski/sildāmi/salokāmi",
    ]:
        assert name in data["mentioned_vars"]
