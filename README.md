# PVVP / CSCOPIED Matcher — Milestone 1

## 1) Install
```sh
pip install -e .
```

## Temp workdir pipeline
Each lego runs inside a temporary directory that mirrors `input/`, `work/`, `out/`, and `logs/`. Inputs are copied into `input/`, processing occurs in `work/`, and results are staged in `out/` then published atomically to the session folder.

Example:
```sh
python pvvp/L03_normalize.py --session EV --project-root /path/to/pvvp --readonly --keep-workdir
python pvvp/L04_chunker.py --session EV --project-root /path/to/pvvp --readonly --keep-workdir
```
