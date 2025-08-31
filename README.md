# PVVP / CSCOPIED Matcher — Milestone 1

## 1) Install
```sh
pip install -e .
```

## Temp workdir pipeline
Each lego runs inside a temporary directory that mirrors `input/`, `work/`, `out/`, and `logs/`. Inputs are copied into `input/`, processing occurs in `work/`, and results are staged in `out/` then published atomically to the session folder.

Example:
```sh
python -m pvvp.L03_normalize --session EV --project-root /path/to/pvvp --readonly --keep-workdir
python -m pvvp.L04_chunker --session EV --project-root /path/to/pvvp --readonly --keep-workdir
```

Develop on a local feature branch and push it to a remote branch before merging. Opening a pull request from your feature branch ensures the remote history remains clean and reviewable.
