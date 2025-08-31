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
python -m pvvp.L06_mapper  --session EV --project-root /path/to/pvvp --readonly --keep-workdir
```

Modules must be run with `-m` so package imports resolve correctly. `L06_mapper` now accepts common flags (`--session/--session-id`, `--project-root`, `--workdir`, `--keep-workdir`, `--readonly`, `--diag`) and safely ignores unknown arguments.

The orchestrator runs each step via module mode, surfaces stderr, and skips optional modules (such as `L09_export_positives` and `L13_summary_finalize`) if they are not present.

Develop on a local feature branch and push it to a remote branch before merging. Opening a pull request from your feature branch ensures the remote history remains clean and reviewable.
