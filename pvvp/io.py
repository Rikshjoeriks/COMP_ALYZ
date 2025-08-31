from __future__ import annotations
from pathlib import Path
from typing import Tuple, Dict, Optional
import logging
import pandas as pd


# Heuristic column resolvers so we tolerate slightly different headers
_ALIAS_MAP = {
"nr_code": ["nr code", "nr", "nr_code", "code nr", "nr. code"],
"variable_name": ["variable name", "variable", "variable_name", "main variable", "name"],
"full_key": ["full key", "full_key", "pilna atslēga", "pilna atslega"],
"section_tt": ["section tt", "tt", "section", "tt section"],
}


class LoadResult(pd.DataFrame):
	"""Just to hint types; behaves as a DataFrame."""
	pass


def _normalize_cols(cols) -> Dict[str, str]:
	lowered = {c: str(c).strip().lower() for c in cols}
	resolved: Dict[str, str] = {}
	for std, aliases in _ALIAS_MAP.items():
		for c, lc in lowered.items():
			if lc in aliases and std not in resolved:
				resolved[std] = c
				break
	return resolved




def _find_or_fail(resolved: Dict[str, str], key: str) -> str:
	if key not in resolved:
		raise SystemExit(
			f"Required column not found: {key!r}. Ensure your headers resemble: {', '.join(_ALIAS_MAP[key])}."
		)
	return resolved[key]



def _try_read_csv(path: Path, delimiter: str, preferred_encoding: str) -> pd.DataFrame:
	"""Try UTF-8 first, then fall back to common encodings, re-save to UTF-8 in run dir later."""
	candidates = [preferred_encoding, "utf-8-sig", "cp1257", "cp1252", "latin-1"]
	last_err: Optional[Exception] = None
	for enc in candidates:
		try:
			df = pd.read_csv(path, encoding=enc, sep=delimiter, dtype=str, keep_default_na=False)
			logging.info("Loaded %s with encoding=%s", path, enc)
			return df
		except Exception as e:
			last_err = e
			continue
	raise SystemExit(f"Failed to read {path} with encodings {candidates}: {last_err}")



def load_masterlist(
	path: Path,
	delimiter: str = ",",
	preferred_encoding: str = "utf-8",
) -> Tuple[LoadResult, str, str]:
	"""
	Returns: (df, nr_code_col, variable_name_col)
	- Does *not* mutate numbering; adds helper boolean column: __is_tt
	- Keeps all rows; TT rows flagged via empty Variable Name
	"""
	df = _try_read_csv(path, delimiter, preferred_encoding)
	resolved = _normalize_cols(df.columns)
	nr_col = _find_or_fail(resolved, "nr_code")
	var_col = _find_or_fail(resolved, "variable_name")

	# Mark TT rows: empty or whitespace variable name
	is_tt = df[var_col].astype(str).str.strip().eq("")
	df["__is_tt"] = is_tt

	# Keep original order & numbers intact; no renumbering
	logging.info("Masterlist rows: total=%d, TT=%d, real=%d", len(df), int(is_tt.sum()), int((~is_tt).sum()))
	return LoadResult(df), nr_col, var_col




def write_subset_artifacts(df: pd.DataFrame, run_dir: Path, base_name: str = "masterlist") -> None:
	run_dir.mkdir(parents=True, exist_ok=True)
	raw_path = run_dir / f"{base_name}.raw.csv"
	tt_path = run_dir / f"{base_name}.tt.csv"
	real_path = run_dir / f"{base_name}.real.csv"

	df.to_csv(raw_path, index=False, encoding="utf-8")
	df[df["__is_tt"]].to_csv(tt_path, index=False, encoding="utf-8")
	df[~df["__is_tt"]].to_csv(real_path, index=False, encoding="utf-8")

	logging.info("Artifacts written: %s | %s | %s", raw_path, tt_path, real_path)