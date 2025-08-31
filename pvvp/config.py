from __future__ import annotations
from pathlib import Path
from typing import Optional
from pydantic import BaseModel, Field, ValidationError
import yaml


class VehiclePaths(BaseModel):
	MCAICE: Path
	MCAFHEV: Path
	MCAPHEV: Path
	EV: Path
	BEV: Path


class AppConfig(BaseModel):
	masterlists: VehiclePaths = Field(..., description="Paths to masterlist CSVs by vehicle type")
output_dir: Path = Field(default=Path("runs"))
chunk_size: int = Field(default=10, ge=1)
csv_delimiter: str = Field(default=",")
csv_encoding: str = Field(default="utf-8")
keep_tt_rows: bool = Field(default=True)
skip_tt_for_matching: bool = Field(default=True)


@classmethod
def from_yaml(cls, path: Path) -> "AppConfig":
	with Path(path).open("r", encoding="utf-8") as f:
		data = yaml.safe_load(f)
	try:
		cfg = cls.model_validate(data)
	except ValidationError as e:
		# re-raise with path info for easier debugging
		raise SystemExit(f"Config validation error in {path}:\n{e}")
	return cfg


def to_yaml(self, path: Path) -> None:
	path.parent.mkdir(parents=True, exist_ok=True)
	with Path(path).open("w", encoding="utf-8") as f:
		yaml.safe_dump(self.model_dump(), f, sort_keys=False, allow_unicode=True)