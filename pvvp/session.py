from __future__ import annotations
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
import json
import logging
import uuid
from typing import Optional


from .constants import VehicleType


RUNS_DIR_NAME = "runs"


@dataclass
class Session:
	id: str
	started_at: str
	vehicle_type: VehicleType
	config_path: str
	output_dir: str
	masterlist_path: str
	rows_total: int = 0
	rows_tt: int = 0
	rows_real: int = 0

	@classmethod
	def start(
		cls,
		vehicle_type: VehicleType,
		config_path: Path,
		output_root: Path,
		masterlist_path: Path,
	) -> "Session":
		sid = str(uuid.uuid4())
		started = datetime.now(timezone.utc).isoformat()
		run_dir = output_root / sid
		run_dir.mkdir(parents=True, exist_ok=True)
		# Logging
		log_path = run_dir / "session.log"
		logging.basicConfig(
			level=logging.INFO,
			format="%(asctime)s [%(levelname)s] %(message)s",
			handlers=[
				logging.FileHandler(log_path, encoding="utf-8"),
				logging.StreamHandler(),
			],
		)
		logging.info("Session %s started for vehicle_type=%s", sid, vehicle_type)
		return cls(
			id=sid,
			started_at=started,
			vehicle_type=vehicle_type,
			config_path=str(config_path),
			output_dir=str(run_dir),
			masterlist_path=str(masterlist_path),
		)

	@property
	def run_dir(self) -> Path:
		return Path(self.output_dir)

	def save(self) -> None:
		meta_path = Path(self.output_dir) / "session.json"
		with meta_path.open("w", encoding="utf-8") as f:
			json.dump(asdict(self), f, ensure_ascii=False, indent=2)