from __future__ import annotations
from enum import Enum
import typer
import yaml
from pathlib import Path
import pandas as pd

config = {
	"masterlists": {
		"MCAICE": "C:/Users/vanag/Documents/INCHCAPE/pvvp_app/MasterlistsLVcsv/LV_MCAICEEmasterlist.csv",
		"MCAFHEV": "C:/Users/vanag/Documents/INCHCAPE/pvvp_app/MasterlistsLVcsv/LV_MCAFHEVVmasterlist.csv",
		"MCAPHEV": "C:/Users/vanag/Documents/INCHCAPE/pvvp_app/MasterlistsLVcsv/LV_MCAPHEVVmasterlist.csv",
		"EV": "C:/Users/vanag/Documents/INCHCAPE/pvvp_app/MasterlistsLVcsv/LV_EVmasterlist.csv",
		"BEV": "C:/Users/vanag/Documents/INCHCAPE/pvvp_app/MasterlistsLVcsv/LV_BEVmasterlist.csv",
	},
	"output_dir": "runs",
	"chunk_size": 10,
	"csv_delimiter": ",",
	"csv_encoding": "utf-8",
	"keep_tt_rows": True,
	"skip_tt_for_matching": True,
}

app = typer.Typer()

class VehicleType(str, Enum):
	MCAICE = "MCAICE"
	MCAFHEV = "MCAFHEV"
	MCAPHEV = "MCAPHEV"
	EV = "EV"
	BEV = "BEV"

class AppConfig:
	def __init__(self, data):
		self.masterlists = data["masterlists"]
		self.output_dir = data["output_dir"]
		self.csv_delimiter = data["csv_delimiter"]
		self.csv_encoding = data["csv_encoding"]

	@staticmethod
	def from_yaml(path: Path):
		with path.open("r", encoding="utf-8") as f:
			data = yaml.safe_load(f)
		return AppConfig(data)

# Example usage for writing a sample config YAML file
def write_example_config(output: Path, sample: dict):
	output.parent.mkdir(parents=True, exist_ok=True)
	with output.open("w", encoding="utf-8") as f:
		yaml.safe_dump(sample, f, sort_keys=False, allow_unicode=True)
	typer.secho(f"Wrote example config to {output}", fg=typer.colors.GREEN)

@app.command("init-session")
def init_session(
	config: Path = typer.Option(..., exists=True, readable=True, help="Path to YAML config"),
	vehicle_type: VehicleType = typer.Option(..., help="Vehicle Type masterlist to use"),
):
	"""Start a session, load masterlist, persist initial artifacts (raw/tt/real) and metadata."""
	cfg = AppConfig.from_yaml(config)
	mapp = cfg.masterlists  # Already a dict
	mpath = Path(mapp[vehicle_type.value])

	# Dummy Session class for demonstration
	class Session:
		@staticmethod
		def start(vehicle_type, config_path, output_root, masterlist_path):
			typer.echo(f"Session started for {vehicle_type} using {masterlist_path}")

	s = Session.start(
		vehicle_type=vehicle_type.value,
		config_path=config,
		output_root=cfg.output_dir,
		masterlist_path=mpath,
	)

@app.command("validate-masterlist")
def validate_masterlist(
	path: Path = typer.Argument(..., exists=True, readable=True, help="CSV to validate"),
	delimiter: str = typer.Option(",", help="CSV delimiter"),
	encoding: str = typer.Option("utf-8", help="Preferred encoding to try first"),
):
	typer.echo(f"Validating {path} with delimiter '{delimiter}' and encoding '{encoding}'")

@app.command("init-config")
def init_config(
    output: Path = typer.Option(Path("example_config.yaml"), help="Path to write example config"),
):
    """Write an example config YAML file."""
    write_example_config(output, config)