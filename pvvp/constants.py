from typing import Literal


VehicleType = Literal["MCAICE", "MCAFHEV", "MCAPHEV", "EV", "BEV"]
REQUIRED_COLUMNS_HINT = [
"Nr Code", # required for chunking/matching by NR
"Variable Name", # empty => TT row
]
# We will *not* mutate numbering; we only subset.