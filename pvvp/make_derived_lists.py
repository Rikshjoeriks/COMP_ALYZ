import pandas as pd
from pathlib import Path

masterlists = [
    "LV_MCAFHEVmasterlist.csv",
    "LV_BEVmasterlist.csv",
    "LV_MCAPHEVmasterlist.csv",
    "LV_MCAICEmasterlist.csv",
    "LV_EVmasterlist.csv",
    # Add more if you have them
]

src_dir = Path("MasterlistsLVcsv")
dst_dir = src_dir  # Save derived lists in the same folder

for fname in masterlists:
    df = pd.read_csv(src_dir / fname, encoding="utf-8")
    # Filter out TT rows (empty Variable Name)
    real_vars = df["Variable Name"].dropna().str.strip()
    real_vars = real_vars[real_vars != ""]
    # Create output filename: LV_BEV_PVVP.txt, etc.
    out_name = fname.replace("masterlist.csv", "PVVP.txt")
    with open(dst_dir / out_name, "w", encoding="utf-8") as f:
        for var in real_vars:
            f.write(var + "\n")
    print(f"Created {out_name} with {len(real_vars)} variables.")