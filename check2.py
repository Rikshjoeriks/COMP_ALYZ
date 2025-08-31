import glob
for p in glob.glob(r".\pvvp\sessions\*\text_normalized.txt"):
    print("\n===", p, "===")
    print(open(p,"r",encoding="utf-8").read(200))