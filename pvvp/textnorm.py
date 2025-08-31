import unicodedata, re

DASHES = {
    "\u2013": "-",
    "\u2014": "-",
    "\u2212": "-",
}


def norm_basic(s: str) -> str:
    if s is None:
        return ""
    s = unicodedata.normalize("NFKC", s)
    s = s.translate(str.maketrans(DASHES))
    s = s.replace("\u00A0", " ").replace("\u2007", " ").replace("\u202F", " ")
    s = re.sub(r"[ \t]+", " ", s)
    return s.strip()
