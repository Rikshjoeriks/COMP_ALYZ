import unicodedata, re

DASHES = {
    "\u2013": "-",
    "\u2014": "-",
    "\u2212": "-",
}

SPACES = {
    "\u00A0": " ",
    "\u2007": " ",
    "\u202F": " ",
}


def norm_basic(s: str) -> str:
    if s is None:
        return ""
    s = unicodedata.normalize("NFKC", s)
    s = s.translate(str.maketrans(DASHES))
    s = s.translate(str.maketrans(SPACES))
    s = re.sub(r"[ \t]+", " ", s)
    return s.strip()


def norm_lv(s: str) -> str:
    if s is None:
        return ""
    s = unicodedata.normalize("NFKC", s)
    s = s.translate(str.maketrans(DASHES))
    s = s.translate(str.maketrans(SPACES))
    # collapse spaces/tabs (keep newlines if needed)
    s = re.sub(r"[ \t]+", " ", s)
    # join digits + unit letters like "12 V" -> "12V", "10 Kw" -> "10Kw"
    s = re.sub(r"(\d)\s+([A-Za-zĀ-ž])", r"\1\2", s)
    return s.strip().lower()
