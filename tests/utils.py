import re

COLLAPSE_RE = re.compile(r"\s+")


def collapse_whitespace(s):
    return COLLAPSE_RE.sub(" ", s)
