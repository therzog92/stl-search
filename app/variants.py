"""Generate lightweight query variants for creator / model names."""

from __future__ import annotations

import re

# Within a single line: | or the word OR
_LINE_OR = re.compile(r"\s*(?:\||\bOR\b)\s*", re.IGNORECASE)


def split_or_terms(query: str) -> list[str]:
    """
    Split a query into independent search phrases (OR).

    Newlines always separate terms. Within a line, `|` or `OR` also split.
    """
    raw = (query or "").replace("\r\n", "\n").replace("\r", "\n").strip()
    if not raw:
        return []

    terms: list[str] = []
    seen: set[str] = set()

    # One term per line first — don't let whitespace collapsing glue lines together
    for line in raw.split("\n"):
        line = line.strip()
        if not line:
            continue
        for part in _LINE_OR.split(line):
            cleaned = " ".join(part.strip().split())
            if not cleaned:
                continue
            key = cleaned.casefold()
            if key not in seen:
                seen.add(key)
                terms.append(cleaned)
    return terms


def _variants_for_term(q: str) -> list[str]:
    variants: list[str] = []
    seen: set[str] = set()

    def add(value: str) -> None:
        cleaned = " ".join(value.strip().split())
        if not cleaned:
            return
        key = cleaned.casefold()
        if key not in seen:
            seen.add(key)
            variants.append(cleaned)

    add(q)
    add(q.replace("-", " "))
    add(q.replace("_", " "))
    add(re.sub(r"[\s_-]+", "", q))
    add(re.sub(r"[\s_-]+", "-", q))
    add(re.sub(r"[\s_-]+", "_", q))

    parts = re.split(r"[\s_-]+", q)
    if len(parts) > 1:
        add("".join(parts))
        add(" ".join(parts))
        add("-".join(parts))
        add("".join(p.capitalize() for p in parts if p))

    return variants


def generate_variants(query: str, max_per_term: int | None = None) -> list[str]:
    """
    Expand one or more OR terms into Telegram search strings.

    Examples:
      "Yosh Studios"
      "Yosh Studios | gojo | toji | life size"
      one term per line
    """
    from app.config import SEARCH_MAX_VARIANTS_PER_TERM

    cap = SEARCH_MAX_VARIANTS_PER_TERM if max_per_term is None else max_per_term
    terms = split_or_terms(query)
    if not terms:
        return []

    out: list[str] = []
    seen: set[str] = set()
    for term in terms:
        for variant in _variants_for_term(term)[: max(1, cap)]:
            key = variant.casefold()
            if key not in seen:
                seen.add(key)
                out.append(variant)
    return out
