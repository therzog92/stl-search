"""Generate lightweight query variants for creator / model names.

Supports OR terms (|, OR, newlines) and wildcards with *:
  gojo*life  → Telegram searches "gojo"/"life", keeps only hits matching gojo…life
"""

from __future__ import annotations

import re
from dataclasses import dataclass

# Within a single line: | or the word OR
_LINE_OR = re.compile(r"\s*(?:\||\bOR\b)\s*", re.IGNORECASE)


_SEP_CHARS = re.compile(r"[\s_\-]+")


def _hay_contains_query(query: str, hay: str) -> bool:
    """True if query appears in hay (literal or ignoring spaces/_/-).

    Blocks Telegram fuzzy matches like gay→gary or penis→peny.
    Still allows gojo life ↔ gojo_life via separator folding.
    """
    q = (query or "").strip()
    if not q:
        return True
    h = hay or ""
    if q.casefold() in h.casefold():
        return True
    qn = _SEP_CHARS.sub("", q.casefold())
    hn = _SEP_CHARS.sub("", h.casefold())
    return bool(qn) and qn in hn


@dataclass(frozen=True)
class SearchVariant:
    """One Telegram API search string, with optional local wildcard filter."""

    telegram_query: str
    # Shown on results as “matched …” (original gojo*life for wildcards)
    label: str
    # If set, filename/caption must match after Telegram returns candidates
    match_re: re.Pattern[str] | None = None

    def matches_text(self, file_name: str, text: str) -> bool:
        hay = f"{file_name or ''}\n{text or ''}"
        if self.match_re is not None:
            return self.match_re.search(hay) is not None
        # Telegram fuzzy-matches tokens — require the real query text to appear.
        return _hay_contains_query(self.telegram_query, hay) or _hay_contains_query(
            self.label, hay
        )


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


def _wildcard_match_re(term: str) -> re.Pattern[str] | None:
    """Build a regex for gojo*life → gojo.*?life (case-insensitive)."""
    if "*" not in term:
        return None
    chunks = term.split("*")
    # Escape literals; empty chunk (leading/trailing *) ⇒ open-ended
    pieces: list[str] = []
    for i, chunk in enumerate(chunks):
        if chunk:
            pieces.append(re.escape(chunk))
        if i < len(chunks) - 1:
            pieces.append(".*?")
    if not any(c for c in chunks if c):
        return None
    return re.compile("".join(pieces), re.IGNORECASE | re.DOTALL)


def _telegram_queries_for_wildcard(term: str, cap: int) -> list[str]:
    """Queries broad enough for Telegram; local regex does the real filtering."""
    parts = [p.strip() for p in term.split("*") if p.strip()]
    if not parts:
        return []
    out: list[str] = []
    seen: set[str] = set()

    def add(value: str) -> None:
        cleaned = " ".join(value.strip().split())
        if not cleaned or len(cleaned) < 2:
            return
        key = cleaned.casefold()
        if key not in seen:
            seen.add(key)
            out.append(cleaned)

    # Prefer longer tokens (more selective Telegram searches)
    for part in sorted(parts, key=len, reverse=True):
        add(part)
        if len(out) >= cap:
            return out[:cap]
    # Also try space-joined (Telegram often treats as multi-word)
    if len(parts) > 1:
        add(" ".join(parts))
    return out[: max(1, cap)]


def build_search_plan(query: str, max_per_term: int | None = None) -> list[SearchVariant]:
    """
    Expand OR terms into Telegram searches + optional local wildcard filters.

    Each OR line is independent:
      gojo*life   → only hits matching gojo…life, label gojo*life
      gojo        → normal spacing variants; can still match alone
    """
    from app.config import SEARCH_MAX_VARIANTS_PER_TERM

    cap = SEARCH_MAX_VARIANTS_PER_TERM if max_per_term is None else max_per_term
    terms = split_or_terms(query)
    if not terms:
        return []

    plan: list[SearchVariant] = []
    seen_keys: set[str] = set()

    for term in terms:
        match_re = _wildcard_match_re(term)
        if match_re is not None:
            queries = _telegram_queries_for_wildcard(term, max(1, cap))
            for tq in queries:
                key = f"w:{term.casefold()}|{tq.casefold()}"
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                plan.append(
                    SearchVariant(
                        telegram_query=tq,
                        label=term,
                        match_re=match_re,
                    )
                )
            continue

        for variant in _variants_for_term(term)[: max(1, cap)]:
            key = f"n:{variant.casefold()}"
            if key in seen_keys:
                continue
            seen_keys.add(key)
            plan.append(
                SearchVariant(
                    telegram_query=variant,
                    label=variant,
                    match_re=None,
                )
            )
    return plan


def generate_variants(query: str, max_per_term: int | None = None) -> list[str]:
    """
    Expand one or more OR terms into Telegram search strings.

    Examples:
      "Yosh Studios"
      "Yosh Studios | gojo | toji | life size"
      "gojo*life"  (wildcard — also see build_search_plan)
    """
    # Unique telegram queries in plan order (for API / status compatibility)
    out: list[str] = []
    seen: set[str] = set()
    for item in build_search_plan(query, max_per_term=max_per_term):
        key = item.telegram_query.casefold()
        if key not in seen:
            seen.add(key)
            out.append(item.telegram_query)
    return out
