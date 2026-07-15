"""Telemetr.io catalog search — fast channel discovery from a pre-built index."""

from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass

import httpx

from app.config import (
    DISCOVERY_QUERIES,
    MIN_CHANNEL_MEMBERS,
    TELEMETR_API_BASE,
    TELEMETR_API_KEY,
)

logger = logging.getLogger(__name__)

TME_LINK = re.compile(
    r"(?:https?://)?(?:t\.me|telegram\.me)/([A-Za-z][A-Za-z0-9_]{3,})",
    re.IGNORECASE,
)


@dataclass
class CatalogHit:
    username: str
    title: str
    members: int
    internal_id: str


class CatalogError(Exception):
    """Raised when Telemetr is misconfigured or the API rejects the request."""


def has_api_key() -> bool:
    return bool(TELEMETR_API_KEY and TELEMETR_API_KEY.strip())


def _username_from_link(link: str | None) -> str | None:
    if not link:
        return None
    match = TME_LINK.search(link)
    if not match:
        return None
    name = match.group(1)
    if name.casefold() in {"joinchat", "addstickers", "share", "proxy", "c", "s"}:
        return None
    if name.startswith("+"):
        return None
    return name


async def search_stl_channels(
    queries: tuple[str, ...] | list[str] | None = None,
    min_members: int = MIN_CHANNEL_MEMBERS,
    pages_per_query: int = 3,
    page_size: int = 30,
) -> list[CatalogHit]:
    """
    Query Telemetr for STL / 3D-related channels and return public usernames
    with at least `min_members` subscribers.
    """
    if not has_api_key():
        raise CatalogError(
            "TELEMETR_API_KEY is missing. Get a free key from "
            "https://t.me/telemetrio_api_bot (/api_key) and put it in .env"
        )

    terms = list(queries or DISCOVERY_QUERIES)
    headers = {
        "accept": "application/json",
        "x-api-key": TELEMETR_API_KEY.strip(),
    }

    # internal_id -> provisional row (members, title)
    candidates: dict[str, dict] = {}

    async with httpx.AsyncClient(
        base_url=TELEMETR_API_BASE,
        headers=headers,
        timeout=30.0,
    ) as client:
        catalog_ok = True
        for term in terms:
            if catalog_ok:
                try:
                    rows = await _search_catalog(client, term, min_members, pages_per_query, page_size)
                    for row in rows:
                        candidates[row["internal_id"]] = row
                    await asyncio.sleep(0.15)
                    continue
                except httpx.HTTPStatusError as exc:
                    if exc.response.status_code in {403, 404, 426}:
                        logger.info(
                            "Catalog search unavailable (%s); falling back to channels/search",
                            exc.response.status_code,
                        )
                        catalog_ok = False
                    else:
                        raise CatalogError(_api_error_message(exc)) from exc
                except CatalogError:
                    raise

            # channels/search fallback (filter members client-side)
            try:
                rows = await _search_channels(client, term, pages_per_query, page_size)
                for row in rows:
                    if int(row.get("members_count") or 0) < min_members:
                        continue
                    candidates[row["internal_id"]] = row
            except httpx.HTTPStatusError as exc:
                raise CatalogError(_api_error_message(exc)) from exc
            await asyncio.sleep(0.15)

        if not candidates:
            return []

        # Resolve internal IDs -> t.me usernames via batch info
        hits = await _resolve_usernames(client, list(candidates.keys()), candidates, min_members)

    # Dedupe by username
    by_user: dict[str, CatalogHit] = {}
    for hit in hits:
        key = hit.username.casefold()
        prev = by_user.get(key)
        if prev is None or hit.members > prev.members:
            by_user[key] = hit
    return sorted(by_user.values(), key=lambda h: (-h.members, h.username.casefold()))


async def _search_catalog(
    client: httpx.AsyncClient,
    term: str,
    min_members: int,
    pages: int,
    page_size: int,
) -> list[dict]:
    out: list[dict] = []
    for page in range(pages):
        skip = page * page_size
        resp = await client.get(
            "/v1/catalog/search",
            params={
                "term": term,
                "search_in_about": "true",
                "members_min": min_members,
                "privacy": "Public",
                "sort_by": "members_count",
                "sort_direction": "desc",
                "limit": page_size,
                "skip": skip,
            },
        )
        resp.raise_for_status()
        data = resp.json()
        items = data.get("items") if isinstance(data, dict) else data
        if not items:
            break
        for item in items:
            iid = item.get("internal_id")
            if not iid:
                continue
            out.append(
                {
                    "internal_id": iid,
                    "title": item.get("title") or iid,
                    "members_count": int(item.get("members_count") or 0),
                }
            )
        if len(items) < page_size:
            break
        await asyncio.sleep(0.1)
    return out


async def _search_channels(
    client: httpx.AsyncClient,
    term: str,
    pages: int,
    page_size: int,
) -> list[dict]:
    out: list[dict] = []
    for page in range(pages):
        skip = page * page_size
        resp = await client.get(
            "/v1/channels/search",
            params={
                "term": term,
                "search_in_about": "true",
                "peer_type": "Channel",
                "limit": min(page_size, 30),
                "skip": skip,
            },
        )
        resp.raise_for_status()
        data = resp.json()
        items = data if isinstance(data, list) else data.get("items") or data.get("channels") or []
        if not items:
            break
        for item in items:
            iid = item.get("internal_id")
            if not iid:
                continue
            out.append(
                {
                    "internal_id": iid,
                    "title": item.get("title") or iid,
                    "members_count": int(item.get("members_count") or 0),
                }
            )
        if len(items) < page_size:
            break
        await asyncio.sleep(0.1)
    return out


async def _resolve_usernames(
    client: httpx.AsyncClient,
    internal_ids: list[str],
    candidates: dict[str, dict],
    min_members: int,
) -> list[CatalogHit]:
    hits: list[CatalogHit] = []
    # Batch up to 50 at a time to stay polite with quota
    chunk_size = 50
    for i in range(0, len(internal_ids), chunk_size):
        chunk = internal_ids[i : i + chunk_size]
        try:
            resp = await client.get(
                "/v1/channels/info-batch",
                params={"ids": ",".join(chunk)},
            )
            if resp.status_code == 403 or resp.status_code == 404:
                # Fall back to per-channel info
                for iid in chunk:
                    hit = await _resolve_one(client, iid, candidates.get(iid, {}), min_members)
                    if hit:
                        hits.append(hit)
                    await asyncio.sleep(0.08)
                continue
            resp.raise_for_status()
            data = resp.json()
            channels = data.get("channels") if isinstance(data, dict) else data
            if not isinstance(channels, list):
                channels = []
            by_id = {c.get("internal_id"): c for c in channels if c.get("internal_id")}
            for iid in chunk:
                info = by_id.get(iid) or {}
                username = _username_from_link(info.get("link"))
                if not username:
                    continue
                members = int(
                    info.get("members_count")
                    or candidates.get(iid, {}).get("members_count")
                    or 0
                )
                if members < min_members:
                    continue
                title = info.get("title") or candidates.get(iid, {}).get("title") or username
                hits.append(
                    CatalogHit(
                        username=username,
                        title=title,
                        members=members,
                        internal_id=iid,
                    )
                )
        except httpx.HTTPStatusError as exc:
            logger.warning("info-batch failed (%s); trying singles", exc.response.status_code)
            for iid in chunk:
                hit = await _resolve_one(client, iid, candidates.get(iid, {}), min_members)
                if hit:
                    hits.append(hit)
                await asyncio.sleep(0.08)
        await asyncio.sleep(0.12)
    return hits


async def _resolve_one(
    client: httpx.AsyncClient,
    internal_id: str,
    provisional: dict,
    min_members: int,
) -> CatalogHit | None:
    try:
        resp = await client.get("/v1/channel/info", params={"internal_id": internal_id})
        resp.raise_for_status()
        data = resp.json()
        info = data[0] if isinstance(data, list) and data else data
        if not isinstance(info, dict):
            return None
        username = _username_from_link(info.get("link"))
        if not username:
            return None
        members = int(info.get("members_count") or provisional.get("members_count") or 0)
        if members < min_members:
            return None
        return CatalogHit(
            username=username,
            title=info.get("title") or provisional.get("title") or username,
            members=members,
            internal_id=internal_id,
        )
    except Exception as exc:
        logger.debug("channel info failed for %s: %s", internal_id, exc)
        return None


def _api_error_message(exc: httpx.HTTPStatusError) -> str:
    status = exc.response.status_code
    try:
        body = exc.response.json()
        detail = body.get("message") or body.get("error") or body
    except Exception:
        detail = exc.response.text[:300]
    if status == 401:
        return f"Telemetr API key rejected (401). Check TELEMETR_API_KEY. Details: {detail}"
    if status == 426:
        return f"Telemetr quota reached (426). Details: {detail}"
    return f"Telemetr API error {status}: {detail}"
