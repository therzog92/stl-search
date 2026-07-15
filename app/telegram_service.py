from __future__ import annotations

import asyncio
import json
import logging
import re
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from telethon import TelegramClient
from telethon.errors import FloodWaitError, ChannelPrivateError, UsernameNotOccupiedError
from telethon.tl.functions.channels import (
    GetChannelRecommendationsRequest,
    GetFullChannelRequest,
)
from telethon.tl.functions.contacts import SearchRequest as ContactsSearchRequest
from telethon.tl.types import (
    Channel,
    DocumentAttributeFilename,
    InputMessagesFilterDocument,
    InputMessagesFilterEmpty,
    InputMessagesFilterUrl,
    Message,
    MessageEntityTextUrl,
    MessageEntityUrl,
)

from app.catalog import CatalogError, has_api_key as telemetr_has_key, search_stl_channels
from app.extract_archive import (
    DestinationExists,
    extract_download_archive,
    is_extractable_archive,
)
from app.fast_download import DownloadCancelled, parallel_download_to_path
from app.config import (
    API_HASH,
    API_ID,
    BLACKLIST_FILE,
    CHANNEL_CACHE_FILE,
    DISCOVERED_FILE,
    DISCOVERY_QUERIES,
    DOWNLOAD_CACHE_DIR,
    DOWNLOAD_CONNECTIONS,
    DOWNLOAD_DIR,
    DOWNLOAD_HISTORY_FILE,
    DOWNLOAD_INDEX_FILE,
    DOWNLOAD_PART_KB,
    FILE_EXTENSIONS,
    LINK_CRAWL_LIMIT,
    MAX_AGE_DAYS,
    MIN_CHANNEL_MEMBERS,
    SEARCH_DELAY_SECONDS,
    SESSION_PATH,
    THUMBS_DIR,
    load_seed_channels,
)
from app.variants import generate_variants

logger = logging.getLogger(__name__)

ProgressCb = Callable[[dict[str, Any]], Any]

STL_HINT = re.compile(
    r"\b(stl|3mf|3d\s*print|printing|filament|resin|gcode|fdm)\b",
    re.IGNORECASE,
)
TME_USERNAME = re.compile(
    r"(?:https?://)?(?:t\.me|telegram\.me)/([A-Za-z][A-Za-z0-9_]{3,31})",
    re.IGNORECASE,
)
# Official Telegram public username shape
VALID_USERNAME = re.compile(r"^[a-zA-Z][\w\d]{3,30}[a-zA-Z\d]$")
TME_SKIP = {
    "joinchat",
    "addstickers",
    "share",
    "proxy",
    "socks",
    "iv",
    "c",
    "s",
    "bg",
    "invoice",
    "setlanguage",
    "sharegame",
    "confirmphone",
    "msg",
    "addtheme",
    "nft",
    "boost",
}


@dataclass
class ChannelInfo:
    username: str
    title: str
    members: int
    included: bool  # user-enabled for search (checkbox)
    reason: str = ""
    link: str = ""
    source: str = "seed"  # seed | catalog | similar | linked | search | joined | discovered
    description: str = ""
    valid: bool = True  # False = do not store / show as channel
    favorite: bool = False
    banned: bool = False  # blacklisted — hidden from home/search, shown on manage


@dataclass
class SearchHit:
    channel_username: str
    channel_title: str
    message_id: int
    date: str
    text: str
    file_name: str
    file_ext: str
    file_size: int
    message_link: str
    thumb_url: str | None
    query_matched: str


@dataclass
class SearchState:
    running: bool = False
    status: str = "idle"
    progress: str = ""
    query: str = ""
    mode: str = "search"  # search | browse
    browse_username: str = ""
    browse_title: str = ""
    results: list[dict] = field(default_factory=list)
    channels_scanned: int = 0
    channels_total: int = 0
    errors: list[str] = field(default_factory=list)
    finished_at: str | None = None


@dataclass
class DiscoveryState:
    running: bool = False
    mode: str = ""  # catalog | deep | refresh
    progress: str = ""
    error: str | None = None
    message: str | None = None


class TelegramService:
    def __init__(self) -> None:
        self.client = TelegramClient(str(SESSION_PATH), API_ID, API_HASH)
        self._login_phone: str | None = None
        self._phone_code_hash: str | None = None
        self.search_state = SearchState()
        self._search_lock = asyncio.Lock()
        self._resolve_lock = asyncio.Lock()
        self._channel_cache: list[ChannelInfo] | None = None
        self.discovery_message: str | None = None
        self.discovery_error: str | None = None
        self.discovery_state = DiscoveryState()
        self._search_cancel_requested = False
        self._download_lock = asyncio.Lock()
        self._download_jobs: dict[str, dict[str, Any]] = {}
        self._download_queue: asyncio.Queue[str] = asyncio.Queue()
        self._download_worker_task: asyncio.Task | None = None
        self._download_job_seq = 0

    def request_stop_search(self) -> bool:
        """Signal the active search to stop after the current step."""
        if not self.search_state.running:
            return False
        self._search_cancel_requested = True
        self.search_state.progress = "Stopping…"
        return True

    def _search_stop_requested(self) -> bool:
        return self._search_cancel_requested

    async def connect(self) -> None:
        await self.client.connect()

    async def disconnect(self) -> None:
        await self.client.disconnect()

    async def is_authorized(self) -> bool:
        """Fast auth check with timeout so the UI never hangs forever."""
        session_file = Path(f"{SESSION_PATH}.session")
        try:
            if not self.client.is_connected():
                await asyncio.wait_for(self.connect(), timeout=8)
            return await asyncio.wait_for(self.client.is_user_authorized(), timeout=8)
        except (asyncio.TimeoutError, OSError) as exc:
            logger.warning("Auth check timed out: %s", exc)
            # Don't bounce the user to login if we already have a session on disk
            return session_file.exists()
        except Exception as exc:
            logger.warning("Auth check failed: %s", exc)
            return session_file.exists()

    def get_channels_fast(self) -> list[ChannelInfo]:
        """Memory or disk cache — no Telegram calls. Safe for page renders."""
        if self._channel_cache is not None:
            return self._channel_cache
        cached = self._load_channel_cache()
        discovered = {c.username.casefold(): c for c in self._load_discovered_channels()}
        if cached:
            # Merge richer member counts from discovered metadata when cache is stale/zero
            merged: list[ChannelInfo] = []
            seen: set[str] = set()
            for c in cached:
                key = c.username.casefold()
                seen.add(key)
                disc = discovered.get(key)
                if disc and disc.members > (c.members or 0):
                    c.members = disc.members
                    if disc.title:
                        c.title = disc.title
                    if not c.banned and c.members >= MIN_CHANNEL_MEMBERS:
                        c.included = True
                        c.reason = c.reason or "OK"
                if disc and disc.favorite:
                    c.favorite = True
                if disc and disc.banned:
                    c.banned = True
                if disc and disc.description and not c.description:
                    c.description = disc.description
                if c.banned:
                    c.included = False
                    c.favorite = False
                merged.append(c)
            for key, disc in discovered.items():
                if key in seen:
                    continue
                merged.append(disc)
            self._channel_cache = self._merge_blacklist_stubs([c for c in merged if c.valid])
            return self._channel_cache

        stubs = [
            ChannelInfo(
                username=name,
                title=name,
                members=0,
                included=True,
                reason="Pending refresh",
                link=f"https://t.me/{name}",
                source="seed",
                description="",
                valid=True,
                favorite=False,
                banned=False,
            )
            for name in load_seed_channels()
            if self._is_valid_username(name)
        ]
        for disc in discovered.values():
            if not disc.valid or not self._is_valid_username(disc.username):
                continue
            if any(s.username.casefold() == disc.username.casefold() for s in stubs):
                continue
            stubs.append(disc)
        self._channel_cache = self._merge_blacklist_stubs(stubs)
        return self._channel_cache

    def _load_channel_cache(self) -> list[ChannelInfo]:
        if not CHANNEL_CACHE_FILE.exists():
            return []
        try:
            data = json.loads(CHANNEL_CACHE_FILE.read_text(encoding="utf-8"))
            if not isinstance(data, list):
                return []
            out: list[ChannelInfo] = []
            for row in data:
                if not isinstance(row, dict) or not row.get("username"):
                    continue
                username = str(row["username"]).lstrip("@")
                if not self._is_valid_username(username):
                    continue
                if row.get("valid") is False:
                    continue
                out.append(
                    ChannelInfo(
                        username=username,
                        title=row.get("title") or username,
                        members=int(row.get("members") or 0),
                        included=bool(row.get("included", True)),
                        reason=row.get("reason") or "",
                        link=row.get("link") or f"https://t.me/{username}",
                        source=row.get("source") or "catalog",
                        description=row.get("description") or "",
                        valid=True,
                        favorite=bool(row.get("favorite", False)),
                        banned=bool(row.get("banned", False)),
                    )
                )
            # Reconcile with durable blacklist file
            banned_keys = self._blacklist_keys()
            for c in out:
                if c.username.casefold() in banned_keys:
                    c.banned = True
                    c.included = False
                    c.favorite = False
            return out
        except Exception:
            logger.exception("Failed reading channel cache")
            return []

    def _save_channel_cache(self, channels: list[ChannelInfo]) -> None:
        CHANNEL_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        # Never persist invalid / malformed usernames
        clean = [
            c
            for c in channels
            if c.valid and self._is_valid_username(c.username)
        ]
        payload = [asdict(c) for c in clean]
        CHANNEL_CACHE_FILE.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    def _is_valid_username(self, username: str) -> bool:
        name = (username or "").lstrip("@")
        if not name or name.casefold() in TME_SKIP:
            return False
        return bool(VALID_USERNAME.match(name))

    def _invalid_channel(
        self, username: str, reason: str, source: str = "catalog"
    ) -> ChannelInfo:
        name = (username or "").lstrip("@") or "unknown"
        return ChannelInfo(
            username=name,
            title=name,
            members=0,
            included=False,
            reason=reason,
            link=f"https://t.me/{name}",
            source=source,
            description="",
            valid=False,
            favorite=False,
            banned=False,
        )

    def _preserve_user_enabled(
        self, previous: ChannelInfo | None, fresh: ChannelInfo
    ) -> ChannelInfo:
        if previous is not None and previous.valid:
            fresh.included = previous.included
            fresh.favorite = previous.favorite
            fresh.banned = previous.banned
            if not (fresh.description or "").strip() and (previous.description or "").strip():
                fresh.description = previous.description
            if (fresh.members or 0) <= 0 and (previous.members or 0) > 0:
                fresh.members = previous.members
                if fresh.reason in ("", "Member count unknown", "Pending member count"):
                    fresh.reason = previous.reason or fresh.reason
            if fresh.banned or self._is_blacklisted(fresh.username):
                fresh.banned = True
                fresh.included = False
                fresh.favorite = False
        elif self._is_blacklisted(fresh.username):
            fresh.banned = True
            fresh.included = False
            fresh.favorite = False
        return fresh

    def _blacklist_keys(self) -> set[str]:
        if not BLACKLIST_FILE.exists():
            return set()
        try:
            data = json.loads(BLACKLIST_FILE.read_text(encoding="utf-8"))
            if not isinstance(data, list):
                return set()
            keys: set[str] = set()
            for item in data:
                if isinstance(item, str):
                    name = item.lstrip("@")
                elif isinstance(item, dict) and item.get("username"):
                    name = str(item["username"]).lstrip("@")
                else:
                    continue
                if name:
                    keys.add(name.casefold())
            return keys
        except Exception:
            logger.exception("Failed reading blacklist")
            return set()

    def _is_blacklisted(self, username: str) -> bool:
        name = (username or "").lstrip("@")
        return bool(name) and name.casefold() in self._blacklist_keys()

    def _save_blacklist(self, channels: list[ChannelInfo]) -> None:
        """Write blacklist from in-memory banned channels (source of truth)."""
        BLACKLIST_FILE.parent.mkdir(parents=True, exist_ok=True)
        banned = [
            c
            for c in channels
            if c.banned and c.valid and self._is_valid_username(c.username)
        ]
        payload = [
            {
                "username": c.username,
                "title": c.title,
                "members": int(c.members or 0),
                "source": c.source,
                "description": c.description or "",
                "link": c.link or f"https://t.me/{c.username}",
            }
            for c in sorted(banned, key=lambda x: x.username.casefold())
        ]
        BLACKLIST_FILE.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    def _merge_blacklist_stubs(self, channels: list[ChannelInfo]) -> list[ChannelInfo]:
        """Ensure blacklist-only usernames still appear under Manage."""
        by_key = {c.username.casefold(): c for c in channels}
        if not BLACKLIST_FILE.exists():
            return channels
        try:
            data = json.loads(BLACKLIST_FILE.read_text(encoding="utf-8"))
            if not isinstance(data, list):
                return channels
            for item in data:
                if isinstance(item, str):
                    name = item.lstrip("@")
                    title, members, source, description, link = name, 0, "catalog", "", f"https://t.me/{name}"
                elif isinstance(item, dict) and item.get("username"):
                    name = str(item["username"]).lstrip("@")
                    title = item.get("title") or name
                    members = int(item.get("members") or 0)
                    source = item.get("source") or "catalog"
                    description = item.get("description") or ""
                    link = item.get("link") or f"https://t.me/{name}"
                else:
                    continue
                if not self._is_valid_username(name):
                    continue
                key = name.casefold()
                if key in by_key:
                    by_key[key].banned = True
                    by_key[key].included = False
                    by_key[key].favorite = False
                    continue
                by_key[key] = ChannelInfo(
                    username=name,
                    title=title,
                    members=members,
                    included=False,
                    reason="Banned",
                    link=link,
                    source=source,
                    description=description,
                    valid=True,
                    favorite=False,
                    banned=True,
                )
            return list(by_key.values())
        except Exception:
            logger.exception("Failed merging blacklist stubs")
            return channels

    def _persist_channels(self, channels: list[ChannelInfo]) -> None:
        self._channel_cache = channels
        self._save_channel_cache(channels)
        self._save_discovered_channels([c for c in channels if c.source != "seed"])
        self._save_blacklist(channels)

    def ban_channel(self, username: str) -> ChannelInfo | None:
        name = (username or "").lstrip("@")
        channels = [c for c in self.get_channels_fast() if c.valid]
        target = next((c for c in channels if c.username.casefold() == name.casefold()), None)
        if target is None:
            # Still record username on blacklist so discovery won't re-add it
            stub = ChannelInfo(
                username=name,
                title=name,
                members=0,
                included=False,
                reason="Banned",
                link=f"https://t.me/{name}",
                source="catalog",
                description="",
                valid=True,
                favorite=False,
                banned=True,
            )
            if not self._is_valid_username(name):
                return None
            channels.append(stub)
            target = stub
        target.banned = True
        target.included = False
        target.favorite = False
        self._persist_channels(channels)
        return target

    def unban_channel(self, username: str) -> ChannelInfo | None:
        name = (username or "").lstrip("@")
        channels = [c for c in self.get_channels_fast() if c.valid]
        target = next((c for c in channels if c.username.casefold() == name.casefold()), None)
        if target is None:
            # Remove from blacklist file only
            keys = self._blacklist_keys()
            keys.discard(name.casefold())
            if BLACKLIST_FILE.exists():
                try:
                    data = json.loads(BLACKLIST_FILE.read_text(encoding="utf-8"))
                    if isinstance(data, list):
                        kept = []
                        for item in data:
                            uname = item if isinstance(item, str) else (item or {}).get("username")
                            if uname and str(uname).lstrip("@").casefold() != name.casefold():
                                kept.append(item)
                        BLACKLIST_FILE.write_text(json.dumps(kept, indent=2), encoding="utf-8")
                except Exception:
                    pass
            return None
        target.banned = False
        target.included = True
        self._persist_channels(channels)
        return target

    def set_many_enabled(self, enabled_usernames: set[str]) -> None:
        """Checkbox form: usernames in the set are enabled; others are disabled."""
        channels = [c for c in self.get_channels_fast() if c.valid]
        enabled_keys = {u.lstrip("@").casefold() for u in enabled_usernames}
        for c in channels:
            if c.banned:
                c.included = False
                continue
            c.included = c.username.casefold() in enabled_keys
        self._persist_channels(channels)

    def set_many_favorites(self, favorite_usernames: set[str]) -> None:
        """Star form: usernames in the set are favorites; others are not."""
        channels = [c for c in self.get_channels_fast() if c.valid]
        fav_keys = {u.lstrip("@").casefold() for u in favorite_usernames}
        for c in channels:
            if c.banned:
                c.favorite = False
                continue
            c.favorite = c.username.casefold() in fav_keys
        self._persist_channels(channels)

    def set_channel_favorite(self, username: str, favorite: bool) -> ChannelInfo | None:
        name = (username or "").lstrip("@")
        channels = [c for c in self.get_channels_fast() if c.valid]
        target = next((c for c in channels if c.username.casefold() == name.casefold()), None)
        if target is None or target.banned:
            return None
        target.favorite = bool(favorite)
        self._persist_channels(channels)
        return target

    def get_channel(self, username: str) -> ChannelInfo | None:
        name = (username or "").lstrip("@")
        return next(
            (c for c in self.get_channels_fast() if c.username.casefold() == name.casefold()),
            None,
        )

    async def add_channel_manual(self, username_or_link: str) -> ChannelInfo:
        raw = (username_or_link or "").strip()
        match = TME_USERNAME.search(raw)
        username = match.group(1) if match else raw.lstrip("@")
        if not self._is_valid_username(username):
            return self._invalid_channel(username or raw, "Invalid username format", "catalog")

        info = await self._inspect_channel(username, source="catalog")
        if not info.valid:
            return info

        channels = [c for c in self.get_channels_fast() if c.valid]
        key = info.username.casefold()
        existing = next((c for c in channels if c.username.casefold() == key), None)
        info.included = True
        info.favorite = False if existing is None else existing.favorite
        info.banned = False
        info.source = "catalog" if (existing is None or existing.source != "seed") else existing.source
        if existing is None:
            channels.append(info)
        else:
            existing.banned = False
            channels = [
                self._preserve_user_enabled(existing, info)
                if c.username.casefold() == key
                else c
                for c in channels
            ]
            # Ensure manually added channel is usable again
            for c in channels:
                if c.username.casefold() == key:
                    c.banned = False
                    c.included = True

        self._persist_channels(channels)
        return info

    def remove_invalid_channels(self) -> int:
        channels = self.get_channels_fast()
        before = len(channels)
        cleaned = [c for c in channels if c.valid and self._is_valid_username(c.username)]
        removed = before - len(cleaned)
        self._persist_channels(cleaned)
        return removed

    async def start_login(self, phone: str) -> dict[str, str]:
        if not self.client.is_connected():
            await self.connect()
        self._login_phone = phone.strip()
        result = await self.client.send_code_request(self._login_phone)
        self._phone_code_hash = result.phone_code_hash
        return {"status": "code_sent", "phone": self._login_phone}

    async def confirm_code(self, code: str, password: str | None = None) -> dict[str, str]:
        from telethon.errors import SessionPasswordNeededError

        if not self._login_phone or not self._phone_code_hash:
            return {"status": "error", "message": "Start login with your phone number first."}

        if password:
            await self.client.sign_in(password=password)
            return {"status": "authorized"}

        try:
            await self.client.sign_in(
                phone=self._login_phone,
                code=code.strip(),
                phone_code_hash=self._phone_code_hash,
            )
        except SessionPasswordNeededError:
            return {"status": "password_required", "message": "Two-factor password required."}
        return {"status": "authorized"}

    async def resolve_channels(
        self,
        force: bool = False,
        discover: bool = False,
        deep_crawl: bool = False,
    ) -> list[ChannelInfo]:
        async with self._resolve_lock:
            return await self._resolve_channels_locked(
                force=force, discover=discover, deep_crawl=deep_crawl
            )

    async def _resolve_channels_locked(
        self,
        force: bool = False,
        discover: bool = False,
        deep_crawl: bool = False,
    ) -> list[ChannelInfo]:
        if self._channel_cache is not None and not force and not discover and not deep_crawl:
            return self._channel_cache

        # Warm from disk when not forcing a full Telegram refresh
        if not force and not discover and not deep_crawl:
            return self.get_channels_fast()

        seed_names = {n.casefold(): n for n in load_seed_channels()}
        discovered_map = {c.username.casefold(): c for c in self._load_discovered_channels()}
        infos_by_key: dict[str, ChannelInfo] = {}

        mode = "deep" if deep_crawl else "catalog" if discover else "refresh"
        self.discovery_state = DiscoveryState(running=True, mode=mode, progress="Loading seeds…")

        try:
            # 1) Seed list (Telegram inspect — small list)
            for idx, username in enumerate(seed_names.values(), start=1):
                self.discovery_state.progress = f"Checking seed {idx}/{len(seed_names)}…"
                info = await self._inspect_channel(username, source="seed")
                infos_by_key[info.username.casefold()] = info
                await asyncio.sleep(0.15)

            # 2) Previously discovered — keep known member counts when possible
            prior_cache = {c.username.casefold(): c for c in self._load_channel_cache()}
            for key, disc in discovered_map.items():
                if key in infos_by_key:
                    continue
                cached = prior_cache.get(key)
                best = disc
                if cached and (cached.members or 0) > (disc.members or 0):
                    best = cached
                elif cached and (disc.members or 0) <= 0:
                    best = cached
                infos_by_key[key] = best

            # Refresh: re-inspect discovered channels missing members or description
            if force and not discover and not deep_crawl:
                missing = [
                    c
                    for c in infos_by_key.values()
                    if c.source != "seed"
                    and ((c.members or 0) <= 0 or not (c.description or "").strip())
                ]
                for idx, channel in enumerate(missing, start=1):
                    self.discovery_state.progress = (
                        f"Fetching details @{channel.username} ({idx}/{len(missing)})…"
                    )
                    info = await self._inspect_channel(
                        channel.username, source=channel.source or "catalog"
                    )
                    if not info.valid:
                        infos_by_key.pop(channel.username.casefold(), None)
                    else:
                        prev = infos_by_key.get(info.username.casefold())
                        infos_by_key[info.username.casefold()] = self._preserve_user_enabled(
                            prev, info
                        )
                    await asyncio.sleep(0.2)

            # 3a) Fast path: Telemetr catalog — trust index member counts
            if discover:
                self.discovery_error = None
                self.discovery_message = None
                self.discovery_state.progress = "Querying Telemetr catalog…"
                try:
                    catalog_hits = await search_stl_channels()
                    added = 0
                    for hit in catalog_hits:
                        key = hit.username.casefold()
                        if key in seed_names:
                            continue
                        if not self._is_valid_username(hit.username):
                            continue
                        if self._is_blacklisted(hit.username):
                            continue
                        reason = (
                            "OK (Telemetr)"
                            if hit.members >= MIN_CHANNEL_MEMBERS
                            else f"Below {MIN_CHANNEL_MEMBERS:,} members (still usable)"
                        )
                        fresh = ChannelInfo(
                            username=hit.username,
                            title=hit.title or hit.username,
                            members=hit.members,
                            included=True,
                            reason=reason,
                            link=f"https://t.me/{hit.username}",
                            source="catalog",
                            description="",
                            valid=True,
                        )
                        prev = prior_cache.get(key) or discovered_map.get(key)
                        if prev and prev.banned:
                            continue
                        infos_by_key[key] = self._preserve_user_enabled(prev, fresh)
                        if key not in prior_cache and key not in discovered_map:
                            added += 1
                    self.discovery_message = (
                        f"Catalog found {len(catalog_hits)} channel(s); "
                        f"{added} newly added."
                    )
                    self.discovery_state.message = self.discovery_message
                except CatalogError as exc:
                    self.discovery_error = str(exc)
                    self.discovery_state.error = str(exc)
                    logger.warning("Catalog discovery failed: %s", exc)

            # 3b) Slow path: similar channels + t.me link snowball
            if deep_crawl:
                self.discovery_error = None
                self.discovery_state.progress = "Deep crawling seeds…"
                seed_usernames = [
                    c.username for c in infos_by_key.values() if c.source == "seed" and c.valid
                ]
                found = await self._discover_from_seeds(seed_usernames)
                kept = 0
                skipped = 0
                for username, source in found:
                    key = username.casefold()
                    if key in infos_by_key or key in seed_names:
                        continue
                    if not self._is_valid_username(username):
                        skipped += 1
                        continue
                    if self._is_blacklisted(username):
                        skipped += 1
                        continue
                    self.discovery_state.progress = f"Inspecting @{username}…"
                    info = await self._inspect_channel(username, source=source)
                    if not info.valid:
                        skipped += 1
                        await asyncio.sleep(0.1)
                        continue
                    prev = prior_cache.get(key) or discovered_map.get(key)
                    if prev and prev.banned:
                        skipped += 1
                        continue
                    infos_by_key[key] = self._preserve_user_enabled(prev, info)
                    kept += 1
                    await asyncio.sleep(0.15)
                self.discovery_message = (
                    f"Deep crawl finished — kept {kept} valid channel(s), skipped {skipped} invalid."
                )
                self.discovery_state.message = self.discovery_message

            if discover or deep_crawl or force:
                await self._backfill_missing_members(infos_by_key)
                # Drop anything invalid before save
                infos_by_key = {
                    k: v
                    for k, v in infos_by_key.items()
                    if v.valid and self._is_valid_username(v.username)
                }
                # Keep blacklisted channels so they remain visible under Manage → Blacklist
                for key, prev in {**discovered_map, **prior_cache}.items():
                    if prev.banned and key not in infos_by_key and prev.valid:
                        infos_by_key[key] = prev
                self._save_discovered_channels(
                    [c for c in infos_by_key.values() if c.source != "seed" and c.valid]
                )

            source_rank = {
                "seed": 0,
                "catalog": 1,
                "similar": 2,
                "linked": 3,
                "joined": 4,
                "search": 5,
                "discovered": 6,
            }
            infos = sorted(
                infos_by_key.values(),
                key=lambda c: (
                    source_rank.get(c.source, 9),
                    -(c.members or 0),
                    c.title.casefold(),
                ),
            )
            self._channel_cache = infos
            self._save_channel_cache(infos)
            self._save_blacklist(infos)
            return infos
        finally:
            self.discovery_state.running = False
            if not self.discovery_state.progress.startswith("Done"):
                self.discovery_state.progress = "Done"

    async def _backfill_missing_members(self, infos_by_key: dict[str, ChannelInfo]) -> None:
        """Resolve member counts / descriptions for channels still missing them."""
        missing = [
            c
            for c in list(infos_by_key.values())
            if (c.members or 0) <= 0 or not (c.description or "").strip()
        ]
        if not missing:
            return
        for idx, channel in enumerate(missing, start=1):
            self.discovery_state.progress = (
                f"Backfilling @{channel.username} ({idx}/{len(missing)})…"
            )
            info = await self._inspect_channel(channel.username, source=channel.source or "catalog")
            if not info.valid:
                infos_by_key.pop(channel.username.casefold(), None)
                continue
            prev = infos_by_key.get(info.username.casefold())
            infos_by_key[info.username.casefold()] = self._preserve_user_enabled(prev, info)
            await asyncio.sleep(0.2)

    async def enrich_channel_details(self, only_incomplete: bool = True) -> None:
        """Fetch members + description from Telegram for manage-page display."""
        if self.discovery_state.running:
            return
        self.discovery_state = DiscoveryState(
            running=True, mode="enrich", progress="Preparing channel details…"
        )
        try:
            channels = [c for c in self.get_channels_fast() if c.valid]
            if only_incomplete:
                targets = [
                    c
                    for c in channels
                    if not c.banned
                    and ((c.members or 0) <= 0 or not (c.description or "").strip())
                ]
            else:
                targets = [c for c in channels if not c.banned]

            if not targets:
                self.discovery_state.message = "All channels already have members & descriptions."
                self.discovery_state.progress = "Done"
                return

            by_key = {c.username.casefold(): c for c in channels}
            updated = 0
            removed = 0
            for idx, channel in enumerate(targets, start=1):
                self.discovery_state.progress = (
                    f"Fetching details @{channel.username} ({idx}/{len(targets)})…"
                )
                info = await self._inspect_channel(
                    channel.username, source=channel.source or "catalog"
                )
                key = channel.username.casefold()
                if not info.valid:
                    by_key.pop(key, None)
                    removed += 1
                    await asyncio.sleep(0.15)
                    continue
                info.source = channel.source or info.source
                by_key[info.username.casefold()] = self._preserve_user_enabled(channel, info)
                updated += 1
                await asyncio.sleep(0.2)

            merged = list(by_key.values())
            self._persist_channels(merged)
            self.discovery_state.message = (
                f"Updated {updated} channel(s)"
                + (f", removed {removed} invalid" if removed else "")
                + "."
            )
            self.discovery_state.progress = "Done"
        except Exception as exc:
            logger.exception("enrich_channel_details failed")
            self.discovery_state.error = str(exc)
            self.discovery_state.progress = "Error"
        finally:
            self.discovery_state.running = False

    def _load_discovered_channels(self) -> list[ChannelInfo]:
        if not DISCOVERED_FILE.exists():
            return []
        try:
            data = json.loads(DISCOVERED_FILE.read_text(encoding="utf-8"))
            if not isinstance(data, list):
                return []
            out: list[ChannelInfo] = []
            for item in data:
                if isinstance(item, str):
                    name = item.lstrip("@")
                    if not self._is_valid_username(name):
                        continue
                    out.append(
                        ChannelInfo(
                            username=name,
                            title=name,
                            members=0,
                            included=True,
                            reason="Pending member count",
                            link=f"https://t.me/{name}",
                            source="catalog",
                            description="",
                            valid=True,
                            favorite=False,
                        )
                    )
                elif isinstance(item, dict) and item.get("username"):
                    name = str(item["username"]).lstrip("@")
                    if not self._is_valid_username(name):
                        continue
                    if item.get("valid") is False:
                        continue
                    members = int(item.get("members") or 0)
                    out.append(
                        ChannelInfo(
                            username=name,
                            title=item.get("title") or name,
                            members=members,
                            included=bool(item.get("included", True)),
                            reason=item.get("reason")
                            or ("OK" if members else "Pending member count"),
                            link=item.get("link") or f"https://t.me/{name}",
                            source=item.get("source") or "catalog",
                            description=item.get("description") or "",
                            valid=True,
                            favorite=bool(item.get("favorite", False)),
                            banned=bool(item.get("banned", False)),
                        )
                    )
            banned_keys = self._blacklist_keys()
            for c in out:
                if c.username.casefold() in banned_keys:
                    c.banned = True
                    c.included = False
                    c.favorite = False
            return out
        except Exception:
            logger.exception("Failed reading discovered channels file")
            return []

    def _load_discovered_usernames(self) -> list[str]:
        return [c.username for c in self._load_discovered_channels()]

    def _save_discovered_channels(self, channels: list[ChannelInfo]) -> None:
        DISCOVERED_FILE.parent.mkdir(parents=True, exist_ok=True)
        by_key: dict[str, ChannelInfo] = {}
        for c in channels:
            if not c.username:
                continue
            key = c.username.casefold()
            prev = by_key.get(key)
            if prev is None or (c.members or 0) >= (prev.members or 0):
                by_key[key] = c
        payload = [
            {
                "username": c.username,
                "title": c.title,
                "members": int(c.members or 0),
                "included": bool(c.included),
                "favorite": bool(c.favorite),
                "banned": bool(c.banned),
                "reason": c.reason,
                "link": c.link or f"https://t.me/{c.username}",
                "source": c.source,
                "description": c.description or "",
                "valid": True,
            }
            for c in sorted(
                by_key.values(), key=lambda x: (-(x.members or 0), x.username.casefold())
            )
            if c.valid and self._is_valid_username(c.username)
        ]
        DISCOVERED_FILE.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    def _save_discovered_usernames(self, usernames: list[str]) -> None:
        existing = {c.username.casefold(): c for c in self._load_discovered_channels()}
        channels: list[ChannelInfo] = []
        for name in usernames:
            key = name.casefold()
            channels.append(
                existing.get(key)
                or ChannelInfo(
                    username=name,
                    title=name,
                    members=0,
                    included=True,
                    reason="Pending member count",
                    link=f"https://t.me/{name}",
                    source="catalog",
                )
            )
        self._save_discovered_channels(channels)

    async def _discover_from_seeds(self, seed_usernames: list[str]) -> list[tuple[str, str]]:
        """
        Grow the list from seeds:
        1) Telegram "Similar channels" for each seed
        2) t.me links in each seed's description + recent posts
        3) Keyword search + joined STL chats as a light fallback
        """
        found: dict[str, str] = {}  # username -> source
        by_key: dict[str, str] = {}  # casefold -> username
        priority = {"similar": 0, "linked": 1, "joined": 2, "search": 3}
        seed_keys = {s.casefold() for s in seed_usernames}

        def add(username: str | None, source: str) -> None:
            if not username:
                return
            username = username.lstrip("@")
            key = username.casefold()
            if not username or key in TME_SKIP or key in seed_keys:
                return
            existing_name = by_key.get(key)
            if existing_name is None:
                found[username] = source
                by_key[key] = username
                return
            if priority[source] < priority.get(found[existing_name], 99):
                del found[existing_name]
                found[username] = source
                by_key[key] = username

        for seed in seed_usernames:
            # Official similar-channel recommendations (no join — public only)
            try:
                entity = await self.client.get_entity(seed)
                recs = await self.client(GetChannelRecommendationsRequest(channel=entity))
                for chat in getattr(recs, "chats", []) or []:
                    if isinstance(chat, Channel) and getattr(chat, "username", None):
                        add(chat.username, "similar")
            except FloodWaitError as exc:
                await asyncio.sleep(exc.seconds + 1)
            except Exception as exc:
                logger.warning("Similar channels failed for @%s: %s", seed, exc)
            await asyncio.sleep(0.7)

            # t.me links from about text + recent messages
            try:
                for linked in await self._extract_tme_links_from_channel(seed):
                    add(linked, "linked")
            except FloodWaitError as exc:
                await asyncio.sleep(exc.seconds + 1)
            except Exception as exc:
                logger.warning("Link crawl failed for @%s: %s", seed, exc)
            await asyncio.sleep(0.5)

        # One extra hop: similar channels of the strongest new finds
        hop_from = [u for u, s in found.items() if s == "similar"][:12]
        for username in hop_from:
            try:
                entity = await self.client.get_entity(username)
                recs = await self.client(GetChannelRecommendationsRequest(channel=entity))
                for chat in getattr(recs, "chats", []) or []:
                    if isinstance(chat, Channel) and getattr(chat, "username", None):
                        add(chat.username, "similar")
            except Exception as exc:
                logger.debug("Hop recommendations failed for @%s: %s", username, exc)
            await asyncio.sleep(0.7)

        # Fallback: keyword search + already-joined STL dialogs
        for query in DISCOVERY_QUERIES[:4]:
            try:
                result = await self.client(ContactsSearchRequest(q=query, limit=30))
                for chat in result.chats or []:
                    if not isinstance(chat, Channel):
                        continue
                    username = getattr(chat, "username", None)
                    title = (chat.title or "") + " " + (username or "")
                    if username and STL_HINT.search(title):
                        add(username, "search")
            except Exception as exc:
                logger.warning("Discovery search %r failed: %s", query, exc)
            await asyncio.sleep(0.6)

        try:
            async for dialog in self.client.iter_dialogs(limit=300):
                entity = dialog.entity
                if not isinstance(entity, Channel):
                    continue
                username = getattr(entity, "username", None)
                if username and STL_HINT.search(f"{dialog.name or ''} {username}"):
                    add(username, "joined")
        except Exception as exc:
            logger.warning("Dialog discovery failed: %s", exc)

        return list(found.items())

    async def _extract_tme_links_from_channel(
        self, username: str, message_limit: int | None = None
    ) -> set[str]:
        """
        Pull public @usernames from channel about + posts that actually contain links.

        Prefer URL-filtered history and a t.me text search so we cover hundreds of
        link posts instead of only the newest N messages of any type.
        """
        limit = message_limit if message_limit is not None else LINK_CRAWL_LIMIT
        found: set[str] = set()
        seen_ids: set[int] = set()
        entity = await self.client.get_entity(username)

        try:
            full = await self.client(GetFullChannelRequest(entity))
            about = getattr(full.full_chat, "about", "") or ""
            found.update(self._usernames_from_text(about))
        except Exception:
            pass

        async def consume(iterator) -> None:
            async for message in iterator:
                if not isinstance(message, Message) or message.id in seen_ids:
                    continue
                seen_ids.add(message.id)
                found.update(self._usernames_from_message(message))

        # 1) Messages Telegram classifies as containing URLs
        try:
            await consume(
                self.client.iter_messages(
                    entity,
                    filter=InputMessagesFilterUrl(),
                    limit=limit,
                )
            )
        except Exception as exc:
            logger.warning("URL-filter crawl failed for @%s: %s", username, exc)

        await asyncio.sleep(0.35)

        # 2) Explicit search for t.me mentions (catches more referral posts)
        try:
            await consume(
                self.client.iter_messages(
                    entity,
                    search="t.me",
                    limit=limit,
                )
            )
        except Exception as exc:
            logger.warning("t.me search crawl failed for @%s: %s", username, exc)

        await asyncio.sleep(0.35)

        # 3) telegram.me variant
        try:
            await consume(
                self.client.iter_messages(
                    entity,
                    search="telegram.me",
                    limit=min(200, limit),
                )
            )
        except Exception as exc:
            logger.debug("telegram.me search crawl failed for @%s: %s", username, exc)

        found.discard(username.lstrip("@"))
        found = {u for u in found if u.casefold() != username.casefold()}
        return found

    def _usernames_from_message(self, message: Message) -> set[str]:
        text = message.message or ""
        found = self._usernames_from_text(text)
        for ent in message.entities or []:
            if isinstance(ent, MessageEntityTextUrl):
                found.update(self._usernames_from_text(ent.url or ""))
            elif isinstance(ent, MessageEntityUrl):
                chunk = text[ent.offset : ent.offset + ent.length]
                found.update(self._usernames_from_text(chunk))
        return found

    def _usernames_from_text(self, text: str) -> set[str]:
        if not text:
            return set()
        out: set[str] = set()
        for match in TME_USERNAME.finditer(text):
            name = match.group(1)
            if name.startswith("+") or name.casefold() in TME_SKIP:
                continue
            if self._is_valid_username(name):
                out.add(name)
        return out

    async def _inspect_channel(self, username: str, source: str = "seed") -> ChannelInfo:
        name = (username or "").lstrip("@")
        if not self._is_valid_username(name):
            return self._invalid_channel(name, "Invalid username format", source)

        link = f"https://t.me/{name}"
        try:
            entity = await self.client.get_entity(name)
            if not isinstance(entity, Channel):
                return self._invalid_channel(name, "Not a channel/supergroup", source)

            public_username = getattr(entity, "username", None) or name
            if not self._is_valid_username(public_username):
                return self._invalid_channel(
                    public_username, "Channel has no valid public username", source
                )

            members = int(getattr(entity, "participants_count", 0) or 0)
            description = ""
            try:
                # Read-only: never JoinChannel — GetFullChannel works for public channels
                full = await self.client(GetFullChannelRequest(entity))
                if members <= 0:
                    members = int(getattr(full.full_chat, "participants_count", 0) or 0)
                description = (getattr(full.full_chat, "about", "") or "").strip()
            except Exception:
                pass

            title = entity.title or public_username
            link = f"https://t.me/{public_username}"

            if members and members < MIN_CHANNEL_MEMBERS:
                reason = f"Below {MIN_CHANNEL_MEMBERS:,} members (still usable)"
            elif members:
                reason = "OK"
            else:
                reason = "Member count unknown"

            # Valid public channels are kept (even under floor). Seeds always kept.
            return ChannelInfo(
                username=public_username,
                title=title,
                members=members,
                included=True,
                reason=reason,
                link=link,
                source=source,
                description=description,
                valid=True,
            )
        except (UsernameNotOccupiedError, ChannelPrivateError, ValueError) as exc:
            return self._invalid_channel(name, str(exc), source)
        except FloodWaitError as exc:
            await asyncio.sleep(exc.seconds + 1)
            return await self._inspect_channel(name, source=source)
        except Exception as exc:
            msg = str(exc)
            if "Nobody is using this username" in msg or "username is unacceptable" in msg:
                return self._invalid_channel(name, msg, source)
            if "No user has" in msg:
                return self._invalid_channel(name, msg, source)
            return self._invalid_channel(name, f"Error: {exc}", source)

    async def run_search(
        self,
        query: str,
        files_only: bool = True,
        max_age_days: int = MAX_AGE_DAYS,
        per_channel_limit: int = 40,
    ) -> None:
        async with self._search_lock:
            if self.search_state.running:
                return
            self._search_cancel_requested = False
            self.search_state = SearchState(
                running=True,
                status="running",
                progress="Preparing…",
                query=query,
                mode="search",
                results=[],
            )

        seen_keys: set[str] = set()
        stopped = False

        try:
            active = [c for c in self.get_channels_fast() if c.included and not c.banned]
            self.search_state.channels_total = len(active)
            self.search_state.progress = f"Searching {len(active)} channel(s)…"
            variants = generate_variants(query)
            cutoff = datetime.now(timezone.utc) - timedelta(days=max_age_days)

            for idx, channel in enumerate(active, start=1):
                if self._search_stop_requested():
                    stopped = True
                    break

                self.search_state.channels_scanned = idx
                self.search_state.progress = f"Searching @{channel.username} ({idx}/{len(active)})…"
                try:
                    entity = await self.client.get_entity(channel.username)
                    await self._search_channel(
                        entity=entity,
                        channel=channel,
                        variants=variants,
                        files_only=files_only,
                        cutoff=cutoff,
                        per_channel_limit=per_channel_limit,
                        seen_keys=seen_keys,
                    )
                    if self._search_stop_requested():
                        stopped = True
                        break
                except FloodWaitError as exc:
                    if self._search_stop_requested():
                        stopped = True
                        break
                    msg = f"Rate limited on @{channel.username}: waiting {exc.seconds}s"
                    logger.warning(msg)
                    self.search_state.errors.append(msg)
                    await asyncio.sleep(exc.seconds + 1)
                except Exception as exc:
                    msg = f"@{channel.username}: {exc}"
                    logger.exception(msg)
                    self.search_state.errors.append(msg)

                if self._search_stop_requested():
                    stopped = True
                    break

                await asyncio.sleep(SEARCH_DELAY_SECONDS)

            self.search_state.results.sort(key=lambda r: r.get("date", ""), reverse=True)
            if stopped:
                self.search_state.status = "stopped"
                n = len(self.search_state.results)
                src = sum(len(g.get("sources") or []) for g in self.search_state.results)
                self.search_state.progress = f"Stopped — {n} unique file(s) from {src} post(s)"
            else:
                self.search_state.status = "done"
                n = len(self.search_state.results)
                src = sum(len(g.get("sources") or []) for g in self.search_state.results)
                self.search_state.progress = f"Found {n} unique file(s) from {src} post(s)"
            self.search_state.finished_at = datetime.now(timezone.utc).isoformat()
        except Exception as exc:
            self.search_state.status = "error"
            self.search_state.progress = str(exc)
            self.search_state.errors.append(str(exc))
        finally:
            self.search_state.running = False
            self._search_cancel_requested = False

    async def run_browse_channel(self, username: str, limit: int = 150) -> None:
        """List recent STL/3MF/ZIP files from one channel, newest first."""
        name = (username or "").lstrip("@")
        channel = self.get_channel(name)
        if channel is None or not channel.valid:
            async with self._search_lock:
                self.search_state = SearchState(
                    running=False,
                    status="error",
                    progress=f"Unknown channel @{name}",
                    query=f"@{name}",
                    mode="browse",
                    browse_username=name,
                    browse_title=name,
                    results=[],
                    errors=[f"Channel @{name} is not in your list."],
                    finished_at=datetime.now(timezone.utc).isoformat(),
                )
            return

        async with self._search_lock:
            if self.search_state.running:
                return
            self._search_cancel_requested = False
            self.search_state = SearchState(
                running=True,
                status="running",
                progress=f"Browsing @{channel.username}…",
                query=f"@{channel.username}",
                mode="browse",
                browse_username=channel.username,
                browse_title=channel.title or channel.username,
                results=[],
                channels_total=1,
            )

        seen_keys: set[str] = set()
        stopped = False

        try:
            entity = await self.client.get_entity(channel.username)
            self.search_state.progress = f"Loading files from @{channel.username}…"
            found = 0

            async for message in self.client.iter_messages(
                entity,
                filter=InputMessagesFilterDocument(),
                limit=max(limit * 4, limit),  # overscan: many docs aren't STL/ZIP
            ):
                if self._search_stop_requested():
                    stopped = True
                    break
                if not isinstance(message, Message):
                    continue
                if not self._document_matches_extensions(message):
                    continue

                hit = await self._to_hit(message, channel, "browse")
                if hit:
                    self._append_search_result(hit, seen_keys)
                    found += 1
                    self.search_state.progress = (
                        f"@{channel.username}: {found} file(s)…"
                    )
                    if found >= limit:
                        break

            self.search_state.channels_scanned = 1
            self.search_state.results.sort(key=lambda r: r.get("date", ""), reverse=True)
            n = len(self.search_state.results)
            if stopped:
                self.search_state.status = "stopped"
                self.search_state.progress = f"Stopped — {n} file(s) in @{channel.username}"
            else:
                self.search_state.status = "done"
                self.search_state.progress = f"{n} file(s) in @{channel.username}"
            self.search_state.finished_at = datetime.now(timezone.utc).isoformat()
        except FloodWaitError as exc:
            self.search_state.status = "error"
            self.search_state.progress = f"Rate limited: wait {exc.seconds}s"
            self.search_state.errors.append(self.search_state.progress)
            self.search_state.finished_at = datetime.now(timezone.utc).isoformat()
        except Exception as exc:
            self.search_state.status = "error"
            self.search_state.progress = str(exc)
            self.search_state.errors.append(str(exc))
            self.search_state.finished_at = datetime.now(timezone.utc).isoformat()
        finally:
            self.search_state.running = False
            self._search_cancel_requested = False

    def _file_group_key(self, file_name: str, file_size: int, message_id: int, channel: str) -> str:
        """Same name+size => same file; no-name posts stay unique per message."""
        name = (file_name or "").strip()
        if name and file_size > 0:
            return f"{name.casefold()}|{file_size}"
        if name:
            return f"{name.casefold()}|0"
        return f"msg|{channel.casefold()}|{message_id}"

    def _group_index(self, group_key: str) -> int | None:
        for i, row in enumerate(self.search_state.results):
            if row.get("group_key") == group_key:
                return i
        return None

    def _group_has_thumb(self, group_key: str) -> bool:
        idx = self._group_index(group_key)
        if idx is None:
            return False
        return bool(self.search_state.results[idx].get("thumb_url"))

    def _append_search_result(self, hit: SearchHit, seen_keys: set[str]) -> None:
        msg_key = f"{hit.channel_username}:{hit.message_id}"
        if msg_key in seen_keys:
            return
        seen_keys.add(msg_key)

        group_key = self._file_group_key(
            hit.file_name, hit.file_size, hit.message_id, hit.channel_username
        )
        source = {
            "channel_username": hit.channel_username,
            "channel_title": hit.channel_title,
            "message_id": hit.message_id,
            "date": hit.date,
            "message_link": hit.message_link,
            "query_matched": hit.query_matched,
        }

        idx = self._group_index(group_key)
        if idx is None:
            self.search_state.results.append(
                {
                    "group_key": group_key,
                    "file_name": hit.file_name,
                    "file_ext": hit.file_ext,
                    "file_size": hit.file_size,
                    "text": hit.text,
                    "thumb_url": hit.thumb_url,
                    "date": hit.date,
                    "query_matched": hit.query_matched,
                    "channel_username": hit.channel_username,
                    "channel_title": hit.channel_title,
                    "message_id": hit.message_id,
                    "message_link": hit.message_link,
                    "sources": [source],
                }
            )
            return

        group = self.search_state.results[idx]
        # One entry per channel for this file — keep the newest post
        existing_i = next(
            (
                i
                for i, s in enumerate(group["sources"])
                if s["channel_username"].casefold() == hit.channel_username.casefold()
            ),
            None,
        )
        if existing_i is not None:
            old = group["sources"][existing_i]
            if hit.date and (not old.get("date") or hit.date >= old["date"]):
                group["sources"][existing_i] = source
        else:
            group["sources"].append(source)

        # Prefer newest date + fill in preview/text if missing
        if hit.date and (not group.get("date") or hit.date > group["date"]):
            group["date"] = hit.date
            group["message_link"] = hit.message_link
            group["channel_username"] = hit.channel_username
            group["channel_title"] = hit.channel_title
            group["message_id"] = hit.message_id
        if hit.thumb_url and not group.get("thumb_url"):
            group["thumb_url"] = hit.thumb_url
        if hit.text and not group.get("text"):
            group["text"] = hit.text
        if hit.query_matched and not group.get("query_matched"):
            group["query_matched"] = hit.query_matched

    async def _search_channel(
        self,
        entity: Channel,
        channel: ChannelInfo,
        variants: list[str],
        files_only: bool,
        cutoff: datetime,
        per_channel_limit: int,
        seen_keys: set[str],
    ) -> None:
        msg_filter = InputMessagesFilterDocument() if files_only else InputMessagesFilterEmpty()
        channel_seen: set[int] = set()

        for variant in variants:
            if self._search_stop_requested():
                return
            try:
                async for message in self.client.iter_messages(
                    entity,
                    search=variant,
                    filter=msg_filter,
                    limit=per_channel_limit,
                    offset_date=None,
                ):
                    if self._search_stop_requested():
                        return
                    if not isinstance(message, Message):
                        continue
                    if message.id in channel_seen:
                        continue
                    if message.date:
                        msg_date = message.date
                        if msg_date.tzinfo is None:
                            msg_date = msg_date.replace(tzinfo=timezone.utc)
                        if msg_date < cutoff:
                            break

                    if files_only and not self._has_wanted_file(message):
                        if not message.document:
                            continue
                        if not self._document_matches_extensions(message):
                            continue

                    hit = await self._to_hit(message, channel, variant)
                    if hit:
                        channel_seen.add(message.id)
                        self._append_search_result(hit, seen_keys)
            except FloodWaitError:
                raise
            except Exception as exc:
                self.search_state.errors.append(f"@{channel.username} / '{variant}': {exc}")
            await asyncio.sleep(0.4)

    def _has_wanted_file(self, message: Message) -> bool:
        return self._document_matches_extensions(message)

    def _document_matches_extensions(self, message: Message) -> bool:
        if not message.document:
            return False
        name = self._file_name(message).lower()
        return any(name.endswith(ext) for ext in FILE_EXTENSIONS)

    def _file_name(self, message: Message) -> str:
        if not message.document:
            return ""
        for attr in message.document.attributes or []:
            if isinstance(attr, DocumentAttributeFilename):
                return attr.file_name
        return f"file_{message.id}"

    def _file_ext(self, file_name: str) -> str:
        return Path(file_name).suffix.lower()

    async def _to_hit(self, message: Message, channel: ChannelInfo, variant: str) -> SearchHit | None:
        file_name = self._file_name(message)
        text = (message.message or message.text or "").strip()
        if not file_name and not text:
            return None

        file_size = int(message.document.size) if message.document else 0
        group_key = self._file_group_key(file_name, file_size, message.id, channel.username)

        # Skip slow preview fetch when we already have this file + a thumb
        thumb_url = None
        if not self._group_has_thumb(group_key):
            thumb_path = await self._save_preview(message, channel.username)
            if thumb_path:
                thumb_url = f"/thumbs/{thumb_path.name}"

        username = channel.username
        link = f"https://t.me/{username}/{message.id}"
        date = ""
        if message.date:
            date = message.date.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

        return SearchHit(
            channel_username=username,
            channel_title=channel.title,
            message_id=message.id,
            date=date,
            text=text[:400],
            file_name=file_name,
            file_ext=self._file_ext(file_name),
            file_size=file_size,
            message_link=link,
            thumb_url=thumb_url,
            query_matched=variant,
        )

    async def _save_preview(self, message: Message, channel_username: str) -> Path | None:
        """
        ZIP / STL posts almost never have useful thumbs. Prefer:
        1) photo in the same album
        2) nearest photo posted before/after the file (typical channel pattern)
        3) the message's own photo
        4) document embedded thumb as last resort
        """
        out = THUMBS_DIR / f"{channel_username}_{message.id}.jpg"
        if out.exists() and out.stat().st_size > 0:
            return out
        if out.exists() and out.stat().st_size == 0:
            out.unlink(missing_ok=True)

        photo_message = await self._resolve_preview_message(message, channel_username)

        try:
            if photo_message:
                path = await self.client.download_media(photo_message, file=str(out))
                if path and out.exists() and out.stat().st_size > 0:
                    return out

            if message.photo:
                path = await self.client.download_media(message, file=str(out))
                if path and out.exists() and out.stat().st_size > 0:
                    return out

            if message.document and getattr(message.document, "thumbs", None):
                path = await self.client.download_media(message, file=str(out), thumb=-1)
                if path and out.exists() and out.stat().st_size > 0:
                    return out
        except Exception as exc:
            logger.debug("Preview download failed for %s/%s: %s", channel_username, message.id, exc)
            if out.exists() and out.stat().st_size == 0:
                out.unlink(missing_ok=True)

        return None

    async def _resolve_preview_message(
        self, message: Message, channel_username: str, window: int = 12
    ) -> Message | None:
        """Find the best nearby photo for a file post."""
        chat = channel_username
        nearby_ids = [
            i
            for i in range(message.id - window, message.id + window + 1)
            if i > 0 and i != message.id
        ]

        neighbors: list[Message] = []
        try:
            fetched = await self.client.get_messages(chat, ids=nearby_ids)
            if not isinstance(fetched, list):
                fetched = [fetched]
            neighbors = [m for m in fetched if isinstance(m, Message)]
        except Exception as exc:
            logger.debug("Neighbor fetch failed for %s/%s: %s", channel_username, message.id, exc)
            return None

        # Album siblings first
        if message.grouped_id:
            album_photos = [
                m
                for m in neighbors
                if m.grouped_id == message.grouped_id and m.photo
            ]
            if album_photos:
                album_photos.sort(key=lambda m: abs(m.id - message.id))
                return album_photos[0]

        # Typical STL channel: render photo(s), then the ZIP a few messages later
        photos = [m for m in neighbors if m.photo]
        if not photos:
            return None

        def score(m: Message) -> tuple[int, int]:
            # Prefer photos before the file, then closest by id
            before = 0 if m.id < message.id else 1
            return (before, abs(m.id - message.id))

        photos.sort(key=score)
        return photos[0]

    @staticmethod
    def _safe_filename(name: str) -> str:
        raw = (name or "file").strip().replace("\\", "_").replace("/", "_")
        cleaned = re.sub(r'[<>:"|?*\x00-\x1f]', "_", raw).strip(" .")
        return cleaned[:180] or "file"

    def _unique_path(self, folder: Path, filename: str) -> Path:
        folder.mkdir(parents=True, exist_ok=True)
        safe = self._safe_filename(filename)
        path = folder / safe
        if not path.exists() and not Path(str(path) + ".tgdlpart").exists():
            return path
        stem = path.stem
        suffix = path.suffix
        i = 1
        while True:
            cand = folder / f"{stem} ({i}){suffix}"
            if not cand.exists() and not Path(str(cand) + ".tgdlpart").exists():
                return cand
            i += 1

    @staticmethod
    def _next_unique_dir_name(folder: Path, name: str) -> str:
        safe = "".join(c if c not in '<>:"|?*' else "_" for c in name).strip(" .") or "folder"
        safe = safe[:120]
        if not (folder / safe).exists():
            return safe
        i = 1
        while True:
            alt = f"{safe} ({i})"
            if not (folder / alt).exists():
                return alt
            i += 1

    def _desktop_conflicts_for_filename(self, filename: str) -> list[dict[str, Any]]:
        """Existing file/folder on disk in DOWNLOAD_DIR that would collide."""
        DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
        safe = self._safe_filename(filename)
        path = DOWNLOAD_DIR / safe
        conflicts: list[dict[str, Any]] = []
        if path.exists():
            kind = "folder" if path.is_dir() else "file"
            conflicts.append(
                {
                    "kind": kind,
                    "name": path.name,
                    "path": str(path.resolve()),
                    "suggested": self._unique_path(DOWNLOAD_DIR, filename).name,
                }
            )
        suffix = path.suffix.lower()
        if suffix in {".zip", ".rar"}:
            stem_dir = DOWNLOAD_DIR / path.stem
            if stem_dir.exists() and stem_dir.is_dir():
                resolved = str(stem_dir.resolve())
                if not any(c["path"] == resolved for c in conflicts):
                    conflicts.append(
                        {
                            "kind": "folder",
                            "name": stem_dir.name,
                            "path": resolved,
                            "suggested": self._next_unique_dir_name(
                                DOWNLOAD_DIR, path.stem
                            ),
                        }
                    )
        return conflicts

    @staticmethod
    def _download_index_key(channel: str, message_id: int) -> str:
        return f"{(channel or '').lstrip('@').casefold()}:{int(message_id)}"

    def _load_download_index(self) -> dict[str, Any]:
        path = DOWNLOAD_INDEX_FILE
        if not path.exists():
            return {}
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            return data if isinstance(data, dict) else {}
        except Exception:
            logger.debug("Could not read download index", exc_info=True)
            return {}

    def _save_download_index(self, data: dict[str, Any]) -> None:
        DOWNLOAD_INDEX_FILE.parent.mkdir(parents=True, exist_ok=True)
        DOWNLOAD_INDEX_FILE.write_text(
            json.dumps(data, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    def _record_download_index(self, job: dict[str, Any]) -> None:
        """Remember where a successful PC download landed (for future duplicate checks)."""
        if job.get("mode") != "desktop" or job.get("status") != "done":
            return
        mid = int(job.get("message_id") or 0)
        if mid <= 0:
            return
        dest = job.get("path")
        if not dest:
            return
        dest_path = Path(dest)
        if not dest_path.exists():
            return
        key = self._download_index_key(str(job.get("channel") or ""), mid)
        index = self._load_download_index()
        index[key] = {
            "channel": job.get("channel"),
            "message_id": mid,
            "source_filename": job.get("source_filename") or job.get("filename"),
            "path": str(dest_path.resolve()),
            "extracted": bool(job.get("extracted")),
            "finished_at": job.get("finished_at"),
        }
        if len(index) > 500:
            items = sorted(
                index.items(),
                key=lambda kv: kv[1].get("finished_at") or "",
                reverse=True,
            )
            index = dict(items[:500])
        self._save_download_index(index)

    def _bootstrap_index_from_history(self) -> None:
        """Seed the on-disk index from history entries whose paths still exist."""
        index = self._load_download_index()
        changed = False
        for entry in self._load_download_history():
            if entry.get("status") != "done" or entry.get("mode") != "desktop":
                continue
            mid = int(entry.get("message_id") or 0)
            if mid <= 0:
                continue
            path = Path(str(entry.get("path") or ""))
            if not path.exists():
                continue
            key = self._download_index_key(str(entry.get("channel") or ""), mid)
            existing = index.get(key)
            if existing and Path(str(existing.get("path") or "")).exists():
                continue
            index[key] = {
                "channel": entry.get("channel"),
                "message_id": mid,
                "source_filename": entry.get("source_filename") or entry.get("filename"),
                "path": str(path.resolve()),
                "extracted": bool(entry.get("extracted")),
                "finished_at": entry.get("finished_at"),
            }
            changed = True
        if changed:
            self._save_download_index(index)

    def _index_conflicts(
        self, channel: str, message_id: int, filename: str
    ) -> list[dict[str, Any]]:
        """
        Conflicts from prior successful downloads — only if the saved path
        still exists on disk (deleted folders do not count).
        """
        self._bootstrap_index_from_history()
        conflicts: list[dict[str, Any]] = []
        index = self._load_download_index()
        dirty = False
        key = self._download_index_key(channel, message_id)
        entry = index.get(key)
        if entry:
            path = Path(str(entry.get("path") or ""))
            if path.exists():
                suggested = (
                    self._next_unique_dir_name(DOWNLOAD_DIR, path.name)
                    if path.is_dir()
                    else self._unique_path(DOWNLOAD_DIR, path.name).name
                )
                conflicts.append(
                    {
                        "kind": "folder" if path.is_dir() else "file",
                        "name": path.name,
                        "path": str(path.resolve()),
                        "suggested": suggested,
                        "from_index": True,
                    }
                )
            else:
                # Stale: user deleted the files — drop the index entry
                index.pop(key, None)
                dirty = True

        # Also match by prior source filename → dest still present
        safe_name = self._safe_filename(filename).casefold()
        for ikey, entry in list(index.items()):
            if ikey == key:
                continue
            src = self._safe_filename(str(entry.get("source_filename") or "")).casefold()
            if not src or src != safe_name:
                continue
            path = Path(str(entry.get("path") or ""))
            if not path.exists():
                index.pop(ikey, None)
                dirty = True
                continue
            resolved = str(path.resolve())
            if any(c["path"] == resolved for c in conflicts):
                continue
            suggested = (
                self._next_unique_dir_name(DOWNLOAD_DIR, path.name)
                if path.is_dir()
                else self._unique_path(DOWNLOAD_DIR, path.name).name
            )
            conflicts.append(
                {
                    "kind": "folder" if path.is_dir() else "file",
                    "name": path.name,
                    "path": resolved,
                    "suggested": suggested,
                    "from_index": True,
                }
            )

        if dirty:
            self._save_download_index(index)
        return conflicts

    async def check_desktop_conflict(
        self,
        channel_username: str,
        message_id: int,
        filename_hint: str = "",
    ) -> dict[str, Any]:
        """Look up Telegram filename and report PC-folder collisions (disk only)."""
        name = (channel_username or "").lstrip("@")
        mid = int(message_id)
        file_name = (filename_hint or "").strip() or f"{name}_{mid}"
        try:
            if not self.client.is_connected():
                await self.connect()
            entity = await self.client.get_entity(name)
            message = await self.client.get_messages(entity, ids=mid)
            if message and getattr(message, "document", None):
                file_name = self._file_name(message) or file_name
        except Exception as exc:
            logger.debug("Conflict check could not resolve message: %s", exc)

        conflicts = self._desktop_conflicts_for_filename(file_name)
        for c in self._index_conflicts(name, mid, file_name):
            if not any(x["path"] == c["path"] for x in conflicts):
                conflicts.append(c)

        suggested = conflicts[0]["suggested"] if conflicts else file_name
        return {
            "filename": file_name,
            "conflicts": conflicts,
            "has_conflict": bool(conflicts),
            "suggested": suggested,
            "folder": str(DOWNLOAD_DIR.resolve()),
        }

    @staticmethod
    def _partial_path(final: Path) -> Path:
        return Path(str(final) + ".tgdlpart")

    @staticmethod
    def _delete_download_artifact(*paths: Path | str | None) -> None:
        for raw in paths:
            if not raw:
                continue
            path = Path(raw)
            try:
                path.unlink(missing_ok=True)
            except Exception:
                logger.debug("Could not delete %s", path, exc_info=True)

    def cleanup_incomplete_downloads(self) -> int:
        """Remove leftover *.tgdlpart files from PC + cache download folders."""
        removed = 0
        for folder in (DOWNLOAD_DIR, DOWNLOAD_CACHE_DIR):
            try:
                folder.mkdir(parents=True, exist_ok=True)
                for path in folder.glob("*.tgdlpart"):
                    try:
                        path.unlink(missing_ok=True)
                        removed += 1
                    except Exception:
                        logger.debug("Could not delete partial %s", path, exc_info=True)
            except Exception:
                logger.debug("Could not scan %s for partials", folder, exc_info=True)
        if removed:
            logger.info("Cleaned %s incomplete download file(s)", removed)
        return removed

    def list_download_jobs(self) -> list[dict[str, Any]]:
        jobs = list(self._download_jobs.values())
        jobs.sort(key=lambda j: j.get("created_at") or "", reverse=True)
        return jobs[:40]

    def get_download_job(self, job_id: str) -> dict[str, Any] | None:
        return self._download_jobs.get(job_id)

    def _history_snapshot(self, job: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": job.get("id"),
            "channel": job.get("channel"),
            "message_id": job.get("message_id"),
            "filename": job.get("filename"),
            "source_filename": job.get("source_filename") or job.get("filename"),
            "mode": job.get("mode"),
            "status": job.get("status"),
            "path": job.get("path"),
            "folder": job.get("folder"),
            "extracted": bool(job.get("extracted")),
            "extract_note": job.get("extract_note"),
            "error": job.get("error"),
            "created_at": job.get("created_at"),
            "finished_at": job.get("finished_at"),
        }

    def _load_download_history(self) -> list[dict[str, Any]]:
        path = DOWNLOAD_HISTORY_FILE
        if not path.exists():
            return []
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            return data if isinstance(data, list) else []
        except Exception:
            logger.debug("Could not read download history", exc_info=True)
            return []

    def _save_download_history(self, entries: list[dict[str, Any]]) -> None:
        DOWNLOAD_HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
        DOWNLOAD_HISTORY_FILE.write_text(
            json.dumps(entries[:200], indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    def _record_download_history(self, job: dict[str, Any]) -> None:
        """Persist finished jobs so Clear can drop them from the live dock."""
        status = job.get("status")
        if status not in ("done", "error", "cancelled"):
            return
        snap = self._history_snapshot(job)
        entries = self._load_download_history()
        jid = snap.get("id")
        entries = [e for e in entries if e.get("id") != jid]
        entries.insert(0, snap)
        self._save_download_history(entries)
        if status == "done":
            self._record_download_index(job)

    def list_download_history(self, limit: int = 100) -> list[dict[str, Any]]:
        entries = self._load_download_history()
        return entries[: max(1, min(int(limit), 200))]

    def clear_finished_downloads(self) -> int:
        """Remove finished jobs from the live queue (history already saved)."""
        finished = {"done", "error", "cancelled"}
        remove_ids = [
            jid
            for jid, job in self._download_jobs.items()
            if job.get("status") in finished
        ]
        for jid in remove_ids:
            job = self._download_jobs.pop(jid, None)
            if job:
                self._record_download_history(job)
        return len(remove_ids)

    def clear_download_history(self) -> int:
        entries = self._load_download_history()
        n = len(entries)
        self._save_download_history([])
        return n

    def enqueue_download(
        self,
        channel_username: str,
        message_id: int,
        *,
        mode: str = "desktop",
        filename_hint: str = "",
        allow_duplicate: bool = False,
    ) -> dict[str, Any]:
        """Queue a download; returns immediately so browsing can continue."""
        name = (channel_username or "").lstrip("@")
        mid = int(message_id)
        mode_norm = "local" if mode == "local" else "desktop"
        self._download_job_seq += 1
        job_id = f"dl-{self._download_job_seq}-{mid}"
        job: dict[str, Any] = {
            "id": job_id,
            "channel": name,
            "message_id": mid,
            "filename": filename_hint or f"{name}_{mid}",
            "source_filename": filename_hint or f"{name}_{mid}",
            "mode": mode_norm,
            "status": "queued",
            "progress": 0.0,
            "received": 0,
            "total": 0,
            "path": None,
            "partial_path": None,
            "folder": str((DOWNLOAD_DIR if mode_norm == "desktop" else DOWNLOAD_CACHE_DIR).resolve()),
            "error": None,
            "cancel_requested": False,
            "allow_duplicate": bool(allow_duplicate),
            "extracting": False,
            "extracted": False,
            "extract_note": None,
            "conflict": None,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "finished_at": None,
        }
        self._download_jobs[job_id] = job
        self._ensure_download_worker()
        self._download_queue.put_nowait(job_id)
        return job

    def resolve_download_conflict(self, job_id: str, *, proceed: bool) -> dict[str, Any] | None:
        """User answered needs_confirm: download/extract as (1) or keep existing."""
        job = self._download_jobs.get(job_id)
        if not job or job.get("status") != "needs_confirm":
            return job
        if not proceed:
            conflict = job.get("conflict") or {}
            existing = Path(str(conflict.get("path") or ""))
            archive = job.get("path")
            if existing.exists():
                # User chose to keep what's already on disk — remember it for next time
                job["status"] = "done"
                job["extracted"] = existing.is_dir() or bool(job.get("extracted"))
                job["path"] = str(existing.resolve())
                job["filename"] = existing.name
                job["extract_note"] = "Skipped — kept existing on disk"
                job["conflict"] = None
                job["finished_at"] = datetime.now(timezone.utc).isoformat()
                self._record_download_history(job)
            elif archive and Path(archive).exists() and is_extractable_archive(archive):
                job["status"] = "done"
                job["extracted"] = False
                job["extract_note"] = "Skipped — kept existing folder, archive left as-is"
                job["conflict"] = None
                job["finished_at"] = datetime.now(timezone.utc).isoformat()
                self._record_download_history(job)
            else:
                job["status"] = "cancelled"
                job["error"] = "Skipped — already exists"
                job["conflict"] = None
                job["finished_at"] = datetime.now(timezone.utc).isoformat()
                self._record_download_history(job)
            return job

        job["allow_duplicate"] = True
        job["conflict"] = None
        job["error"] = None
        job["cancel_requested"] = False
        archive = job.get("path")
        if (
            archive
            and Path(archive).exists()
            and is_extractable_archive(archive)
            and job.get("mode") == "desktop"
            and not job.get("extracted")
        ):
            job["status"] = "running"
            job["extracting"] = True
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                return job
            loop.create_task(self._resume_extract_job(job_id))
            return job

        job["status"] = "queued"
        job["progress"] = 0.0
        self._ensure_download_worker()
        self._download_queue.put_nowait(job_id)
        return job

    async def _resume_extract_job(self, job_id: str) -> None:
        job = self._download_jobs.get(job_id)
        if not job:
            return
        archive = Path(job.get("path") or "")
        dest_dir = DOWNLOAD_DIR
        async with self._download_lock:
            try:
                if job.get("cancel_requested"):
                    self._mark_job_cancelled(job, archive)
                    return
                job["status"] = "running"
                job["extracting"] = True
                extracted = await asyncio.to_thread(
                    extract_download_archive,
                    archive,
                    dest_dir,
                    allow_duplicate=True,
                )
                job["extracted"] = True
                job["extract_note"] = extracted.note
                job["path"] = str(extracted.path)
                job["filename"] = extracted.path.name
                job["folder"] = str(extracted.path.parent.resolve())
                job["progress"] = 100.0
                job["status"] = "done"
                job["finished_at"] = datetime.now(timezone.utc).isoformat()
                self._record_download_history(job)
            except Exception as exc:
                logger.warning("Resume extract failed for %s: %s", job_id, exc)
                job["extracted"] = False
                job["extract_note"] = f"Saved archive (extract failed: {exc})"
                job["status"] = "done"
                job["finished_at"] = datetime.now(timezone.utc).isoformat()
                self._record_download_history(job)
            finally:
                job["extracting"] = False

    def cancel_download(self, job_id: str) -> dict[str, Any] | None:
        """Cancel a queued or running download."""
        job = self._download_jobs.get(job_id)
        if not job:
            return None
        status = job.get("status")
        if status in ("done", "error", "cancelled"):
            return job
        job["cancel_requested"] = True
        if status in ("queued", "needs_confirm"):
            self._mark_job_cancelled(job)
        else:
            job["error"] = "Cancelling…"
        return job

    def _mark_job_cancelled(self, job: dict[str, Any], *paths: Path | str | None) -> None:
        job["status"] = "cancelled"
        job["error"] = "Cancelled"
        job["finished_at"] = datetime.now(timezone.utc).isoformat()
        self._delete_download_artifact(*paths, job.get("partial_path"), job.get("path"))
        job["partial_path"] = None
        job["path"] = None
        self._record_download_history(job)

    def _fail_download_job(
        self, job: dict[str, Any], error: str, *paths: Path | str | None
    ) -> None:
        job["status"] = "error"
        job["error"] = error
        job["finished_at"] = datetime.now(timezone.utc).isoformat()
        self._delete_download_artifact(*paths, job.get("partial_path"))
        job["partial_path"] = None
        job["path"] = None
        self._record_download_history(job)

    def _ensure_download_worker(self) -> None:
        task = self._download_worker_task
        if task is not None and not task.done():
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        self._download_worker_task = loop.create_task(self._download_worker_loop())

    async def _download_worker_loop(self) -> None:
        while True:
            job_id = await self._download_queue.get()
            try:
                await self._run_download_job(job_id)
            except Exception:
                logger.exception("Download worker crashed on %s", job_id)
            finally:
                self._download_queue.task_done()

    async def _run_download_job(self, job_id: str) -> None:
        job = self._download_jobs.get(job_id)
        if not job:
            return
        if job.get("cancel_requested") or job.get("status") == "cancelled":
            self._mark_job_cancelled(job)
            return
        job["status"] = "running"
        job["progress"] = 0.0
        name = job["channel"]
        mid = int(job["message_id"])
        to_desktop = job["mode"] == "desktop"
        final_path: Path | None = None
        partial: Path | None = None

        async with self._download_lock:
            try:
                if job.get("cancel_requested"):
                    self._mark_job_cancelled(job)
                    return
                entity = await self.client.get_entity(name)
                message = await self.client.get_messages(entity, ids=mid)
                if not message or not getattr(message, "document", None):
                    self._fail_download_job(job, "No file found on that message.")
                    return

                file_name = self._file_name(message) or job["filename"]
                job["filename"] = file_name
                job["source_filename"] = file_name
                dest_dir = DOWNLOAD_DIR if to_desktop else DOWNLOAD_CACHE_DIR
                job["folder"] = str(dest_dir.resolve())
                allow_dup = bool(job.get("allow_duplicate"))
                desired = dest_dir / self._safe_filename(file_name)

                if to_desktop and not allow_dup:
                    # Disk checks: living files/folders + prior extract path if still present
                    pre_conflicts = self._desktop_conflicts_for_filename(file_name)
                    for c in self._index_conflicts(name, mid, file_name):
                        if not any(x["path"] == c["path"] for x in pre_conflicts):
                            pre_conflicts.append(c)
                    if pre_conflicts:
                        c0 = pre_conflicts[0]
                        job["status"] = "needs_confirm"
                        job["conflict"] = {
                            "kind": c0["kind"],
                            "name": c0["name"],
                            "path": c0["path"],
                            "suggested": c0["suggested"],
                            "phase": "download",
                        }
                        job["error"] = None
                        return

                if to_desktop and not allow_dup:
                    final_path = desired
                else:
                    final_path = self._unique_path(dest_dir, file_name)
                partial = self._partial_path(final_path)
                job["partial_path"] = str(partial)
                total_hint = int(getattr(message.document, "size", 0) or 0)
                if total_hint:
                    job["total"] = total_hint

                def progress(current: int, total: int) -> None:
                    if job.get("cancel_requested"):
                        raise DownloadCancelled()
                    t = int(total or total_hint or 0)
                    job["received"] = int(current or 0)
                    job["total"] = t
                    if t > 0:
                        job["progress"] = round(min(100.0, (current / t) * 100.0), 1)
                    else:
                        job["progress"] = 0.0

                def should_cancel() -> bool:
                    return bool(job.get("cancel_requested"))

                # Always write to a temp .tgdlpart; only promote on success.
                self._delete_download_artifact(partial)
                try:
                    path = await parallel_download_to_path(
                        self.client,
                        message.document,
                        str(partial),
                        progress_callback=progress,
                        should_cancel=should_cancel,
                        part_size_kb=float(DOWNLOAD_PART_KB),
                        max_connections=DOWNLOAD_CONNECTIONS,
                    )
                except DownloadCancelled:
                    self._mark_job_cancelled(job, partial, final_path)
                    return
                except Exception as parallel_exc:
                    if job.get("cancel_requested"):
                        self._mark_job_cancelled(job, partial, final_path)
                        return
                    logger.warning(
                        "Parallel download failed (@%s/%s), falling back: %s",
                        name,
                        mid,
                        parallel_exc,
                    )
                    from telethon.tl.types import InputDocumentFileLocation

                    loc = InputDocumentFileLocation(
                        id=message.document.id,
                        access_hash=message.document.access_hash,
                        file_reference=message.document.file_reference,
                        thumb_size="",
                    )
                    self._delete_download_artifact(partial)
                    try:
                        path = await self.client.download_file(
                            loc,
                            file=str(partial),
                            part_size_kb=float(DOWNLOAD_PART_KB),
                            file_size=total_hint or None,
                            progress_callback=progress,
                        )
                    except DownloadCancelled:
                        self._mark_job_cancelled(job, partial, final_path)
                        return
                if job.get("cancel_requested"):
                    self._mark_job_cancelled(job, partial, final_path)
                    return
                if not path:
                    self._fail_download_job(
                        job, "Telegram returned no file.", partial, final_path
                    )
                    return
                out = Path(path)
                if not out.exists() or out.stat().st_size <= 0:
                    self._fail_download_job(
                        job, "Download produced an empty file.", partial, final_path
                    )
                    return

                # Promote partial → final name
                try:
                    if final_path.exists():
                        # Never overwrite an existing file (race / unexpected).
                        final_path = self._unique_path(
                            final_path.parent, final_path.name
                        )
                    out.replace(final_path)
                except Exception:
                    # Cross-device fallback
                    final_path.write_bytes(out.read_bytes())
                    self._delete_download_artifact(out)

                job["path"] = str(final_path.resolve())
                job["filename"] = final_path.name
                job["partial_path"] = None
                job["received"] = final_path.stat().st_size
                job["total"] = job["total"] or final_path.stat().st_size
                job["progress"] = 100.0

                # Desktop ZIP/RAR → auto-extract (single root folder unwraps;
                # otherwise contain under a folder named like the archive).
                if to_desktop and is_extractable_archive(final_path):
                    if job.get("cancel_requested"):
                        self._mark_job_cancelled(job, final_path)
                        return
                    job["extracting"] = True
                    try:
                        extracted = await asyncio.to_thread(
                            extract_download_archive,
                            final_path,
                            dest_dir,
                            allow_duplicate=allow_dup,
                        )
                        job["extracted"] = True
                        job["extract_note"] = extracted.note
                        job["path"] = str(extracted.path)
                        job["filename"] = extracted.path.name
                        job["folder"] = str(extracted.path.parent.resolve())
                    except DestinationExists as conflict:
                        job["extracted"] = False
                        job["status"] = "needs_confirm"
                        job["conflict"] = {
                            "kind": conflict.kind,
                            "name": conflict.existing.name,
                            "path": str(conflict.existing.resolve()),
                            "suggested": conflict.suggested_name,
                            "phase": "extract",
                        }
                        job["extract_note"] = (
                            f"Downloaded — “{conflict.existing.name}” already exists"
                        )
                        job["extracting"] = False
                        return
                    except Exception as extract_exc:
                        logger.warning(
                            "Download OK but extract failed for %s: %s",
                            final_path.name,
                            extract_exc,
                        )
                        job["extracted"] = False
                        job["extract_note"] = f"Saved archive (extract failed: {extract_exc})"
                    finally:
                        job["extracting"] = False

                job["status"] = "done"
                job["finished_at"] = datetime.now(timezone.utc).isoformat()
                self._record_download_history(job)
            except DownloadCancelled:
                self._mark_job_cancelled(job, partial, final_path)
            except FloodWaitError as exc:
                if job.get("cancel_requested"):
                    self._mark_job_cancelled(job, partial, final_path)
                    return
                self._fail_download_job(
                    job, f"Rate limited — wait {exc.seconds}s", partial, final_path
                )
            except Exception as exc:
                if job.get("cancel_requested"):
                    self._mark_job_cancelled(job, partial, final_path)
                    return
                logger.exception("Download failed for @%s/%s", name, mid)
                self._fail_download_job(job, str(exc), partial, final_path)

    async def download_telegram_file(
        self,
        channel_username: str,
        message_id: int,
        *,
        to_desktop: bool,
        filename_hint: str = "",
    ) -> dict[str, Any]:
        """Enqueue and wait until finished (used for older sync endpoints)."""
        job = self.enqueue_download(
            channel_username,
            message_id,
            mode="desktop" if to_desktop else "local",
            filename_hint=filename_hint,
        )
        job_id = job["id"]
        while True:
            current = self._download_jobs.get(job_id)
            if not current:
                return {"ok": False, "error": "Job missing"}
            if current["status"] in ("done", "error", "cancelled"):
                if current["status"] == "done":
                    return {
                        "ok": True,
                        "path": current["path"],
                        "filename": current["filename"],
                        "size": current.get("received") or 0,
                        "desktop": to_desktop,
                        "folder": current.get("folder"),
                        "job_id": job_id,
                    }
                return {
                    "ok": False,
                    "error": current.get("error") or "Download failed",
                    "job_id": job_id,
                }
            await asyncio.sleep(0.25)


telegram_service = TelegramService()
