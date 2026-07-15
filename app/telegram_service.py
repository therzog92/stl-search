from __future__ import annotations

import asyncio
import json
import logging
import re
import threading
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

_DISPLAY_TZ = None


def _central_tz():
    """America/Chicago when tzdata is available; else fixed UTC-6 labeled CST."""
    global _DISPLAY_TZ
    if _DISPLAY_TZ is not None:
        return _DISPLAY_TZ
    try:
        from zoneinfo import ZoneInfo

        tz = ZoneInfo("America/Chicago")
        # Force load so missing tzdata fails here, not mid-request
        datetime.now(timezone.utc).astimezone(tz)
        _DISPLAY_TZ = tz
    except Exception:
        _DISPLAY_TZ = timezone(timedelta(hours=-6), name="CST")
    return _DISPLAY_TZ


def _format_result_date(dt: datetime) -> str:
    """MM-DD-YYYY h:mm AM/PM CST for result cards."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    local = dt.astimezone(_central_tz())
    text = local.strftime("%m-%d-%Y %I:%M %p CST")
    # "07-15-2026 01:30 PM CST" -> "07-15-2026 1:30 PM CST"
    if len(text) > 11 and text[11] == "0":
        text = text[:11] + text[12:]
    return text

from telethon import TelegramClient
from telethon.errors import (
    FloodWaitError,
    ChannelPrivateError,
    UsernameNotOccupiedError,
    UserAlreadyParticipantError,
    InviteRequestSentError,
    ChannelsTooMuchError,
)
from telethon.tl.functions.account import UpdateNotifySettingsRequest
from telethon.tl.functions.channels import (
    GetChannelRecommendationsRequest,
    GetFullChannelRequest,
    JoinChannelRequest,
    LeaveChannelRequest,
)
from telethon.tl.functions.contacts import SearchRequest as ContactsSearchRequest
from telethon.tl.types import (
    Channel,
    DocumentAttributeFilename,
    InputMessagesFilterDocument,
    InputMessagesFilterEmpty,
    InputMessagesFilterUrl,
    InputNotifyPeer,
    InputPeerNotifySettings,
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
    DOWNLOAD_JOB_CONCURRENCY,
    DOWNLOAD_PART_KB,
    DEEP_CRAWL_MAX_ROOTS,
    DEEP_CRAWL_MAX_INSPECT,
    FILE_EXTENSIONS,
    JOIN_DELAY_SECONDS,
    JOIN_MAX_WAIT_SECONDS,
    JOINED_CHANNELS_FILE,
    LINK_CRAWL_LIMIT,
    MAX_AGE_DAYS,
    MIN_CHANNEL_MEMBERS,
    SEARCH_CONCURRENCY,
    SEARCH_DELAY_SECONDS,
    SEARCH_VARIANT_DELAY,
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
# Skip channels that mention crypto / web3 in title, username, or about
CRYPTO_HINT = re.compile(
    r"\b("
    r"crypto|cryptocurrency|cryptocurrencies|"
    r"bitcoin|\bbtc\b|ethereum|\beth\b|"
    r"blockchain|web3|\bnft\b|airdrop|defi|"
    r"token\s*sale|altcoin|memecoin"
    r")\b",
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
    current_channel: str = ""
    current_variant: str = ""
    step: str = ""
    errors: list[str] = field(default_factory=list)
    finished_at: str | None = None


@dataclass
class DiscoveryState:
    running: bool = False
    mode: str = ""  # catalog | deep | refresh | join_mute | leave | join_web
    progress: str = ""
    error: str | None = None
    message: str | None = None
    phase: str = ""  # join_web: await_login | joining | …


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
        self._discovery_cancel_requested = False
        self._web_join_controller = None  # lazy WebJoinController
        self._web_join_thread = None
        self._preview_tasks: set[asyncio.Task] = set()
        self._preview_sem = asyncio.Semaphore(3)
        self._download_jobs: dict[str, dict[str, Any]] = {}
        self._download_queue: asyncio.Queue[str] = asyncio.Queue()
        self._download_worker_tasks: list[asyncio.Task] = []
        self._download_job_seq = 0
        self._logged_out = False

    def request_stop_search(self) -> bool:
        """Signal the active search to stop after the current step."""
        if not self.search_state.running:
            return False
        self._search_cancel_requested = True
        self.search_state.progress = "Stopping…"
        return True

    def _search_stop_requested(self) -> bool:
        return self._search_cancel_requested

    def request_stop_discovery(self) -> bool:
        """Signal discover / refresh / enrich / join / leave / web-join to stop ASAP."""
        if not self.discovery_state.running:
            return False
        self._discovery_cancel_requested = True
        self.discovery_state.progress = "Stopping…"
        ctrl = getattr(self, "_web_join_controller", None)
        if ctrl is not None and self.discovery_state.mode == "join_web":
            ctrl.stop.set()
            ctrl.login_ready.set()  # unblock wait so the worker can exit
        return True

    def confirm_web_join_login(self) -> bool:
        """User logged into Telegram Web — continue the auto-join worker."""
        if not (
            self.discovery_state.running and self.discovery_state.mode == "join_web"
        ):
            return False
        ctrl = getattr(self, "_web_join_controller", None)
        if ctrl is None:
            return False
        self.discovery_state.phase = "joining"
        self.discovery_state.progress = "Starting joins…"
        ctrl.login_ready.set()
        return True

    def start_join_via_telegram_web(self, *, delay_seconds: float = 3.0) -> dict[str, Any]:
        """Open Telegram Web and auto-click Join (runs in a background thread)."""
        if self.discovery_state.running:
            return {"ok": False, "error": "Another job is already running."}
        if self.search_state.running:
            return {"ok": False, "error": "Stop search before auto-joining."}

        from app.telegram_web_join import WebJoinController, load_progress, run_telegram_web_join

        # Promote prior web-join successes into the join log so they drop off unjoined
        try:
            for name in load_progress().get("done") or []:
                if name:
                    self._record_joined(str(name), muted=False, already_member=False)
        except Exception:
            logger.exception("Could not sync web-join progress into join log")

        targets = [c.username for c in self.list_unjoined_channels(enabled_only=True)]
        if not targets:
            return {"ok": False, "error": "No enabled unjoined channels to process."}

        self._discovery_cancel_requested = False
        ctrl = WebJoinController()
        ctrl.reset()
        self._web_join_controller = ctrl
        self.discovery_state = DiscoveryState(
            running=True,
            mode="join_web",
            phase="starting",
            progress="Starting Telegram Web joiner…",
        )

        def worker() -> None:
            def on_phase(phase: str, msg: str) -> None:
                self.discovery_state.phase = phase
                self.discovery_state.progress = msg
                if phase == "error":
                    self.discovery_state.error = msg
                if phase == "done":
                    self.discovery_state.message = msg
                if phase == "stopped":
                    self.discovery_state.message = msg

            def on_joined(username: str, result: str) -> None:
                # Persist so the next run (and Mute job) skip already-subscribed channels
                try:
                    self._record_joined(
                        username,
                        muted=False,
                        already_member=(result == "already"),
                    )
                except Exception:
                    logger.exception("Failed to record web-join @%s", username)

            try:
                run_telegram_web_join(
                    targets,
                    delay_seconds=delay_seconds,
                    controller=ctrl,
                    on_phase=on_phase,
                    on_joined=on_joined,
                )
            except Exception as exc:
                logger.exception("Telegram Web join failed")
                self.discovery_state.error = str(exc)
                self.discovery_state.phase = "error"
                self.discovery_state.progress = "Error"
            finally:
                self.discovery_state.running = False
                self._discovery_cancel_requested = False
                if self.discovery_state.phase not in {
                    "done",
                    "stopped",
                    "error",
                }:
                    self.discovery_state.phase = "done"
                    if not self.discovery_state.progress:
                        self.discovery_state.progress = "Done"

        self._web_join_thread = threading.Thread(
            target=worker, name="stl-web-join", daemon=True
        )
        self._web_join_thread.start()
        return {"ok": True, "queued": len(targets)}

    def _discovery_stop_requested(self) -> bool:
        return self._discovery_cancel_requested

    async def connect(self) -> None:
        await self.client.connect()

    async def disconnect(self) -> None:
        await self.client.disconnect()

    async def is_authorized(self) -> bool:
        """Fast auth check with timeout so the UI never hangs forever."""
        if getattr(self, "_logged_out", False):
            return False
        try:
            if not self.client.is_connected():
                await asyncio.wait_for(self.connect(), timeout=8)
            authorized = bool(
                await asyncio.wait_for(self.client.is_user_authorized(), timeout=8)
            )
            if authorized:
                self._logged_out = False
            return authorized
        except Exception as exc:
            # Don't treat "session file exists" as logged-in. After an API key swap,
            # a hollow session causes: The key is not registered in the system.
            logger.warning("Auth check failed: %s", exc)
            return False

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
            self._logged_out = False
            return {"status": "authorized"}

        try:
            await self.client.sign_in(
                phone=self._login_phone,
                code=code.strip(),
                phone_code_hash=self._phone_code_hash,
            )
        except SessionPasswordNeededError:
            return {"status": "password_required", "message": "Two-factor password required."}
        self._logged_out = False
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
        self._discovery_cancel_requested = False
        self.discovery_state = DiscoveryState(running=True, mode=mode, progress="Loading seeds…")
        stopped = False

        try:
            # 1) Seed list (Telegram inspect — small list)
            for idx, username in enumerate(seed_names.values(), start=1):
                if self._discovery_stop_requested():
                    stopped = True
                    break
                self.discovery_state.progress = f"Checking seed {idx}/{len(seed_names)}…"
                info = await self._inspect_channel(username, source="seed")
                infos_by_key[info.username.casefold()] = info
                await asyncio.sleep(0.15)

            # 2) Previously discovered — keep known member counts when possible
            prior_cache = {c.username.casefold(): c for c in self._load_channel_cache()}
            if not stopped:
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
            if force and not discover and not deep_crawl and not stopped:
                missing = [
                    c
                    for c in infos_by_key.values()
                    if c.source != "seed"
                    and ((c.members or 0) <= 0 or not (c.description or "").strip())
                ]
                for idx, channel in enumerate(missing, start=1):
                    if self._discovery_stop_requested():
                        stopped = True
                        break
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
            if discover and not stopped:
                self.discovery_error = None
                self.discovery_message = None
                self.discovery_state.progress = "Querying Telemetr catalog…"
                try:
                    catalog_hits = await search_stl_channels()
                    added = 0
                    for hit in catalog_hits:
                        if self._discovery_stop_requested():
                            stopped = True
                            break
                        key = hit.username.casefold()
                        if key in seed_names:
                            continue
                        if not self._is_valid_username(hit.username):
                            continue
                        if self._is_blacklisted(hit.username):
                            continue
                        if self._mentions_crypto(f"{hit.title} {hit.username}"):
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
                    if not stopped:
                        self.discovery_message = (
                            f"Catalog found {len(catalog_hits)} channel(s); "
                            f"{added} newly added."
                        )
                        self.discovery_state.message = self.discovery_message
                except CatalogError as exc:
                    self.discovery_error = str(exc)
                    self.discovery_state.error = str(exc)
                    logger.warning("Catalog discovery failed: %s", exc)

            # 3b) Slow path: similar + t.me links from seeds AND discovered channels
            if deep_crawl and not stopped:
                self.discovery_error = None
                crawl_roots = self._deep_crawl_roots(infos_by_key)
                self.discovery_state.progress = (
                    f"Deep crawling {len(crawl_roots)} channel(s) "
                    f"(seeds + discovered)…"
                )
                found, crawl_stopped = await self._discover_from_channels(crawl_roots)
                if crawl_stopped:
                    stopped = True
                kept = 0
                skipped = 0
                inspect_budget = max(1, DEEP_CRAWL_MAX_INSPECT)
                to_inspect = list(found.items())[:inspect_budget]
                if len(found) > inspect_budget:
                    self.discovery_state.progress = (
                        f"Found {len(found)} candidates — inspecting first "
                        f"{inspect_budget}…"
                    )
                for idx, (username, meta) in enumerate(to_inspect, start=1):
                    if self._discovery_stop_requested():
                        stopped = True
                        break
                    source = meta.get("source") if isinstance(meta, dict) else meta
                    key = username.casefold()
                    if key in infos_by_key or key in seed_names:
                        continue
                    if not self._is_valid_username(username):
                        skipped += 1
                        continue
                    if self._is_blacklisted(username):
                        skipped += 1
                        continue
                    title_hint = ""
                    members_hint = 0
                    if isinstance(meta, dict):
                        title_hint = str(meta.get("title") or "")
                        members_hint = int(meta.get("members") or 0)
                        if self._mentions_crypto(f"{title_hint} {username}"):
                            skipped += 1
                            continue
                    self.discovery_state.progress = (
                        f"Inspecting @{username} ({idx}/{len(to_inspect)}; "
                        f"kept {kept})…"
                    )
                    # Prefer lightweight accept from recommendation metadata;
                    # only hit Telegram fully when we lack a usable title.
                    if title_hint and not self._mentions_crypto(
                        f"{title_hint} {username}"
                    ):
                        info = ChannelInfo(
                            username=username,
                            title=title_hint or username,
                            members=members_hint,
                            included=True,
                            reason=(
                                "OK (similar/linked)"
                                if members_hint >= MIN_CHANNEL_MEMBERS
                                else "From deep crawl (member count may be incomplete)"
                            ),
                            link=f"https://t.me/{username}",
                            source=str(source or "similar"),
                            description="",
                            valid=True,
                        )
                    else:
                        info = await self._inspect_channel(
                            username, source=str(source or "similar")
                        )
                        if not info.valid:
                            skipped += 1
                            await asyncio.sleep(0.1)
                            continue
                        if self._mentions_crypto(
                            f"{info.title} {info.username} {info.description}"
                        ):
                            skipped += 1
                            continue
                    prev = prior_cache.get(key) or discovered_map.get(key)
                    if prev and prev.banned:
                        skipped += 1
                        continue
                    infos_by_key[key] = self._preserve_user_enabled(prev, info)
                    kept += 1
                    # Persist as we go so Stop / crash doesn't lose work
                    if kept % 8 == 0:
                        self._save_discovered_channels(
                            [
                                c
                                for c in infos_by_key.values()
                                if c.source != "seed" and c.valid
                            ]
                        )
                        self._save_channel_cache(list(infos_by_key.values()))
                    await asyncio.sleep(0.08)
                if stopped:
                    self.discovery_message = (
                        f"Stopped — kept {kept} new channel(s), skipped {skipped}. "
                        "Progress saved."
                    )
                else:
                    self.discovery_message = (
                        f"Deep crawl finished — kept {kept} valid channel(s), "
                        f"skipped {skipped} (invalid/crypto/blacklist"
                        f"{'' if len(found) <= inspect_budget else f'; {len(found) - inspect_budget} not inspected yet — run again'})."
                    )
                self.discovery_state.message = self.discovery_message

            if (discover or deep_crawl or force) and not stopped:
                # Do NOT auto-backfill via Telegram here — it hammers ResolveUsername /
                # GetFullChannel and trips FloodWait. Use Manage → "Load members &
                # descriptions" when you explicitly want that.
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
            elif stopped and infos_by_key:
                # Persist partial progress so Stop doesn't throw away work
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
            if stopped:
                self.discovery_state.message = (
                    self.discovery_state.message or "Stopped."
                )
                self.discovery_state.progress = "Stopped"
            return infos
        finally:
            self.discovery_state.running = False
            self._discovery_cancel_requested = False
            if not self.discovery_state.progress.startswith(("Done", "Stopped", "Error", "Paused")):
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
            if self._discovery_stop_requested():
                self.discovery_state.progress = "Stopped"
                return
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
        self._discovery_cancel_requested = False
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
            stopped = False
            for idx, channel in enumerate(targets, start=1):
                if self._discovery_stop_requested():
                    stopped = True
                    break
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
            if stopped:
                self.discovery_state.message = (
                    f"Stopped — updated {updated} channel(s)"
                    + (f", removed {removed} invalid" if removed else "")
                    + "."
                )
                self.discovery_state.progress = "Stopped"
            else:
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
            self._discovery_cancel_requested = False

    # Telegram treats this as "mute forever"
    _MUTE_UNTIL_FOREVER = 2147483647

    def _load_joined_log(self) -> dict[str, Any]:
        if not JOINED_CHANNELS_FILE.exists():
            return {"channels": []}
        try:
            data = json.loads(JOINED_CHANNELS_FILE.read_text(encoding="utf-8"))
            if not isinstance(data, dict):
                return {"channels": []}
            channels = data.get("channels")
            if not isinstance(channels, list):
                data["channels"] = []
            return data
        except Exception:
            logger.exception("Failed reading joined channels log")
            return {"channels": []}

    def _save_joined_log(self, data: dict[str, Any]) -> None:
        JOINED_CHANNELS_FILE.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "channels": data.get("channels") or [],
        }
        JOINED_CHANNELS_FILE.write_text(
            json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8"
        )

    def _record_joined(self, username: str, *, muted: bool, already_member: bool) -> None:
        name = (username or "").lstrip("@")
        if not name:
            return
        data = self._load_joined_log()
        rows: list[dict[str, Any]] = list(data.get("channels") or [])
        key = name.casefold()
        now = datetime.now(timezone.utc).isoformat()
        existing = next((r for r in rows if str(r.get("username", "")).casefold() == key), None)
        if existing:
            existing["username"] = name
            existing["muted"] = muted
            existing["last_seen_at"] = now
            if already_member:
                existing["already_member"] = True
            else:
                existing["joined_at"] = existing.get("joined_at") or now
                existing["joined_by_app"] = True
        else:
            rows.append(
                {
                    "username": name,
                    "joined_at": now,
                    "muted": muted,
                    "joined_by_app": not already_member,
                    "already_member": already_member,
                    "last_seen_at": now,
                }
            )
        data["channels"] = rows
        self._save_joined_log(data)

    def list_joined_for_leave(self, *, only_joined_by_app: bool = True) -> list[str]:
        """Usernames to leave later. Default: only channels this app newly joined."""
        data = self._load_joined_log()
        names: list[str] = []
        seen: set[str] = set()
        for row in data.get("channels") or []:
            name = str(row.get("username") or "").lstrip("@")
            if not name:
                continue
            key = name.casefold()
            if key in seen:
                continue
            if only_joined_by_app and not row.get("joined_by_app"):
                continue
            if not only_joined_by_app and not (row.get("joined_by_app") or row.get("muted")):
                continue
            seen.add(key)
            names.append(name)
        return names

    def known_joined_usernames(self) -> set[str]:
        """Casefolded usernames from the local join log (no Telegram API calls)."""
        out: set[str] = set()
        for row in self._load_joined_log().get("channels") or []:
            name = str(row.get("username") or "").lstrip("@")
            if not name:
                continue
            if row.get("muted") or row.get("joined_by_app") or row.get("already_member"):
                out.add(name.casefold())
        return out

    def joined_status_map(self) -> dict[str, dict[str, bool]]:
        """
        casefold username -> {joined: True, muted: bool}.
        muted reflects the log flag (refresh from Telegram to sync real notify settings).
        """
        out: dict[str, dict[str, bool]] = {}
        for row in self._load_joined_log().get("channels") or []:
            name = str(row.get("username") or "").lstrip("@")
            if not name:
                continue
            if not (row.get("muted") or row.get("joined_by_app") or row.get("already_member")):
                continue
            out[name.casefold()] = {
                "joined": True,
                "muted": bool(row.get("muted")),
            }
        return out

    def count_joined_unmuted(self) -> int:
        return sum(1 for st in self.joined_status_map().values() if not st.get("muted"))

    @staticmethod
    def _notify_is_muted(mute_until: Any) -> bool:
        """True if Telegram notify settings indicate the chat is muted."""
        if mute_until is None:
            return False
        # Telethon may return unix int or datetime
        if isinstance(mute_until, datetime):
            ts = int(mute_until.timestamp())
        else:
            try:
                ts = int(mute_until)
            except (TypeError, ValueError):
                return False
        if ts <= 0:
            return False
        now = int(datetime.now(timezone.utc).timestamp())
        return ts > now

    async def refresh_mute_status_from_telegram(self) -> dict[str, Any]:
        """
        Walk dialogs and align join-log muted flags with Telegram notify settings.
        Only updates channels already in the join log (or enabled catalog peers we find).
        """
        if self.discovery_state.running:
            return {"ok": False, "error": "Another job is already running."}
        if self.search_state.running:
            return {"ok": False, "error": "Stop search first."}

        self._discovery_cancel_requested = False
        self.discovery_state = DiscoveryState(
            running=True,
            mode="mute_sync",
            progress="Reading mute status from Telegram…",
        )
        updated = 0
        checked = 0
        unmuted = 0
        muted = 0
        try:
            # username.casefold() -> muted bool from Telegram
            live: dict[str, bool] = {}
            async for dialog in self.client.iter_dialogs(limit=None):
                if self._discovery_stop_requested():
                    break
                entity = dialog.entity
                if not isinstance(entity, Channel):
                    continue
                username = getattr(entity, "username", None)
                if not username:
                    continue
                ns = getattr(dialog.dialog, "notify_settings", None)
                mute_until = getattr(ns, "mute_until", None) if ns else None
                is_muted = self._notify_is_muted(mute_until)
                live[username.casefold()] = is_muted
                checked += 1
                if checked % 40 == 0:
                    self.discovery_state.progress = (
                        f"Checked {checked} channels for mute status…"
                    )

            data = self._load_joined_log()
            rows: list[dict[str, Any]] = list(data.get("channels") or [])
            by_key = {
                str(r.get("username", "")).lstrip("@").casefold(): r for r in rows if r.get("username")
            }
            now = datetime.now(timezone.utc).isoformat()

            # Update existing join-log rows from live dialog mute state
            for key, is_muted in live.items():
                row = by_key.get(key)
                if row is None:
                    continue
                if bool(row.get("muted")) != is_muted:
                    row["muted"] = is_muted
                    row["last_seen_at"] = now
                    updated += 1
                if is_muted:
                    muted += 1
                else:
                    unmuted += 1

            # Also recount join-log rows that weren't in dialogs (keep prior flag)
            for row in rows:
                key = str(row.get("username", "")).lstrip("@").casefold()
                if key in live:
                    continue
                if row.get("muted"):
                    muted += 1
                elif row.get("joined_by_app") or row.get("already_member"):
                    unmuted += 1

            data["channels"] = rows
            self._save_joined_log(data)
            msg = (
                f"Mute sync: checked {checked} Telegram chats, updated {updated} log "
                f"entries — muted {muted}, unmuted {unmuted} in log."
            )
            self.discovery_state.message = msg
            self.discovery_state.progress = "Done"
            return {
                "ok": True,
                "checked": checked,
                "updated": updated,
                "muted": muted,
                "unmuted": unmuted,
                "message": msg,
            }
        except Exception as exc:
            logger.exception("refresh_mute_status_from_telegram failed")
            self.discovery_state.error = str(exc)
            self.discovery_state.progress = "Error"
            return {"ok": False, "error": str(exc)}
        finally:
            self.discovery_state.running = False
            self._discovery_cancel_requested = False

    def list_unjoined_channels(self, *, enabled_only: bool = True) -> list[ChannelInfo]:
        """Enabled (or all) catalog channels not yet in the local mute/join log."""
        joined = self.known_joined_usernames()
        out: list[ChannelInfo] = []
        for c in self.get_channels_fast():
            if not c.valid or c.banned or not c.username:
                continue
            if enabled_only and not c.included:
                continue
            if c.username.casefold() in joined:
                continue
            out.append(c)
        out.sort(key=lambda c: (-(c.members or 0), c.username.casefold()))
        return out

    async def _mute_peer_forever(self, entity: Any) -> None:
        await self.client(
            UpdateNotifySettingsRequest(
                peer=InputNotifyPeer(peer=await self.client.get_input_entity(entity)),
                settings=InputPeerNotifySettings(
                    show_previews=False,
                    silent=True,
                    mute_until=self._MUTE_UNTIL_FOREVER,
                ),
            )
        )

    async def join_channel_muted(self, username: str) -> dict[str, Any]:
        """Join one channel and mute forever. Uses local join log; one Telegram call."""
        name = (username or "").lstrip("@").strip()
        if not name:
            return {"ok": False, "error": "Missing username", "status": "error"}
        if self.discovery_state.running:
            return {
                "ok": False,
                "error": "Another channel job is running — wait or stop it first.",
                "status": "busy",
                "username": name,
            }
        if self.search_state.running:
            return {
                "ok": False,
                "error": "Search is running — stop it before joining.",
                "status": "busy",
                "username": name,
            }
        if name.casefold() in self.known_joined_usernames():
            status = self.joined_status_map().get(name.casefold()) or {}
            if status.get("muted"):
                return {
                    "ok": True,
                    "username": name,
                    "already_done": True,
                    "status": "already",
                }
            # Already joined (e.g. web join) but not muted in our log — mute only
            try:
                entity = await self.client.get_entity(name)
                await self._mute_peer_forever(entity)
                self._record_joined(name, muted=True, already_member=True)
                return {
                    "ok": True,
                    "username": name,
                    "already_member": True,
                    "already_done": False,
                    "status": "muted",
                }
            except FloodWaitError as exc:
                wait_s = int(getattr(exc, "seconds", 0) or 0)
                return {
                    "ok": False,
                    "username": name,
                    "status": "rate_limit",
                    "error": f"Rate limited — wait {self._fmt_flood_wait(wait_s)} then try again.",
                    "wait_seconds": wait_s,
                }
            except Exception as exc:
                return {
                    "ok": False,
                    "username": name,
                    "status": "error",
                    "error": str(exc),
                }
        try:
            entity = await self.client.get_entity(name)
            already = False
            try:
                await self.client(JoinChannelRequest(entity))
            except UserAlreadyParticipantError:
                already = True
            except InviteRequestSentError:
                return {
                    "ok": False,
                    "username": name,
                    "status": "approval",
                    "error": "Join request sent (private channel) — wait for approval.",
                }
            await self._mute_peer_forever(entity)
            self._record_joined(name, muted=True, already_member=already)
            return {
                "ok": True,
                "username": name,
                "already_member": already,
                "already_done": False,
                "status": "muted" if already else "joined",
            }
        except ChannelsTooMuchError:
            return {
                "ok": False,
                "username": name,
                "status": "error",
                "error": "Telegram limit: too many channels joined on this account.",
            }
        except FloodWaitError as exc:
            wait_s = int(getattr(exc, "seconds", 0) or 0)
            return {
                "ok": False,
                "username": name,
                "status": "rate_limit",
                "error": f"Rate limited — wait {self._fmt_flood_wait(wait_s)} then try again.",
                "wait_seconds": wait_s,
            }
        except Exception as exc:
            logger.warning("join_channel_muted failed @%s: %s", name, exc)
            return {
                "ok": False,
                "username": name,
                "status": "error",
                "error": str(exc),
            }

    async def join_enabled_channels_muted(self) -> None:
        """Join every enabled Manage channel and mute notifications forever."""
        if self.discovery_state.running:
            return
        self._discovery_cancel_requested = False
        self.discovery_state = DiscoveryState(
            running=True,
            mode="join_mute",
            progress="Preparing to join & mute…",
        )
        joined = 0
        muted_only = 0
        skipped = 0
        already_done = 0
        errors = 0
        stopped_rate = False
        try:
            done_keys = {
                str(row.get("username", "")).lstrip("@").casefold()
                for row in (self._load_joined_log().get("channels") or [])
                if row.get("username") and row.get("muted")
            }
            targets = [
                c
                for c in self.get_channels_fast()
                if c.valid and c.included and not c.banned and c.username
            ]
            if not targets:
                self.discovery_state.message = "No enabled channels to join."
                self.discovery_state.progress = "Done"
                return

            pending = [c for c in targets if c.username.casefold() not in done_keys]
            already_done = len(targets) - len(pending)
            if already_done:
                self.discovery_state.progress = (
                    f"Skipping {already_done} already muted — "
                    f"{len(pending)} left…"
                )

            if not pending:
                self.discovery_state.message = (
                    f"All {len(targets)} enabled channel(s) already joined/muted."
                )
                self.discovery_state.progress = "Done"
                return

            delay = max(0.5, JOIN_DELAY_SECONDS)
            max_wait = max(15, JOIN_MAX_WAIT_SECONDS)
            success_streak = 0

            for idx, channel in enumerate(pending, start=1):
                if self._discovery_stop_requested():
                    stopped_rate = False
                    self.discovery_state.message = (
                        f"Stopped — joined {joined}, muted {muted_only}. "
                        "Run Join again to continue."
                    )
                    self.discovery_state.progress = "Stopped"
                    break
                self.discovery_state.progress = (
                    f"Join/mute @{channel.username} ({idx}/{len(pending)})…"
                )
                try:
                    entity = await self.client.get_entity(channel.username)
                    already = False
                    try:
                        await self.client(JoinChannelRequest(entity))
                    except UserAlreadyParticipantError:
                        already = True
                    except InviteRequestSentError:
                        skipped += 1
                        self.discovery_state.progress = (
                            f"@{channel.username}: join request sent (private)"
                        )
                        await asyncio.sleep(delay)
                        continue

                    await self._mute_peer_forever(entity)
                    self._record_joined(
                        channel.username, muted=True, already_member=already
                    )
                    if already:
                        muted_only += 1
                    else:
                        joined += 1
                    success_streak += 1
                except ChannelsTooMuchError:
                    msg = "Telegram limit: too many channels joined on this account."
                    self.discovery_state.error = msg
                    self.discovery_state.progress = "Error"
                    errors += 1
                    break
                except FloodWaitError as exc:
                    wait_s = int(getattr(exc, "seconds", 0) or 0)
                    pretty = self._fmt_flood_wait(wait_s)
                    success_streak = 0
                    if wait_s > max_wait:
                        # Stop the batch — multi-hour waits aren't worth blocking the UI
                        left = len(pending) - idx + 1
                        msg = (
                            f"Rate limited on @{channel.username} ({pretty}). "
                            f"Stopped with {left} channel(s) left — run Join again later."
                        )
                        logger.warning(msg)
                        self.discovery_state.error = msg
                        self.discovery_state.progress = "Paused (rate limit)"
                        stopped_rate = True
                        errors += 1
                        break
                    self.discovery_state.progress = (
                        f"Rate limited — waiting {pretty}, then continuing…"
                    )
                    await asyncio.sleep(wait_s + 2)
                    # Extra cool-down after a flood wait so we don't re-trip immediately
                    await asyncio.sleep(min(30.0, delay * 3))
                    try:
                        entity = await self.client.get_entity(channel.username)
                        already = False
                        try:
                            await self.client(JoinChannelRequest(entity))
                        except UserAlreadyParticipantError:
                            already = True
                        await self._mute_peer_forever(entity)
                        self._record_joined(
                            channel.username, muted=True, already_member=already
                        )
                        if already:
                            muted_only += 1
                        else:
                            joined += 1
                        success_streak += 1
                    except FloodWaitError as again:
                        wait2 = int(getattr(again, "seconds", 0) or 0)
                        msg = (
                            f"Rate limited again ({self._fmt_flood_wait(wait2)}). "
                            "Stopped — run Join again later."
                        )
                        self.discovery_state.error = msg
                        self.discovery_state.progress = "Paused (rate limit)"
                        stopped_rate = True
                        errors += 1
                        break
                    except Exception as inner:
                        errors += 1
                        logger.warning("Retry join/mute failed @%s: %s", channel.username, inner)
                except Exception as exc:
                    errors += 1
                    logger.warning("Join/mute failed @%s: %s", channel.username, exc)
                    self.discovery_state.progress = f"@{channel.username}: {exc}"

                # Breather every 10 successes so we don't cliff into FloodWait as fast
                if success_streak > 0 and success_streak % 10 == 0:
                    self.discovery_state.progress = (
                        f"Cooling down after {success_streak} joins…"
                    )
                    await asyncio.sleep(delay * 4)
                else:
                    await asyncio.sleep(delay)

            if self.discovery_state.progress != "Stopped":
                parts = [
                    f"Joined {joined} new",
                    f"muted {muted_only} already-member",
                ]
                if already_done:
                    parts.append(f"skipped {already_done} already done")
                if skipped:
                    parts.append(f"{skipped} need approval")
                if errors and not stopped_rate:
                    parts.append(f"{errors} error(s)")
                suffix = (
                    " Run Join again later to continue."
                    if stopped_rate
                    else f" Tracked in {JOINED_CHANNELS_FILE.name}."
                )
                self.discovery_state.message = ", ".join(parts) + "." + suffix
                if not self.discovery_state.error:
                    self.discovery_state.progress = "Done"
        except Exception as exc:
            logger.exception("join_enabled_channels_muted failed")
            self.discovery_state.error = str(exc)
            self.discovery_state.progress = "Error"
        finally:
            self.discovery_state.running = False
            self._discovery_cancel_requested = False

    async def leave_tracked_channels(
        self,
        usernames: list[str] | None = None,
        *,
        only_joined_by_app: bool = True,
    ) -> dict[str, Any]:
        """Leave channels recorded in the join log (or an explicit list)."""
        targets = (
            usernames
            if usernames is not None
            else self.list_joined_for_leave(only_joined_by_app=only_joined_by_app)
        )
        left = 0
        missing = 0
        errors: list[str] = []
        cleared: set[str] = set()

        for idx, name in enumerate(targets, start=1):
            username = name.lstrip("@")
            key = username.casefold()
            if self._discovery_stop_requested():
                break
            if self.discovery_state.running and self.discovery_state.mode == "leave":
                self.discovery_state.progress = (
                    f"Leaving @{username} ({idx}/{len(targets)})…"
                )
            try:
                entity = await self.client.get_entity(username)
                await self.client(LeaveChannelRequest(entity))
                left += 1
                cleared.add(key)
            except (ChannelPrivateError, UsernameNotOccupiedError, ValueError) as exc:
                missing += 1
                cleared.add(key)
                errors.append(f"@{username}: {exc}")
            except FloodWaitError as exc:
                wait_s = int(getattr(exc, "seconds", 0) or 0)
                if wait_s > 90:
                    errors.append(
                        f"@{username}: rate limited ({self._fmt_flood_wait(wait_s)}) — stopped"
                    )
                    break
                await asyncio.sleep(wait_s + 1)
                try:
                    entity = await self.client.get_entity(username)
                    await self.client(LeaveChannelRequest(entity))
                    left += 1
                    cleared.add(key)
                except Exception as inner:
                    errors.append(f"@{username}: {inner}")
            except Exception as exc:
                msg = str(exc).lower()
                if "not a participant" in msg or "left the channel" in msg:
                    missing += 1
                    cleared.add(key)
                else:
                    errors.append(f"@{username}: {exc}")
            await asyncio.sleep(0.35)

        log = self._load_joined_log()
        remaining_rows = [
            row
            for row in (log.get("channels") or [])
            if str(row.get("username", "")).lstrip("@").casefold() not in cleared
        ]
        self._save_joined_log({"channels": remaining_rows})
        return {
            "left": left,
            "missing": missing,
            "errors": errors,
            "remaining": len(remaining_rows),
        }

    async def leave_joined_channels_job(self, *, only_joined_by_app: bool = True) -> None:
        """Background leave using the Manage/Settings progress channel."""
        if self.discovery_state.running:
            return
        targets = self.list_joined_for_leave(only_joined_by_app=only_joined_by_app)
        self._discovery_cancel_requested = False
        self.discovery_state = DiscoveryState(
            running=True,
            mode="leave",
            progress="Preparing to leave channels…",
        )
        try:
            if not targets:
                self.discovery_state.message = (
                    "No app-joined channels to leave "
                    "(only newly joined ones are listed by default)."
                )
                self.discovery_state.progress = "Done"
                return
            result = await self.leave_tracked_channels(
                targets, only_joined_by_app=only_joined_by_app
            )
            if self._discovery_stop_requested():
                self.discovery_state.message = (
                    f"Stopped — left {result['left']}. "
                    f"{result['remaining']} still tracked."
                )
                self.discovery_state.progress = "Stopped"
                return
            parts = [f"Left {result['left']}"]
            if result["missing"]:
                parts.append(f"{result['missing']} already gone")
            if result["remaining"]:
                parts.append(f"{result['remaining']} still tracked")
            if result["errors"]:
                parts.append(f"{len(result['errors'])} error(s)")
            self.discovery_state.message = "; ".join(parts) + "."
            if result["errors"]:
                self.discovery_state.error = "; ".join(result["errors"][:3])
            self.discovery_state.progress = "Done"
        except Exception as exc:
            logger.exception("leave_joined_channels_job failed")
            self.discovery_state.error = str(exc)
            self.discovery_state.progress = "Error"
        finally:
            self.discovery_state.running = False
            self._discovery_cancel_requested = False

    def _session_paths(self) -> list[Path]:
        import app.config as cfg

        base = Path(str(cfg.SESSION_PATH))
        return [
            Path(f"{base}.session"),
            Path(f"{base}.session-journal"),
            Path(f"{base}.session.lock"),
        ]

    def clear_telegram_session_files(self) -> None:
        for path in self._session_paths():
            for _ in range(5):
                try:
                    path.unlink(missing_ok=True)
                    break
                except Exception:
                    time.sleep(0.05)
            if path.exists():
                logger.warning("Could not remove session file %s", path)

    def reset_channel_lists_fresh(self) -> None:
        """Wipe discovered/manage cache/blacklist/joined; keep seed channels.txt."""
        for path in (
            DISCOVERED_FILE,
            CHANNEL_CACHE_FILE,
            BLACKLIST_FILE,
            JOINED_CHANNELS_FILE,
        ):
            try:
                path.unlink(missing_ok=True)
            except Exception:
                logger.warning("Could not remove %s", path)
        self._channel_cache = None

    def _new_telegram_client(self) -> TelegramClient:
        import app.config as cfg

        return TelegramClient(str(cfg.SESSION_PATH), cfg.API_ID, cfg.API_HASH)

    async def logout_telegram(self) -> None:
        """Log out this session and delete local session files."""
        self._logged_out = True
        try:
            if self.client.is_connected():
                try:
                    # log_out() also drops the local session when it succeeds
                    await self.client.log_out()
                except Exception:
                    try:
                        await self.client.disconnect()
                    except Exception:
                        pass
        except Exception:
            logger.exception("logout_telegram disconnect failed")

        # Recreate client so SQLite releases the .session file handle
        try:
            if self.client.is_connected():
                await self.client.disconnect()
        except Exception:
            pass
        self.client = self._new_telegram_client()
        self.clear_telegram_session_files()
        self._login_phone = None
        self._phone_code_hash = None
        self._channel_cache = None

    async def switch_telegram_api(
        self,
        api_id: int,
        api_hash: str,
        *,
        transfer_channel_data: bool = True,
    ) -> dict[str, Any]:
        """
        Swap Telegram API credentials in .env, drop the old session, and rebuild
        the Telethon client. Channel lists can be kept (transfer) or wiped (fresh).
        """
        from app.settings_store import apply_telegram_api_to_runtime, update_env_values

        if self.discovery_state.running or self.search_state.running:
            return {
                "ok": False,
                "error": "Stop search / background jobs before swapping API keys.",
            }

        api_hash = (api_hash or "").strip()
        if api_id <= 0 or not api_hash:
            return {"ok": False, "error": "API ID and API hash are required."}

        # Drop current MTProto session (bound to the previous api_id)
        try:
            if self.client.is_connected():
                await self.client.disconnect()
        except Exception:
            pass
        self.clear_telegram_session_files()

        if transfer_channel_data:
            # Old account memberships don't apply to the new login
            try:
                JOINED_CHANNELS_FILE.unlink(missing_ok=True)
            except Exception:
                pass
        else:
            self.reset_channel_lists_fresh()

        update_env_values(
            {
                "TELEGRAM_API_ID": str(api_id),
                "TELEGRAM_API_HASH": api_hash,
            }
        )
        apply_telegram_api_to_runtime(api_id, api_hash)

        import app.config as cfg

        self.client = self._new_telegram_client()
        self._login_phone = None
        self._phone_code_hash = None
        self._channel_cache = None
        self._logged_out = True
        try:
            await self.connect()
        except Exception as exc:
            logger.warning("Reconnect after API swap failed (login next): %s", exc)

        return {
            "ok": True,
            "transfer": transfer_channel_data,
            "message": (
                "API keys updated. Channel list transferred — log in with the new Telegram account."
                if transfer_channel_data
                else "API keys updated. Channel data reset — log in and rebuild your list."
            ),
        }

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

    @staticmethod
    def _mentions_crypto(text: str) -> bool:
        return bool(text and CRYPTO_HINT.search(text))

    def _deep_crawl_roots(self, infos_by_key: dict[str, ChannelInfo]) -> list[str]:
        """
        Crawl roots: all seeds first, then other valid non-banned channels
        (discovered/catalog/etc) by member count. Cap with DEEP_CRAWL_MAX_ROOTS.
        """
        seeds: list[str] = []
        others: list[ChannelInfo] = []
        for c in infos_by_key.values():
            if not c.valid or not c.username or c.banned:
                continue
            if c.source == "seed":
                seeds.append(c.username)
            else:
                others.append(c)
        others.sort(key=lambda c: (-(c.members or 0), c.username.casefold()))
        roots: list[str] = []
        seen: set[str] = set()
        for name in seeds + [c.username for c in others]:
            key = name.casefold()
            if key in seen:
                continue
            seen.add(key)
            roots.append(name)
            if len(roots) >= max(1, DEEP_CRAWL_MAX_ROOTS):
                break
        return roots

    async def _discover_from_channels(
        self, root_usernames: list[str]
    ) -> tuple[dict[str, dict[str, Any]], bool]:
        """
        Grow the list from crawl roots (seeds + discovered).
        Returns (username -> {source, title, members}, stopped).
        """
        found: dict[str, dict[str, Any]] = {}
        by_key: dict[str, str] = {}
        priority = {"similar": 0, "linked": 1, "joined": 2, "search": 3}
        root_keys = {s.casefold() for s in root_usernames}
        stopped = False

        def add(
            username: str | None,
            source: str,
            *,
            title: str = "",
            members: int = 0,
        ) -> None:
            if not username:
                return
            username = username.lstrip("@")
            key = username.casefold()
            if not username or key in TME_SKIP or key in root_keys:
                return
            if self._mentions_crypto(f"{title} {username}"):
                return
            existing_name = by_key.get(key)
            meta = {
                "source": source,
                "title": (title or "").strip(),
                "members": int(members or 0),
            }
            if existing_name is None:
                found[username] = meta
                by_key[key] = username
                return
            if priority[source] < priority.get(found[existing_name].get("source", ""), 99):
                del found[existing_name]
                found[username] = meta
                by_key[key] = username
            else:
                # Keep better title/members if upgrading same source priority
                cur = found[existing_name]
                if not cur.get("title") and meta["title"]:
                    cur["title"] = meta["title"]
                if (cur.get("members") or 0) <= 0 and meta["members"]:
                    cur["members"] = meta["members"]

        total_roots = len(root_usernames)
        for idx, seed in enumerate(root_usernames, start=1):
            if self._discovery_stop_requested():
                stopped = True
                break
            self.discovery_state.progress = (
                f"Crawling @{seed} ({idx}/{total_roots}) — similar…"
            )
            try:
                entity = await self.client.get_entity(seed)
                recs = await self.client(GetChannelRecommendationsRequest(channel=entity))
                for chat in getattr(recs, "chats", []) or []:
                    if isinstance(chat, Channel) and getattr(chat, "username", None):
                        add(
                            chat.username,
                            "similar",
                            title=getattr(chat, "title", "") or "",
                            members=int(getattr(chat, "participants_count", 0) or 0),
                        )
            except FloodWaitError as exc:
                await asyncio.sleep(exc.seconds + 1)
            except Exception as exc:
                logger.warning("Similar channels failed for @%s: %s", seed, exc)
            await asyncio.sleep(0.7)

            if self._discovery_stop_requested():
                stopped = True
                break
            self.discovery_state.progress = (
                f"Crawling @{seed} ({idx}/{total_roots}) — links…"
            )
            try:
                for linked in await self._extract_tme_links_from_channel(seed):
                    add(linked, "linked")
            except FloodWaitError as exc:
                await asyncio.sleep(exc.seconds + 1)
            except Exception as exc:
                logger.warning("Link crawl failed for @%s: %s", seed, exc)
            await asyncio.sleep(0.5)

        hop_from = [
            u for u, meta in found.items() if meta.get("source") == "similar"
        ][:20]
        for username in hop_from:
            if self._discovery_stop_requested():
                stopped = True
                break
            try:
                entity = await self.client.get_entity(username)
                recs = await self.client(GetChannelRecommendationsRequest(channel=entity))
                for chat in getattr(recs, "chats", []) or []:
                    if isinstance(chat, Channel) and getattr(chat, "username", None):
                        add(
                            chat.username,
                            "similar",
                            title=getattr(chat, "title", "") or "",
                            members=int(getattr(chat, "participants_count", 0) or 0),
                        )
            except Exception as exc:
                logger.debug("Hop recommendations failed for @%s: %s", username, exc)
            await asyncio.sleep(0.7)

        if not stopped:
            for query in DISCOVERY_QUERIES[:4]:
                if self._discovery_stop_requested():
                    stopped = True
                    break
                try:
                    result = await self.client(ContactsSearchRequest(q=query, limit=30))
                    for chat in result.chats or []:
                        if not isinstance(chat, Channel):
                            continue
                        username = getattr(chat, "username", None)
                        title = (chat.title or "") + " " + (username or "")
                        if (
                            username
                            and STL_HINT.search(title)
                            and not self._mentions_crypto(title)
                        ):
                            add(
                                username,
                                "search",
                                title=chat.title or "",
                                members=int(getattr(chat, "participants_count", 0) or 0),
                            )
                except Exception as exc:
                    logger.warning("Discovery search %r failed: %s", query, exc)
                await asyncio.sleep(0.6)

        if not stopped:
            try:
                async for dialog in self.client.iter_dialogs(limit=300):
                    entity = dialog.entity
                    if not isinstance(entity, Channel):
                        continue
                    username = getattr(entity, "username", None)
                    blob = f"{dialog.name or ''} {username or ''}"
                    if (
                        username
                        and STL_HINT.search(blob)
                        and not self._mentions_crypto(blob)
                    ):
                        add(
                            username,
                            "joined",
                            title=dialog.name or "",
                            members=int(getattr(entity, "participants_count", 0) or 0),
                        )
            except Exception as exc:
                logger.warning("Dialog discovery failed: %s", exc)

        return found, stopped

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

            # Don't pull crypto/web3 channels into the STL catalog (seeds still allowed)
            if source != "seed" and self._mentions_crypto(
                f"{title} {public_username} {description}"
            ):
                return self._invalid_channel(
                    public_username, "Mentions crypto / web3 — skipped", source
                )

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

    @staticmethod
    def _fmt_flood_wait(seconds: int) -> str:
        s = max(0, int(seconds))
        if s < 60:
            return f"{s}s"
        if s < 3600:
            return f"{s // 60}m {s % 60}s"
        if s < 86400:
            return f"{s // 3600}h {(s % 3600) // 60}m"
        return f"{s // 86400}d {(s % 86400) // 3600}h"

    async def run_search(
        self,
        query: str,
        files_only: bool = True,
        max_age_days: int = MAX_AGE_DAYS,
        per_channel_limit: int = 40,
    ) -> None:
        """
        Search via Telegram global search (like the official app), then keep
        only hits from enabled Manage channels. Much fewer API calls than
        iterating every channel with messages.search.
        """
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
                step="preparing",
                results=[],
            )

        seen_keys: set[str] = set()
        stopped = False
        max_wait_seconds = 45

        try:
            active = [c for c in self.get_channels_fast() if c.included and not c.banned]
            allowed = {c.username.casefold(): c for c in active if c.username}
            variants = generate_variants(query)
            if not variants:
                self.search_state.status = "done"
                self.search_state.step = "done"
                self.search_state.progress = "Empty query"
                self.search_state.finished_at = datetime.now(timezone.utc).isoformat()
                return

            self.search_state.channels_total = len(variants)
            self.search_state.channels_scanned = 0
            self.search_state.step = "searching"
            self.search_state.progress = (
                f"Global search across {len(allowed)} enabled channel(s)…"
            )

            cutoff = datetime.now(timezone.utc) - timedelta(days=max_age_days)
            msg_filter = (
                InputMessagesFilterDocument() if files_only else InputMessagesFilterEmpty()
            )
            # Overall pull per variant (global), not per-channel
            global_limit = max(80, per_channel_limit * 3)
            chat_cache: dict[int, ChannelInfo | None] = {}
            channels_hit: set[str] = set()

            async def resolve_allowed_channel(message: Message) -> ChannelInfo | None:
                chat = message.chat
                if chat is None:
                    try:
                        chat = await message.get_chat()
                    except Exception:
                        return None
                if not isinstance(chat, Channel):
                    return None
                cid = int(chat.id)
                if cid in chat_cache:
                    return chat_cache[cid]
                uname = (getattr(chat, "username", None) or "").lstrip("@")
                info = allowed.get(uname.casefold()) if uname else None
                chat_cache[cid] = info
                return info

            for v_idx, variant in enumerate(variants, start=1):
                if self._search_stop_requested():
                    stopped = True
                    break

                self.search_state.current_variant = variant
                self.search_state.current_channel = ""
                self.search_state.step = "searching"
                self.search_state.progress = (
                    f"Global search “{variant}” ({v_idx}/{len(variants)})…"
                )
                self.search_state.channels_scanned = v_idx - 1

                try:
                    async for message in self.client.iter_messages(
                        None,
                        search=variant,
                        filter=msg_filter,
                        limit=global_limit,
                    ):
                        if self._search_stop_requested():
                            stopped = True
                            break
                        if not isinstance(message, Message):
                            continue
                        if message.date:
                            msg_date = message.date
                            if msg_date.tzinfo is None:
                                msg_date = msg_date.replace(tzinfo=timezone.utc)
                            if msg_date < cutoff:
                                # Global results aren't strictly date-sorted; don't break
                                continue

                        channel = await resolve_allowed_channel(message)
                        if channel is None:
                            continue

                        if files_only and not self._has_wanted_file(message):
                            if not message.document:
                                continue
                            if not self._document_matches_extensions(message):
                                continue

                        hit = await self._to_hit(
                            message, channel, variant, defer_preview=True
                        )
                        if hit:
                            self._append_search_result(hit, seen_keys)
                            channels_hit.add(channel.username.casefold())
                            self.search_state.progress = (
                                f"Global “{variant}” ({v_idx}/{len(variants)}) · "
                                f"{len(self.search_state.results)} file(s) · "
                                f"{len(channels_hit)}/{len(allowed)} ch"
                            )
                except FloodWaitError as exc:
                    if self._search_stop_requested():
                        stopped = True
                        break
                    wait_s = int(getattr(exc, "seconds", 0) or 0)
                    pretty = self._fmt_flood_wait(wait_s)
                    if wait_s > max_wait_seconds:
                        msg = (
                            f"Rate limited on global search “{variant}”: "
                            f"skipping term (Telegram asked for {pretty})"
                        )
                        logger.warning(msg)
                        self.search_state.errors.append(msg)
                        self.search_state.progress = f"Skipped “{variant}” (rate limit)"
                    else:
                        msg = f"Rate limited on global search: waiting {pretty}"
                        logger.warning(msg)
                        self.search_state.errors.append(msg)
                        self.search_state.step = "waiting"
                        self.search_state.progress = msg
                        await asyncio.sleep(wait_s + 1)
                except Exception as exc:
                    msg = f"Global search “{variant}”: {exc}"
                    logger.exception(msg)
                    self.search_state.errors.append(msg)

                self.search_state.channels_scanned = v_idx
                if stopped or self._search_stop_requested():
                    stopped = True
                    break
                if SEARCH_VARIANT_DELAY > 0:
                    await asyncio.sleep(SEARCH_VARIANT_DELAY)

            self.search_state.results.sort(key=lambda r: r.get("date", ""), reverse=True)
            self.search_state.current_channel = ""
            self.search_state.current_variant = ""
            n = len(self.search_state.results)
            src = sum(len(g.get("sources") or []) for g in self.search_state.results)
            hit_n = len(channels_hit)
            if stopped or self._search_stop_requested():
                self.search_state.status = "stopped"
                self.search_state.step = "stopped"
                self.search_state.progress = (
                    f"Stopped — {n} unique file(s) from {src} post(s) "
                    f"in {hit_n} channel(s)"
                )
            else:
                self.search_state.status = "done"
                self.search_state.step = "done"
                self.search_state.progress = (
                    f"Found {n} unique file(s) from {src} post(s) "
                    f"in {hit_n}/{len(allowed)} enabled channel(s)"
                )
            self.search_state.finished_at = datetime.now(timezone.utc).isoformat()
        except Exception as exc:
            self.search_state.status = "error"
            self.search_state.step = "error"
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
                step="browsing",
                current_channel=channel.username,
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
        seen_lock: asyncio.Lock | None = None,
        progress_lock: asyncio.Lock | None = None,
        defer_preview: bool = False,
    ) -> None:
        msg_filter = InputMessagesFilterDocument() if files_only else InputMessagesFilterEmpty()
        channel_seen: set[int] = set()

        for variant in variants:
            if self._search_stop_requested():
                return
            if progress_lock:
                async with progress_lock:
                    self.search_state.current_channel = channel.username
                    self.search_state.current_variant = variant
            else:
                self.search_state.current_channel = channel.username
                self.search_state.current_variant = variant
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

                    hit = await self._to_hit(
                        message, channel, variant, defer_preview=defer_preview
                    )
                    if hit:
                        channel_seen.add(message.id)
                        if seen_lock:
                            async with seen_lock:
                                self._append_search_result(hit, seen_keys)
                        else:
                            self._append_search_result(hit, seen_keys)
            except FloodWaitError:
                raise
            except Exception as exc:
                err = f"@{channel.username} / '{variant}': {exc}"
                if progress_lock:
                    async with progress_lock:
                        self.search_state.errors.append(err)
                else:
                    self.search_state.errors.append(err)
            if SEARCH_VARIANT_DELAY > 0:
                await asyncio.sleep(SEARCH_VARIANT_DELAY)

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

    def _thumb_disk_path(self, channel_username: str, message_id: int) -> Path:
        return THUMBS_DIR / f"{channel_username}_{message_id}.jpg"

    def _schedule_preview_enrich(
        self, message: Message, channel: ChannelInfo, group_key: str
    ) -> None:
        """Fetch missing thumbs in the background so search isn't blocked."""

        async def _run() -> None:
            async with self._preview_sem:
                if self._group_has_thumb(group_key):
                    return
                path = await self._save_preview(message, channel.username)
                if not path:
                    return
                thumb_url = f"/thumbs/{path.name}"
                idx = self._group_index(group_key)
                if idx is not None and not self.search_state.results[idx].get("thumb_url"):
                    self.search_state.results[idx]["thumb_url"] = thumb_url

        try:
            task = asyncio.create_task(_run())
        except RuntimeError:
            return
        self._preview_tasks.add(task)
        task.add_done_callback(self._preview_tasks.discard)

    async def _to_hit(
        self,
        message: Message,
        channel: ChannelInfo,
        variant: str,
        defer_preview: bool = False,
    ) -> SearchHit | None:
        file_name = self._file_name(message)
        text = (message.message or message.text or "").strip()
        if not file_name and not text:
            return None

        file_size = int(message.document.size) if message.document else 0
        group_key = self._file_group_key(file_name, file_size, message.id, channel.username)

        # Skip slow preview fetch when we already have this file + a thumb
        thumb_url = None
        if not self._group_has_thumb(group_key):
            disk = self._thumb_disk_path(channel.username, message.id)
            if disk.exists() and disk.stat().st_size > 0:
                thumb_url = f"/thumbs/{disk.name}"
            elif defer_preview:
                self._schedule_preview_enrich(message, channel, group_key)
            else:
                thumb_path = await self._save_preview(message, channel.username)
                if thumb_path:
                    thumb_url = f"/thumbs/{thumb_path.name}"

        username = channel.username
        link = f"https://t.me/{username}/{message.id}"
        date = ""
        if message.date:
            date = _format_result_date(message.date)

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
        """Keep a pool of workers so several downloads run in parallel."""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        wanted = max(1, min(8, int(DOWNLOAD_JOB_CONCURRENCY or 4)))
        # Drop finished tasks
        self._download_worker_tasks = [
            t for t in self._download_worker_tasks if not t.done()
        ]
        while len(self._download_worker_tasks) < wanted:
            self._download_worker_tasks.append(
                loop.create_task(self._download_worker_loop())
            )

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
                # Share media connections across concurrent jobs (avoid DC stalls).
                jobs_n = max(1, min(8, int(DOWNLOAD_JOB_CONCURRENCY or 4)))
                per_file = max(2, min(int(DOWNLOAD_CONNECTIONS or 4), max(2, 12 // jobs_n)))
                path = await parallel_download_to_path(
                    self.client,
                    message.document,
                    str(partial),
                    progress_callback=progress,
                    should_cancel=should_cancel,
                    part_size_kb=float(DOWNLOAD_PART_KB),
                    max_connections=per_file,
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
