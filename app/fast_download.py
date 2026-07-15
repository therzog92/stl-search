"""
Faster Telethon downloads via parallel DC connections.

Based on mautrix-telegram parallel_file_transfer.py / painor's FastTelethon
(MIT / with permission to redistribute). Official Telegram apps also pull
multiple chunks in parallel; stock Telethon downloads sequentially.

When several files download at once, a global media-connection budget keeps
Telegram from stalling (~4 files × 12 conns was freezing progress ~5%).
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import math
import os
from collections import defaultdict
from pathlib import Path
from typing import Any, Awaitable, Callable, DefaultDict, List, Optional, Union

from telethon import TelegramClient, helpers, utils
from telethon.crypto import AuthKey
from telethon.network import MTProtoSender
from telethon.tl.alltlobjects import LAYER
from telethon.tl.functions import InvokeWithLayerRequest
from telethon.tl.functions.auth import ExportAuthorizationRequest, ImportAuthorizationRequest
from telethon.tl.functions.upload import GetFileRequest
from telethon.tl.types import (
    Document,
    InputDocumentFileLocation,
    InputFileLocation,
    InputPeerPhotoFileLocation,
    InputPhotoFileLocation,
)

log = logging.getLogger("stl.fast_download")

# Serialize ExportAuthorization per DC only (not the whole download).
_auth_export_locks: DefaultDict[int, asyncio.Lock] = defaultdict(asyncio.Lock)
_exported_auth_keys: dict[int, AuthKey] = {}

# Cap total concurrent media TCP connections across ALL file downloads.
# Telegram gets unhappy past ~12–16 GetFile streams on one account.
_MEDIA_CONN_BUDGET = max(4, min(24, int(os.getenv("STL_DOWNLOAD_CONN_BUDGET", "12"))))
_media_slots = asyncio.Semaphore(_MEDIA_CONN_BUDGET)
_GETFILE_TIMEOUT = float(os.getenv("STL_DOWNLOAD_CHUNK_TIMEOUT", "45"))


class DownloadCancelled(Exception):
    """Raised when the user cancels an in-progress download."""


class DownloadChunkTimeout(Exception):
    """Raised when a media DC stops answering GetFile."""


TypeLocation = Union[
    Document,
    InputDocumentFileLocation,
    InputPeerPhotoFileLocation,
    InputFileLocation,
    InputPhotoFileLocation,
]

ProgressCallback = Callable[[int, int], Any]


class DownloadSender:
    def __init__(
        self,
        client: TelegramClient,
        sender: MTProtoSender,
        file: TypeLocation,
        offset: int,
        limit: int,
        stride: int,
        count: int,
    ) -> None:
        self.client = client
        self.sender = sender
        self.request = GetFileRequest(file, offset=offset, limit=limit)
        self.stride = stride
        self.remaining = count

    async def next(self) -> Optional[bytes]:
        if not self.remaining:
            return None
        try:
            result = await asyncio.wait_for(
                self.client._call(self.sender, self.request),
                timeout=_GETFILE_TIMEOUT,
            )
        except asyncio.TimeoutError as exc:
            raise DownloadChunkTimeout(
                f"GetFile timed out after {_GETFILE_TIMEOUT:.0f}s"
            ) from exc
        self.remaining -= 1
        self.request.offset += self.stride
        return result.bytes

    def disconnect(self) -> Awaitable[None]:
        return self.sender.disconnect()


class ParallelTransferrer:
    def __init__(self, client: TelegramClient, dc_id: Optional[int] = None) -> None:
        self.client = client
        self.loop = client.loop
        self.dc_id = dc_id or client.session.dc_id
        self.auth_key = (
            None
            if dc_id and client.session.dc_id != dc_id
            else client.session.auth_key
        )
        self.senders: Optional[List[DownloadSender]] = None
        self._held_slots = 0

    async def _cleanup(self) -> None:
        try:
            if self.senders:
                await asyncio.gather(
                    *[sender.disconnect() for sender in self.senders],
                    return_exceptions=True,
                )
        finally:
            self.senders = None
            while self._held_slots > 0:
                _media_slots.release()
                self._held_slots -= 1

    @staticmethod
    def _get_connection_count(
        file_size: int,
        max_count: int = 4,
        full_size: int = 80 * 1024 * 1024,
    ) -> int:
        # Keep per-file low so concurrent jobs share the global budget.
        max_count = max(1, min(int(max_count), 8))
        if file_size >= full_size:
            return max_count
        if file_size <= 0:
            return min(2, max_count)
        count = math.ceil((file_size / full_size) * max_count)
        return max(1, min(max_count, max(2, count) if max_count >= 2 else 1))

    async def _reserve_slots(self, wanted: int) -> int:
        """Grab up to `wanted` media slots (at least 1), without starving forever."""
        wanted = max(1, wanted)
        await _media_slots.acquire()
        held = 1
        # Opportunistically take more without blocking long.
        while held < wanted:
            try:
                await asyncio.wait_for(_media_slots.acquire(), timeout=0.05)
                held += 1
            except asyncio.TimeoutError:
                break
        self._held_slots = held
        return held

    async def _connect_sender(self, auth_key: AuthKey | None) -> MTProtoSender:
        dc = await self.client._get_dc(self.dc_id)
        sender = MTProtoSender(auth_key, loggers=self.client._log)
        await sender.connect(
            self.client._connection(
                dc.ip_address,
                dc.port,
                dc.id,
                loggers=self.client._log,
                proxy=self.client._proxy,
                local_addr=getattr(self.client, "_local_addr", None),
            )
        )
        return sender

    async def _create_sender(self) -> MTProtoSender:
        # Reuse exported media-DC auth across concurrent file downloads.
        auth_key = self.auth_key or _exported_auth_keys.get(self.dc_id)
        if auth_key is not None:
            self.auth_key = auth_key
            return await self._connect_sender(auth_key)

        async with _auth_export_locks[self.dc_id]:
            auth_key = self.auth_key or _exported_auth_keys.get(self.dc_id)
            if auth_key is not None:
                self.auth_key = auth_key
                return await self._connect_sender(auth_key)

            log.debug("Exporting auth to DC %s", self.dc_id)
            sender = await self._connect_sender(None)
            auth = await self.client(ExportAuthorizationRequest(self.dc_id))
            self.client._init_request.query = ImportAuthorizationRequest(
                id=auth.id, bytes=auth.bytes
            )
            req = InvokeWithLayerRequest(LAYER, self.client._init_request)
            await sender.send(req)
            self.auth_key = sender.auth_key
            if self.auth_key is not None:
                _exported_auth_keys[self.dc_id] = self.auth_key
            return sender

    async def _create_download_sender(
        self,
        file: TypeLocation,
        index: int,
        part_size: int,
        stride: int,
        part_count: int,
    ) -> DownloadSender:
        return DownloadSender(
            self.client,
            await self._create_sender(),
            file,
            index * part_size,
            part_size,
            stride,
            part_count,
        )

    async def _init_download(
        self, connections: int, file: TypeLocation, part_count: int, part_size: int
    ) -> None:
        minimum, remainder = divmod(part_count, connections)

        def get_part_count() -> int:
            nonlocal remainder
            if remainder > 0:
                remainder -= 1
                return minimum + 1
            return minimum

        # First sender exports auth; create it alone before the rest.
        first = await self._create_download_sender(
            file, 0, part_size, connections * part_size, get_part_count()
        )
        rest: list[DownloadSender] = []
        if connections > 1:
            rest = list(
                await asyncio.gather(
                    *[
                        self._create_download_sender(
                            file, i, part_size, connections * part_size, get_part_count()
                        )
                        for i in range(1, connections)
                    ]
                )
            )
        self.senders = [first, *rest]

    async def download(
        self,
        file: TypeLocation,
        file_size: int,
        *,
        part_size_kb: float = 512,
        connection_count: Optional[int] = None,
        max_connections: int = 4,
    ):
        wanted = connection_count or self._get_connection_count(
            file_size, max_count=max_connections
        )
        connections = await self._reserve_slots(wanted)
        part_size = int(part_size_kb * 1024)
        part_count = max(1, math.ceil(file_size / part_size)) if file_size else 1
        log.info(
            "Parallel download DC%s: connections=%s (wanted %s) part_size=%s parts=%s budget=%s",
            self.dc_id,
            connections,
            wanted,
            part_size,
            part_count,
            _MEDIA_CONN_BUDGET,
        )
        try:
            await self._init_download(connections, file, part_count, part_size)
            part = 0
            while part < part_count:
                assert self.senders is not None
                # Await in sender order so bytes are written sequentially.
                results = await asyncio.gather(
                    *[sender.next() for sender in self.senders]
                )
                for data in results:
                    if not data:
                        return
                    yield data
                    part += 1
                    if part >= part_count:
                        return
        finally:
            await self._cleanup()


async def parallel_download_to_path(
    client: TelegramClient,
    document: Document,
    dest_path: str,
    *,
    progress_callback: ProgressCallback | None = None,
    should_cancel: Callable[[], bool] | None = None,
    part_size_kb: float = 512,
    max_connections: int = 4,
) -> str:
    """Download a document with multiple connections into dest_path.

    Multiple files may download concurrently. Total media connections across
    all files are capped so Telegram does not stall mid-transfer.
    """
    size = int(getattr(document, "size", 0) or 0)
    dc_id, location = utils.get_input_location(document)
    if should_cancel and should_cancel():
        raise DownloadCancelled()
    # Share the global budget across concurrent jobs (e.g. 12 / 4 jobs ≈ 3 each).
    safe_max = max(1, min(int(max_connections or 4), 8))
    downloader = ParallelTransferrer(client, dc_id)
    helpers.ensure_parent_dir_exists(dest_path)
    downloaded = 0
    try:
        with open(dest_path, "wb") as out:
            async for chunk in downloader.download(
                location,
                size,
                part_size_kb=part_size_kb,
                max_connections=safe_max,
            ):
                if should_cancel and should_cancel():
                    raise DownloadCancelled()
                out.write(chunk)
                downloaded += len(chunk)
                if progress_callback:
                    result = progress_callback(downloaded, size or downloaded)
                    if inspect.isawaitable(result):
                        await result
        return dest_path
    except DownloadCancelled:
        try:
            Path(dest_path).unlink(missing_ok=True)
        except Exception:
            pass
        raise
