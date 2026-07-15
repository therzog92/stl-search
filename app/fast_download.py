"""
Faster Telethon downloads via parallel DC connections.

Based on mautrix-telegram parallel_file_transfer.py / painor's FastTelethon
(MIT / with permission to redistribute). Official Telegram apps also pull
multiple chunks in parallel; stock Telethon downloads sequentially.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import math
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


class DownloadCancelled(Exception):
    """Raised when the user cancels an in-progress download."""


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
        result = await self.client._call(self.sender, self.request)
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

    async def _cleanup(self) -> None:
        if not self.senders:
            return
        await asyncio.gather(*[sender.disconnect() for sender in self.senders])
        self.senders = None

    @staticmethod
    def _get_connection_count(
        file_size: int,
        max_count: int = 12,
        full_size: int = 80 * 1024 * 1024,
    ) -> int:
        # Telegram tolerates ~8–12; more helps big ZIPs once AES is native-speed.
        max_count = max(1, min(int(max_count), 16))
        if file_size >= full_size:
            return max_count
        if file_size <= 0:
            return 2
        count = math.ceil((file_size / full_size) * max_count)
        return max(2, min(max_count, count))

    async def _create_sender(self) -> MTProtoSender:
        dc = await self.client._get_dc(self.dc_id)
        sender = MTProtoSender(self.auth_key, loggers=self.client._log)
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
        if not self.auth_key:
            log.debug("Exporting auth to DC %s", self.dc_id)
            auth = await self.client(ExportAuthorizationRequest(self.dc_id))
            self.client._init_request.query = ImportAuthorizationRequest(
                id=auth.id, bytes=auth.bytes
            )
            req = InvokeWithLayerRequest(LAYER, self.client._init_request)
            await sender.send(req)
            self.auth_key = sender.auth_key
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
        rest = await asyncio.gather(
            *[
                self._create_download_sender(
                    file, i, part_size, connections * part_size, get_part_count()
                )
                for i in range(1, connections)
            ]
        )
        self.senders = [first, *rest]

    async def download(
        self,
        file: TypeLocation,
        file_size: int,
        *,
        part_size_kb: float = 512,
        connection_count: Optional[int] = None,
        max_connections: int = 12,
    ):
        connections = connection_count or self._get_connection_count(
            file_size, max_count=max_connections
        )
        part_size = int(part_size_kb * 1024)
        part_count = max(1, math.ceil(file_size / part_size)) if file_size else 1
        log.debug(
            "Parallel download: connections=%s part_size=%s parts=%s",
            connections,
            part_size,
            part_count,
        )
        await self._init_download(connections, file, part_count, part_size)
        try:
            part = 0
            while part < part_count:
                # Await in sender order so bytes are written sequentially.
                results = await asyncio.gather(*[sender.next() for sender in self.senders])
                for data in results:
                    if not data:
                        return
                    yield data
                    part += 1
                    if part >= part_count:
                        return
        finally:
            await self._cleanup()


_parallel_locks: DefaultDict[int, asyncio.Lock] = defaultdict(asyncio.Lock)


async def parallel_download_to_path(
    client: TelegramClient,
    document: Document,
    dest_path: str,
    *,
    progress_callback: ProgressCallback | None = None,
    should_cancel: Callable[[], bool] | None = None,
    part_size_kb: float = 512,
    max_connections: int = 12,
) -> str:
    """Download a document with multiple connections into dest_path."""
    size = int(getattr(document, "size", 0) or 0)
    dc_id, location = utils.get_input_location(document)
    lock = _parallel_locks[dc_id]
    async with lock:
        if should_cancel and should_cancel():
            raise DownloadCancelled()
        downloader = ParallelTransferrer(client, dc_id)
        helpers.ensure_parent_dir_exists(dest_path)
        downloaded = 0
        try:
            with open(dest_path, "wb") as out:
                async for chunk in downloader.download(
                    location,
                    size,
                    part_size_kb=part_size_kb,
                    max_connections=max_connections,
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
