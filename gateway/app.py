"""
Failover reverse proxy for STL Search.

Prefers the Windows PC upstream while /health is OK; otherwise proxies to the
Synology container. Used so https://stl… on the NAS can reach PC-folder downloads
when the Alienware is online.
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Optional

import httpx
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse

log = logging.getLogger("stl.gateway")

WINDOWS_UPSTREAM = (
    os.getenv("STL_WINDOWS_UPSTREAM") or "http://192.168.0.88:8787"
).rstrip("/")
NAS_UPSTREAM = (os.getenv("STL_NAS_UPSTREAM") or "http://stl-search:8787").rstrip("/")
HEALTH_PATH = os.getenv("STL_GATEWAY_HEALTH_PATH") or "/health"
CHECK_INTERVAL = float(os.getenv("STL_GATEWAY_CHECK_INTERVAL") or "3")
CHECK_TIMEOUT = float(os.getenv("STL_GATEWAY_CHECK_TIMEOUT") or "2")

_prefer_windows = False
_windows_ok = False
_lock = asyncio.Lock()
_client: Optional[httpx.AsyncClient] = None
_checker_task: Optional[asyncio.Task] = None

HOP_BY_HOP = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
    "host",
    "content-length",
}


def _active_upstream() -> str:
    return WINDOWS_UPSTREAM if _prefer_windows else NAS_UPSTREAM


def _active_label() -> str:
    return "windows" if _prefer_windows else "synology"


async def _probe_windows() -> bool:
    assert _client is not None
    url = f"{WINDOWS_UPSTREAM}{HEALTH_PATH}"
    try:
        resp = await _client.get(url, timeout=CHECK_TIMEOUT)
        if resp.status_code != 200:
            return False
        data = resp.json()
        return bool(data.get("ok", True))
    except Exception as exc:
        log.debug("Windows health check failed: %s", exc)
        return False


async def _health_loop() -> None:
    global _prefer_windows, _windows_ok
    while True:
        ok = await _probe_windows()
        async with _lock:
            changed = ok != _windows_ok
            _windows_ok = ok
            _prefer_windows = ok
        if changed:
            log.info(
                "Upstream switched to %s (%s)",
                _active_label(),
                _active_upstream(),
            )
        await asyncio.sleep(CHECK_INTERVAL)


app = FastAPI(title="STL Search Gateway", docs_url=None, redoc_url=None)


@app.on_event("startup")
async def _startup() -> None:
    global _client, _checker_task, _prefer_windows, _windows_ok
    _client = httpx.AsyncClient(
        follow_redirects=False,
        timeout=httpx.Timeout(300.0, connect=10.0),
    )
    _windows_ok = await _probe_windows()
    _prefer_windows = _windows_ok
    log.info(
        "Gateway ready — primary=%s fallback=%s active=%s",
        WINDOWS_UPSTREAM,
        NAS_UPSTREAM,
        _active_label(),
    )
    _checker_task = asyncio.create_task(_health_loop())


@app.on_event("shutdown")
async def _shutdown() -> None:
    global _client, _checker_task
    if _checker_task:
        _checker_task.cancel()
        try:
            await _checker_task
        except asyncio.CancelledError:
            pass
        _checker_task = None
    if _client:
        await _client.aclose()
        _client = None


@app.get("/_gateway/status")
async def gateway_status():
    return {
        "ok": True,
        "active": _active_label(),
        "windows_ok": _windows_ok,
        "windows_upstream": WINDOWS_UPSTREAM,
        "nas_upstream": NAS_UPSTREAM,
    }


async def _proxy(request: Request, path: str) -> Response:
    global _prefer_windows, _windows_ok
    assert _client is not None
    upstream = _active_upstream()
    # Preserve empty path as root
    suffix = path if path else ""
    url = f"{upstream}/{suffix}" if suffix else f"{upstream}/"
    if request.url.query:
        url = f"{url}?{request.url.query}"

    headers = {
        k: v
        for k, v in request.headers.items()
        if k.lower() not in HOP_BY_HOP
    }
    headers["x-stl-gateway-active"] = _active_label()

    body = await request.body()
    try:
        upstream_resp = await _client.request(
            request.method,
            url,
            headers=headers,
            content=body or None,
        )
    except httpx.RequestError as exc:
        # If Windows was selected but dies mid-request, try NAS once
        if upstream == WINDOWS_UPSTREAM:
            log.warning("Windows proxy error (%s); falling back to NAS", exc)
            async with _lock:
                _prefer_windows = False
                _windows_ok = False
            nas_url = f"{NAS_UPSTREAM}/{suffix}" if suffix else f"{NAS_UPSTREAM}/"
            if request.url.query:
                nas_url = f"{nas_url}?{request.url.query}"
            try:
                upstream_resp = await _client.request(
                    request.method,
                    nas_url,
                    headers=headers,
                    content=body or None,
                )
            except httpx.RequestError as nas_exc:
                return JSONResponse(
                    {
                        "ok": False,
                        "error": f"Both upstreams failed: {exc}; {nas_exc}",
                    },
                    status_code=502,
                )
        else:
            return JSONResponse(
                {"ok": False, "error": f"Upstream error: {exc}"},
                status_code=502,
            )

    out_headers = {
        k: v
        for k, v in upstream_resp.headers.items()
        if k.lower() not in HOP_BY_HOP
    }
    out_headers["x-stl-gateway-active"] = _active_label()
    return Response(
        content=upstream_resp.content,
        status_code=upstream_resp.status_code,
        headers=out_headers,
        media_type=upstream_resp.headers.get("content-type"),
    )


@app.api_route("/", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"])
async def proxy_root(request: Request) -> Response:
    return await _proxy(request, "")


@app.api_route(
    "/{path:path}",
    methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"],
)
async def proxy_path(request: Request, path: str) -> Response:
    # Don't swallow our own status endpoint (matched first above when exact)
    if path == "_gateway/status":
        return await gateway_status()
    return await _proxy(request, path)
