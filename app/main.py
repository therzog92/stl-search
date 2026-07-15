from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Form, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

# Must run before Telethon encrypts anything (downloads/MTProto).
from app.crypto_accel import install_tgcrypto_for_telethon

install_tgcrypto_for_telethon()

from app.catalog import has_api_key as telemetr_has_key
from app.config import DOWNLOAD_DIR, MAX_AGE_DAYS, MIN_CHANNEL_MEMBERS, THUMBS_DIR
from app.settings_store import mask_secret
from app.telegram_service import telegram_service
from app.variants import generate_variants

BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

_SOURCE_LABELS = {
    "seed": "seed",
    "catalog": "catalog",
    "similar": "similar",
    "linked": "linked",
    "search": "search",
    # How the channel was found during discovery — not Telegram membership
    "joined": "from chats",
    "discovered": "discovered",
}


def _source_label(source: str) -> str:
    key = (source or "").strip().lower()
    return _SOURCE_LABELS.get(key, source or "unknown")


templates.env.filters["source_label"] = _source_label


def _session_file() -> Path:
    import app.config as cfg

    return Path(f"{cfg.SESSION_PATH}.session")


def _has_local_session() -> bool:
    """True only if a session file exists and we are not forced logged-out."""
    if getattr(telegram_service, "_logged_out", False):
        return False
    return _session_file().exists()


async def _require_login():
    """Redirect to login unless Telegram reports a real authorized session."""
    if await telegram_service.is_authorized():
        return None
    return _auth_redirect()


@asynccontextmanager
async def lifespan(_: FastAPI):
    try:
        await asyncio.wait_for(telegram_service.connect(), timeout=15)
    except Exception:
        # App still serves UI/cache even if Telegram is slow at boot
        pass
    try:
        telegram_service.cleanup_incomplete_downloads()
    except Exception:
        pass
    yield
    try:
        # Drop any in-progress partials before shutdown
        telegram_service.cleanup_incomplete_downloads()
    except Exception:
        pass
    try:
        await telegram_service.disconnect()
    except Exception:
        pass


app = FastAPI(title="Telegram STL Search", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
app.mount("/thumbs", StaticFiles(directory=str(THUMBS_DIR)), name="thumbs")


def _auth_redirect():
    return RedirectResponse("/login", status_code=303)


@app.get("/health")
async def health():
    return {"ok": True, "discovery_running": telegram_service.discovery_state.running}


@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    if not await telegram_service.is_authorized():
        return _auth_redirect()

    channels = telegram_service.get_channels_fast()
    active = [c for c in channels if c.valid and not c.banned]
    seed_channels = [c for c in active if c.source == "seed"]
    other_channels = [c for c in active if c.source != "seed"]
    favorite_channels = sorted(
        [c for c in active if c.favorite],
        key=lambda c: (-(c.members or 0), c.username.casefold()),
    )
    joined = telegram_service.known_joined_usernames()
    joined_status = telegram_service.joined_status_map()
    joined_lookup = {u: True for u in joined}

    discovery_error = telegram_service.discovery_error or telegram_service.discovery_state.error
    discovery_message = (
        telegram_service.discovery_message or telegram_service.discovery_state.message
    )
    if not telegram_service.discovery_state.running:
        telegram_service.discovery_error = None
        telegram_service.discovery_message = None
        telegram_service.discovery_state.error = None
        telegram_service.discovery_state.message = None

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "seed_channels": seed_channels,
            "other_channels": other_channels,
            "favorite_channels": favorite_channels,
            "channels": channels,
            "joined_usernames": sorted(joined),
            "joined_lookup": joined_lookup,
            "joined_status": joined_status,
            "min_members": MIN_CHANNEL_MEMBERS,
            "max_age_days": MAX_AGE_DAYS,
            "state": telegram_service.search_state,
            "variants_preview": [],
            "telemetr_configured": telemetr_has_key(),
            "discovery_error": discovery_error,
            "discovery_message": discovery_message,
            "discovery_running": telegram_service.discovery_state.running,
            "discovery_progress": telegram_service.discovery_state.progress,
        },
    )


@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    # Only skip login when Telegram actually authorizes the session —
    # a leftover .session file after Log out must still show the login form.
    if await telegram_service.is_authorized():
        return RedirectResponse("/", status_code=303)
    return templates.TemplateResponse(
        "login.html",
        {"request": request, "step": "phone", "error": None, "phone": ""},
    )


@app.post("/login/phone", response_class=HTMLResponse)
async def login_phone(request: Request, phone: str = Form(...)):
    try:
        await telegram_service.start_login(phone)
        return templates.TemplateResponse(
            "login.html",
            {"request": request, "step": "code", "error": None, "phone": phone.strip()},
        )
    except Exception as exc:
        return templates.TemplateResponse(
            "login.html",
            {"request": request, "step": "phone", "error": str(exc), "phone": phone},
            status_code=400,
        )


@app.post("/login/code", response_class=HTMLResponse)
async def login_code(
    request: Request,
    phone: str = Form(...),
    code: str = Form(...),
    password: str = Form(""),
):
    try:
        result = await telegram_service.confirm_code(code, password or None)
        if result.get("status") == "password_required":
            return templates.TemplateResponse(
                "login.html",
                {
                    "request": request,
                    "step": "password",
                    "error": None,
                    "phone": phone,
                    "code": code,
                },
            )
        # Warm seed list in background after login
        asyncio.create_task(
            telegram_service.resolve_channels(force=True, discover=False, deep_crawl=False)
        )
        return RedirectResponse("/", status_code=303)
    except Exception as exc:
        step = "password" if password else "code"
        return templates.TemplateResponse(
            "login.html",
            {
                "request": request,
                "step": step,
                "error": str(exc),
                "phone": phone,
                "code": code,
            },
            status_code=400,
        )


@app.post("/search")
async def start_search(
    query: str = Form(...),
    files_only: str = Form("off"),
    max_age_days: int = Form(MAX_AGE_DAYS),
):
    if not await telegram_service.is_authorized():
        return _auth_redirect()
    if telegram_service.search_state.running:
        return RedirectResponse("/results", status_code=303)

    only_files = files_only == "on"
    asyncio.create_task(
        telegram_service.run_search(
            query=query.strip(),
            files_only=only_files,
            max_age_days=max_age_days,
        )
    )
    return RedirectResponse("/results", status_code=303)


@app.post("/search/stop")
async def stop_search():
    if not await telegram_service.is_authorized():
        return JSONResponse({"error": "not_authorized"}, status_code=401)
    stopped = telegram_service.request_stop_search()
    return {"ok": True, "stopped": stopped}


@app.post("/api/search")
async def api_search(
    query: str = Form(...),
    files_only: bool = Form(True),
    max_age_days: int = Form(MAX_AGE_DAYS),
):
    if not await telegram_service.is_authorized():
        return JSONResponse({"error": "not_authorized"}, status_code=401)
    if telegram_service.search_state.running:
        return JSONResponse({"error": "busy"}, status_code=409)

    async def _run():
        await telegram_service.run_search(
            query=query.strip(),
            files_only=files_only,
            max_age_days=max_age_days,
        )

    asyncio.create_task(_run())
    return {"status": "started", "variants": generate_variants(query)}


@app.get("/api/status")
async def api_status():
    state = telegram_service.search_state
    source_count = sum(len(g.get("sources") or []) for g in state.results)
    return {
        "running": state.running,
        "status": state.status,
        "progress": state.progress,
        "query": state.query,
        "mode": state.mode,
        "browse_username": state.browse_username,
        "browse_title": state.browse_title,
        "channels_scanned": state.channels_scanned,
        "channels_total": state.channels_total,
        "current_channel": state.current_channel,
        "current_variant": state.current_variant,
        "step": state.step,
        "result_count": len(state.results),
        "source_count": source_count,
        "errors": state.errors,
        "finished_at": state.finished_at,
        "results": state.results,
        "discovery_running": telegram_service.discovery_state.running,
        "discovery_progress": telegram_service.discovery_state.progress,
    }


@app.get("/api/discovery")
async def api_discovery():
    ds = telegram_service.discovery_state
    return {
        "running": ds.running,
        "mode": ds.mode,
        "phase": getattr(ds, "phase", "") or "",
        "progress": ds.progress,
        "error": ds.error or telegram_service.discovery_error,
        "message": ds.message or telegram_service.discovery_message,
        "channel_count": len(telegram_service.get_channels_fast()),
    }


@app.get("/results", response_class=HTMLResponse)
async def results_page(request: Request):
    if not await telegram_service.is_authorized():
        return _auth_redirect()
    state = telegram_service.search_state
    return templates.TemplateResponse(
        "results.html",
        {
            "request": request,
            "state": state,
            "browse": state.mode == "browse",
            "browse_username": state.browse_username,
            "browse_title": state.browse_title,
            "download_dir": str(DOWNLOAD_DIR.resolve()),
            "variants": generate_variants(state.query)
            if state.query and state.mode != "browse"
            else [],
        },
    )


@app.get("/channel/{username}", response_class=HTMLResponse)
async def browse_channel(request: Request, username: str):
    if not await telegram_service.is_authorized():
        return _auth_redirect()
    name = username.lstrip("@")
    channel = telegram_service.get_channel(name)
    if channel is None or not channel.valid:
        return RedirectResponse("/manage?error=Unknown%20channel", status_code=303)

    async def _start_browse():
        state = telegram_service.search_state
        same = (
            state.running
            and state.mode == "browse"
            and state.browse_username.casefold() == name.casefold()
        )
        if same:
            return
        if state.running:
            telegram_service.request_stop_search()
            for _ in range(60):
                if not telegram_service.search_state.running:
                    break
                await asyncio.sleep(0.2)
        await telegram_service.run_browse_channel(name)

    asyncio.create_task(_start_browse())

    return templates.TemplateResponse(
        "results.html",
        {
            "request": request,
            "state": telegram_service.search_state,
            "browse": True,
            "browse_username": channel.username,
            "browse_title": channel.title or channel.username,
            "download_dir": str(DOWNLOAD_DIR.resolve()),
            "variants": [],
        },
    )


@app.get("/api/download/config")
async def download_config():
    return {
        "desktop_dir": str(DOWNLOAD_DIR.resolve()),
        "desktop_label": str(DOWNLOAD_DIR),
    }


@app.post("/api/download/clear")
async def download_clear_finished():
    """Remove finished jobs from the live dock (kept in /downloads history)."""
    if not await telegram_service.is_authorized():
        return JSONResponse({"ok": False, "error": "not_authorized"}, status_code=401)
    removed = telegram_service.clear_finished_downloads()
    return JSONResponse({"ok": True, "removed": removed, "jobs": telegram_service.list_download_jobs()})


@app.get("/api/download/queue")
async def download_queue_status():
    return {"jobs": telegram_service.list_download_jobs()}


@app.post("/api/download/check")
async def download_check(request: Request):
    """Check whether a PC-folder download would collide with an existing file/folder."""
    if not await telegram_service.is_authorized():
        return JSONResponse({"ok": False, "error": "not_authorized"}, status_code=401)
    payload = await request.json()
    channel = str(payload.get("channel") or "").lstrip("@")
    filename_hint = str(payload.get("filename") or "")
    try:
        message_id = int(payload.get("message_id"))
    except (TypeError, ValueError):
        return JSONResponse({"ok": False, "error": "Invalid message id"}, status_code=400)
    if not channel or message_id <= 0:
        return JSONResponse({"ok": False, "error": "Missing channel/message"}, status_code=400)
    info = await telegram_service.check_desktop_conflict(
        channel, message_id, filename_hint=filename_hint
    )
    return JSONResponse({"ok": True, **info})


@app.post("/api/download/queue")
async def download_queue_add(request: Request):
    """Enqueue a desktop/local download and return immediately."""
    if not await telegram_service.is_authorized():
        return JSONResponse({"ok": False, "error": "not_authorized"}, status_code=401)
    payload = await request.json()
    channel = str(payload.get("channel") or "").lstrip("@")
    mode = str(payload.get("mode") or "desktop")
    filename_hint = str(payload.get("filename") or "")
    allow_duplicate = bool(payload.get("allow_duplicate"))
    try:
        message_id = int(payload.get("message_id"))
    except (TypeError, ValueError):
        return JSONResponse({"ok": False, "error": "Invalid message id"}, status_code=400)
    if not channel or message_id <= 0:
        return JSONResponse({"ok": False, "error": "Missing channel/message"}, status_code=400)
    job = telegram_service.enqueue_download(
        channel,
        message_id,
        mode=mode,
        filename_hint=filename_hint,
        allow_duplicate=allow_duplicate,
    )
    return JSONResponse({"ok": True, "job": job})


@app.post("/api/download/resolve-conflict")
async def download_resolve_conflict(request: Request):
    if not await telegram_service.is_authorized():
        return JSONResponse({"ok": False, "error": "not_authorized"}, status_code=401)
    payload = await request.json()
    job_id = str(payload.get("job_id") or "")
    proceed = bool(payload.get("proceed"))
    if not job_id:
        return JSONResponse({"ok": False, "error": "Missing job_id"}, status_code=400)
    job = telegram_service.resolve_download_conflict(job_id, proceed=proceed)
    if not job:
        return JSONResponse({"ok": False, "error": "Unknown job"}, status_code=404)
    return JSONResponse({"ok": True, "job": job})


@app.post("/api/download/cancel")
async def download_cancel(request: Request):
    if not await telegram_service.is_authorized():
        return JSONResponse({"ok": False, "error": "not_authorized"}, status_code=401)
    payload = await request.json()
    job_id = str(payload.get("job_id") or "")
    if not job_id:
        return JSONResponse({"ok": False, "error": "Missing job_id"}, status_code=400)
    job = telegram_service.cancel_download(job_id)
    if not job:
        return JSONResponse({"ok": False, "error": "Unknown job"}, status_code=404)
    return JSONResponse({"ok": True, "job": job})


@app.post("/api/download/desktop")
async def download_desktop(request: Request):
    """Legacy alias: queue to PC folder and return job immediately."""
    if not await telegram_service.is_authorized():
        return JSONResponse({"ok": False, "error": "not_authorized"}, status_code=401)
    payload = await request.json()
    channel = str(payload.get("channel") or "").lstrip("@")
    filename_hint = str(payload.get("filename") or "")
    try:
        message_id = int(payload.get("message_id"))
    except (TypeError, ValueError):
        return JSONResponse({"ok": False, "error": "Invalid message id"}, status_code=400)
    if not channel or message_id <= 0:
        return JSONResponse({"ok": False, "error": "Missing channel/message"}, status_code=400)
    job = telegram_service.enqueue_download(
        channel,
        message_id,
        mode="desktop",
        filename_hint=filename_hint,
    )
    return JSONResponse({"ok": True, "queued": True, "job": job})


@app.get("/download/job/{job_id}")
async def download_job_file(job_id: str):
    """Stream a finished local-queue job to the browser."""
    if not await telegram_service.is_authorized():
        return _auth_redirect()
    job = telegram_service.get_download_job(job_id)
    if not job:
        return JSONResponse({"ok": False, "error": "Unknown job"}, status_code=404)
    if job.get("status") != "done" or not job.get("path"):
        return JSONResponse(
            {"ok": False, "error": job.get("error") or "Not ready", "job": job},
            status_code=409,
        )
    path = Path(job["path"])
    if not path.exists():
        return JSONResponse({"ok": False, "error": "File missing on disk"}, status_code=404)
    return FileResponse(
        path=path,
        filename=job.get("filename") or path.name,
        media_type="application/octet-stream",
        content_disposition_type="attachment",
    )


@app.get("/download/{channel}/{message_id}")
async def download_local(channel: str, message_id: int):
    """Queue + wait, then stream (fallback for direct links)."""
    if not await telegram_service.is_authorized():
        return _auth_redirect()
    name = channel.lstrip("@")
    result = await telegram_service.download_telegram_file(
        name, message_id, to_desktop=False
    )
    if not result.get("ok"):
        return JSONResponse(result, status_code=400)
    path = Path(result["path"])
    return FileResponse(
        path=path,
        filename=result["filename"],
        media_type="application/octet-stream",
        content_disposition_type="attachment",
    )


@app.post("/channels/stop")
async def stop_channels_job():
    """Stop discover / refresh / deep crawl / enrich / join / leave."""
    if not _has_local_session() and not await telegram_service.is_authorized():
        return _auth_redirect()
    stopped = telegram_service.request_stop_discovery()
    return JSONResponse({"ok": True, "stopped": stopped})


@app.post("/channels/refresh")
async def refresh_channels():
    if not await telegram_service.is_authorized():
        return _auth_redirect()
    if not telegram_service.discovery_state.running:
        asyncio.create_task(
            telegram_service.resolve_channels(force=True, discover=False, deep_crawl=False)
        )
    return RedirectResponse("/", status_code=303)


@app.post("/channels/discover")
async def discover_channels():
    """Fast Telemetr catalog discovery (background)."""
    if not await telegram_service.is_authorized():
        return _auth_redirect()
    if not telegram_service.discovery_state.running:
        asyncio.create_task(
            telegram_service.resolve_channels(force=False, discover=True, deep_crawl=False)
        )
    return RedirectResponse("/", status_code=303)


@app.post("/channels/deep-crawl")
async def deep_crawl_channels():
    """Slow snowball in background."""
    if not await telegram_service.is_authorized():
        return _auth_redirect()
    if not telegram_service.discovery_state.running:
        asyncio.create_task(
            telegram_service.resolve_channels(force=False, discover=False, deep_crawl=True)
        )
    return RedirectResponse("/", status_code=303)


@app.get("/downloads", response_class=HTMLResponse)
async def downloads_history(request: Request):
    if not _has_local_session() and not await telegram_service.is_authorized():
        return _auth_redirect()
    return templates.TemplateResponse(
        request,
        "downloads.html",
        {
            "history": telegram_service.list_download_history(100),
            "download_dir": str(DOWNLOAD_DIR),
        },
    )


@app.post("/downloads/clear-history")
async def downloads_clear_history():
    if not _has_local_session() and not await telegram_service.is_authorized():
        return _auth_redirect()
    telegram_service.clear_download_history()
    return RedirectResponse("/downloads", status_code=303)


@app.get("/manage", response_class=HTMLResponse)
async def manage_channels(request: Request, saved: str | None = None, error: str | None = None):
    if not await telegram_service.is_authorized():
        return _auth_redirect()
    removed = telegram_service.remove_invalid_channels()
    all_channels = telegram_service.get_channels_fast()
    blacklist = sorted(
        [c for c in all_channels if c.banned and c.valid],
        key=lambda c: c.username.casefold(),
    )
    channels = sorted(
        [c for c in all_channels if not c.banned and c.valid],
        key=lambda c: (
            0 if c.favorite else 1,
            0 if c.source == "seed" else 1,
            -(c.members or 0),
            c.username.casefold(),
        ),
    )
    incomplete = sum(
        1
        for c in channels
        if (c.members or 0) <= 0 or not (c.description or "").strip()
    )
    message = None
    if saved == "1":
        message = "Selection saved."
    elif saved == "added":
        message = "Channel added."
    elif saved == "purged":
        message = f"Removed {removed} invalid leftover(s)." if removed else "No invalid leftovers found."
    elif saved == "enrich":
        message = "Fetching members & descriptions in the background — this page will refresh when done."
    ds = telegram_service.discovery_state
    enrich_running = ds.running and ds.mode == "enrich"
    joined = telegram_service.known_joined_usernames()
    unjoined_enabled = telegram_service.list_unjoined_channels(enabled_only=True)
    return templates.TemplateResponse(
        "manage.html",
        {
            "request": request,
            "channels": channels,
            "blacklist": blacklist,
            "enabled_count": sum(1 for c in channels if c.included),
            "favorite_count": sum(1 for c in channels if c.favorite),
            "incomplete_count": incomplete,
            "min_members": MIN_CHANNEL_MEMBERS,
            "joined_usernames": sorted(joined),
            "joined_lookup": {u: True for u in joined},
            "joined_status": telegram_service.joined_status_map(),
            "unjoined_enabled_count": len(unjoined_enabled),
            "joined_tracked_count": len(
                telegram_service.list_joined_for_leave(only_joined_by_app=True)
            ),
            "message": message
            or (ds.message if ds.mode == "enrich" and not ds.running else None),
            "error": error or (ds.error if ds.mode == "enrich" else None),
            "enrich_running": enrich_running,
            "enrich_progress": ds.progress if enrich_running else "",
            "job_running": enrich_running,
        },
    )


@app.get("/manage/export-unjoined")
async def manage_export_unjoined(
    enabled_only: str = "1",
    fmt: str = "txt",
):
    """Download usernames not yet in the app join/mute log (for offline joining)."""
    if not await telegram_service.is_authorized():
        return _auth_redirect()
    only_enabled = enabled_only not in {"0", "false", "no"}
    channels = telegram_service.list_unjoined_channels(enabled_only=only_enabled)
    fmt = (fmt or "txt").lower().strip()
    if fmt == "csv":
        lines = ["username,title,members,source,link"]
        for c in channels:
            title = (c.title or "").replace('"', "'")
            lines.append(
                f'{c.username},"{title}",{c.members or 0},{c.source},{c.link or f"https://t.me/{c.username}"}'
            )
        body = "\n".join(lines) + "\n"
        media = "text/csv; charset=utf-8"
        filename = "unjoined_channels.csv"
    elif fmt == "html":
        items: list[str] = []
        for c in channels:
            user = c.username
            link = c.link or f"https://t.me/{user}"
            web = f"https://web.telegram.org/k/#@{user}"
            title = (c.title or user).replace("<", "").replace(">", "")
            items.append(
                "<li>"
                f"<strong>{title}</strong> "
                f"<span>@{user}</span> "
                f'<a href="{link}" target="_blank" rel="noopener">Open in Telegram</a> · '
                f'<a href="{web}" target="_blank" rel="noopener">Web</a>'
                "</li>"
            )
        body = (
            "<!DOCTYPE html><html><head><meta charset='utf-8'/>"
            "<meta name='viewport' content='width=device-width, initial-scale=1'/>"
            "<title>Unjoined channels</title>"
            "<style>body{font:16px/1.45 system-ui,sans-serif;max-width:42rem;margin:1.5rem auto;padding:0 1rem}"
            "li{margin:0.65rem 0;padding:0.55rem 0;border-bottom:1px solid #ddd}"
            "a{margin-right:0.35rem}</style></head><body>"
            f"<h1>Join these channels ({len(channels)})</h1>"
            "<p>Open each link in the Telegram app or website and tap Join. "
            "When finished, run <strong>Join enabled (muted)</strong> in STL Search "
            "Settings to mute them and update the log.</p>"
            f"<ol>{''.join(items)}</ol></body></html>"
        )
        media = "text/html; charset=utf-8"
        filename = "unjoined_channels.html"
    elif fmt == "links":
        body = "\n".join(
            (c.link or f"https://t.me/{c.username}") for c in channels
        ) + ("\n" if channels else "")
        media = "text/plain; charset=utf-8"
        filename = "unjoined_channel_links.txt"
    else:
        body = "\n".join(c.username for c in channels) + ("\n" if channels else "")
        media = "text/plain; charset=utf-8"
        filename = "unjoined_channels.txt"
    from fastapi.responses import Response

    headers = {"X-Unjoined-Count": str(len(channels))}
    if fmt == "html":
        headers["Content-Disposition"] = 'inline; filename="unjoined_channels.html"'
    else:
        headers["Content-Disposition"] = f'attachment; filename="{filename}"'

    return Response(
        content=body,
        media_type=media,
        headers=headers,
    )


@app.post("/manage/enrich")
async def manage_enrich():
    if not _has_local_session() and not await telegram_service.is_authorized():
        return _auth_redirect()
    if telegram_service.discovery_state.running:
        from urllib.parse import quote

        return RedirectResponse(
            f"/manage?error={quote('Already fetching/discovering — wait for it to finish.')}",
            status_code=303,
        )
    asyncio.create_task(telegram_service.enrich_channel_details(only_incomplete=True))
    return RedirectResponse("/manage?saved=enrich", status_code=303)


@app.post("/manage/join-muted")
async def manage_join_muted(username: str = Form(...)):
    """Join a single channel muted. Local joined badge uses the join log (no list sync)."""
    if not await telegram_service.is_authorized():
        return JSONResponse({"ok": False, "error": "Not logged in", "status": "error"}, status_code=401)
    result = await telegram_service.join_channel_muted(username)
    status_code = 200 if result.get("ok") else (
        429 if result.get("status") == "rate_limit" else
        409 if result.get("status") == "busy" else
        400
    )
    return JSONResponse(result, status_code=status_code)


@app.get("/settings", response_class=HTMLResponse)
async def settings_page(
    request: Request,
    saved: str | None = None,
    error: str | None = None,
):
    if not await telegram_service.is_authorized():
        return _auth_redirect()

    import app.config as cfg

    ds = telegram_service.discovery_state
    join_running = ds.running and ds.mode == "join_mute"
    leave_running = ds.running and ds.mode == "leave"
    web_join_running = ds.running and ds.mode == "join_web"
    mute_sync_running = ds.running and ds.mode == "mute_sync"
    job_running = join_running or leave_running or web_join_running or mute_sync_running

    message = None
    if saved == "join_mute":
        message = "Muting / API-joining enabled channels — this page will refresh when done."
    elif saved == "leave":
        message = "Leaving app-joined channels — this page will refresh when done."
    elif saved == "join_web":
        message = (
            "Telegram Web joiner started — a Chromium window should open. "
            "Log in if asked, then click Continue on this page."
        )
    elif saved == "mute_sync":
        message = "Refreshing mute status from Telegram…"
    elif saved == "logout":
        message = "Logged out. Sign in again when ready."
    elif ds.message and ds.mode in {"join_mute", "leave", "join_web", "mute_sync"} and not ds.running:
        message = ds.message

    return templates.TemplateResponse(
        "settings.html",
        {
            "request": request,
            "message": message,
            "error": error
            or (ds.error if ds.mode in {"join_mute", "leave", "join_web", "mute_sync"} else None),
            "api_id": cfg.API_ID or "",
            "api_hash_masked": mask_secret(cfg.API_HASH),
            "form_api_id": "",
            "form_api_hash": "",
            "joined_tracked_count": len(
                telegram_service.list_joined_for_leave(only_joined_by_app=True)
            ),
            "unjoined_enabled_count": len(
                telegram_service.list_unjoined_channels(enabled_only=True)
            ),
            "joined_unmuted_count": telegram_service.count_joined_unmuted(),
            "job_running": job_running,
            "job_progress": ds.progress if job_running else "",
            "join_running": join_running,
            "leave_running": leave_running,
            "web_join_running": web_join_running,
            "web_join_phase": ds.phase if web_join_running else "",
            "mute_sync_running": mute_sync_running,
            "search_running": telegram_service.search_state.running,
        },
    )


@app.post("/settings/sync-mute")
async def settings_sync_mute():
    if not await telegram_service.is_authorized():
        return _auth_redirect()
    from urllib.parse import quote

    if telegram_service.discovery_state.running:
        return RedirectResponse(
            f"/settings?error={quote('Another job is already running.')}",
            status_code=303,
        )
    if telegram_service.search_state.running:
        return RedirectResponse(
            f"/settings?error={quote('Stop search first.')}",
            status_code=303,
        )
    asyncio.create_task(telegram_service.refresh_mute_status_from_telegram())
    return RedirectResponse("/settings?saved=mute_sync", status_code=303)


@app.post("/settings/join-web/start")
async def settings_join_web_start():
    if not await telegram_service.is_authorized():
        return _auth_redirect()
    from urllib.parse import quote

    result = telegram_service.start_join_via_telegram_web(delay_seconds=3.0)
    if not result.get("ok"):
        return RedirectResponse(
            f"/settings?error={quote(result.get('error') or 'Could not start')}",
            status_code=303,
        )
    return RedirectResponse("/settings?saved=join_web", status_code=303)


@app.post("/settings/join-web/continue")
async def settings_join_web_continue():
    if not await telegram_service.is_authorized():
        return _auth_redirect()
    from urllib.parse import quote

    if not telegram_service.confirm_web_join_login():
        return RedirectResponse(
            f"/settings?error={quote('No Telegram Web join waiting for Continue.')}",
            status_code=303,
        )
    return RedirectResponse("/settings?saved=join_web", status_code=303)


@app.post("/settings/join-muted")
async def settings_join_muted():
    if not _has_local_session() and not await telegram_service.is_authorized():
        return _auth_redirect()
    from urllib.parse import quote

    if telegram_service.discovery_state.running:
        return RedirectResponse(
            f"/settings?error={quote('Already busy — wait for the current job to finish.')}",
            status_code=303,
        )
    if telegram_service.search_state.running:
        return RedirectResponse(
            f"/settings?error={quote('Search is running — stop it before joining channels.')}",
            status_code=303,
        )
    asyncio.create_task(telegram_service.join_enabled_channels_muted())
    return RedirectResponse("/settings?saved=join_mute", status_code=303)


@app.post("/settings/leave-joined")
async def settings_leave_joined(confirm: str = Form("")):
    if not _has_local_session() and not await telegram_service.is_authorized():
        return _auth_redirect()
    from urllib.parse import quote

    if (confirm or "").strip() != "LEAVE":
        return RedirectResponse(
            f"/settings?error={quote('Type LEAVE (all caps) in the box to confirm leaving.')}",
            status_code=303,
        )
    if telegram_service.discovery_state.running:
        return RedirectResponse(
            f"/settings?error={quote('Already busy — wait for the current job to finish.')}",
            status_code=303,
        )
    if telegram_service.search_state.running:
        return RedirectResponse(
            f"/settings?error={quote('Search is running — stop it before leaving channels.')}",
            status_code=303,
        )
    asyncio.create_task(telegram_service.leave_joined_channels_job(only_joined_by_app=True))
    return RedirectResponse("/settings?saved=leave", status_code=303)


@app.post("/settings/api-keys")
async def settings_api_keys(
    request: Request,
    api_id: str = Form(...),
    api_hash: str = Form(...),
    channel_data: str = Form("transfer"),
):
    from urllib.parse import quote

    import app.config as cfg

    try:
        new_id = int(str(api_id).strip())
    except ValueError:
        return RedirectResponse(
            f"/settings?error={quote('API ID must be a number.')}",
            status_code=303,
        )
    new_hash = (api_hash or "").strip()
    transfer = (channel_data or "transfer").strip().lower() != "fresh"

    result = await telegram_service.switch_telegram_api(
        new_id,
        new_hash,
        transfer_channel_data=transfer,
    )
    if not result.get("ok"):
        return templates.TemplateResponse(
            "settings.html",
            {
                "request": request,
                "message": None,
                "error": result.get("error") or "Could not update API keys.",
                "api_id": cfg.API_ID or "",
                "api_hash_masked": mask_secret(cfg.API_HASH),
                "form_api_id": str(api_id),
                "form_api_hash": api_hash,
                "joined_tracked_count": len(
                    telegram_service.list_joined_for_leave(only_joined_by_app=True)
                ),
                "job_running": False,
                "job_progress": "",
                "join_running": False,
                "leave_running": False,
                "search_running": False,
            },
            status_code=400,
        )

    # Force re-auth with the new credentials
    return RedirectResponse("/login", status_code=303)


@app.post("/settings/logout")
async def settings_logout():
    await telegram_service.logout_telegram()
    return RedirectResponse("/login", status_code=303)


@app.post("/manage/ban")
async def manage_ban(request: Request, username: str = Form(...)):
    if not _has_local_session() and not await telegram_service.is_authorized():
        return _auth_redirect()
    info = telegram_service.ban_channel(username)
    if info is None:
        from urllib.parse import quote

        if "application/json" in (request.headers.get("accept") or ""):
            return JSONResponse({"ok": False, "error": "Could not ban channel"}, status_code=400)
        return RedirectResponse(
            f"/manage?error={quote('Could not ban channel')}",
            status_code=303,
        )
    if "application/json" in (request.headers.get("accept") or ""):
        channels = [c for c in telegram_service.get_channels_fast() if c.valid and not c.banned]
        return JSONResponse(
            {
                "ok": True,
                "username": info.username,
                "title": info.title,
                "members": info.members,
                "source": info.source,
                "description": info.description or "",
                "link": info.link or f"https://t.me/{info.username}",
                "enabled_count": sum(1 for c in channels if c.included),
                "favorite_count": sum(1 for c in channels if c.favorite),
                "total": len(channels),
            }
        )
    return RedirectResponse("/manage", status_code=303)


@app.post("/manage/unban")
async def manage_unban(request: Request, username: str = Form(...)):
    if not _has_local_session() and not await telegram_service.is_authorized():
        return _auth_redirect()
    info = telegram_service.unban_channel(username)
    if info is None:
        from urllib.parse import quote

        if "application/json" in (request.headers.get("accept") or ""):
            return JSONResponse({"ok": False, "error": "Could not unban channel"}, status_code=400)
        return RedirectResponse(
            f"/manage?error={quote('Could not unban channel')}",
            status_code=303,
        )
    if "application/json" in (request.headers.get("accept") or ""):
        return JSONResponse(
            {
                "ok": True,
                "username": info.username,
                "title": info.title,
                "members": info.members,
                "source": info.source,
                "description": info.description or "",
                "link": info.link or f"https://t.me/{info.username}",
                "included": info.included,
                "favorite": info.favorite,
            }
        )
    return RedirectResponse("/manage", status_code=303)


@app.post("/manage/save")
async def manage_save(request: Request):
    if not _has_local_session() and not await telegram_service.is_authorized():
        return _auth_redirect()
    form = await request.form()
    enabled = set()
    for value in form.getlist("enabled"):
        enabled.add(str(value).lstrip("@"))
    favorites = set()
    for value in form.getlist("favorite"):
        favorites.add(str(value).lstrip("@"))
    telegram_service.set_many_enabled(enabled)
    telegram_service.set_many_favorites(favorites)
    # Auto-save from manage page uses fetch(); prefer JSON so the page does not reload.
    wants_json = "application/json" in (request.headers.get("accept") or "")
    if wants_json or form.get("ajax") == "1":
        channels = [c for c in telegram_service.get_channels_fast() if c.valid and not c.banned]
        return JSONResponse(
            {
                "ok": True,
                "enabled_count": sum(1 for c in channels if c.included),
                "favorite_count": sum(1 for c in channels if c.favorite),
                "total": len(channels),
            }
        )
    return RedirectResponse("/manage", status_code=303)


@app.post("/api/favorite")
async def api_favorite(request: Request):
    """Toggle favorite from the home channel list (JSON)."""
    if not _has_local_session() and not await telegram_service.is_authorized():
        return JSONResponse({"ok": False, "error": "not_authorized"}, status_code=401)
    payload = await request.json()
    username = str(payload.get("username") or "").lstrip("@")
    if not username:
        return JSONResponse({"ok": False, "error": "Missing username"}, status_code=400)
    if "favorite" in payload:
        want = bool(payload.get("favorite"))
    else:
        current = telegram_service.get_channel(username)
        want = not (current.favorite if current else False)
    info = telegram_service.set_channel_favorite(username, want)
    if not info:
        return JSONResponse({"ok": False, "error": "Unknown channel"}, status_code=404)
    return JSONResponse(
        {
            "ok": True,
            "username": info.username,
            "favorite": bool(info.favorite),
            "title": info.title,
        }
    )


@app.post("/manage/favorite")
async def manage_favorite(username: str = Form(...), favorite: str = Form("1")):
    if not _has_local_session() and not await telegram_service.is_authorized():
        return _auth_redirect()
    want = favorite not in ("0", "false", "False", "")
    telegram_service.set_channel_favorite(username, want)
    return RedirectResponse("/#favorites", status_code=303)


@app.post("/manage/add")
async def manage_add(username: str = Form(...)):
    if not _has_local_session() and not await telegram_service.is_authorized():
        return _auth_redirect()
    info = await telegram_service.add_channel_manual(username)
    if not info.valid:
        from urllib.parse import quote

        return RedirectResponse(
            f"/manage?error={quote(info.reason or 'Could not add channel')}",
            status_code=303,
        )
    return RedirectResponse("/manage?saved=added", status_code=303)


@app.post("/manage/purge")
async def manage_purge():
    if not _has_local_session() and not await telegram_service.is_authorized():
        return _auth_redirect()
    telegram_service.remove_invalid_channels()
    return RedirectResponse("/manage?saved=purged", status_code=303)
