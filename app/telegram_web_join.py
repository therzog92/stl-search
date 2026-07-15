"""Join channels via Telegram Web (Playwright) — controlled from the STL Search UI."""

from __future__ import annotations

import json
import logging
import re
import threading
import time
from dataclasses import dataclass, field
from typing import Callable

from app.config import ROOT

logger = logging.getLogger("stl.web_join")

PROFILE_DIR = ROOT / "data" / "telegram_web_profile"
PROGRESS_FILE = ROOT / "data" / "join_web_progress.json"
DEBUG_DIR = ROOT / "data" / "web_join_debug"


@dataclass
class WebJoinController:
    """Thread-safe signals between FastAPI and the Playwright worker."""

    stop: threading.Event = field(default_factory=threading.Event)
    login_ready: threading.Event = field(default_factory=threading.Event)

    def reset(self) -> None:
        self.stop.clear()
        self.login_ready.clear()


def load_progress() -> dict:
    if not PROGRESS_FILE.exists():
        return {"done": [], "failed": [], "skipped": []}
    try:
        data = json.loads(PROGRESS_FILE.read_text(encoding="utf-8"))
        for key in ("done", "failed", "skipped"):
            data.setdefault(key, [])
        return data
    except Exception:
        return {"done": [], "failed": [], "skipped": []}


def save_progress(data: dict) -> None:
    PROGRESS_FILE.parent.mkdir(parents=True, exist_ok=True)
    PROGRESS_FILE.write_text(json.dumps(data, indent=2), encoding="utf-8")


def _find_subscribe_button(page):
    """Return a locator for the bottom SUBSCRIBE / JOIN bar, or None."""
    selectors = [
        "button.chat-input-control-button",
        "button.chat-input-plate-button",
        "button.btn-primary.btn-color-primary",
        "button.btn-primary",
        "button.btn-color-primary",
        "button.chat-input-control-button .i18n",
        "button:has(span.i18n)",
        "button.Button.primary.fluid",
        "button.Button.primary",
        "button:has-text('SUBSCRIBE')",
        "button:has-text('Subscribe')",
        "button:has-text('JOIN')",
        "button:has-text('Join')",
    ]
    for sel in selectors:
        try:
            loc = page.locator(sel)
            n = loc.count()
            for i in range(min(n, 8)):
                el = loc.nth(i)
                try:
                    tag = el.evaluate("e => e.tagName")
                    target = el
                    if tag == "SPAN":
                        parent = el.locator("xpath=ancestor::button[1]")
                        if parent.count():
                            target = parent.first
                    if not target.is_visible(timeout=400):
                        continue
                    text = (target.inner_text(timeout=800) or "").strip().upper()
                    text = re.sub(r"\s+", " ", text)
                    if text in {
                        "SUBSCRIBE",
                        "JOIN",
                        "JOIN CHANNEL",
                        "JOIN GROUP",
                        "SUBSCRIBE CHANNEL",
                    } or text.startswith("SUBSCRIBE") or text.startswith("JOIN"):
                        return target
                except Exception:
                    continue
        except Exception:
            continue
    return None


def _click_subscribe(page) -> bool:
    """Click SUBSCRIBE / JOIN — stay on channel; no scroll that can close/shift chat."""
    plate_selectors = [
        "button.chat-input-control-button:has-text('SUBSCRIBE')",
        "button.chat-input-plate-button:has-text('SUBSCRIBE')",
        "button.btn-primary.chat-input-control-button",
        "button.chat-input-control-button",
        "button.chat-input-plate-button",
        "button.btn-primary.btn-color-primary:has-text('SUBSCRIBE')",
        "button.btn-primary:has-text('SUBSCRIBE')",
        "button.btn-primary:has-text('JOIN')",
    ]
    for sel in plate_selectors:
        try:
            loc = page.locator(sel)
            n = min(loc.count(), 4)
            for i in range(n):
                el = loc.nth(i)
                try:
                    if not el.is_visible(timeout=400):
                        continue
                    text = re.sub(r"\s+", " ", (el.inner_text(timeout=600) or "").strip()).upper()
                    if text and not (
                        text.startswith("SUBSCRIBE")
                        or text.startswith("JOIN")
                        or text in {"SUBSCRIBE", "JOIN", "JOIN CHANNEL", "JOIN GROUP"}
                    ):
                        continue
                    try:
                        el.click(timeout=4000, no_wait_after=True)
                    except Exception:
                        el.click(timeout=4000, force=True, no_wait_after=True)
                    page.wait_for_timeout(900)
                    return True
                except Exception:
                    continue
        except Exception:
            continue

    btn = _find_subscribe_button(page)
    if btn is not None:
        try:
            box = btn.bounding_box()
            if box:
                page.mouse.click(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2)
                page.wait_for_timeout(900)
                return True
        except Exception as exc:
            logger.warning("mouse subscribe click failed: %s", exc)

    try:
        clicked = page.evaluate(
            """() => {
              const buttons = Array.from(document.querySelectorAll(
                'button.chat-input-control-button, button.chat-input-plate-button, button.btn-primary'
              ));
              for (const el of buttons) {
                const t = (el.innerText || '').replace(/\\s+/g, ' ').trim();
                if (!/^(SUBSCRIBE|JOIN)\\b/i.test(t)) continue;
                const r = el.getBoundingClientRect();
                if (r.width < 20 || r.height < 10) continue;
                el.click();
                return true;
              }
              return false;
            }"""
        )
        if clicked:
            page.wait_for_timeout(900)
            return True
    except Exception as exc:
        logger.warning("JS subscribe click failed: %s", exc)
    return False


def _subscribe_visible(page) -> bool:
    """True only for the big SUBSCRIBE / JOIN plate button (not 'You joined' text)."""
    try:
        return page.evaluate(
            """() => {
              const buttons = document.querySelectorAll(
                'button.chat-input-control-button, button.chat-input-plate-button, ' +
                'button.btn-primary.btn-color-primary, button.btn-primary'
              );
              for (const el of buttons) {
                const t = (el.innerText || '').replace(/\\s+/g, ' ').trim();
                // Exact join CTAs only — ignore other primary buttons
                if (!/^(SUBSCRIBE|JOIN)(\\s|$)/i.test(t) &&
                    !/^(JOIN\\s+CHANNEL|JOIN\\s+GROUP)$/i.test(t)) continue;
                const r = el.getBoundingClientRect();
                if (r.width >= 40 && r.height >= 20) return true;
              }
              return false;
            }"""
        )
    except Exception:
        return _find_subscribe_button(page) is not None


def _joined_service_visible(page) -> bool:
    """
    Joined channels show a service line at the bottom / in history like:
    "You joined this channel"
    """
    try:
        return bool(
            page.evaluate(
                """() => {
                  const re = /you joined (this |the )?channel/i;
                  const center = document.querySelector('#column-center') || document.body;
                  // Service messages + sticky bottom bars
                  const nodes = center.querySelectorAll(
                    '.service-msg, .bubble-service, .bubble.service, .chat-input, ' +
                    '.chat-input-container, .chat-input-main, .bubbles, .bubbles-inner'
                  );
                  for (const el of nodes) {
                    const t = (el.innerText || '').replace(/\\s+/g, ' ').trim();
                    if (re.test(t)) return true;
                  }
                  // Broader scan of center column text chunks
                  const chunks = Array.from(center.querySelectorAll('div, span'))
                    .slice(0, 400);
                  for (const el of chunks) {
                    const t = (el.innerText || '').replace(/\\s+/g, ' ').trim();
                    if (t.length > 80) continue;
                    if (re.test(t)) return true;
                  }
                  return false;
                }"""
            )
        )
    except Exception:
        return False


def _composer_visible(page) -> bool:
    """Joined chats usually get a normal message composer (not the SUBSCRIBE plate)."""
    if _subscribe_visible(page):
        return False
    try:
        return bool(
            page.evaluate(
                """() => {
                  const center = document.querySelector('#column-center');
                  if (!center) return false;
                  const input = center.querySelector(
                    '.input-message-input, .chat-input .input-field-input, ' +
                    '[contenteditable="true"].input-message-input, ' +
                    'div.input-message-input, textarea.chat-input'
                  );
                  if (!input) return false;
                  const r = input.getBoundingClientRect();
                  return r.width > 40 && r.height > 10;
                }"""
            )
        )
    except Exception:
        return False


def _confirmed_member(page) -> bool:
    """
    Membership signals on Web K (Mute alone is unreliable):
      - "You joined this channel" service line
      - normal message composer (can type)
      - Mute / Unmute in the chat header
    SUBSCRIBE plate still present → not treated as member.
    """
    if _subscribe_visible(page):
        return False
    if _joined_service_visible(page):
        return True
    if _composer_visible(page):
        return True
    for sel in ("button:has-text('Mute')", "button:has-text('Unmute')"):
        try:
            loc = page.locator(sel)
            if loc.count() and loc.first.is_visible(timeout=400):
                return True
        except Exception:
            continue
    return False


def _join_succeeded(page) -> bool:
    """After clicking SUBSCRIBE — success if joined chrome appears OR plate is gone."""
    if _joined_service_visible(page) or _composer_visible(page):
        return True
    if not _subscribe_visible(page):
        return True
    return _confirmed_member(page)


def _chat_title(page) -> str:
    """Visible chat title only — never use hash (hash flips without changing the chat)."""
    try:
        return (
            page.evaluate(
                """() => {
                  const titleEl = document.querySelector(
                    '#column-center .peer-title, #column-center .person-title, ' +
                    '.chat-info .peer-title, .chat .peer-title, .top .peer-title, ' +
                    '#column-center .chat-info'
                  );
                  return titleEl ? (titleEl.innerText || '').trim().split('\\n')[0].trim() : '';
                }"""
            )
            or ""
        )
    except Exception:
        return ""


def _page_shows_peer(page, username: str) -> bool:
    """Strong match: URL/header mentions this @username."""
    user = username.lstrip("@").casefold()
    try:
        return bool(
            page.evaluate(
                """(u) => {
                  const hash = (location.hash || '').toLowerCase();
                  if (hash.includes('@' + u)) return true;
                  if (hash.includes('domain=' + u) || hash.includes('domain%3d' + u)) return true;

                  const center = document.querySelector('#column-center') || document.body;
                  const texts = Array.from(center.querySelectorAll(
                    '.peer-title, .person-title, .peer-subtitle, .person-subtitle, ' +
                    '.chat-info, .top .info, a[href*="t.me/"]'
                  )).map(el => (
                    (el.innerText || '') + ' ' + (el.getAttribute('href') || '')
                  ).toLowerCase());
                  for (const t of texts) {
                    if (t.includes('@' + u) || t.includes('t.me/' + u)) return true;
                  }
                  return false;
                }""",
                user,
            )
        )
    except Exception:
        return False


def _dismiss_popups_only(page) -> None:
    """Close popups/banners — never Escape (Escape leaves the open chat in Web K)."""
    for sel in (
        ".popup-close",
        ".btn-icon.popup-close",
        "[aria-label='Close']",
        ".notification-close",
        "button.btn-primary:has-text('OK')",
        "button:has-text('Got it')",
        "button:has-text('Not now')",
    ):
        try:
            loc = page.locator(sel)
            if loc.count() and loc.first.is_visible(timeout=150):
                loc.first.click(timeout=800)
                page.wait_for_timeout(200)
        except Exception:
            pass


def _wait_on_target(page, username: str, prev_title: str, *, timeout_ms: int = 20000) -> bool:
    """
    Success only if:
      - header/hash mentions @username, OR
      - the visible chat TITLE actually changed from the previous channel
    Hash-only changes do NOT count (that was the false-progress bug).
    """
    user = username.lstrip("@")
    deadline = time.time() + timeout_ms / 1000.0
    while time.time() < deadline:
        if _page_shows_peer(page, user):
            logger.info("peer match @%s title=%r", user, _chat_title(page))
            return True
        title = _chat_title(page)
        if title and prev_title and title != prev_title:
            logger.info("title switched %r → %r (want @%s)", prev_title, title, user)
            return True
        if title and not prev_title and (_subscribe_visible(page) or _confirmed_member(page)):
            # First open from empty home
            return True
        page.wait_for_timeout(300)
    title = _chat_title(page)
    ok = _page_shows_peer(page, user) or (bool(title) and bool(prev_title) and title != prev_title)
    logger.info(
        "wait_on_target @%s ok=%s prev=%r now=%r hash=%s",
        user,
        ok,
        prev_title,
        title,
        (page.url or "")[-80:],
    )
    return ok


def _ensure_web_k(page) -> None:
    try:
        cur = page.url or ""
        if "web.telegram.org/k" not in cur:
            page.goto(
                "https://web.telegram.org/k/",
                wait_until="domcontentloaded",
                timeout=90000,
            )
            page.wait_for_timeout(1500)
    except Exception as exc:
        logger.warning("ensure web k failed: %s", exc)


def _try_app_im_open(page, username: str) -> bool:
    """Use Web K internals when available (more reliable than hash alone)."""
    user = username.lstrip("@")
    try:
        result = page.evaluate(
            """async (u) => {
              const im = window.appImManager;
              if (!im) return {ok: false, reason: 'no_appImManager'};
              try {
                if (typeof im.openUrl === 'function') {
                  await im.openUrl('https://t.me/' + u);
                  return {ok: true, via: 'openUrl'};
                }
                if (typeof im.openUsername === 'function') {
                  await im.openUsername({username: u});
                  return {ok: true, via: 'openUsername'};
                }
                if (typeof im.setPeer === 'function' && typeof im.getPeerId === 'function') {
                  // some builds expose helpers differently
                }
              } catch (e) {
                return {ok: false, reason: String(e)};
              }
              // Fallback: assign hash and poke hash handler
              try {
                const next = '#@' + u;
                if (location.hash === next) location.hash = '#';
                location.hash = next;
                if (typeof im.onHashChange === 'function') {
                  await im.onHashChange();
                }
                return {ok: true, via: 'hash+onHashChange'};
              } catch (e2) {
                return {ok: false, reason: String(e2)};
              }
            }""",
            user,
        )
        logger.info("appIm open @%s → %s", user, result)
        return bool(result and result.get("ok"))
    except Exception as exc:
        logger.warning("appIm open failed @%s: %s", user, exc)
        return False


def _open_via_search(page, username: str) -> bool:
    """Search left pane for @user and click the matching row. Returns whether a result was clicked."""
    user = username.lstrip("@")
    _dismiss_popups_only(page)

    # Focus search (icon then input)
    for sel in (
        "#telegram-search-input",
        ".input-search input",
        "input.input-field-input",
        ".sidebar-header input",
        ".sidebar-header .input-search",
        ".input-search",
        "input[type='text']",
    ):
        try:
            loc = page.locator(sel).first
            if not loc.count():
                continue
            try:
                if not loc.is_visible(timeout=500):
                    continue
            except Exception:
                continue
            loc.click(timeout=2000)
            try:
                loc.fill("")
            except Exception:
                pass
            page.keyboard.press("Control+A")
            page.keyboard.press("Backspace")
            # Prefer fill for speed then verify
            try:
                loc.fill(f"@{user}")
            except Exception:
                loc.type(f"@{user}", delay=30)
            page.wait_for_timeout(2000)

            # Prefer exact @username row (global search shows username)
            result_selectors = [
                f".search-group .row:has-text('@{user}')",
                f".search-super-content .row:has-text('@{user}')",
                f"#search-container .row:has-text('@{user}')",
                f".row:has-text('@{user}')",
                f"a:has-text('@{user}')",
                f"text=@{user}",
            ]
            for result_sel in result_selectors:
                try:
                    rows = page.locator(result_sel)
                    n = min(rows.count(), 6)
                    for i in range(n):
                        r = rows.nth(i)
                        if not r.is_visible(timeout=400):
                            continue
                        txt = (r.inner_text(timeout=500) or "").casefold()
                        if f"@{user.casefold()}" not in txt and user.casefold() not in txt:
                            continue
                        r.click(timeout=4000)
                        page.wait_for_timeout(1500)
                        logger.info("search clicked @%s via %s", user, result_sel)
                        return True
                except Exception:
                    continue

            # Enter can open top hit — only if top hit text mentions username
            try:
                page.keyboard.press("Enter")
                page.wait_for_timeout(1500)
                if _page_shows_peer(page, user):
                    return True
            except Exception:
                pass
            return False
        except Exception:
            continue
    return False


def _open_channel(page, username: str, prev_title: str) -> bool:
    """
    Open THIS channel. Returns True only when we're actually on a different chat
    (or already on this @username).
    """
    user = username.lstrip("@")
    _ensure_web_k(page)
    _dismiss_popups_only(page)

    # Already on the requested peer
    if _page_shows_peer(page, user):
        logger.info("already on peer @%s", user)
        return True

    prev = prev_title or _chat_title(page)
    logger.info("opening @%s (prev_title=%r)", user, prev)

    # 1) Left-pane search — most reliable visible navigation
    if _open_via_search(page, user) and _wait_on_target(page, user, prev, timeout_ms=12000):
        _dismiss_popups_only(page)
        return True

    # 2) Web K openUrl / hash handler
    _try_app_im_open(page, user)
    if _wait_on_target(page, user, prev, timeout_ms=10000):
        _dismiss_popups_only(page)
        return True

    # 3) location.assign deep links (full navigation, still verify TITLE change)
    for url in (
        f"https://web.telegram.org/k/#?tgaddr=tg://resolve?domain={user}",
        f"https://web.telegram.org/k/#@{user}",
    ):
        try:
            page.evaluate("(u) => { location.assign(u); }", url)
            page.wait_for_timeout(1000)
            if _wait_on_target(page, user, prev, timeout_ms=10000):
                _dismiss_popups_only(page)
                return True
        except Exception as exc:
            logger.warning("assign %s failed: %s", url, exc)

    # One more search attempt after deep link failure
    if _open_via_search(page, user) and _wait_on_target(page, user, prev, timeout_ms=12000):
        _dismiss_popups_only(page)
        return True

    logger.warning(
        "FAILED to leave chat for @%s — still title=%r",
        user,
        _chat_title(page),
    )
    return False


def _save_debug(page, username: str, reason: str) -> None:
    try:
        DEBUG_DIR.mkdir(parents=True, exist_ok=True)
        stamp = int(time.time())
        shot = DEBUG_DIR / f"{username}_{reason}_{stamp}.png"
        page.screenshot(path=str(shot), full_page=True)
        (DEBUG_DIR / f"{username}_{reason}_{stamp}.html").write_text(
            page.content(), encoding="utf-8", errors="replace"
        )
        meta = {
            "username": username,
            "reason": reason,
            "url": page.url,
            "title": _chat_title(page),
            "subscribe": _subscribe_visible(page),
            "member": _confirmed_member(page),
            "joined_service": _joined_service_visible(page),
            "composer": _composer_visible(page),
            "peer": _page_shows_peer(page, username),
        }
        (DEBUG_DIR / f"{username}_{reason}_{stamp}.json").write_text(
            json.dumps(meta, indent=2), encoding="utf-8"
        )
        logger.info("web-join debug @%s → %s (%s)", username, shot, meta)
    except Exception as exc:
        logger.warning("debug save failed @%s: %s", username, exc)


def try_join_one(page, username: str, prev_title: str = "") -> str:
    """Returns: joined | already | skipped | failed"""
    user = username.lstrip("@")
    try:
        opened = _open_channel(page, user, prev_title)
        if not opened:
            _save_debug(page, user, "wrong_peer")
            return "skipped"

        for attempt in range(16):
            if _subscribe_visible(page):
                if _click_subscribe(page):
                    page.wait_for_timeout(2000)
                    if _join_succeeded(page):
                        return "joined"
                    if attempt < 15:
                        page.wait_for_timeout(400)
                        continue
                    # Click happened; treat as joined if plate eventually gone / joined text
                    if _join_succeeded(page):
                        return "joined"
            elif _confirmed_member(page) or _joined_service_visible(page):
                return "already"
            page.wait_for_timeout(500)

        _save_debug(page, user, "no_subscribe")
        return "skipped"
    except Exception as exc:
        logger.warning("web join failed @%s: %s", user, exc)
        try:
            _save_debug(page, user, "exception")
        except Exception:
            pass
        return "failed"


def run_telegram_web_join(
    usernames: list[str],
    *,
    delay_seconds: float = 3.0,
    controller: WebJoinController,
    on_phase: Callable[[str, str], None],
    on_counts: Callable[[int, int, int, int], None] | None = None,
    on_joined: Callable[[str, str], None] | None = None,
) -> dict:
    """
    Blocking worker. on_phase(phase, progress_message).
    on_joined(username, result) called for "joined" / "already" so the app can
    persist to the join log and skip those next run.
    Incoming usernames should already be the app's unjoined list.
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        on_phase("error", "Playwright not installed. Restart STL Search after install.")
        raise RuntimeError(
            "Playwright missing. Run: "
            ".\\.venv\\Scripts\\pip.exe install playwright && "
            ".\\.venv\\Scripts\\playwright.exe install chromium"
        ) from exc

    progress = load_progress()
    # Only process channels the app still needs; drop stale progress for them
    still_need = {u.lstrip("@").casefold() for u in usernames if u}
    progress["done"] = [
        d for d in progress.get("done", []) if d.casefold() not in still_need
    ]
    progress["skipped"] = [
        s for s in progress.get("skipped", []) if s.casefold() not in still_need
    ]
    progress["failed"] = [
        f for f in progress.get("failed", []) if f.casefold() not in still_need
    ]
    save_progress(progress)

    pending = [u.lstrip("@") for u in usernames if u]
    total = len(pending)
    joined = 0
    skipped = 0
    failed = 0
    already = 0

    def emit_counts() -> None:
        if on_counts:
            on_counts(joined + already, skipped, failed, total)

    if not pending:
        on_phase("done", "Nothing left to join — list empty.")
        emit_counts()
        return progress

    PROFILE_DIR.mkdir(parents=True, exist_ok=True)
    on_phase(
        "await_login",
        f"Browser opening… log into Telegram Web if asked, then Continue "
        f"({total} channel(s) queued).",
    )

    with sync_playwright() as p:
        try:
            context = p.chromium.launch_persistent_context(
                user_data_dir=str(PROFILE_DIR),
                headless=False,
                viewport={"width": 1400, "height": 900},
                args=["--disable-blink-features=AutomationControlled"],
            )
        except Exception as exc:
            on_phase("error", f"Could not start Chromium: {exc}")
            raise

        page = context.pages[0] if context.pages else context.new_page()
        try:
            page.goto(
                "https://web.telegram.org/k/",
                wait_until="domcontentloaded",
                timeout=90000,
            )
        except Exception as exc:
            logger.warning("Initial Telegram Web load issue: %s", exc)

        on_phase(
            "await_login",
            f"Log into Telegram Web, then click Continue ({total} to join).",
        )

        while not controller.login_ready.wait(timeout=0.5):
            if controller.stop.is_set():
                try:
                    context.close()
                except Exception:
                    pass
                on_phase("stopped", "Stopped before joining.")
                emit_counts()
                return progress

        if controller.stop.is_set():
            try:
                context.close()
            except Exception:
                pass
            on_phase("stopped", "Stopped before joining.")
            return progress

        on_phase("joining", f"Joining 0/{total}…")

        prev_title = _chat_title(page)
        for idx, username in enumerate(pending, start=1):
            if controller.stop.is_set():
                on_phase("stopped", f"Stopped after {joined + already} success(es).")
                break

            on_phase("joining", f"@{username} ({idx}/{total}) — opening & SUBSCRIBE…")
            result = try_join_one(page, username, prev_title=prev_title)
            prev_title = _chat_title(page) or prev_title

            if result == "joined":
                joined += 1
                progress.setdefault("done", []).append(username)
                if on_joined:
                    try:
                        on_joined(username, "joined")
                    except Exception:
                        logger.exception("on_joined failed @%s", username)
                on_phase("joining", f"Subscribed @{username} ({idx}/{total})")
            elif result == "already":
                already += 1
                progress.setdefault("done", []).append(username)
                if on_joined:
                    try:
                        on_joined(username, "already")
                    except Exception:
                        logger.exception("on_joined failed @%s", username)
                on_phase("joining", f"Already in @{username} ({idx}/{total})")
            elif result == "skipped":
                skipped += 1
                progress.setdefault("skipped", []).append(username)
                on_phase(
                    "joining",
                    f"Could not open @{username} ({idx}/{total}) — will retry next run",
                )
            else:
                failed += 1
                progress.setdefault("failed", []).append(username)
                on_phase("joining", f"Failed @{username} ({idx}/{total})")

            save_progress(progress)
            emit_counts()

            if idx < total and not controller.stop.is_set():
                end = time.time() + max(1.0, delay_seconds)
                while time.time() < end:
                    if controller.stop.is_set():
                        break
                    time.sleep(0.25)

        try:
            context.close()
        except Exception:
            pass

    if controller.stop.is_set():
        on_phase(
            "stopped",
            f"Stopped — subscribed {joined}, already in {already}, "
            f"missed button {skipped}, failed {failed}. Run again to continue.",
        )
    else:
        on_phase(
            "done",
            f"Done — subscribed {joined}, already in {already}, "
            f"missed button {skipped}, failed {failed}. "
            "Click Mute after joining to mute & update the log.",
        )
    emit_counts()
    return progress
