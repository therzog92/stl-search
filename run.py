"""Launch the STL Search web UI."""

from __future__ import annotations

import atexit
import os
import sys
import threading
import time

import uvicorn


def _windows_keep_awake_loop(stop: threading.Event) -> None:
    """
    Ask Windows not to sleep (or enter away-mode idle) while this process runs.
    Locking the screen is fine; sleep/hibernate would freeze Telegram + uvicorn.
    """
    try:
        import ctypes
    except ImportError:
        return

    ES_CONTINUOUS = 0x80000000
    ES_SYSTEM_REQUIRED = 0x00000001
    ES_AWAYMODE_REQUIRED = 0x00000040
    flags = ES_CONTINUOUS | ES_SYSTEM_REQUIRED | ES_AWAYMODE_REQUIRED
    kernel32 = ctypes.windll.kernel32

    def clear() -> None:
        try:
            kernel32.SetThreadExecutionState(ES_CONTINUOUS)
        except Exception:
            pass

    atexit.register(clear)
    while not stop.wait(25):
        try:
            kernel32.SetThreadExecutionState(flags)
        except Exception:
            break
    clear()


if __name__ == "__main__":
    # 0.0.0.0 = reachable from phone/other devices on the same Wi‑Fi
    # Override with STL_HOST=127.0.0.1 if you want localhost-only again
    host = os.getenv("STL_HOST", "0.0.0.0")
    port = int(os.getenv("STL_PORT", "8787"))

    stop_awake = threading.Event()
    if sys.platform == "win32" and os.getenv("STL_ALLOW_SLEEP", "").strip() not in (
        "1",
        "true",
        "yes",
    ):
        threading.Thread(
            target=_windows_keep_awake_loop,
            args=(stop_awake,),
            name="stl-keep-awake",
            daemon=True,
        ).start()

    try:
        uvicorn.run("app.main:app", host=host, port=port, reload=False)
    finally:
        stop_awake.set()
