"""Settings helpers: update .env and mask secrets."""

from __future__ import annotations

import os
import re
from pathlib import Path

from app.config import ROOT

ENV_PATH = ROOT / ".env"


def mask_secret(value: str, keep: int = 4) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    if len(raw) <= keep:
        return "•" * len(raw)
    return "•" * (len(raw) - keep) + raw[-keep:]


def read_env_file() -> str:
    if ENV_PATH.exists():
        return ENV_PATH.read_text(encoding="utf-8")
    return ""


def update_env_values(updates: dict[str, str]) -> None:
    """
    Upsert keys in .env without wiping unrelated settings.
    Creates .env from .env.example when missing.
    """
    if not ENV_PATH.exists():
        example = ROOT / ".env.example"
        text = example.read_text(encoding="utf-8") if example.exists() else ""
    else:
        text = ENV_PATH.read_text(encoding="utf-8")

    lines = text.splitlines()
    seen: set[str] = set()
    out: list[str] = []

    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in line:
            out.append(line)
            continue
        key, _, _rest = line.partition("=")
        key_name = key.strip()
        if key_name in updates:
            out.append(f"{key_name}={updates[key_name]}")
            seen.add(key_name)
        else:
            out.append(line)

    for key, value in updates.items():
        if key not in seen:
            out.append(f"{key}={value}")

    ENV_PATH.write_text("\n".join(out).rstrip() + "\n", encoding="utf-8")

    # Keep process env in sync for anything that reads os.getenv later
    for key, value in updates.items():
        os.environ[key] = value


_KEY_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def apply_telegram_api_to_runtime(api_id: int, api_hash: str) -> None:
    """Update in-memory config module values used by the running app."""
    import app.config as cfg

    cfg.API_ID = int(api_id)
    cfg.API_HASH = str(api_hash)
    os.environ["TELEGRAM_API_ID"] = str(api_id)
    os.environ["TELEGRAM_API_HASH"] = str(api_hash)
