"""
Speed up Telethon MTProto AES using TgCrypto.

Official Telegram Desktop uses native crypto. Telethon falls back to pure
Python AES when cryptg/libssl are missing — that bottles downloads hard.
TgCrypto (already used by other Telegram clients) provides the same IGE
AES primitives; we teach Telethon to call it.
"""

from __future__ import annotations

import logging
import types

log = logging.getLogger("stl.crypto")


def install_tgcrypto_for_telethon() -> str:
    """Patch telethon.crypto.aes to use tgcrypto when cryptg is absent."""
    try:
        import tgcrypto
    except ImportError:
        log.info("TgCrypto not installed — Telegram AES stays on Python fallback")
        return "none"

    from telethon.crypto import aes as aes_mod

    # If a real cryptg is already wired, leave it alone.
    existing = getattr(aes_mod, "cryptg", None)
    if existing is not None and hasattr(existing, "encrypt_ige") and hasattr(
        existing, "decrypt_ige"
    ):
        log.info("cryptg already active for Telethon AES")
        return "cryptg"

    shim = types.SimpleNamespace(
        encrypt_ige=tgcrypto.ige256_encrypt,
        decrypt_ige=tgcrypto.ige256_decrypt,
    )
    aes_mod.cryptg = shim
    log.info("TgCrypto enabled for Telethon AES (faster downloads)")
    return "tgcrypto"
