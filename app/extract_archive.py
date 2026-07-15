"""
Extract downloaded ZIP/RAR into the PC download folder.

Rules:
- If the archive has exactly one top-level folder → extract so that folder
  lands in the download directory (no extra wrapper).
- Otherwise → extract into a folder named after the archive stem so loose
  files don't spill into the download directory.
"""

from __future__ import annotations

import logging
import shutil
import tempfile
import zipfile
from dataclasses import dataclass
from pathlib import Path

log = logging.getLogger("stl.extract")

ARCHIVE_SUFFIXES = {".zip", ".rar"}


class DestinationExists(Exception):
    """Target file/folder already exists and duplicates were not allowed."""

    def __init__(self, existing: Path, suggested_name: str, kind: str = "folder"):
        self.existing = Path(existing)
        self.suggested_name = suggested_name
        self.kind = kind  # "file" | "folder"
        super().__init__(f"{kind.title()} already exists: {self.existing.name}")


@dataclass
class ExtractResult:
    path: Path
    used_wrapper: bool
    archive_deleted: bool
    note: str = ""


def is_extractable_archive(path: Path | str) -> bool:
    return Path(path).suffix.lower() in ARCHIVE_SUFFIXES


def _sanitize_path_segment(segment: str) -> str:
    """Make a single path segment Windows-safe (no trailing spaces/dots, no illegal chars)."""
    cleaned = "".join(c if c not in '<>:"|?*\x00' else "_" for c in (segment or ""))
    # Windows forbids trailing spaces/dots; also trim leading junk.
    cleaned = cleaned.strip(" .")
    # Avoid reserved device names on Windows.
    if cleaned.upper() in {
        "CON",
        "PRN",
        "AUX",
        "NUL",
        "COM1",
        "COM2",
        "COM3",
        "COM4",
        "LPT1",
        "LPT2",
        "LPT3",
    }:
        cleaned = f"_{cleaned}"
    return cleaned[:180] or "item"


def _sanitize_zip_member(name: str) -> tuple[str, ...] | None:
    """
    Normalize a zip member path into safe relative parts.
    Returns None for empty / directory-only junk after sanitizing.
    """
    raw = (name or "").replace("\\", "/")
    if not raw or raw.startswith("/") or raw.startswith("../") or "/../" in f"/{raw}":
        raise ValueError(f"Unsafe path in zip: {name}")
    parts = [_sanitize_path_segment(p) for p in raw.split("/") if p and p != "."]
    parts = [p for p in parts if p]
    if not parts:
        return None
    return tuple(parts)


def _safe_extract_zip(archive: Path, dest: Path) -> None:
    dest.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(archive, "r") as zf:
        for info in zf.infolist():
            raw = (info.filename or "").replace("\\", "/")
            is_dir = info.is_dir() or raw.endswith("/")
            parts = _sanitize_zip_member(raw)
            if not parts:
                continue
            target = dest.joinpath(*parts)
            try:
                target.relative_to(dest)
            except ValueError as exc:
                raise ValueError(f"Unsafe path in zip: {info.filename}") from exc

            if is_dir:
                target.mkdir(parents=True, exist_ok=True)
                continue

            target.parent.mkdir(parents=True, exist_ok=True)
            with zf.open(info, "r") as src, open(target, "wb") as out:
                shutil.copyfileobj(src, out)


def _find_extractor() -> tuple[str, Path] | None:
    """Return ('7z'|'unrar', path) if a CLI extractor is available."""
    candidates: list[tuple[str, Path]] = []
    for name in ("7z", "7za", "7zr"):
        found = shutil.which(name)
        if found:
            candidates.append(("7z", Path(found)))
    for name in ("UnRAR", "unrar"):
        found = shutil.which(name)
        if found:
            candidates.append(("unrar", Path(found)))

    program_files = [
        Path(p)
        for p in (
            r"C:\Program Files\7-Zip\7z.exe",
            r"C:\Program Files (x86)\7-Zip\7z.exe",
            r"C:\Program Files\WinRAR\UnRAR.exe",
            r"C:\Program Files\WinRAR\WinRAR.exe",
            r"C:\Program Files (x86)\WinRAR\UnRAR.exe",
        )
        if p
    ]
    for path in program_files:
        if path.exists():
            kind = "unrar" if "unrar" in path.name.lower() or "winrar" in path.name.lower() else "7z"
            candidates.append((kind, path))

    bundled = Path(__file__).resolve().parent.parent / "data" / "tools"
    for path in (bundled / "7z.exe", bundled / "7za.exe", bundled / "UnRAR.exe"):
        if path.exists():
            kind = "unrar" if "unrar" in path.name.lower() else "7z"
            candidates.append((kind, path))

    return candidates[0] if candidates else None


def _safe_extract_with_tool(archive: Path, dest: Path) -> None:
    tool = _find_extractor()
    if not tool:
        raise RuntimeError(
            "RAR needs 7-Zip or WinRAR installed (ZIP still extracts automatically)."
        )
    kind, exe = tool
    dest.mkdir(parents=True, exist_ok=True)
    import subprocess

    if kind == "7z":
        cmd = [str(exe), "x", f"-o{dest}", "-y", "--", str(archive)]
    else:
        # UnRAR x -y archive dest\
        cmd = [str(exe), "x", "-y", "-o+", str(archive), str(dest) + "\\"]
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        err = (proc.stderr or proc.stdout or "").strip() or f"exit {proc.returncode}"
        raise RuntimeError(f"Extractor failed: {err[:300]}")


def _unique_dir(parent: Path, name: str) -> Path:
    safe = "".join(c if c not in '<>:"|?*' else "_" for c in name).strip(" .") or "extracted"
    safe = safe[:120]
    candidate = parent / safe
    if not candidate.exists():
        return candidate
    i = 1
    while True:
        alt = parent / f"{safe} ({i})"
        if not alt.exists():
            return alt
        i += 1


def _safe_entry_name(name: str) -> str:
    safe = "".join(c if c not in '<>:"|?*' else "_" for c in name).strip(" .")
    return (safe[:120] or "extracted")


def list_zip_root_entries(archive: Path) -> list[tuple[str, bool]]:
    """Return [(root_name, is_directory), ...] for top-level zip entries."""
    roots: dict[str, bool] = {}
    with zipfile.ZipFile(archive, "r") as zf:
        for info in zf.infolist():
            raw = (info.filename or "").replace("\\", "/")
            try:
                parts = _sanitize_zip_member(raw)
            except ValueError:
                continue
            if not parts:
                continue
            root = parts[0]
            is_dir = len(parts) > 1 or info.is_dir() or raw.endswith("/")
            if root not in roots:
                roots[root] = is_dir
            elif is_dir:
                roots[root] = True
    return [(k, v) for k, v in roots.items()]


def preferred_extract_name(archive: Path) -> tuple[str, bool]:
    """
    Preferred destination folder name and whether a wrapper folder is used.
    Wrapper=False means single root folder inside the archive.
    """
    archive = Path(archive)
    suffix = archive.suffix.lower()
    if suffix == ".zip":
        try:
            roots = list_zip_root_entries(archive)
        except zipfile.BadZipFile:
            return _safe_entry_name(archive.stem), True
        if len(roots) == 1 and roots[0][1]:
            return _safe_entry_name(roots[0][0]), False
        return _safe_entry_name(archive.stem), True
    # RAR: can't cheaply peek without a tool; assume wrapper named like archive.
    return _safe_entry_name(archive.stem), True


def _resolve_extract_target(
    download_dir: Path, preferred_name: str, *, allow_duplicate: bool
) -> Path:
    preferred = download_dir / preferred_name
    if not preferred.exists():
        return preferred
    suggested = _unique_dir(download_dir, preferred_name)
    if not allow_duplicate:
        raise DestinationExists(preferred, suggested.name, kind="folder")
    return suggested


def _staging_children(staging: Path) -> list[Path]:
    return sorted(
        (p for p in staging.iterdir() if p.name not in {".DS_Store", "Thumbs.db"}),
        key=lambda p: p.name.lower(),
    )


def extract_download_archive(
    archive_path: Path,
    download_dir: Path,
    *,
    allow_duplicate: bool = False,
) -> ExtractResult:
    """
    Extract archive into download_dir per the single-folder vs wrapper rules.
    Deletes the archive on success.
    Raises DestinationExists when the target exists and allow_duplicate is False.
    """
    archive = Path(archive_path)
    download_dir = Path(download_dir)
    download_dir.mkdir(parents=True, exist_ok=True)
    suffix = archive.suffix.lower()
    if suffix not in ARCHIVE_SUFFIXES:
        raise ValueError(f"Not an extractable archive: {archive.name}")

    # ZIP: detect destination conflicts before paying for a full extract.
    if suffix == ".zip":
        preferred_name, _ = preferred_extract_name(archive)
        _resolve_extract_target(
            download_dir, preferred_name, allow_duplicate=allow_duplicate
        )

    with tempfile.TemporaryDirectory(prefix="stl_x_", dir=str(download_dir)) as tmp:
        staging = Path(tmp)
        if suffix == ".zip":
            try:
                _safe_extract_zip(archive, staging)
            except zipfile.BadZipFile as exc:
                raise RuntimeError(f"Invalid ZIP: {exc}") from exc
        else:
            _safe_extract_with_tool(archive, staging)

        children = _staging_children(staging)
        if not children:
            raise RuntimeError("Archive was empty.")

        used_wrapper = True
        if len(children) == 1 and children[0].is_dir():
            used_wrapper = False
            preferred_name = _safe_entry_name(children[0].name)
            target = _resolve_extract_target(
                download_dir, preferred_name, allow_duplicate=allow_duplicate
            )
            shutil.move(str(children[0]), str(target))
            result_path = target
            note = f"Extracted folder “{target.name}”"
        else:
            preferred_name = _safe_entry_name(archive.stem)
            target = _resolve_extract_target(
                download_dir, preferred_name, allow_duplicate=allow_duplicate
            )
            target.mkdir(parents=True, exist_ok=False)
            for child in children:
                shutil.move(str(child), str(target / child.name))
            result_path = target
            note = f"Extracted into “{target.name}”"

    try:
        archive.unlink(missing_ok=True)
        deleted = True
    except Exception:
        log.warning("Could not delete archive after extract: %s", archive)
        deleted = False

    return ExtractResult(
        path=result_path.resolve(),
        used_wrapper=used_wrapper,
        archive_deleted=deleted,
        note=note,
    )
