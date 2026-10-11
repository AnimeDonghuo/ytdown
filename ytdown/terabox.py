"""Validated Terabox share URLs and the samraTBdownload API's file listings."""
from __future__ import annotations

import mimetypes
import re
from datetime import datetime
from dataclasses import dataclass
from pathlib import PurePosixPath
from urllib.parse import urlparse

from .core import public_https_url

TERABOX_API_URL = "https://samratbdownload.krishnalucky193.workers.dev/"
TERABOX_HOSTS = ("terabox.com", "1024terabox.com", "teraboxapp.com", "teraboxlink.com",
                 "teraboxshare.com", "terasharelink.com", "terabox.fun")
MAX_TERABOX_FILES = 500


@dataclass(frozen=True)
class TeraboxFile:
    file_id: str
    filename: str
    quality: str
    url: str
    thumbnail: str
    mime: str
    size: int

    @property
    def title(self) -> str:
        return PurePosixPath(self.filename).stem[:200]

    @property
    def video(self) -> bool:
        return self.mime.startswith("video/") or self.filename.lower().endswith((".mkv", ".mp4", ".webm", ".avi", ".mov"))


@dataclass(frozen=True)
class TeraboxListing:
    files: tuple[TeraboxFile, ...]  # One entry per logical file, in API order.
    expires_at: int = 0
    reported_total: int = 0
    variants: tuple[tuple[TeraboxFile, ...], ...] = ()

    def options_for(self, index: int) -> tuple[TeraboxFile, ...]:
        return self.variants[index] if self.variants else (self.files[index],)


def parse_terabox_source(raw: str) -> str:
    text = raw.strip()
    if not public_https_url(text):
        raise ValueError("Send a public HTTPS Terabox share link.")
    host = (urlparse(text).hostname or "").lower()
    if not any(host == suffix or host.endswith("." + suffix) for suffix in TERABOX_HOSTS):
        raise ValueError("Terabox mode accepts only Terabox share links.")
    if len(text) > 2048:
        raise ValueError("Terabox link is too long.")
    return text


def normalize_quality(raw: object, filename: str) -> str:
    if isinstance(raw, (int, str)):
        quality = str(raw).strip().lower()
        match = re.fullmatch(r"(\d{3,4})p?", quality)
        if match:
            return match[1] + "p"
        if quality in ("4k", "2k"):
            return "2160p" if quality == "4k" else "1440p"
        if quality in ("mp3", "audio"):
            return "MP3"
    found = re.search(r"(?:^|[_\s.-])(\d{3,4})p(?:[_\s.-]|$)", filename, flags=re.I)
    return found[1] + "p" if found else "Original"


def normalize_terabox(payload: object) -> TeraboxListing:
    if not isinstance(payload, dict) or payload.get("success") is not True:
        raise ValueError("Terabox API could not resolve this share link.")
    data = payload.get("data")
    if not isinstance(data, dict):
        raise ValueError("Terabox API returned no file listing.")
    raw_files = data.get("files")
    if not isinstance(raw_files, list) or not raw_files:
        raw_files = [data] if data.get("download_url") or data.get("stream_url") else []
    if not raw_files:
        raise ValueError("The API did not provide files for this share/folder.")
    if len(raw_files) > MAX_TERABOX_FILES:
        raise ValueError(f"Terabox folder exceeds the {MAX_TERABOX_FILES}-file free-tier limit.")
    if (len(raw_files) == 1 and str(payload.get("total_files") or "1") == "1"
            and isinstance(raw_files[0], dict) and
            (data.get("download_url") or data.get("stream_url")) and data.get("quality")):
        first = raw_files[0]
        if normalize_quality(data["quality"], str(first.get("filename") or "")) != normalize_quality(
                first.get("quality"), str(first.get("filename") or "")):
            # Some API responses include a second quality at data level in
            # addition to the sole files[] entry; keep both for that file.
            raw_files = [first, {**first, "download_url": data.get("download_url"),
                                 "stream_url": data.get("stream_url"), "quality": data["quality"]}]
    groups: dict[str, list[TeraboxFile]] = {}
    for item in raw_files:
        if not isinstance(item, dict):
            continue
        link = item.get("download_url") or item.get("stream_url")
        if not public_https_url(link):
            continue
        name = item.get("filename") or item.get("name") or data.get("name") or "Terabox file"
        name = str(name).replace("\\", "/").rsplit("/", 1)[-1].strip()[:180] or "Terabox file"
        extension = str(item.get("extension") or "").lower().lstrip(".")
        if not PurePosixPath(name).suffix and re.fullmatch(r"[a-z0-9]{1,8}", extension):
            name += "." + extension
        mime = item.get("mime")
        if (not isinstance(mime, str) or
                not re.fullmatch(r"[a-zA-Z0-9.+-]+/[a-zA-Z0-9.+-]+", mime) or
                mime == "application/octet-stream"):
            mime = mimetypes.guess_type(name)[0] or "application/octet-stream"
        size = item.get("size_bytes") or item.get("size") or 0
        try:
            size = int(size) if isinstance(size, (int, float, str)) else 0
        except (ValueError, OverflowError):
            size = 0
        size = size if 0 < size < 10 * 1024**3 else 0
        thumb = item.get("thumbnail_url") or item.get("thumbnail") or data.get("thumbnail") or ""
        file_id = str(item.get("id") or name)
        variant = TeraboxFile(file_id, name,
                              normalize_quality(item.get("quality") or data.get("quality"), name),
                              link, thumb if public_https_url(thumb) else "", mime, size)
        # Repeat IDs (or repeated names without IDs) represent quality choices,
        # not extra folder entries. Duplicate qualities are not sent twice.
        group = groups.setdefault(file_id, [])
        if all(previous.quality != variant.quality for previous in group):
            group.append(variant)
    if not groups:
        raise ValueError("Terabox API returned no public HTTPS download links.")
    expires = payload.get("expires_at")
    if isinstance(expires, (int, float)):
        expires = int(expires)
    elif isinstance(expires, str):
        try:
            expires = int(expires) if expires.isdigit() else int(datetime.fromisoformat(
                expires.replace("Z", "+00:00")).timestamp())
        except (ValueError, OverflowError):
            expires = 0
    else:
        expires = 0
    # Some providers express expiry as milliseconds since the epoch.
    if expires > 10**11:
        expires //= 1000
    reported = payload.get("total_files")
    try:
        reported = int(reported) if isinstance(reported, (int, float, str)) else 0
    except (ValueError, OverflowError):
        reported = 0
    reported = reported if reported > 0 else len(groups)
    return TeraboxListing(tuple(group[0] for group in groups.values()), expires, reported,
                          tuple(tuple(group) for group in groups.values()))
