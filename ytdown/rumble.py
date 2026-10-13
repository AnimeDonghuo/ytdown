"""Rumble single-video links through the Rumble API (direct MP4 qualities, no ffmpeg needed)."""

from __future__ import annotations

import re
from urllib.parse import urlsplit

from .core import Video, public_https_url

RUMBLE_API_URL = "https://samra-rumble-api.krishnalucky193.workers.dev/"
RUMBLE_HOST = "rumble.com"
# Watch pages (/v4t3sz8-title.html, slug optional) and embeds (/embed/v4t3sz8/).
_VIDEO_PATH = re.compile(r"/(?:(v[a-z0-9]{4,12})(?:-[a-z0-9._-]*)?\.html|embed/(v[a-z0-9]{4,12})/?)", re.IGNORECASE)
_MAX_HEIGHT, _MIN_HEIGHT = 4320, 100
_MAX_ERROR = 120


class RumbleError(ValueError):
    """User-facing Rumble failure. A ValueError, so the generic job handlers still catch it."""


def parse_rumble_source(raw: str) -> str:
    """Return the canonical https://rumble.com/... URL for one Rumble video, or raise RumbleError."""
    text = (raw or "").strip()
    usage = "Send one Rumble video link, for example https://rumble.com/v4t3sz8-title.html"
    if not text or len(text) > 500 or any(ch.isspace() for ch in text):
        raise RumbleError(usage)
    if "://" not in text:
        text = "https://" + text
    try:
        parts = urlsplit(text)
        host = (parts.hostname or "").lower()
    except ValueError as exc:
        raise RumbleError(usage) from exc
    if parts.scheme not in ("http", "https") or parts.username or parts.password:
        raise RumbleError(usage)
    if host not in (RUMBLE_HOST, f"www.{RUMBLE_HOST}"):
        raise RumbleError("Send a rumble.com video link (not another site or a channel page).")
    if not _VIDEO_PATH.fullmatch(parts.path):
        raise RumbleError("Send a Rumble video link: rumble.com/v…-title.html or rumble.com/embed/v… "
                          "(channels, search pages and playlists are not supported).")
    return f"https://{RUMBLE_HOST}{parts.path}"


def _height(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and _MIN_HEIGHT <= value <= _MAX_HEIGHT


def normalize_rumble(payload: object) -> Video:
    """Turn the Rumble API answer into a Video with one MP4 link per quality, e.g. '720p'."""
    if not isinstance(payload, dict) or payload.get("success") is False or payload.get("status") is False:
        message = payload.get("error") if isinstance(payload, dict) else None
        message = message.strip()[:_MAX_ERROR] if isinstance(message, str) and message.strip() else ""
        raise RumbleError(message or "The Rumble API could not find this video.")
    # Rumble reports some renditions with a padded height (1081p is a 1080-line file).
    # Label each MP4 by its real height; when two share a label, keep the exact one.
    chosen: dict[str, tuple[bool, str]] = {}
    streams = payload.get("streams")
    for item in streams if isinstance(streams, list) else []:
        if not isinstance(item, dict) or item.get("type") != "video" or item.get("format") != "mp4":
            continue
        url = item.get("url")
        if not public_https_url(url):
            continue
        height = item.get("real_height") if _height(item.get("real_height")) else item.get("height")
        if not _height(height):
            continue
        exact = item.get("height") == height
        label = f"{height}p"
        previous = chosen.get(label)
        if previous is None or (exact and not previous[0]):
            chosen[label] = (exact, url)
    if not chosen:
        raise RumbleError("The Rumble API returned no MP4 downloads for this video.")
    embed_id = payload.get("embed_id") if isinstance(payload.get("embed_id"), str) else ""
    title = payload.get("title").strip() if isinstance(payload.get("title"), str) else ""
    title = title or (f"Rumble video {embed_id}" if embed_id else "Rumble video")
    duration = payload.get("duration")
    duration = duration if isinstance(duration, int) and not isinstance(duration, bool) and duration >= 0 else 0
    thumbnail = payload.get("thumbnail") if public_https_url(payload.get("thumbnail")) else ""
    return Video(title, {label: url for label, (_, url) in chosen.items()}, duration, thumbnail)
