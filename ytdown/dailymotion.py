"""Dailymotion links, stream metadata from the Cloudflare Worker, and HLS downloads through yt-dlp.

Dailymotion streams are HLS only. The worker returns a master playlist; each
variant is downloaded to a temporary MP4 (yt-dlp remuxes it with ffmpeg) and
then uploaded from disk. ffmpeg is therefore required for this mode.
"""
from __future__ import annotations

import glob
import os
import re
import shutil
import threading
from dataclasses import dataclass
from urllib.parse import parse_qs, urljoin, urlparse

import aiohttp
import yt_dlp
from yt_dlp.utils import DownloadCancelled

from .core import public_https_url

DAILYMOTION_API_URL = "https://samra-dailymotion.krishnalucky193.workers.dev/api/info"
DAILYMOTION_HOSTS = ("dailymotion.com", "dai.ly")
MAX_LINK = 2048
MAX_MANIFEST_BYTES = 256 * 1024
MAX_API_BYTES = 200_000
# Dailymotion IDs are alphanumeric and case-sensitive; older ones start with "x", newer ones may not.
VIDEO_ID = re.compile(r"[A-Za-z0-9]{4,24}")


class DailymotionError(ValueError):
    """A user-facing Dailymotion problem (no stream, no ffmpeg, bad link)."""


@dataclass(frozen=True)
class DMFormat:
    label: str  # "720p"
    url: str  # HLS variant playlist (public HTTPS)
    width: int
    height: int
    bandwidth: int  # Peak bits per second from the manifest; 0 if unknown.


def parse_dailymotion_source(raw: str) -> str:
    """Return the canonical watch URL for a single Dailymotion video, or raise ValueError."""
    text = raw.strip()
    if len(text) > MAX_LINK or not public_https_url(text):
        raise ValueError("Send a full https:// Dailymotion video link.")
    parsed = urlparse(text)
    host = (parsed.hostname or "").lower()
    if not any(host == suffix or host.endswith("." + suffix) for suffix in DAILYMOTION_HOSTS):
        raise ValueError("Dailymotion mode accepts only Dailymotion video links.")
    path = parsed.path.strip("/")
    candidate = ""
    if host == "dai.ly" or host.endswith(".dai.ly"):
        candidate = path.split("/", 1)[0]
    elif re.fullmatch(r"(?:embed/)?video/[^/]+", path):
        candidate = path.rsplit("/", 1)[-1]
    elif path.startswith("player"):
        candidate = parse_qs(parsed.query).get("video", [""])[0]
    if not VIDEO_ID.fullmatch(candidate):
        raise ValueError("This Dailymotion link does not name a single video.")
    return f"https://www.dailymotion.com/video/{candidate}"


def normalize_dailymotion(data: object) -> tuple[str, int, str, str]:
    """Return (title, duration, poster, master manifest URL) from the worker's JSON."""
    if not isinstance(data, dict) or not data.get("success"):
        reason = data.get("error") if isinstance(data, dict) else None
        raise DailymotionError(str(reason)[:160] if reason else "Dailymotion returned no stream for this link.")
    manifest = data.get("master_m3u8")
    if not public_https_url(manifest):
        raise DailymotionError("Dailymotion returned no usable stream for this link.")
    title = str(data.get("title") or "Dailymotion video").strip()[:200] or "Dailymotion video"
    try:
        duration = max(0, int(data.get("duration") or 0))
    except (TypeError, ValueError):
        duration = 0
    poster = data.get("poster")
    return title, duration, poster if public_https_url(poster) else "", manifest


def parse_master(text: str, base_url: str) -> tuple[DMFormat, ...]:
    """Read the HLS master playlist: one format per height, best bandwidth kept, tallest first."""
    best: dict[int, DMFormat] = {}
    pending: tuple[int, int, int] | None = None  # width, height, bandwidth of the next URI line
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if line.startswith("#EXT-X-STREAM-INF"):
            attrs = dict(re.findall(r'([A-Z-]+)=("[^"]*"|[^,]*)', line.split(":", 1)[1] if ":" in line else ""))
            resolution = re.fullmatch(r"(\d+)x(\d+)", attrs.get("RESOLUTION", "").strip('"'))
            try:
                bandwidth = int(attrs.get("BANDWIDTH", "0"))
            except ValueError:
                bandwidth = 0
            pending = (int(resolution.group(1)), int(resolution.group(2)), bandwidth) if resolution else (0, 0, bandwidth)
        elif line and not line.startswith("#") and pending is not None:
            width, height, bandwidth = pending
            pending = None
            url = urljoin(base_url, line)
            if height <= 0 or not public_https_url(url):
                continue
            current = best.get(height)
            if current is None or bandwidth > current.bandwidth:
                best[height] = DMFormat(f"{height}p", url, width, height, bandwidth)
    return tuple(best[h] for h in sorted(best, reverse=True))


def estimate_bytes(fmt: DMFormat, duration: int) -> int:
    """Upper-bound size from peak bandwidth; 0 when the manifest gives no bandwidth."""
    return fmt.bandwidth // 8 * duration if fmt.bandwidth and duration else 0


def ffmpeg_ready() -> bool:
    return bool(shutil.which("ffmpeg") and shutil.which("ffprobe"))


async def read_bounded(session: aiohttp.ClientSession, url: str, limit: int) -> bytes:
    async with session.get(url, allow_redirects=False, timeout=aiohttp.ClientTimeout(total=60, connect=20)) as resp:
        resp.raise_for_status()
        if resp.content_length and resp.content_length > limit:
            raise DailymotionError("Dailymotion returned more data than expected.")
        data = await resp.content.read(limit + 1)
        if len(data) > limit:
            raise DailymotionError("Dailymotion returned more data than expected.")
        return data


@dataclass(frozen=True)
class DailymotionInfo:
    title: str
    duration: int
    poster: str
    formats: tuple[DMFormat, ...] = ()


def download_dailymotion(manifest_url: str, out_dir: str, on_bytes, stop: threading.Event) -> str:
    """Blocking yt-dlp download of one HLS variant into out_dir; returns the MP4 path.

    on_bytes(done, total) is called from the worker thread. Setting `stop` aborts the
    download at the next progress callback.
    """
    if not ffmpeg_ready():
        raise DailymotionError("Dailymotion downloads need ffmpeg and ffprobe on this host.")

    def hook(status: dict) -> None:
        if stop.is_set():
            raise DownloadCancelled("Cancelled")
        if status.get("status") == "downloading":
            on_bytes(int(status.get("downloaded_bytes") or 0),
                     int(status.get("total_bytes") or status.get("total_bytes_estimate") or 0))

    options = {
        "outtmpl": os.path.join(out_dir, "stream.%(ext)s"),
        "format": "best",
        "merge_output_format": "mp4",
        "quiet": True,
        "noprogress": True,
        "no_warnings": True,
        "noplaylist": True,
        "retries": 3,
        "fragment_retries": 3,
        "socket_timeout": 30,
        "concurrent_fragment_downloads": 1,
        "progress_hooks": [hook],
    }
    with yt_dlp.YoutubeDL(options) as ydl:
        ydl.extract_info(manifest_url, download=True)
    mp4 = [path for path in glob.glob(os.path.join(out_dir, "stream.*")) if path.endswith(".mp4")]
    if not mp4:
        raise DailymotionError("The stream could not be converted to MP4.")
    return mp4[0]
