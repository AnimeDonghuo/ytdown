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
from yt_dlp.utils import DownloadCancelled, DownloadError

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
    url: str  # HLS variant playlist or direct MP4 (public HTTPS)
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


def proxy_manifest(data: object) -> str:
    """The Worker's own copy of the master playlist, served from Cloudflare rather than the CDN."""
    value = data.get("proxy_m3u8") if isinstance(data, dict) else None
    return value if public_https_url(value) else ""


YTDLP_BASE = {
    "quiet": True,
    "no_warnings": True,
    "noprogress": True,
    "noplaylist": True,
    "skip_download": True,
    "socket_timeout": 30,
    "retries": 2,
}


def ytdlp_options(proxy: str | None = None, **extra) -> dict:
    """Options shared by every yt-dlp call this module makes."""
    options = dict(YTDLP_BASE)
    options.update(extra)
    if proxy:
        options["proxy"] = proxy
    return options


def extract_dailymotion(watch_url: str, proxy: str | None = None) -> DailymotionInfo:
    """Read the qualities straight from Dailymotion with yt-dlp.

    This is the bypass for a refused Worker playlist: the Worker's copy of the
    master playlist is signed for *its* address, so a 403 there says nothing
    about the video. yt-dlp asks Dailymotion itself and gets URLs minted for the
    address the bot will download from (the pool proxy when proxy mode is on).
    """
    try:
        with yt_dlp.YoutubeDL(ytdlp_options(proxy, noplaylist=True)) as ydl:
            info = ydl.extract_info(watch_url, download=False)
    except DownloadError as exc:
        raise DailymotionError(f"Dailymotion refused this video: {ytdlp_reason(exc)}") from exc
    except Exception as exc:  # ExtractorError, network problems, bad manifest…
        raise DailymotionError(f"Could not read this Dailymotion video: {short_reason(exc)}") from exc
    if not isinstance(info, dict):
        raise DailymotionError("Dailymotion returned no video information.")
    formats = info.get("formats") or []
    best: dict[int, DMFormat] = {}
    for entry in formats:
        if not isinstance(entry, dict):
            continue
        height = entry.get("height")
        protocol = str(entry.get("protocol") or "")
        if not isinstance(height, int) or height <= 0:
            continue
        if protocol.startswith("m3u8"):
            url = entry.get("manifest_url") or entry.get("url")
        elif protocol in ("http", "https") and (entry.get("ext") or "") == "mp4":
            url = entry.get("url")
        else:  # DASH or fragmented formats the remux path does not handle.
            continue
        if not public_https_url(url):
            continue
        bandwidth = int(entry.get("tbr") or 0) * 1000
        current = best.get(height)
        if current is None or bandwidth > current.bandwidth:
            best[height] = DMFormat(f"{height}p", url, int(entry.get("width") or 0), height, bandwidth)
    if not best:
        raise DailymotionError("Dailymotion listed no downloadable qualities.")
    title = str(info.get("title") or "Dailymotion video").strip()[:200] or "Dailymotion video"
    duration = info.get("duration")
    duration = max(0, int(duration)) if isinstance(duration, (int, float)) else 0
    poster = info.get("thumbnail") if public_https_url(info.get("thumbnail")) else ""
    formats = tuple(best[h] for h in sorted(best, reverse=True))
    return DailymotionInfo(title, duration, poster, formats)


def ytdlp_reason(exc: Exception) -> str:
    """The first line of a yt-dlp error, short enough for a chat message."""
    text = str(exc or "").strip()
    text = text.splitlines()[0] if text else type(exc).__name__
    return text[:160] or "no reason given"


def short_reason(exc: Exception, limit: int = 120) -> str:
    text = " ".join(str(exc or "").split()) or type(exc).__name__
    return text[:limit - 1] + "…" if len(text) > limit else text


def ffmpeg_ready() -> bool:
    return bool(shutil.which("ffmpeg") and shutil.which("ffprobe"))


async def read_bounded(session: aiohttp.ClientSession, url: str, limit: int) -> bytes:
    async with session.get(url, allow_redirects=False, timeout=aiohttp.ClientTimeout(total=60, connect=20)) as resp:
        if 300 <= resp.status < 400:  # Not followed: report it instead of parsing an empty body.
            raise aiohttp.ClientResponseError(resp.request_info, resp.history, status=resp.status,
                                              message="redirect not followed")
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


def download_dailymotion(manifest_url: str, out_dir: str, on_bytes, stop: threading.Event,
                        proxy: str | None = None, watch_url: str = "", height: int = 0) -> str:
    """Blocking yt-dlp download of one variant into out_dir; returns the MP4 path.

    on_bytes(done, total) is called from the worker thread. Setting `stop` aborts the
    download at the next progress callback. `proxy` routes the requests through the
    caller's pool proxy (yt-dlp understands http/https/socks proxy URLs).

    Provider URLs expire and are often signed for one address, so when the saved
    ``manifest_url`` is refused and the caller knows the ``watch_url``, yt-dlp is
    asked for a fresh copy of the same quality instead of failing the job.
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
    if proxy:
        options["proxy"] = proxy
    try:
        with yt_dlp.YoutubeDL(options) as ydl:
            ydl.extract_info(manifest_url, download=True)
    except DownloadCancelled:
        raise
    except Exception:
        if not watch_url:
            raise
        # Fresh URLs, same proxy: exactly the route a manual retry would take.
        retry = ytdlp_options(proxy, format=quality_selector(height), outtmpl=options["outtmpl"],
                              merge_output_format="mp4", noprogress=True, quiet=True,
                              no_warnings=True, noplaylist=True, retries=3, fragment_retries=3,
                              socket_timeout=30, concurrent_fragment_downloads=1,
                              progress_hooks=[hook])
        try:
            with yt_dlp.YoutubeDL(retry) as ydl:
                ydl.extract_info(watch_url, download=True)
        except DownloadCancelled:
            raise
        except Exception as second:
            raise DailymotionError(
                f"Could not download this Dailymotion quality: {short_reason(second)}") from second
    mp4 = [path for path in glob.glob(os.path.join(out_dir, "stream.*")) if path.endswith(".mp4")]
    if not mp4:
        raise DailymotionError("The stream could not be converted to MP4.")
    return mp4[0]


def quality_selector(height: int) -> str:
    """yt-dlp format string for one quality, preferring the exact height."""
    if height <= 0:
        return "best"
    return f"best[height={height}]/best[height<={height}]/best"
