"""Input validation, API response normalization, and flat playlist discovery."""
from __future__ import annotations

import ipaddress
import re
from dataclasses import dataclass
from urllib.parse import parse_qs, urlparse

import yt_dlp

VIDEO_ID = re.compile(r"^[A-Za-z0-9_-]{11}$")
PLAYLIST_ID = re.compile(r"^[A-Za-z0-9_-]{10,}$")
VIDEO_QUALITIES = (360, 480, 720, 1080, 1440, 2160)
AUDIO = "mp3"


@dataclass(frozen=True)
class Source:
    kind: str  # video, playlist, terabox, or terabox-folder
    url: str


@dataclass(frozen=True)
class Video:
    title: str
    streams: dict[str, str]
    duration: int = 0
    thumbnail: str = ""


@dataclass(frozen=True)
class PlaylistEntry:
    url: str
    title: str  # From YouTube's playlist metadata, never from the download API.
    position: int = 0  # Original playlist index, including skipped/private entries.


def _playlist_options(**overrides):
    return {
        "extract_flat": "in_playlist", "skip_download": True, "quiet": True,
        "no_warnings": True, "ignoreerrors": True,
        "socket_timeout": 20, "retries": 1, "extractor_retries": 1,
        **overrides,
    }


def playlist_count(url: str, max_scan: int = 5000) -> int:
    """Count public playlist entries without resolving/downloading video formats."""
    try:
        with yt_dlp.YoutubeDL(_playlist_options(lazy_playlist=True)) as ydl:
            info = ydl.extract_info(url, download=False)
            if not info:
                raise ValueError("Could not read this public playlist.")
            count = info.get("playlist_count") or info.get("n_entries")
            if isinstance(count, int) and count > 0:
                if count > max_scan:
                    raise ValueError(f"Playlist exceeds the {max_scan}-entry discovery limit.")
                return count
            # Some extractors do not expose the count. Flat entries are cheap;
            # iterate with a hard bound rather than loading video details.
            entries = info.get("entries")
            if entries is None:
                raise ValueError("Could not determine the playlist size.")
            count = 0
            for _ in entries:
                count += 1
                if count > max_scan:
                    raise ValueError(f"Playlist exceeds the {max_scan}-entry discovery limit.")
            if count:
                return count
    except yt_dlp.utils.DownloadError as exc:
        raise ValueError("Could not read this public playlist. YouTube may be blocking the host.") from exc
    raise ValueError("No videos were found in this playlist.")


def youtube_title(value: object, video_id: str) -> str:
    """Reject placeholders and bound metadata text used in Telegram captions."""
    if not isinstance(value, str):
        return ""
    title = " ".join(value.split())[:200]
    if title.lower() in {"", "na", video_id.lower(), "[private video]", "[deleted video]"}:
        return ""
    return title


def parse_source(text: str) -> Source:
    # The message must be a URL, not free text with an embedded URL.
    raw = text.strip()
    parsed = urlparse(raw)
    host = (parsed.hostname or "").lower()
    if parsed.scheme != "https" or parsed.username or parsed.password or parsed.port or host not in {
        "youtube.com", "www.youtube.com", "m.youtube.com", "music.youtube.com", "youtu.be", "www.youtu.be"
    }:
        raise ValueError("Send a full https:// YouTube video or playlist URL.")
    query = parse_qs(parsed.query)
    if host.endswith("youtu.be"):
        candidate = parsed.path.strip("/")
        if not VIDEO_ID.fullmatch(candidate):
            raise ValueError("This YouTube video link is not valid.")
        return Source("video", f"https://www.youtube.com/watch?v={candidate}")
    # A watch URL with list= is a playlist request (use a plain watch URL for one video).
    lists = query.get("list", [])
    if lists and PLAYLIST_ID.fullmatch(lists[0]):
        return Source("playlist", f"https://www.youtube.com/playlist?list={lists[0]}")
    if parsed.path == "/playlist":
        raise ValueError("This playlist link needs a valid list= ID.")
    candidate = ""
    if parsed.path == "/watch":
        candidate = query.get("v", [""])[0]
    elif parsed.path.startswith(("/shorts/", "/live/", "/embed/")):
        parts = parsed.path.strip("/").split("/")
        if len(parts) == 2:
            candidate = parts[1]
    if not VIDEO_ID.fullmatch(candidate):
        raise ValueError("Send a YouTube watch, Shorts, live, or playlist URL.")
    return Source("video", f"https://www.youtube.com/watch?v={candidate}")


def public_https_url(value: object) -> bool:
    if not isinstance(value, str):
        return False
    try:
        parsed = urlparse(value)
        host = parsed.hostname
        if parsed.scheme != "https" or not host or parsed.username or parsed.password or parsed.port not in (None, 443):
            return False
        if host.lower() == "localhost" or host.lower().endswith((".localhost", ".local")):
            return False
        try:
            return ipaddress.ip_address(host).is_global
        except ValueError:
            return "." in host and all(host.split("."))
    except ValueError:
        return False


def normalize_video(payload: object) -> Video:
    if not isinstance(payload, dict) or payload.get("success") is False or payload.get("status") is False:
        raise ValueError("The link provider could not find this video.")
    streams: dict[str, str] = {}
    downloads = payload.get("downloads")
    if not isinstance(downloads, list):
        downloads = []
    for item in downloads:
        if not isinstance(item, dict) or not public_https_url(item.get("url")):
            continue
        quality, fmt, kind = item.get("quality"), item.get("format"), item.get("type")
        if kind == "video" and fmt == "mp4" and isinstance(quality, str) and re.fullmatch(r"\d{3,4}p", quality):
            streams[quality] = item["url"]
        elif kind == "audio" and fmt == "mp3":
            streams[AUDIO] = item["url"]
    # The direct video link has no guaranteed quality, so don't guess its label.
    # The dedicated audio link does have an unambiguous format.
    if AUDIO not in streams and public_https_url(payload.get("audio_stream_link")):
        streams[AUDIO] = payload["audio_stream_link"]
    if not streams:
        raise ValueError("The link provider returned no usable MP4/MP3 downloads.")
    # Keep the provider title only as a fallback if YouTube metadata is unavailable.
    title = youtube_title(payload.get("title"), str(payload.get("video_id") or ""))
    raw_duration = payload.get("duration")
    duration = 0
    if isinstance(raw_duration, (int, float)) and 0 < raw_duration < 7 * 86400:
        duration = int(raw_duration)
    elif isinstance(raw_duration, str):
        # Providers may send either seconds or YouTube-style HH:MM:SS / MM:SS.
        pieces = raw_duration.strip().split(":")
        if 1 <= len(pieces) <= 3 and all(piece.isdigit() for piece in pieces):
            seconds = 0
            for piece in pieces:
                seconds = seconds * 60 + int(piece)
            if 0 < seconds < 7 * 86400:
                duration = seconds
    thumbnail = payload.get("thumbnail")
    return Video(title, streams, duration, thumbnail if public_https_url(thumbnail) else "")


def extract_title(url: str) -> str:
    """Backup YouTube metadata lookup if oEmbed is unavailable; run in a thread."""
    opts = {
        "skip_download": True, "noplaylist": True, "quiet": True, "no_warnings": True,
        "socket_timeout": 10, "retries": 0, "extractor_retries": 0,
    }
    with yt_dlp.YoutubeDL(opts) as ydl:
        info = ydl.extract_info(url, download=False)
    return youtube_title(info.get("title") if info else None, url.rsplit("=", 1)[-1])


def extract_playlist(url: str, limit: int) -> list[PlaylistEntry]:
    """Compatibility helper: first N flat playlist entries."""
    return _extract_playlist(url, _playlist_options(playlistend=limit), limit)


def extract_playlist_range(url: str, start: int, end: int) -> list[PlaylistEntry]:
    """Fetch only an inclusive 1-indexed range (not all preceding formats)."""
    if start < 1 or end < start or end - start + 1 > 150:
        raise ValueError("Invalid playlist range (maximum 150 videos per job).")
    return _extract_playlist(url, _playlist_options(playliststart=start, playlistend=end),
                             end - start + 1, unique=False)


def _extract_playlist(url: str, opts: dict, limit: int, *, unique: bool = True) -> list[PlaylistEntry]:
    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=False)
    except yt_dlp.utils.DownloadError as exc:
        raise ValueError("Could not read this public playlist. YouTube may be blocking the host.") from exc
    if not info or not info.get("entries"):
        raise ValueError("Could not read this public playlist. Check that it is available.")
    entries = []
    seen = set()
    for offset, entry in enumerate(info["entries"]):
        if not isinstance(entry, dict):
            continue
        video_id = entry.get("id")
        if isinstance(video_id, str) and VIDEO_ID.fullmatch(video_id) and (not unique or video_id not in seen):
            seen.add(video_id)
            playlist_index = entry.get("playlist_index")
            position = (playlist_index if isinstance(playlist_index, int) and playlist_index > 0
                        else opts.get("playliststart", 1) + offset) if not unique else 0
            entries.append(PlaylistEntry(f"https://www.youtube.com/watch?v={video_id}",
                                         youtube_title(entry.get("title"), video_id), position))
        if len(entries) >= limit:
            break
    if not entries:
        raise ValueError("No available videos were found in this playlist.")
    return entries
