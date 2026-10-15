"""MTProto Telegram bot with sequential, bounded-memory media transfers."""
from __future__ import annotations

import asyncio
import hashlib
import io
import ipaddress
import json
import logging
import os
import re
import secrets
import shutil
import signal
import tempfile
import threading
import time
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Mapping
from urllib.parse import parse_qsl, unquote, urlencode, urljoin, urlsplit, urlunsplit

import aiohttp
from aiohttp import web
from telethon import Button, TelegramClient, events, helpers
from telethon.errors import (FloodWaitError, MessageNotModifiedError, PhotoInvalidDimensionsError,
                             PhotoInvalidError, RPCError, VideoContentTypeInvalidError)
from telethon.sessions import MemorySession
from telethon.tl import types
from telethon.tl.functions.upload import SaveBigFilePartRequest, SaveFilePartRequest
from yt_dlp.utils import DownloadError

from .core import (AUDIO, VIDEO_QUALITIES, PlaylistEntry, Source, Video, extract_playlist_range,
                   extract_title, normalize_video, parse_source, playlist_count, public_https_url, youtube_title)
from .terabox import (TERABOX_API_URL, TeraboxFile, TeraboxListing, normalize_terabox,
                      parse_terabox_source)
from .dailymotion import (DAILYMOTION_API_URL, MAX_API_BYTES, MAX_MANIFEST_BYTES, DailymotionError,
                          DailymotionInfo, download_dailymotion,
                          extract_dailymotion, estimate_bytes, ffmpeg_ready, dm_poster_url,
                          normalize_dailymotion, parse_dailymotion_source, parse_master,
                          proxy_manifest, read_bounded)
from .rumble import (RUMBLE_API_URL, RumbleError, extract_rumble, normalize_rumble,
                     parse_rumble_source)
from .mega import (MAX_MEGA_LINKS, MEGA_API_URL, MEGA_HOSTS, MEGA_RETRY_DELAYS, MegaError, MegaFile,
                   MegaListing, find_mega_links, mega_source, merge_listings, normalize_mega,
                   parse_mega_source,
                   video_choice)
from .social import (MAX_SOCIAL_LINKS, SOCIAL_API_URL, SOCIAL_HOSTS, SocialBatch, SocialChoice,
                     SocialDownload, SocialError, SocialMedia, find_social_links, normalize_social,
                     parse_social_source, social_choices, social_source)
from .parciv import (AUTO_SECTIONS, DM_SCAN_LIMIT, MAX_AUTO_LIMIT, MAX_PARCIV_BYTES, PARCIV_API_URL,
                     PARCIV_SECTIONS, PARCIV_STREAM_SECTIONS, ParcivApi, ParcivError, ParcivItem,
                     ParcivMedia,
                     ParcivSource, download_parciv, family_base, is_parciv_media_endpoint,
                     matches_words, parse_catalogue, parse_detail, parse_parciv_source, parse_post,
                     parse_sections, parse_stream, pick_media, search_url_for, title_variants)
from .apitera import (AUTO_INTERVAL_DEFAULT, AUTO_INTERVAL_MIN, CataloguePage, ParcivBatch,
                      already_uploaded, interval_text, limit_text, listing_lines, new_items,
                      normalise_sections, parse_interval, parse_limit, parse_search, past_titles,
                      sections_text, SEARCH_SECTIONS)
from .metrics import ContainerMetrics, human_size
from .media import MediaProbe, thumbnail_jpeg, usable_still
from .proxy import (CHECK_TARGETS, KNOWN_TARGETS, MAX_PROXIES, MIN_LATENCY_MS, SERVICE_KEYS, TOO_FAST,
                    CheckResult, CheckStats, CheckTarget, Proxy, ProxyPool, ProxyRouter, ProxyState,
                    export_text, parse_proxies, parse_proxy_marks, proxy_fault, resolve_targets,
                    run_checks, short_reason, site_marks, target_for, target_label, with_url)

LOG = logging.getLogger(__name__)
MODE_LABELS = {"youtube": "YouTube", "terabox": "Terabox", "dailymotion": "Dailymotion",
               "rumble": "Rumble", "mega": "MEGA", "social": "Instagram/Facebook",
               "parciv": "apitera"}
API_URL = "https://samra-youtube-api.antideploy.com/"
DEFAULT_APIS = {"youtube": API_URL, "terabox": TERABOX_API_URL, "dailymotion": DAILYMOTION_API_URL,
                "rumble": RUMBLE_API_URL, "mega": MEGA_API_URL, "social": SOCIAL_API_URL,
                "parciv": PARCIV_API_URL}
SERVICE_LABELS = {"youtube": "YouTube", "terabox": "Terabox", "dailymotion": "Dailymotion",
                  "rumble": "Rumble", "mega": "MEGA", "social": "Instagram/Facebook",
                  "parciv": "apitera"}
SERVICE_KEYWORDS = {"yt": "youtube", "youtube": "youtube", "tera": "terabox", "terabox": "terabox",
                    "daily": "dailymotion", "dailymotion": "dailymotion", "dm": "dailymotion",
                    "rumble": "rumble", "rum": "rumble", "mega": "mega", "mg": "mega",
                    "mega.nz": "mega", "social": "social", "insta": "social", "ig": "social",
                    "instagram": "social", "fb": "social", "facebook": "social",
                    "parciv": "parciv", "apitera": "parciv", "moviebox": "parciv", "mb": "parciv"}
RESET_WORDS = ("reset", "remove", "default")
MAX_WAITING = 4
MAX_SELECTIONS = 100
SEARCH_LIMIT = 12  # How many matches one /search reply shows (and downloads at most).
SEARCH_PAGES = 2  # Catalogue pages scanned per family when the API has no search call.
SESSION_SECONDS = 1800
# Telegram clients limit uploads to 4,000 parts of 512 KiB (2,000 MiB).
# 2,047 MiB would need 4,094 parts and can fail near the end of an upload.
# The Instagram/Facebook API scrapes the platform and routinely answers late.
SOCIAL_TIMEOUT = aiohttp.ClientTimeout(total=300, connect=30, sock_read=180)
# The MEGA API is slow to wake, so each of its attempts gets room to finish.
MEGA_TIMEOUT = aiohttp.ClientTimeout(total=180, connect=30)
MEGA_DOWNLOAD_WAIT = 15  # Seconds before one last try at a MEGA download URL.
MAX_MT_MB = 2000
PART_BYTES = 512 * 1024
MAX_PARTS = 4000
SPOOL_MB = 64  # For rare providers that do not send Content-Length.
REPORT_SECONDS = 12  # Automatic edits: keep well below Telegram flood limits.
# Every inline-button family the bot makes: s selections, j jobs, m modes, d dashboards,
# p proxy, a apitera. A missing letter here is a button that silently does nothing.
CALLBACK_PATTERN = rb"^(s|j|m|d|p|a):"
MANUAL_REFRESH_SECONDS = 2
MAX_RANGE_VIDEOS = 150
MAX_DISCOVER = 5000
MAX_DASHBOARDS = 100
MAX_COMPLETED_USERS = 200
API_RETRY_DELAYS = (4, 12)  # Seconds before the 2nd and 3rd link-lookup attempts.
LOOKUP_GAP_SECONDS = 2  # Pause between sequential playlist lookups to avoid provider rate limits.
REFRESH_STATUSES = (403, 404, 410)  # Media link refused before any bytes were sent.
THUMB_LIMIT = 256 * 1024  # Bounded read of Telegram's returned still-image thumbnail.
THUMB_REQUEST = 64 * 1024
COVER_CHECK_SECONDS = 20
PROXY_POOL_MAX = 5000  # Proxies kept per user (from /proxychk or PROXY_LIST).
PROXY_CHECK_MAX = 5000  # Proxies checked per /proxychk message or file.
PROXY_FILE_MAX = 2 * 1024 * 1024  # Uploaded proxy lists: bound the download.
PROXY_INLINE_LIST = 20  # More working proxies than this are sent as a .txt file.
PROXY_PROGRESS_SECONDS = 4  # Progress edits during a long pool check.


class PublicResolver(aiohttp.abc.AbstractResolver):
    """Prevent a provider-controlled hostname from resolving to a private address."""

    def __init__(self):
        self.inner = aiohttp.resolver.DefaultResolver()

    async def resolve(self, host, port=0, family=0):
        records = await self.inner.resolve(host, port, family)
        public = [record for record in records if ipaddress.ip_address(record["host"]).is_global]
        if not public:
            raise OSError("Download host resolves only to non-public addresses")
        return public

    async def close(self):
        await self.inner.close()


def env_int(name: str, default: int, low: int, high: int) -> int:
    try:
        return max(low, min(high, int(os.getenv(name, str(default)))))
    except ValueError:
        return default


def env_flag(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    return value.strip().lower() in {"1", "true", "yes", "on", "y"}


class FileTooLarge(Exception):
    pass


class UnknownLength(Exception):
    pass


# Subtitle markers that providers put in titles, e.g. "Episode 15 [MultiSub]".
SUBTITLE_MARK = re.compile(r"[\[(]\s*(?:eng|multi)\s*sub(?:titles?)?\s*[\])]|\b(?:engsub|multisub)\b",
                           re.IGNORECASE)


def split_subtitle_tag(title: str) -> tuple[str, str]:
    """Return (title without subtitle marker, '[MultiSub]' | '[EngSub]' | '').

    The marker is moved after the quality label. If both appear, MultiSub wins.
    """
    kinds = {"multi" if "multi" in match.group().lower() else "eng"
             for match in SUBTITLE_MARK.finditer(title)}
    clean = " ".join(SUBTITLE_MARK.sub(" ", title).split()).strip(" -–—|·:,;/")
    tag = "[MultiSub]" if "multi" in kinds else "[EngSub]" if kinds else ""
    return clean, tag


def media_name(title: str, quality: str, max_length: int, tag: str = "") -> str:
    """Keep the [quality] (and optional subtitle) suffix even when a title is shortened."""
    suffix = f" [{quality.upper() if quality == AUDIO else quality}]" + (f" {tag}" if tag else "")
    return (title[:max_length - len(suffix)].rstrip() or "YouTube video") + suffix


def media_label(title: str, quality: str, max_length: int) -> str:
    """YouTube caption/status label such as 'Title [360p] [MultiSub]'."""
    clean, tag = split_subtitle_tag(title)
    return media_name(clean, quality, max_length, tag)


def cancel_entities(text: str) -> list[types.MessageEntityBotCommand]:
    """Make /cancel_<id> tappable, accounting for Telegram's UTF-16 offsets."""
    return [types.MessageEntityBotCommand(
        len(text[:match.start()].encode("utf-16-le")) // 2,
        len(match.group().encode("utf-16-le")) // 2)
        for match in re.finditer(r"(?<![\w/])/cancel_[a-f0-9]{8}\b", text)]


def human_eta(seconds: float) -> str:
    seconds = max(0, int(seconds))
    if seconds >= 3600:
        return f"{seconds // 3600}h{(seconds % 3600) // 60}m"
    if seconds >= 60:
        return f"{seconds // 60}m{seconds % 60}s"
    return f"{seconds}s"


@dataclass
class Selection:
    token: str
    source: Source
    choices: tuple[str, ...]
    peer: object  # Telethon InputPeer from a real incoming update (has the access hash).
    video: Video | None = None
    selected: set[str] = field(default_factory=set)
    created: float = field(default_factory=time.monotonic)
    api_endpoint: str = API_URL
    start: int = 1
    end: int = 1
    listing: TeraboxListing | MegaListing | None = None
    social: SocialMedia | SocialBatch | None = None
    parciv: ParcivBatch | None = None


@dataclass
class RangeRequest:
    source: Source
    peer: object
    count: int
    api_endpoint: str = API_URL
    listing: TeraboxListing | MegaListing | None = None
    created: float = field(default_factory=time.monotonic)


@dataclass
class Dashboard:
    peer: object
    message_id: int
    token: str = field(default_factory=lambda: secrets.token_hex(4))
    created: float = field(default_factory=time.monotonic)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    next_edit: float = 0.0
    edit_flood_until: float = 0.0
    last_manual: float = 0.0


@dataclass
class LatestPanel:
    """One /latest reply: the catalogue entries it listed and the buttons on it."""

    peer: object
    message_id: int
    token: str = field(default_factory=lambda: secrets.token_hex(4))
    items: tuple[ParcivItem, ...] = ()
    sections: tuple[str, ...] = ()
    page: int = 1
    words: str = ""  # Set on a search panel: Refresh repeats the search, not the listing.
    url: str = ""  # Set when the panel came from a link: Refresh re-reads that link.
    created: float = field(default_factory=time.monotonic)


@dataclass
class Job:
    user_id: int
    peer: object
    message_id: int
    source: Source
    selected: tuple[str, ...]
    video: Video | None
    state: str = "queued"
    detail: str = "Waiting for the download slot."
    task: asyncio.Task | None = None
    token: str = field(default_factory=lambda: secrets.token_hex(4))
    title: str = ""
    quality: str = ""
    item_index: int = 0
    item_total: int = 0
    total_bytes: int = 0
    downloaded: int = 0
    uploaded: int = 0
    speed: float = 0.0
    sample_bytes: int = 0
    sample_at: float = field(default_factory=time.monotonic)
    next_edit: float = 0.0
    edit_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    reporter: asyncio.Task | None = None
    rate_limit_until: float = 0.0
    api_endpoint: str = API_URL
    last_manual: float = 0.0
    edit_flood_until: float = 0.0
    thumbnail: bytes | None = None
    thumbnail_id: str = ""
    thumbnail_urls: tuple[str, ...] = ()
    thumb_size: tuple[int, int] = (0, 0)
    current_url: str = ""
    start: int = 1
    end: int = 1
    listing: TeraboxListing | MegaListing | None = None
    social: SocialMedia | SocialBatch | None = None
    parciv: ParcivBatch | None = None


# Crunchyroll is recognised only to explain why it cannot be downloaded. Every
# stream it serves is Widevine-encrypted, and reading those needs a CDM device
# file plus a Widevine licence request to recover the content keys — DRM
# circumvention this bot does not do.
CRUNCHYROLL_HOSTS = ("crunchyroll.com",)
CRUNCHYROLL_NOTICE = (
    "Crunchyroll is not supported: its streams are Widevine-encrypted, and this bot does not "
    "decrypt DRM. Downloading them would mean asking Widevine for the content keys with a real "
    "CDM device file and then decrypting the video (what tools like "
    "LastPerson07/Crunchyroll-Downloader do with pywidevine + mp4decrypt); that breaks "
    "Crunchyroll's terms and would get the account and the bot banned.\n\n"
    "For offline viewing, use the Crunchyroll app's own downloads with a premium account — "
    "that is the supported route. Everything else here (YouTube, Terabox, Dailymotion, "
    "Rumble) is unaffected.")


def crunchyroll_link(raw: str) -> bool:
    """True for a Crunchyroll URL, whatever subdomain it uses."""
    if not isinstance(raw, str) or not raw.strip():
        return False
    parts = urlsplit(raw.strip())
    host = (parts.hostname or "").lower().rstrip(".")
    return bool(parts.scheme in ("http", "https") and host and
                any(host == suffix or host.endswith("." + suffix) for suffix in CRUNCHYROLL_HOSTS))


def is_media_link(raw: str) -> bool:
    """Keep media links out of a pending /changeapi URL prompt."""
    if find_mega_links(raw or "") or find_social_links(raw or ""):
        return True  # One media link, or a whole list of them.
    if is_parciv_media_endpoint(raw or ""):
        return True
    for parser in (parse_source, parse_terabox_source, parse_dailymotion_source, parse_rumble_source,
                   parse_mega_source, parse_social_source, parse_parciv_source):
        try:
            parser(raw)
            return True
        except ValueError:
            continue
    return False


def normalize_api_endpoint(raw: str) -> str:
    """Allow public HTTPS APIs; replace any url= placeholder with a safe query param."""
    raw = raw.strip()
    if len(raw) > 500 or not public_https_url(raw):
        raise ValueError("API must be a public HTTPS URL (no localhost or private IP).")
    parts = urlsplit(raw)
    host = parts.hostname.lower()
    if host.endswith(("youtube.com", "youtu.be")):
        raise ValueError("Send an API endpoint, not a YouTube video link.")
    if host.endswith("rumble.com"):
        raise ValueError("Send an API endpoint, not a Rumble video link.")
    if host == "crunchyroll.com" or host.endswith(".crunchyroll.com"):
        raise ValueError("Crunchyroll is not supported (its streams are DRM-protected), "
                         "so it has no API to point at.")
    if any(host == suffix or host.endswith("." + suffix) for suffix in MEGA_HOSTS):
        raise ValueError("Send an API endpoint, not a MEGA file or folder link.")
    if any(host == suffix or host.endswith("." + suffix) for suffix in SOCIAL_HOSTS):
        raise ValueError("Send an API endpoint, not an Instagram or Facebook link.")
    if is_parciv_media_endpoint(raw):
        raise ValueError("Send an API endpoint, not one apitera item's stream link "
                         "(use /parciv for that).")
    params = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True) if k.lower() != "url"]
    return urlunsplit((parts.scheme, parts.netloc, parts.path or "/", urlencode(params), ""))


async def fetch_json_api(session: aiohttp.ClientSession, api_endpoint: str, url: str = "",
                         max_bytes: int = 2_000_000,
                         timeout: aiohttp.ClientTimeout | None = None,
                         params: dict | None = None):
    """GET a JSON API endpoint, following a few redirects, on public HTTPS only.

    Most providers take the media link as ``?url=``; apitera's endpoints take their
    own query instead, so ``params`` can replace it.
    """
    endpoint, query = api_endpoint, {"url": url} if params is None else dict(params)
    for _ in range(5):
        if not public_https_url(endpoint):
            raise ValueError("API redirected to an unsafe address.")
        async with session.get(endpoint, params=query, allow_redirects=False,
                               timeout=timeout or aiohttp.ClientTimeout(total=240,
                                                                       connect=20)) as response:
            if response.status in (301, 302, 303, 307, 308):
                location = response.headers.get("Location")
                if not location:
                    raise ValueError("API redirect has no destination.")
                endpoint = urljoin(str(response.url), location)
                query = None  # The Location already contains its own query.
                continue
            response.raise_for_status()
            if response.content_length and response.content_length > max_bytes:
                raise ValueError("The link provider returned too much data.")
            data = await response.content.read(max_bytes + 1)
            if len(data) > max_bytes:
                raise ValueError("The link provider returned too much data.")
            return json.loads(data)
    raise ValueError("Too many API redirects.")


async def fetch_api_with_retries(session: aiohttp.ClientSession, api_endpoint: str, url: str, normalize):
    """Quick transient API failures (rate limits, 5xx, non-JSON pages, dropped connections) are retried."""
    for attempt in range(len(API_RETRY_DELAYS) + 1):
        try:
            return normalize(await fetch_json_api(session, api_endpoint, url))
        except aiohttp.ClientResponseError as exc:
            transient, error = exc.status in (408, 425, 429) or exc.status >= 500, exc
            reason = f"HTTP {exc.status}"
        except json.JSONDecodeError as exc:  # Providers sometimes answer with an HTML rate-limit page.
            transient, error, reason = True, exc, "non-JSON response"
        except aiohttp.ClientConnectionError as exc:
            transient, error, reason = True, exc, type(exc).__name__
        if not transient or attempt == len(API_RETRY_DELAYS):
            raise error
        LOG.info("Link API attempt %d failed (%s); retrying in %ss", attempt + 1, reason,
                 API_RETRY_DELAYS[attempt])
        await asyncio.sleep(API_RETRY_DELAYS[attempt])


async def fetch_video(session: aiohttp.ClientSession, url: str, api_endpoint: str = API_URL) -> Video:
    """YouTube link lookup. Quick transient API failures (rate limits, 5xx, non-JSON pages) are retried."""
    return await fetch_api_with_retries(session, api_endpoint, url, normalize_video)


async def fetch_rumble(session: aiohttp.ClientSession, url: str, endpoint: str = RUMBLE_API_URL,
                        proxy: str | None = None) -> Video:
    """Rumble API first; when it is down or erroring, read the MP4s from Rumble itself."""
    reason = ""
    try:
        return await fetch_api_with_retries(session, endpoint, url, normalize_rumble)
    except aiohttp.ClientResponseError as exc:
        LOG.warning("Rumble API answered HTTP %s", exc.status)
        reason = f"The Rumble API answered HTTP {exc.status}."
    except json.JSONDecodeError:
        reason = "The Rumble API returned an unexpected response."
    except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
        LOG.warning("Rumble API unreachable: %s", type(exc).__name__)
        reason = "Could not reach the Rumble API."
    except RumbleError as exc:
        reason = str(exc)
    try:
        video = await asyncio.to_thread(extract_rumble, url, proxy)
    except RumbleError as exc:
        LOG.warning("Rumble fallback extraction failed: %s", exc)
        raise RumbleError(f"{reason} {exc}".strip()) from exc
    LOG.info("Rumble qualities read straight from Rumble (%s)", reason.rstrip("."))
    return video


async def fetch_terabox(session: aiohttp.ClientSession, url: str,
                         endpoint: str = TERABOX_API_URL) -> TeraboxListing:
    return normalize_terabox(await fetch_json_api(session, endpoint, url, 4_000_000))


def parciv_endpoint(endpoint: str) -> str:
    """The apitera API a parciv job must use.

    A job carries the endpoint it was queued with, but a job built without one (or
    with another service's default) must never be used to leech through: apitera
    URLs are built from this base, so falling back to the built-in API is the only
    safe answer.
    """
    host = urlsplit(str(endpoint or "")).netloc.lower()
    others = {urlsplit(url).netloc.lower() for service, url in DEFAULT_APIS.items()
              if service != "parciv"}
    return endpoint if host and host not in others else DEFAULT_APIS["parciv"]


async def fetch_mega(session: aiohttp.ClientSession, url: str, endpoint: str = MEGA_API_URL,
                     on_wait=None) -> MegaListing:
    """A MEGA file or folder link: the API returns the whole folder tree.

    The API is a free host that sleeps when idle: the first call after a quiet
    spell can take a long time, time out or answer 502 while the service wakes
    up. Those transient failures are retried with growing waits instead of being
    handed to the user at once, and ``on_wait(attempt, delay, attempts)`` (an
    optional async callable) reports each wait so the bot can say so.
    """
    attempts = len(MEGA_RETRY_DELAYS) + 1
    for attempt in range(1, attempts + 1):
        try:
            payload = await fetch_json_api(session, endpoint, url, 8_000_000,
                                           timeout=MEGA_TIMEOUT)
            return normalize_mega(payload)
        except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError, MegaError) as exc:
            if attempt == attempts:
                raise
            delay = MEGA_RETRY_DELAYS[attempt - 1]
            LOG.info("MEGA API attempt %d of %d failed (%s); retrying in %ss",
                     attempt, attempts, type(exc).__name__, delay)
            if on_wait is not None:
                await on_wait(attempt, delay, attempts)
            await asyncio.sleep(delay)


def mega_wait_text(attempt: int, delay: int, attempts: int) -> str:
    """What the user reads while the MEGA API is not answering yet."""
    return (f"⏳ The MEGA API is not answering yet (try {attempt} of {attempts}). It sleeps when "
            f"idle and can take a minute to wake — waiting {delay}s and asking again…")


async def fetch_parciv_json(session: aiohttp.ClientSession, url: str,
                           params: dict | None = None):
    """One apitera endpoint (catalogue page, detail, stream or post) as JSON."""
    return await fetch_json_api(session, url, "", MAX_PARCIV_BYTES, params=params or {})


async def fetch_parciv_catalogue(session: aiohttp.ClientSession, api: ParcivApi, section: str,
                                 page: int = 1) -> CataloguePage:
    """One catalogue page of the apitera API (Moviebox, Anime or Donghua)."""
    url = api.section(section, page)
    payload = await fetch_parciv_json(session, url)
    return CataloguePage(section=section, page=page, url=url,
                         items=parse_catalogue(payload, section, api))


async def fetch_parciv_media(session: aiohttp.ClientSession, api: ParcivApi,
                             item: ParcivItem) -> ParcivMedia:
    """The media of one item: the stream answer, or the post call for Anime/Donghua."""
    if item.section in PARCIV_STREAM_SECTIONS:
        return parse_post(await fetch_parciv_json(session, api.post(item.section, item)), item)
    payload = await fetch_parciv_json(session, api.stream(item.subject_id))
    return pick_media(parse_stream(payload, item.subject_id), item)


async def fetch_parciv_item(session: aiohttp.ClientSession, api: ParcivApi,
                            item: ParcivItem) -> ParcivItem:
    """The detail call: it carries the name, cover and summary the catalogue page lacks."""
    payload = await fetch_parciv_json(session, api.detail(item.subject_id))
    return parse_detail(payload, item)


async def fetch_social(session: aiohttp.ClientSession, url: str,
                       endpoint: str = SOCIAL_API_URL) -> SocialMedia:
    """An Instagram/Facebook link through the DESI API.

    That API scrapes the platforms, so it is slow by nature: it gets the long
    timeout, one retry when the connection itself fails, and nothing that would
    cut a working-but-slow answer short.
    """
    try:
        payload = await fetch_json_api(session, endpoint, url, 2_000_000, timeout=SOCIAL_TIMEOUT)
    except aiohttp.ClientError as exc:
        if isinstance(exc, aiohttp.ClientResponseError):
            raise
        LOG.info("Instagram/Facebook API connection failed (%s); trying once more",
                 type(exc).__name__)
        await asyncio.sleep(3)
        payload = await fetch_json_api(session, endpoint, url, 2_000_000, timeout=SOCIAL_TIMEOUT)
    return normalize_social(payload, url)


def dailymotion_poster(url: str, known: str = "") -> str:
    """The poster to use: what the API or the extraction reported, else Dailymotion's own.

    The Worker's metadata sometimes carries no poster, and that is how Dailymotion
    files arrived with no cover at all. The thumbnail endpoint answers with a
    redirect to the JPEG, and nothing is sent when it does not exist.
    """
    return known or dm_poster_url(url)


async def fetch_dailymotion(session: aiohttp.ClientSession, url: str,
                            endpoint: str = DAILYMOTION_API_URL,
                            proxy: str | None = None) -> DailymotionInfo:
    """Worker metadata and master playlist; straight from Dailymotion when both copies are refused.

    The Worker's playlists are signed for the Worker's own address, so a 403 on
    either copy says nothing about the video. In that case (and when the API
    itself is down) yt-dlp reads the qualities from Dailymotion directly, which
    also mints URLs for the route the download will use.
    """
    data: object = None
    api_error = ""
    try:
        data = await fetch_json_api(session, endpoint, url, MAX_API_BYTES)
    except aiohttp.ClientResponseError as exc:
        LOG.warning("Dailymotion API answered HTTP %s", exc.status)
        api_error = f"The Dailymotion API answered HTTP {exc.status}. Try again later."
    except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
        LOG.warning("Dailymotion API unreachable: %s", type(exc).__name__)
        api_error = "Could not reach the Dailymotion API. Try again later."
    except json.JSONDecodeError:
        api_error = "The Dailymotion API returned an unexpected response."
    title = duration = poster = manifest = proxy_copy = ""
    reason = ""
    if data is not None:
        try:
            title, duration, poster, manifest = normalize_dailymotion(data)
            proxy_copy = proxy_manifest(data)
        except DailymotionError as exc:
            reason = str(exc)
    reason = reason or "No downloadable Dailymotion qualities were listed."
    for label, candidate in (("CDN", manifest), ("Worker proxy", proxy_copy)):
        if not candidate:
            continue
        try:
            text = (await read_bounded(session, candidate, MAX_MANIFEST_BYTES)).decode("utf-8", "replace")
        except aiohttp.ClientResponseError as exc:
            LOG.warning("Dailymotion %s playlist refused: HTTP %s", label, exc.status)
            reason = f"Dailymotion refused the {label} playlist (HTTP {exc.status})."
            continue
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            LOG.warning("Dailymotion %s playlist unreachable: %s", label, type(exc).__name__)
            reason = f"Could not reach the Dailymotion {label} playlist ({type(exc).__name__})."
            continue
        formats = parse_master(text, candidate)
        if formats:
            LOG.info("Dailymotion qualities read from the %s playlist", label)
            return DailymotionInfo(title, duration, dailymotion_poster(url, poster), formats)
        reason = "No downloadable Dailymotion qualities were listed."
    try:
        info = await asyncio.to_thread(extract_dailymotion, url, proxy)
    except DailymotionError as exc:
        LOG.warning("Dailymotion fallback extraction failed: %s", exc)
        raise DailymotionError(api_error or reason or str(exc)) from exc
    LOG.info("Dailymotion qualities read straight from Dailymotion (Worker playlists refused)")
    return DailymotionInfo(title or info.title, duration or info.duration,
                           dailymotion_poster(url, poster or info.poster), info.formats,
                           info.media)


async def get_youtube_title(session, url: str, proxy: str | None = None) -> str:
    """YouTube title first; caller uses the API title if metadata is unavailable."""
    video_id = url.rsplit("=", 1)[-1]
    try:
        async with session.get("https://www.youtube.com/oembed", params={"url": url, "format": "json"},
                               timeout=aiohttp.ClientTimeout(total=12, connect=5)) as response:
            response.raise_for_status()
            data = await response.content.read(64 * 1024 + 1)
            if len(data) <= 64 * 1024:
                title = youtube_title(json.loads(data).get("title"), video_id)
                if title:
                    return title
    except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, TypeError, AttributeError) as exc:
        LOG.info("YouTube oEmbed lookup failed for %s: %s", video_id, exc)
    try:
        return await asyncio.wait_for(asyncio.to_thread(extract_title, url, proxy), timeout=35) or ""
    except Exception as exc:
        LOG.warning("YouTube metadata lookup failed for %s: %s", video_id, exc)
    return ""


BROWSER_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
              "Chrome/124.0 Safari/537.36")
MAX_SOCIAL_IMAGE_BYTES = 12 * 1024 * 1024


async def download_social_image(session: aiohttp.ClientSession, url: str, headers: dict,
                                limit: int) -> bytes:
    """Fetch an Instagram/Facebook image, bounded, following at most four redirects."""
    for _ in range(5):
        if not public_https_url(url):
            raise ValueError("The provider supplied an unsafe download link.")
        async with session.get(url, allow_redirects=False, headers=headers,
                               timeout=aiohttp.ClientTimeout(total=60, connect=20)) as response:
            if response.status in (301, 302, 303, 307, 308):
                location = response.headers.get("Location")
                if not location:
                    raise ValueError("Download redirect has no destination.")
                url = urljoin(str(response.url), location)
                continue
            response.raise_for_status()
            if response.content_type in ("text/html", "application/json"):
                raise ValueError("The media link returned a page instead of a file.")
            if response.content_length and response.content_length > limit:
                raise FileTooLarge()
            data = await response.content.read(limit + 1)
            if not data:
                raise ValueError("The media link returned an empty file.")
            if len(data) > limit:
                raise FileTooLarge()
            return data
    raise ValueError("Too many download redirects.")


def media_filename(title: str, item, index: int, total: int) -> str:
    """A safe file name for one social media item, numbered inside a selection."""
    safe = re.sub(r"[^\w .-]", "_", str(title or "media"), flags=re.UNICODE).strip(" .")[:80] or "media"
    prefix = f"{index:02d}-{total:02d} " if total > 1 else ""
    tag = re.sub(r"[^\w .-]", "_", item.label)[:24] or item.kind  # Spaces read better than "_".
    return f"{prefix}{safe} [{tag}]{item.suffix}"


def choose_title(youtube: str, provider: str, url: str) -> str:
    return youtube or provider or f"YouTube video {url.rsplit('=', 1)[-1]}"


class HTTPReader:
    """Async read(n) adapter for Telethon.upload_file; never buffers more than one part."""

    def __init__(self, response: aiohttp.ClientResponse, size: int, on_download=None, on_data=None):
        self.content = response.content
        self.remaining = size
        self.on_download = on_download
        self.on_data = on_data

    async def read(self, count: int) -> bytes:
        size = min(count, self.remaining)
        if not size:
            return b""
        data = await self.content.readexactly(size)
        self.remaining -= len(data)
        if self.on_download:
            self.on_download(len(data))
        if self.on_data:
            self.on_data(data)
        return data


async def upload_parts(client, reader: HTTPReader, size: int, filename: str,
                       in_flight: int, on_upload=None, on_flood=None):
    """Pipeline 1–3 MTProto parts, using the correct small/big file protocol."""
    file_id = helpers.generate_random_long()
    parts = (size + PART_BYTES - 1) // PART_BYTES
    if parts > MAX_PARTS:
        raise FileTooLarge()
    is_big = size > 10 * 1024 * 1024
    digest = hashlib.md5() if not is_big else None  # Telegram requires MD5 for small files.
    pending: set[asyncio.Task] = set()
    confirmed = 0

    async def send_part(index: int, data: bytes) -> None:
        nonlocal confirmed
        for attempt in range(3):
            try:
                request = (SaveBigFilePartRequest(file_id, index, parts, data) if is_big else
                           SaveFilePartRequest(file_id, index, data))
                if not await client(request):
                    raise ValueError(f"Telegram rejected upload part {index}.")
                confirmed += len(data)
                if on_upload:
                    on_upload(confirmed, size)
                return
            except FloodWaitError as exc:
                if attempt == 2 or exc.seconds > 300:
                    raise
                if on_flood:
                    on_flood(exc.seconds)
                await asyncio.sleep(exc.seconds + 1)
                if on_flood:
                    on_flood(0)

    try:
        for index in range(parts):
            data = await reader.read(PART_BYTES)
            if digest:
                digest.update(data)
            task = asyncio.create_task(send_part(index, data))
            pending.add(task)
            if len(pending) >= in_flight:
                done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                for completed in done:
                    completed.result()
        if pending:
            await asyncio.gather(*pending)
    except BaseException:
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        raise
    return (types.InputFileBig(file_id, parts, filename) if is_big else
            types.InputFile(file_id, parts, filename, digest.hexdigest()))


async def upload_stream(client: TelegramClient, session: aiohttp.ClientSession, url: str,
                        filename: str, max_bytes: int, *, on_total=None, on_download=None,
                        on_upload=None, on_flood=None, on_data=None, in_flight: int = 2,
                        expected_size: int = 0, headers: dict | None = None):
    """Download from CDN while uploading MTProto parts. Never store a 2 GB file on Koyeb.

Pipelines bounded saveBigFilePart/saveFilePart requests from the HTTP stream,
    with MD5 for small files; no unbounded buffering.
    """
    for _ in range(5):
        if not public_https_url(url):
            raise ValueError("The provider supplied an unsafe download link.")
        async with session.get(url, allow_redirects=False, auto_decompress=False, headers=headers,
                               timeout=aiohttp.ClientTimeout(total=None, connect=20, sock_read=120)) as response:
            if response.status in (301, 302, 303, 307, 308):
                location = response.headers.get("Location")
                if not location:
                    raise ValueError("Download redirect has no destination.")
                url = urljoin(url, location)
                continue
            response.raise_for_status()
            if response.content_type in ("text/html", "application/json", "text/plain"):
                raise ValueError("The download link returned a page instead of media.")
            size = response.content_length if response.content_length is not None else (expected_size or None)
            if size is not None:
                if size > min(max_bytes, MAX_PARTS * PART_BYTES):
                    raise FileTooLarge()
                if size == 0:
                    raise ValueError("The download link returned an empty file.")
                if on_total:
                    on_total(size)
                reader = HTTPReader(response, size, on_download, on_data)
                handle = await upload_parts(client, reader, size, filename,
                                            max(1, min(3, in_flight)), on_upload, on_flood)
                if reader.remaining or await response.content.read(1):
                    raise ValueError("The download size did not match Content-Length.")
                return handle
            # Without Content-Length, MTProto requires us to determine the size
            # first. Limit this disk fallback so Koyeb free is not overloaded.
            with tempfile.TemporaryDirectory(prefix="ytdown-small-") as directory:
                path = Path(directory) / ("audio.mp3" if filename.endswith(".mp3") else "video.mp4")
                count = 0
                with path.open("wb") as output:
                    async for chunk in response.content.iter_chunked(PART_BYTES):
                        count += len(chunk)
                        if count > min(max_bytes, SPOOL_MB * 1024 * 1024):
                            raise UnknownLength()
                        output.write(chunk)
                        if on_download:
                            on_download(len(chunk))
                        if on_data:
                            on_data(chunk)
                if not count:
                    raise ValueError("The download link returned an empty file.")
                if on_total:
                    on_total(count)
                return await client.upload_file(str(path), file_name=filename,
                                                part_size_kb=PART_BYTES // 1024,
                                                progress_callback=on_upload)
    raise ValueError("Too many download redirects.")


class DownloaderBot:
    def __init__(self, client: TelegramClient, session: aiohttp.ClientSession, *,
                 timeout: aiohttp.ClientTimeout | None = None, headers: dict | None = None):
        self.client = client
        self.http = session
        self.timeout = timeout or aiohttp.ClientTimeout(total=240, connect=20)
        self.headers = headers or {"User-Agent": "ytdown-telegram-bot/1.0"}
        self.selections: dict[int, Selection] = {}
        self.lookups: dict[int, asyncio.Task] = {}
        self.jobs: dict[str, Job] = {}
        self.api_overrides: dict[int, dict[str, str]] = {}  # uid -> {service: custom endpoint}
        self.awaiting_api: set[int] = set()
        self.awaiting_service: dict[int, str] = {}
        self.modes: dict[int, str] = {}
        self.mode_tokens: dict[int, str] = {}
        self.pending_ranges: dict[int, RangeRequest] = {}
        self.dashboards: dict[int, Dashboard] = {}
        # apitera automation: the watcher, its sections, its pace and its DM check.
        self.auto_enabled: dict[int, bool] = {}
        self.auto_tasks: dict[int, asyncio.Task] = {}
        self.auto_peers: dict[int, object] = {}
        self.auto_sections: dict[int, tuple[str, ...]] = {}
        self.auto_limits: dict[int, int] = {}
        self.auto_running: set[int] = set()
        self.auto_last: dict[int, float] = {}
        self.auto_tokens: dict[int, str] = {}
        self.latest_panels: dict[int, LatestPanel] = {}
        self.auto_interval = float(env_int("PARCIV_AUTO_INTERVAL", AUTO_INTERVAL_DEFAULT,
                                           AUTO_INTERVAL_MIN, 24 * 3600))
        self.auto_limit_default = env_int("PARCIV_AUTO_LIMIT", 3, 1, MAX_AUTO_LIMIT)
        self.status_lock = asyncio.Lock()  # Serialize /status replacements.
        self.slot = asyncio.Semaphore(1)
        self.turn = asyncio.Condition()  # Preserve enqueue order even if status edits are slow.
        self.max_bytes = env_int("MAX_FILE_MB", MAX_MT_MB, 1, MAX_MT_MB) * 1024 * 1024
        self.playlist_limit = env_int("MAX_RANGE_VIDEOS", MAX_RANGE_VIDEOS, 1, MAX_RANGE_VIDEOS)
        self.in_flight = env_int("UPLOAD_IN_FLIGHT", 2, 1, 3)
        self.metrics = ContainerMetrics()
        # Proxy pools are per user: each user brings (and checks) their own list.
        self.pools: dict[int, ProxyPool] = {}
        self.routers: dict[int, ProxyRouter] = {}
        self.proxy_state: dict[int, bool] = {}
        self.proxy_tokens: dict[int, str] = {}
        self.awaiting_proxy: set[int] = set()
        self.proxy_checks: dict[int, asyncio.Task] = {}
        self.proxy_default_on = env_flag("PROXY_ENABLED", False)
        self.proxy_strict = env_flag("PROXY_STRICT", False)
        self.proxy_pool_max = env_int("PROXY_POOL_MAX", PROXY_POOL_MAX, 1, MAX_PROXIES)
        self.proxy_check_max = env_int("PROXY_CHECK_MAX", PROXY_CHECK_MAX, 1, MAX_PROXIES)
        self.proxy_workers = env_int("PROXY_CHECK_WORKERS", 24, 1, 100)
        self.proxy_timeout = float(env_int("PROXY_CHECK_TIMEOUT", 8, 2, 60))
        self.proxy_fail_limit = env_int("PROXY_FAIL_LIMIT", 2, 1, 10)
        # Replies faster than this cannot have come from a real round trip.
        self.proxy_min_latency = env_int("PROXY_MIN_LATENCY_MS", MIN_LATENCY_MS, 0, 5000)
        self.proxy_min_latency_user: dict[int, int] = {}  # Per-user /proxychk fast override.
        check_url = os.getenv("PROXY_CHECK_URL", "").strip()
        check_url = check_url if public_https_url(check_url) else ""
        self.proxy_check_keys = os.getenv("PROXY_CHECK_SITES", "") or os.getenv("PROXY_CHECK_SITE", "")
        # Extra URLs added with /proxychk url, applied on top of the site probes.
        self.proxy_check_urls: list[str] = [check_url] if check_url else []
        # The sites a check tests; per-user API probes are resolved at check time.
        self.proxy_check_targets = resolve_targets(self.proxy_check_keys, check_url)
        seed_text = os.getenv("PROXY_LIST", "")
        self.proxy_seed = parse_proxies(seed_text, MAX_PROXIES)[0]
        # A list exported by /proxychk export can go straight into PROXY_LIST:
        # its site marks come back too.
        self.proxy_seed_marks = parse_proxy_marks(seed_text)

    def user_jobs(self, uid: int) -> list[Job]:
        return [job for job in self.jobs.values() if job.user_id == uid]

    def api_for(self, uid: int, service: str = "youtube") -> str:
        return self.api_overrides.get(uid, {}).get(service, DEFAULT_APIS[service])

    def apis_for(self, uid: int) -> dict[str, str]:
        """Every service endpoint this user's traffic uses: built-in or their own."""
        return {service: self.api_for(uid, service) for service in SERVICE_KEYS}

    def api_hosts_for(self, uid: int) -> dict[str, str]:
        """API host -> service, so pool routing can tell which API a request hits."""
        hosts: dict[str, str] = {}
        for service, endpoint in self.apis_for(uid).items():
            host = (urlsplit(endpoint).hostname or "").lower()
            if host:
                hosts[host] = service
        return hosts

    def set_api(self, uid: int, raw: str, service: str = "youtube") -> str:
        endpoint = normalize_api_endpoint(raw)
        self.api_overrides.setdefault(uid, {})[service] = endpoint
        self.awaiting_api.discard(uid)
        router = self.routers.get(uid)
        if router is not None:  # Keep site-aware routing on the new host.
            router.api_hosts = self.api_hosts_for(uid)
        return endpoint

    def refresh_check_targets(self) -> None:
        """Rebuild the site-level probe list after /proxychk sites or url changes."""
        self.proxy_check_targets = self.apply_check_urls(resolve_targets(self.proxy_check_keys))

    def apply_check_urls(self, targets: tuple[CheckTarget, ...]) -> tuple[CheckTarget, ...]:
        """Layer the user's own check URLs on top; a site URL replaces that site's probe."""
        for url in self.proxy_check_urls:
            targets = with_url(targets, url)
        return targets

    def api_side_note(self, count: int, results: list[CheckResult]) -> str:
        """Explain proxies that got a real HTTP answer from an API that was unusable."""
        reasons: dict[str, int] = {}
        for result in results:
            if result.verdict != "api":
                continue
            for site in result.sites:
                if not site.ok and site.detail:
                    reasons[site.detail] = reasons.get(site.detail, 0) + 1
        top = sorted(reasons.items(), key=lambda item: (-item[1], item[0]))[:2]
        detail = " · ".join(f"{reason} ×{hits}" for reason, hits in top)
        return (f"⚠️ {count} prox{'y' if count == 1 else 'ies'} reached the API but got no usable "
                f"answer ({detail}). They stay in the pool instead of being marked dead"
                + (": that many identical answers usually mean the API itself is failing — check it "
                   "with /apis and /changeapi." if count > 1 else "."))

    def check_targets_for(self, uid: int) -> tuple[CheckTarget, ...]:
        """The probes a check runs for this user: their own APIs where they exist.

        Probing the API the bot really calls — not just the site's front page —
        is what tells a proxy that can *use* the service from one that merely
        reaches it.
        """
        return self.apply_check_urls(
            resolve_targets(self.proxy_check_keys, apis=self.apis_for(uid)))

    # -- proxy pool ------------------------------------------------------
    def pool_for(self, uid: int) -> ProxyPool:
        """This user's pool, seeded once from the PROXY_LIST environment value."""
        pool = self.pools.get(uid)
        if pool is None:
            pool = ProxyPool(self.proxy_pool_max, self.proxy_fail_limit)
            if self.proxy_seed:
                pool.add(self.proxy_seed, self.proxy_seed_marks)
            self.pools[uid] = pool
        return pool

    def proxy_enabled(self, uid: int) -> bool:
        return self.proxy_state.get(uid, self.proxy_default_on)

    def min_latency_for(self, uid: int) -> int:
        return self.proxy_min_latency_user.get(uid, self.proxy_min_latency)

    def fast_guard_text(self, uid: int) -> str:
        value = self.min_latency_for(uid)
        return (f"Replies under {value} ms auto-fail (PROXY_MIN_LATENCY_MS / /proxychk fast)"
                if value else "Too-fast guard off")

    def router_for(self, uid: int) -> ProxyRouter:
        router = self.routers.get(uid)
        if router is None:
            router = ProxyRouter(self.pool_for(uid), self.http, timeout=self.timeout,
                                 headers=self.headers, strict=self.proxy_strict,
                                 on_drop=lambda state, detail: LOG.warning(
                                     "User %s lost proxy %s: %s", uid, state.proxy.label, detail))
            router.api_hosts = self.api_hosts_for(uid)
            self.routers[uid] = router
        return router

    def http_for(self, uid: int):
        """Session used for this user's provider traffic (the router when proxy mode is on)."""
        return self.router_for(uid) if self.proxy_enabled(uid) else self.http

    def proxy_for(self, uid: int, site: str = "youtube") -> Proxy | None:
        """One live proxy for a URL-only caller (yt-dlp), or None for direct.

        The yt-dlp helpers all read YouTube, so a proxy already known to be
        blocked by YouTube is skipped in favour of one that reached it.
        """
        if not self.proxy_enabled(uid):
            return None
        return self.router_for(uid).next_proxy(site)

    async def youtube_task(self, uid: int, func, *args):
        """Run a blocking yt-dlp helper, moving on when its proxy is unusable."""
        for attempt in range(3):
            proxy = self.proxy_for(uid)
            try:
                return await asyncio.to_thread(func, *args, proxy=proxy.url if proxy else None)
            except Exception as exc:
                router = self.routers.get(uid)
                if proxy is None or router is None or attempt == 2 or not proxy_fault(exc):
                    raise
                dropped = router.note_failure_url(proxy.url, short_reason(exc))
                LOG.warning("%s failed through %s: %s%s", getattr(func, "__name__", "yt-dlp"),
                            proxy.label, short_reason(exc),
                            " (proxy removed)" if dropped else "")
        raise ValueError("The link provider could not be reached.")

    async def close_router(self, uid: int) -> None:
        router = self.routers.pop(uid, None)
        if router is not None:
            await router.close()

    async def close(self) -> None:
        """Cancel proxy checks and release every routed session."""
        checks = [task for task in self.proxy_checks.values() if task]
        for task in checks:
            task.cancel()
        await asyncio.gather(*checks, return_exceptions=True)
        self.proxy_checks.clear()
        for router in list(self.routers.values()):
            await router.close()
        self.routers.clear()

    def proxy_line(self, uid: int) -> str:
        if not self.proxy_enabled(uid):
            return ""
        return f"\n🌐 Proxy: {self.router_for(uid).describe()}"

    def proxy_status_text(self, uid: int) -> str:
        pool = self.pool_for(uid)
        on = self.proxy_enabled(uid)
        targets = self.check_targets_for(uid)
        lines = [f"🌐 Proxy mode: {'ON' if on else 'OFF'}",
                 "Check sites: " + " + ".join(target_label(target.key) for target in targets)
                 + " — /proxychk sites youtube,terabox,rumble or /proxychk url <https URL> "
                   "changes this",
                 self.fast_guard_text(uid),
                 pool.summary()]
        counts = (pool.last_check.sites if pool.last_check
                  else {target.key: len(pool.live_for(target.key)) for target in targets})
        if counts:
            lines.append("Reaching: " + " · ".join(f"{target_label(key)} {value}"
                                                   for key, value in counts.items()))
        if on:
            router = self.router_for(uid)
            lines.append(f"Route: {router.describe()}")
            live = sorted(pool.live(), key=lambda state: state.latency_ms or 10 ** 6)[:10]
            if live:
                lines.append("Live now: " + ", ".join(
                    f"{state.proxy.label}" + (f" ({state.latency_ms} ms)" if state.latency_ms else "")
                    for state in live) + (f" … +{len(pool.live()) - len(live)} more"
                                          if len(pool.live()) > len(live) else ""))
            if router.dropped:
                lines.append("Recently removed: " + "; ".join(list(router.dropped)[-3:]))
            lines.append("Fallback: " + ("none (strict: requests fail without a proxy)"
                                         if self.proxy_strict else
                                         "a direct connection when no proxy is usable"))
        else:
            lines.append("Requests go direct while proxy mode is off.")
        lines.append("Re-check or add proxies with /proxychk.")
        return "\n".join(lines)

    def proxy_buttons(self, uid: int):
        token = self.proxy_tokens[uid]
        on = self.proxy_enabled(uid)
        return [[Button.inline(("✅ " if on else "▫️ ") + "ON", data=f"p:{token}:on".encode()),
                 Button.inline(("✅ " if not on else "▫️ ") + "OFF", data=f"p:{token}:off".encode())],
                [Button.inline("🔄 Re-check pool", data=f"p:{token}:recheck".encode()),
                 Button.inline("📊 Pool status", data=f"p:{token}:status".encode())],
                [Button.inline("🧹 Remove dead", data=f"p:{token}:purge".encode()),
                 Button.inline("🗑 Clear pool", data=f"p:{token}:clear".encode())],
                [Button.inline("📄 Export live", data=f"p:{token}:export".encode())]]

    async def show_proxy_panel(self, event) -> None:
        uid = event.sender_id
        self.proxy_tokens[uid] = secrets.token_hex(4)
        await event.respond(self.proxy_status_text(uid), parse_mode=None,
                            buttons=self.proxy_buttons(uid))

    async def edit_proxy_panel(self, event, uid: int, text: str) -> None:
        try:
            await event.edit(text, parse_mode=None, buttons=self.proxy_buttons(uid))
        except Exception:
            LOG.warning("Could not update the proxy panel", exc_info=True)

    async def proxy_action(self, event, uid: int, action: str) -> None:
        if action == "on":
            self.proxy_state[uid] = True
            pool = self.pool_for(uid)
            await event.answer("Proxy mode is ON")
            notice = "" if len(pool) else "\nThe pool is empty — add proxies with /proxychk."
            await self.edit_proxy_panel(event, uid, self.proxy_status_text(uid) + notice)
        elif action == "off":
            self.proxy_state[uid] = False
            await self.close_router(uid)
            await event.answer("Proxy mode is OFF")
            await self.edit_proxy_panel(event, uid, self.proxy_status_text(uid))
        elif action == "status":
            await event.answer()
            await self.edit_proxy_panel(event, uid, self.proxy_status_text(uid))
        elif action == "purge":
            removed = self.pool_for(uid).purge_dead()
            await event.answer(f"Removed {removed} dead proxies")
            await self.edit_proxy_panel(event, uid, self.proxy_status_text(uid))
        elif action == "clear":
            count = len(self.pool_for(uid))
            self.pool_for(uid).clear()
            self.proxy_state[uid] = False
            await self.close_router(uid)
            await event.answer(f"Cleared {count} proxies")
            await self.edit_proxy_panel(event, uid, self.proxy_status_text(uid))
        elif action == "export":
            await event.answer("Sending the list…")
            await self.proxy_export_command(event)
        elif action == "recheck":
            if uid in self.proxy_checks:
                await event.answer("A proxy check is already running.", alert=True)
                return
            proxies = [state.proxy for state in self.pool_for(uid).entries.values()]
            if not proxies:
                await event.answer("The pool is empty. Use /proxychk to add proxies.", alert=True)
                return
            await event.answer(f"Checking {len(proxies)} proxies…")
            self.start_proxy_check(uid, proxies,
                                   edit=lambda text: self.edit_proxy_panel(event, uid, text))
        else:
            await event.answer("Unknown proxy action.", alert=True)

    async def edit_text(self, edit, text: str) -> None:
        """Progress edits are best-effort: a Telegram hiccup must not stop a check."""
        try:
            await edit(text)
        except Exception:
            LOG.warning("Could not edit a proxy progress message", exc_info=True)

    def start_proxy_check(self, uid: int, proxies: list[Proxy], *, edit, notice=None,
                          notes: list[str] | None = None,
                          marks: Mapping[str, Mapping[str, bool]] | None = None) -> None:
        task = asyncio.create_task(self.run_proxy_check(uid, proxies, edit=edit, notice=notice,
                                                        notes=notes or [], marks=marks or {}))
        self.proxy_checks[uid] = task

        def finished(done: asyncio.Task) -> None:
            if self.proxy_checks.get(uid) is done:
                self.proxy_checks.pop(uid, None)
            if not done.cancelled() and done.exception() is not None:
                LOG.error("Proxy check failed", exc_info=done.exception())

        task.add_done_callback(finished)

    async def run_proxy_check(self, uid: int, proxies: list[Proxy], *, edit, notice=None,
                              notes: list[str],
                              marks: Mapping[str, Mapping[str, bool]] | None = None) -> None:
        """Check every proxy against every configured site and report what works."""
        pool = self.pool_for(uid)
        added = pool.add(proxies, marks)  # marks: an exported list pasted back
        targets = self.check_targets_for(uid)
        names = " + ".join(target.label for target in targets)
        min_latency = self.min_latency_for(uid)
        started, finished = time.monotonic(), 0
        working = 0
        site_counts = {target.key: 0 for target in targets}
        next_edit = started
        await self.edit_text(edit, f"🔎 Checking {len(proxies)} proxies against {names}"
                                   + (f" · under {min_latency} ms fails" if min_latency else "")
                                   + "…")

        api_side = 0

        async def on_progress(count: int, total: int, result: CheckResult) -> None:
            nonlocal next_edit, finished, working, api_side
            finished = count
            working += 1 if result.ok else 0
            api_side += 1 if result.verdict == "api" else 0
            for site in result.sites:
                if site.ok:
                    site_counts[site.key] = site_counts.get(site.key, 0) + 1
            pool.mark_checked(result)  # Saved as it goes, so /cancel keeps the results.
            now = time.monotonic()
            if now >= next_edit or count == total:
                next_edit = now + PROXY_PROGRESS_SECONDS
                per_site = " · ".join(f"{target_label(key)} {value}"
                                      for key, value in site_counts.items())
                await self.edit_text(edit, f"🔎 Checking {total} proxies ({names})… {count}/{total} "
                                           f"done · {per_site}")

        try:
            results = await run_checks(proxies, targets=targets, timeout=self.proxy_timeout,
                                       workers=self.proxy_workers,
                                       min_latency_ms=min_latency, on_progress=on_progress)
        except asyncio.CancelledError:
            pool.last_check = CheckStats(time.monotonic(), finished, working, dict(site_counts))
            await self.edit_text(edit, f"Proxy check cancelled after {finished}/{len(proxies)} "
                                       f"({working} reachable).\n" + pool.summary())
            raise
        pool.last_check = CheckStats(time.monotonic(), len(results), working, dict(site_counts))
        live = [result for result in results if result.ok]
        lines = [f"✅ Checked {len(results)} proxies in {human_eta(time.monotonic() - started)} "
                 f"against {names}",
                 "Per site: " + " · ".join(f"{target_label(key)} {value} ok"
                                           for key, value in site_counts.items()),
                 f"Reachable: {working} · no service answered: {len(results) - working}",
                 pool.summary()]
        if api_side:
            lines.append(self.api_side_note(api_side, results))
        if added.overflow:
            lines.append(f"Pool limit is {pool.limit}, so {added.overflow} proxies were not stored.")
        if added.duplicates:
            lines.append(f"{added.duplicates} were already in the pool.")
        too_fast = [result for result in results
                    if result.sites and all(site.detail.startswith(TOO_FAST) for site in result.sites)]
        fast_sites = sum(1 for result in results for site in result.sites
                         if site.detail.startswith(TOO_FAST))
        if fast_sites and min_latency:
            examples = ", ".join(result.proxy.label for result in too_fast[:3])
            lines.append(f"⚡ {fast_sites} repl{'y' if fast_sites == 1 else 'ies'} arrived under "
                         f"{min_latency} ms and failed automatically — that is too fast for a real "
                         "round trip through a proxy"
                         + (f" (for example {examples})" if examples else "")
                         + ". /proxychk fast <ms> tunes this, /proxychk fast off disables it.")
        lines.extend(notes[:3])
        if any(target.json for target in targets):
            lines.append("Checked with each service's own API"
                         + (" (including your custom endpoints)" if self.api_overrides.get(uid) else "")
                         + " — only proxies that get a real API answer pass.")
        if live:
            fastest = sorted(live, key=lambda item: item.latency_ms)
            if working <= PROXY_INLINE_LIST:
                lines.append("Working proxies:")
                lines.extend(f"• {item.proxy.label} — {site_marks(item.marks())} "
                             f"({item.latency_ms} ms)" for item in fastest)
            else:
                lines.append("Fastest: " + ", ".join(
                    f"{item.proxy.label} [{site_marks(item.marks())}] ({item.latency_ms} ms)"
                    for item in fastest[:5]))
        else:
            lines.append("No proxy reached any checked site; they stay in the pool marked dead.")
        lines.append("Proxy mode is " + ("ON." if self.proxy_enabled(uid)
                                         else "OFF — turn it on with /proxy."))
        await self.edit_text(edit, "\n".join(lines))
        if notice is not None and live:
            # Site marks travel as comments so the file can be pasted back later.
            await self.send_proxy_list(notice, [f"{result.proxy.url}  # {site_marks(result.marks())}"
                                                for result in live])
        LOG.info("Proxy check for user %s: %d/%d reachable (%d stored, %d duplicates)",
                 uid, working, len(results), added.added, added.duplicates)

    async def send_proxy_list(self, notice, urls: list[str]) -> None:
        """Hand back the working list so a large pool can be reused after a restart."""
        if len(urls) <= PROXY_INLINE_LIST:
            return
        buffer = io.BytesIO(("\n".join(urls) + "\n").encode())
        buffer.name = "working_proxies.txt"
        try:
            await notice.respond(file=buffer)
        except Exception:
            LOG.warning("Could not send the working proxy list", exc_info=True)

    def exportable(self, uid: int, everything: bool = False) -> list[ProxyState]:
        """Pool entries worth saving: the live ones, plus untested ones on request."""
        states = [state for state in self.pool_for(uid).entries.values()
                  if state.status == "alive" or (everything and state.status == "untested")]
        return sorted(states, key=lambda state: (state.status != "alive", state.latency_ms))

    async def proxy_export_command(self, event, argument: str = "") -> None:
        """Send the pool back as a .txt list that can be pasted in after a restart."""
        uid = event.sender_id
        everything = argument.strip().lower() in ("all", "untested", "every", "everything")
        pool = self.pool_for(uid)
        states = self.exportable(uid, everything)
        if not states:
            if not len(pool):
                note = "The pool is empty — add proxies with /proxychk."
            elif everything:
                note = f"All {len(pool)} stored proxies are marked dead — nothing to export."
            else:
                note = (f"The pool holds {len(pool)} proxies, but none of them is live yet — run "
                        "/proxychk, or use /proxychk export all to save the untested ones too.")
            await event.respond(note, parse_mode=None)
            return
        live = sum(1 for state in states if state.status == "alive")
        header = (f"# {live} live of {len(states)} proxies exported with /proxychk export on "
                  f"{time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())}. Paste this file back "
                  "with /proxychk: the marks after # (DM, RU, YT, TB) come back with it.")
        buffer = io.BytesIO(export_text(states, header).encode())
        buffer.name = "working_proxies.txt"
        caption = (f"📄 {live} live prox{'y' if live == 1 else 'ies'}"
                   + (f" and {len(states) - live} untested" if len(states) > live else "")
                   + " saved. Paste this file into /proxychk after a restart and they come back "
                     "alive with their site marks. To restore them automatically at every start, "
                     "put the same lines in the PROXY_LIST environment variable.")
        try:
            await event.respond(caption, file=buffer, parse_mode=None)
        except Exception:
            LOG.warning("Could not send the exported proxy list", exc_info=True)
            if buffer.getbuffer().nbytes <= 3_500:  # Small pools can travel as text.
                await event.respond(caption + "\n\nCould not send the file, so here is the "
                                    "list:\n" + buffer.getvalue().decode(), parse_mode=None)
            else:
                await event.respond("Could not send the proxy file here — try "
                                    "/proxychk export again.", parse_mode=None)

    async def proxy_url_command(self, event, url: str) -> None:
        """Point proxy checks at your own API host instead of the first site."""
        if not url:
            await event.respond("Send the URL to test with, for example "
                                "/proxychk url https://api.example.com/healthz")
            return
        if not public_https_url(url):
            await event.respond("Use a public HTTPS URL for this check "
                                "(no localhost, private IPs, or explicit ports).")
            return
        host = urlsplit(url).hostname or "custom"
        replaced = next((target for target in self.proxy_check_targets
                         if target.url == url or target.key == (target_for(url) or "")), None)
        if url not in self.proxy_check_urls:
            self.proxy_check_urls = (self.proxy_check_urls + [url])[-3:]  # At most one custom probe.
        self.refresh_check_targets()
        action = (f"replaces the {replaced.label} probe" if replaced is not None
                  else "is added to the checks")
        await event.respond(f"{url} ({host}) {action}: now testing "
                            + " + ".join(f"{target.label} ({target.url})"
                                         for target in self.check_targets_for(event.sender_id))
                            + ". Send /proxychk to test a list, or /proxy → Re-check pool.")

    async def proxy_sites_command(self, event, keys: str) -> None:
        """Choose which sites a proxy check tests."""
        known = {target.key for target in KNOWN_TARGETS}  # YouTube stays selectable by name.
        parts = [part for part in re.split(r"[,;\s]+", keys.lower()) if part]
        if parts and all(part in ("default", "all", "reset", "builtin") for part in parts):
            self.proxy_check_keys = ""
            self.refresh_check_targets()
            await event.respond("Proxy checks restored to the default "
                                + " + ".join(target.label for target in CHECK_TARGETS) + ".")
            return
        unknown = [part for part in parts if part not in known]
        if unknown:
            await event.respond("Unknown site: " + ", ".join(unknown) + ". Use dailymotion, rumble, "
                                "youtube, terabox, mega, instagram, facebook or apitera (for example "
                                "/proxychk sites rumble,youtube), or /proxychk sites default for "
                                "Dailymotion + Rumble.")
            return
        self.proxy_check_keys = keys
        self.refresh_check_targets()
        await event.respond("Proxy checks will test "
                            + " + ".join(f"{target.label} ({target.url})"
                                         for target in self.check_targets_for(event.sender_id))
                            + ". Send /proxychk to test a list, or /proxy → Re-check pool.")

    async def proxy_fast_command(self, event, value: str) -> None:
        """Set the too-fast guard: replies faster than this always fail."""
        uid = event.sender_id
        if value.lower() in ("off", "0", "none"):
            self.proxy_min_latency_user[uid] = 0
            await event.respond("Too-fast guard off: instant replies count as a pass again.")
            return
        if not value.isdigit():
            await event.respond(f"Use /proxychk fast <milliseconds> or off. Current: "
                                f"{self.fast_guard_text(uid)}")
            return
        milliseconds = max(0, min(5000, int(value)))
        self.proxy_min_latency_user[uid] = milliseconds
        await event.respond(f"Proxy replies faster than {milliseconds} ms now fail the check "
                            "instead of counting as working." if milliseconds else
                            "Too-fast guard off.")

    async def on_proxy_text(self, event, text: str) -> None:
        uid = event.sender_id
        if uid in self.proxy_checks:
            await event.respond("A proxy check is already running. Wait for it, or stop it with "
                                "/proxychk stop.")
            return
        proxies, errors = parse_proxies(text, self.proxy_check_max)
        if not proxies:
            detail = f"\nFirst problem: {errors[0]}" if errors else ""
            await event.respond("No usable proxies found. Send lines such as host:port or "
                                "user:pass@host:port." + detail)
            return
        marks = parse_proxy_marks(text)  # Live again before the check finishes.
        notice = await event.respond(
            f"Checking {len(proxies)} proxies against "
            + " + ".join(target.label for target in self.proxy_check_targets) + "…"
            + (f" {len(marks)} carry marks from an earlier export — they are live again."
               if marks else "")
            + " Large lists take a while; /proxychk stop cancels.")
        self.start_proxy_check(uid, proxies, edit=lambda body: notice.edit(body),
                               notice=notice, notes=errors, marks=marks)

    async def on_proxy_file(self, event) -> None:
        message = getattr(event, "message", None)
        document = getattr(message, "document", None)
        if document is None:
            await event.respond("Send the proxies as text or as a .txt file.")
            return
        size = getattr(document, "size", 0) or 0
        name = ""
        for attribute in getattr(document, "attributes", None) or []:
            name = getattr(attribute, "file_name", None) or name
        suffix = Path(name).suffix.lower()
        mime = (getattr(document, "mime_type", "") or "").lower()
        if suffix not in (".txt", ".csv", ".list", ".text", ".conf", ".json", ".proxies") \
                and not mime.startswith("text/"):
            await event.respond("Send the proxy list as a .txt or .csv file (got "
                                f"{suffix or mime or 'an unknown file'}).")
            return
        if size > PROXY_FILE_MAX:
            await event.respond(f"That file is too large ({human_size(size)}); keep proxy lists "
                                f"under {human_size(PROXY_FILE_MAX)}.")
            return
        buffer = io.BytesIO()
        try:
            data = await event.client.download_media(message, file=buffer)
        except (RPCError, OSError, ValueError) as exc:
            LOG.warning("Could not read a proxy file: %s", type(exc).__name__)
            await event.respond("Could not read that file. Try pasting the proxies instead.")
            return
        payload = data.getvalue() if hasattr(data, "getvalue") else bytes(data or b"")
        if len(payload) > PROXY_FILE_MAX:
            await event.respond("That file is too large; keep proxy lists under "
                                f"{human_size(PROXY_FILE_MAX)}.")
            return
        await self.on_proxy_text(event, payload.decode("utf-8", errors="replace"))

    def clear_api(self, uid: int, service: str | None = None) -> bool:
        """Restore one service's built-in API, or all of them when service is None."""
        overrides = self.api_overrides.get(uid, {})
        if service is None:
            removed = bool(overrides)
            self.api_overrides.pop(uid, None)
        else:
            removed = overrides.pop(service, None) is not None
            if not overrides:
                self.api_overrides.pop(uid, None)
        self.awaiting_api.discard(uid)
        return removed

    def api_lines(self, uid: int) -> list[str]:
        lines = []
        for service, label in SERVICE_LABELS.items():
            custom = self.api_overrides.get(uid, {}).get(service)
            lines.append(f"{label}: {custom} (custom)" if custom else f"{label}: {DEFAULT_APIS[service]} (built-in)")
        return lines


    def register(self) -> None:
        self.client.add_event_handler(self.on_message, events.NewMessage(incoming=True))
        self.client.add_event_handler(self.callback, events.CallbackQuery(pattern=CALLBACK_PATTERN))

    def job_buttons(self, job: Job):
        if job.state == "finished":
            return None
        return [[Button.inline("🔄 Refresh", data=f"j:{job.token}:refresh".encode())]]

    def format_job(self, job: Job, *, include_metrics: bool = True) -> str:
        now = time.monotonic()
        elapsed = now - job.sample_at
        if elapsed >= 3:
            new_speed = max(0, job.uploaded - job.sample_bytes) / elapsed
            job.speed = new_speed if job.speed == 0 else (0.5 * job.speed + 0.5 * new_speed)
            job.sample_bytes, job.sample_at = job.uploaded, now
        total = job.total_bytes
        ratio = min(1.0, job.uploaded / total) if total else 0
        filled = min(12, int(ratio * 12))
        eta = human_eta((total - job.uploaded) / job.speed) if total and job.speed > 1 else "—"
        waiting = [j for j in self.jobs.values() if j.state == "queued"]
        item = f"{job.item_index}/{job.item_total}" if job.item_total else "—"
        # Terabox status lines keep their original format; YouTube titles get subtitle tags.
        youtube = job.source.kind in ("video", "playlist", "rumble")
        clean, tag = split_subtitle_tag(job.title) if youtube else (job.title, "")
        title = clean[:95] or ("Playlist" if job.source.kind == "playlist" else
                               "Terabox files" if job.source.kind.startswith("terabox") else
                               "Dailymotion video" if job.source.kind == "dailymotion" else
                               "Rumble video" if job.source.kind == "rumble" else
                               "apitera batch" if job.source.kind == "parciv" else "YouTube video")
        quality = (f" [{'MP3' if youtube and job.quality == AUDIO else job.quality}]" if job.quality else "") + (
            f" {tag}" if tag else "")
        status = (f"Telegram rate limit: waiting {human_eta(job.rate_limit_until - now)}"
                  if job.rate_limit_until > now else job.detail)
        queue_note = f" · Queue #{waiting.index(job) + 1}" if job in waiting else ""
        return (f"🎬 {title}{quality}\n"
                f"📋 Video {item} · {job.state.title()}{queue_note}\n\n"
                f"{'█' * filled}{'░' * (12 - filled)}  {ratio * 100:.1f}%\n"
                f"⬆ Telegram: {human_size(job.uploaded)} / {human_size(total) if total else 'unknown'}\n"
                f"⬇ Source: {human_size(job.downloaded)}\n"
                f"🚀 {human_size(job.speed)}/s · ⏳ ETA {eta}\n\n"
                f"📌 {status}\n"
                f"🆔 {job.token}"
                + (f" · /cancel_{job.token}" if job.state != "finished" else "")
                + (f"\n📊 {self.metrics.describe()}\nTasks in bot: {len(self.jobs)}" if include_metrics else ""))

    def dashboard_buttons(self, uid: int, dashboard: Dashboard):
        return [[Button.inline("🔄 Refresh all", data=f"d:{dashboard.token}:refresh".encode())]]

    def format_dashboard(self, uid: int) -> str:
        # Only running and queued jobs are shown; finished or cancelled jobs disappear at once.
        active = self.user_jobs(uid)
        panels = [self.format_job(job, include_metrics=False) for job in active]
        if not panels:
            panels = ["No active transfers. Send a link to begin."]
        body = "📋 Your downloads\n\n" + "\n\n──────────\n\n".join(panels)
        if uid in self.lookups:
            body += "\n\n🔎 Looking up a link…"
        body += (f"\n\n📊 {self.metrics.describe()}\n"
                 f"Active/queued: {len(active)} · tap a /cancel_JOBID command to stop it"
                 + self.proxy_line(uid))
        # Leave room for UTF-16 surrogate pairs (Telegram's 4,096-unit limit).
        return body if len(body) <= 3600 else body[:3500] + "\n… use /cancel for job IDs."

    async def render_dashboard(self, uid: int, *, force: bool = False) -> None:
        dashboard = self.dashboards.get(uid)
        if not dashboard:
            return
        async with dashboard.lock:
            if self.dashboards.get(uid) is not dashboard:
                return
            now = time.monotonic()
            if now < dashboard.edit_flood_until or (not force and now < dashboard.next_edit):
                return
            dashboard.next_edit = now + REPORT_SECONDS
            try:
                text = self.format_dashboard(uid)
                await self.client.edit_message(dashboard.peer, dashboard.message_id,
                                               text, parse_mode=None,
                                               formatting_entities=cancel_entities(text),
                                               buttons=self.dashboard_buttons(uid, dashboard))
            except MessageNotModifiedError:
                pass
            except FloodWaitError as exc:
                dashboard.edit_flood_until = time.monotonic() + exc.seconds + 3
                dashboard.next_edit = dashboard.edit_flood_until
            except RPCError:
                LOG.warning("Could not update dashboard for %s; will retry", uid, exc_info=True)

    async def show_dashboard(self, event) -> None:
        peer = await event.get_input_chat()
        if not await self.replace_dashboard(event.sender_id, peer, event.respond):
            await event.respond("Too many live status dashboards. Please try again later.")

    async def replace_dashboard(self, uid: int, peer, send) -> bool:
        """Publish a new combined status and retire the old status panels.

        Used by /status and by Done, so a newly queued job is immediately
        visible without requiring an explicit /status. The send completes
        before old messages are deleted to avoid losing the only status.
        """
        async with self.status_lock:
            if uid not in self.dashboards and len(self.dashboards) >= MAX_DASHBOARDS:
                idle = [(owner, panel) for owner, panel in self.dashboards.items()
                        if not self.user_jobs(owner)]
                if not idle:
                    return False
                oldest = min(idle, key=lambda item: item[1].created)[0]
                self.dashboards.pop(oldest, None)
            old = self.dashboards.get(uid)
            dashboard = Dashboard(peer, 0)
            text = self.format_dashboard(uid)
            message = await send(text, parse_mode=None,
                                 formatting_entities=cancel_entities(text),
                                 buttons=self.dashboard_buttons(uid, dashboard))
            dashboard.message_id = message.id
            self.dashboards[uid] = dashboard
            ids = [job.message_id for job in self.user_jobs(uid) if job.message_id != message.id]
            if old and old.message_id != message.id:
                ids.append(old.message_id)
            if ids:
                try:
                    await self.client.delete_messages(peer, list(set(ids)))
                except RPCError:
                    LOG.warning("Could not remove old status panels", exc_info=True)
            return True

    async def render(self, job: Job, *, force: bool = False) -> None:
        if job.user_id in self.dashboards:
            await self.render_dashboard(job.user_id, force=force)
            return
        async with job.edit_lock:
            now = time.monotonic()
            if now < job.edit_flood_until or (not force and now < job.next_edit):
                return
            job.next_edit = now + REPORT_SECONDS
            try:
                text = self.format_job(job)
                await self.client.edit_message(job.peer, job.message_id, text,
                                               buttons=self.job_buttons(job), parse_mode=None,
                                               formatting_entities=cancel_entities(text))
            except MessageNotModifiedError:
                pass
            except FloodWaitError as exc:
                job.edit_flood_until = time.monotonic() + exc.seconds + 3
                job.next_edit = job.edit_flood_until
                LOG.info("Throttling status edits for %ss due to Telegram flood wait", exc.seconds)
            except RPCError:
                LOG.warning("Could not update job status", exc_info=True)

    async def report_loop(self, job: Job) -> None:
        while True:
            await asyncio.sleep(REPORT_SECONDS)
            await self.render(job)


    async def handle_changeapi(self, event, text: str) -> None:
        uid = event.sender_id
        arg = text.partition(" ")[2].strip()
        head, _, tail = arg.partition(" ")
        head, tail = head.lower(), tail.strip()
        if not arg:
            await event.respond("Choose a service with /changeapi yt, tera, daily, rumble, mega or ig, "
                                "then send its public HTTPS API URL, for example "
                                "/changeapi yt https://example.com/?url=. /changeapi show lists them; "
                                "/changeapi reset restores all built-in APIs.")
            return
        if head == "show":
            await event.respond("\n".join(self.api_lines(uid)), parse_mode=None)
            return
        if head in RESET_WORDS:
            if tail and tail.lower() not in SERVICE_KEYWORDS:
                await event.respond("Unknown service. Use yt, tera, daily, rumble, mega or ig.")
                return
            service = SERVICE_KEYWORDS[tail.lower()] if tail else None
            self.clear_api(uid, service)
            await event.respond(f"Built-in {SERVICE_LABELS[service]} API restored. Queued jobs are unchanged."
                                if service else "All built-in APIs restored. Queued jobs are unchanged.")
            return
        if head in SERVICE_KEYWORDS:
            service, rest = SERVICE_KEYWORDS[head], tail
        else:
            # Legacy form: /changeapi <URL> sets the YouTube API.
            service, rest = "youtube", arg
        label, keyword = SERVICE_LABELS[service], head if head in SERVICE_KEYWORDS else "yt"
        if rest.lower() == "show":
            await event.respond(f"{label}: {self.api_overrides.get(uid, {}).get(service) or DEFAULT_APIS[service]}"
                                f" ({'custom' if service in self.api_overrides.get(uid, {}) else 'built-in'})",
                                parse_mode=None)
            return
        if rest.lower() in RESET_WORDS:
            self.clear_api(uid, service)
            await event.respond(f"Built-in {label} API restored. Queued jobs are unchanged.")
            return
        if not rest:
            self.awaiting_api.add(uid)
            self.awaiting_service[uid] = service
            await event.respond(f"Send the public HTTPS API URL for {label} now (for example "
                                f"https://example.com/?url=). /changeapi {keyword} reset restores the built-in API, "
                                "or /cancel to stop.")
            return
        try:
            self.set_api(uid, rest, service)
        except ValueError as exc:
            await event.respond(str(exc))
        else:
            await event.respond(f"Custom {label} API saved for your future {label} links. "
                                f"/changeapi {keyword} reset restores the built-in API.")

    async def on_message(self, event) -> None:
        if not event.is_private:
            await event.respond("Please use this bot in a private chat.")
            return
        text = (event.raw_text or "").strip()
        command = text.split(maxsplit=1)[0].split("@", 1)[0].lower() if text.startswith("/") else ""
        if command in ("/start", "/help"):
            await event.respond(
                "Choose YouTube, Terabox, Dailymotion, Rumble, MEGA or Instagram/Facebook with /change, "
                "then send a link for that service. "
                "Pick one or several available qualities and tap Done. Playlists/folders first show "
                "their file count; send a range such as 1-29 or 200-300, then select qualities. "
                "Send another link while a job runs to queue it. Transfers stay sequential.\n\n"
                "A watch link containing list= is treated as a playlist; remove it for just one video.\n\n"
                "/change — YouTube / Terabox / Dailymotion / Rumble / MEGA / Instagram-Facebook buttons\n"
                "MEGA: send a file or folder link, or several links at once. Instagram/Facebook: send a "
                "post/reel/video link — that API is slow, so give it a minute.\n"
                "apitera (Moviebox/Anime/Donghua): /latest lists page 1 by hand, /search <words> finds "
                "a title, /auto on uploads every new title automatically, /parciv <link|id> reads one "
                "item.\n"
                "/status or /queue — one live status for running and queued jobs\n"
                "/cancel — list jobs; /cancel_<job-id> stops one with a tappable command\n"
                "/apis — numbered API list; /apis remove 3 resets a custom API\n"
                "/changeapi yt|tera|daily|rumble|mega|ig <URL> — set your own API for one service; /changeapi reset restores all\n"
                "/proxychk — paste or upload a proxy list; checks Dailymotion + Rumble APIs\n"
                "/proxychk url <https://…> — also check your own API host\n"
                "/proxychk sites dailymotion,rumble,youtube,terabox,mega,social,apitera — pick the sites\n"
                "/proxychk fast <ms|off> — replies faster than this fail automatically\n"
                "/proxychk export [all] — save the pool as a .txt to paste back after a restart\n"
                "/auto [on|off|now|sections|limit <n>|every <15m>] — automatic apitera uploads\n"
                "/latest [movies,anime,donghua] [count] [page=2] — browse the catalogue now\n"
                "/search [movies,anime] <words> — find a title in every apitera catalogue\n"
                "/parciv <apitera link or subject id> — read and queue one item\n"
                "/proxy — proxy mode ON/OFF, pool status, re-check or clear the pool\n"
                "/limits — size and queue limits\n/help — these instructions\n\n"
                "Only download media you have permission to use.")
        elif command == "/limits":
            await event.respond(
                f"MTProto limit: {self.max_bytes // 1024 // 1024} MiB per file (up to 4,000 parts). "
                f"YouTube playlist: up to {MAX_DISCOVER} entries, {self.playlist_limit} per selected range. "
                f"Terabox folders: up to 500 listed files. One job at a time, "
                f"{self.in_flight} concurrent upload parts; up to {MAX_WAITING} waiting jobs. "
                "Files with neither Content-Length nor API size are limited to 64 MiB. "
                f"Proxy pool: up to {self.proxy_pool_max} stored, {self.proxy_check_max} checked per "
                f"/proxychk against "
                + " + ".join(target.label for target in self.proxy_check_targets)
                + f", {self.proxy_workers} parallel checks, "
                + ("replies under " + str(self.proxy_min_latency) + " ms auto-failing. "
                   if self.proxy_min_latency else "too-fast guard off. ")
                + "Jobs/selections are not restored after a restart.")
        elif command == "/change":
            uid = event.sender_id
            # A pending /changeapi or /proxychk prompt must never capture the next URL.
            self.awaiting_api.discard(uid)
            self.awaiting_proxy.discard(uid)
            token = secrets.token_hex(4)
            self.mode_tokens[uid] = token
            active = self.modes.get(uid, "youtube")
            await event.respond(f"Current downloader: {MODE_LABELS.get(active, 'YouTube')}. "
                                "Choose a service for future links:",
                                buttons=[[Button.inline("▶️ YouTube", data=f"m:{token}:youtube".encode()),
                                          Button.inline("📁 Terabox", data=f"m:{token}:terabox".encode())],
                                         [Button.inline("🎞 Dailymotion", data=f"m:{token}:dailymotion".encode()),
                                           Button.inline("🟣 Rumble", data=f"m:{token}:rumble".encode())],
                                         [Button.inline("🗂 MEGA", data=f"m:{token}:mega".encode()),
                                           Button.inline("📸 Instagram/Facebook",
                                                         data=f"m:{token}:social".encode())],
                                         [Button.inline("🎬 apitera (Moviebox/Anime/Donghua)",
                                                        data=f"m:{token}:parciv".encode())]])
        elif command == "/changeapi":
            await self.handle_changeapi(event, text)
        elif command == "/auto":
            await self.handle_auto(event, text)
        elif command == "/latest":
            await self.handle_latest(event, text)
        elif command in ("/search", "/find"):
            await self.handle_search(event, text)
        elif command == "/parciv":
            await self.handle_parciv(event, text)
        elif command in ("/status", "/queue"):
            await self.show_dashboard(event)
        elif command == "/proxychk":
            uid = event.sender_id
            argument = text.partition(" ")[2].strip()
            head = argument.split(maxsplit=1)[0].lower() if argument else ""
            rest = argument[len(head):].strip()
            if head in ("stop", "cancel", "off"):
                task = self.proxy_checks.get(uid)
                if task:
                    task.cancel()
                    await event.respond("Proxy check cancellation requested.")
                else:
                    await event.respond("No proxy check is running.")
            elif head == "url":
                await self.proxy_url_command(event, rest)
            elif head in ("sites", "site"):
                await self.proxy_sites_command(event, rest)
            elif head in ("fast", "min", "speed"):
                await self.proxy_fast_command(event, rest)
            elif head in ("export", "save", "download", "dump"):
                await self.proxy_export_command(event, rest)
            else:
                self.awaiting_api.discard(uid)
                self.awaiting_proxy.add(uid)
                await event.respond(
                    "Send the proxies to check:\n"
                    "• paste them here (one per line)\n"
                    f"• or send a .txt/.csv file for large lists (up to {self.proxy_check_max:,} "
                    f"proxies, {human_size(PROXY_FILE_MAX)} max)\n\n"
                    "Accepted forms: host:port · host:port:user:pass · user:pass@host:port · "
                    "socks5://user:pass@host:1080\n\n"
                    "Every proxy is tested against " + " + ".join(
                        f"{target.label} ({urlsplit(target.url).hostname})"
                        for target in self.check_targets_for(uid)) + ": a proxy that reaches only "
                    "some sites is still usable for those, and only a real JSON answer from an API "
                    "counts as a pass. Working ones join your pool and dead ones are marked, with "
                    f"{self.proxy_workers} parallel workers. /proxychk stop cancels. /proxychk sites "
                    "picks the sites (youtube, terabox, dailymotion, rumble, mega, instagram, "
                    "facebook, apitera); /proxychk url "
                    "<https://…> adds your own API host. " + self.fast_guard_text(uid) + ".",
                    parse_mode=None)
        elif command == "/proxy":
            uid = event.sender_id
            argument = text.partition(" ")[2].strip().lower()
            self.awaiting_proxy.discard(uid)
            if argument in ("on", "off"):
                self.proxy_state[uid] = argument == "on"
                if argument == "off":
                    await self.close_router(uid)
                else:
                    self.pool_for(uid)
                await event.respond(f"Proxy mode is {argument.upper()}.\n"
                                    f"{self.pool_for(uid).summary()}"
                                    + ("" if len(self.pool_for(uid)) else
                                       "\nThe pool is empty — add proxies with /proxychk."),
                                    parse_mode=None)
            elif argument in ("clear", "reset", "offline"):
                count = len(self.pool_for(uid))
                self.pool_for(uid).clear()
                self.proxy_state[uid] = False
                await self.close_router(uid)
                await event.respond(f"Cleared {count} proxies and turned proxy mode off.")
            elif argument in ("", "status", "pool", "list"):
                await self.show_proxy_panel(event)
            elif argument.split(maxsplit=1)[0] in ("export", "save", "download", "dump"):
                await self.proxy_export_command(event, argument.partition(" ")[2])
            else:
                await event.respond("Use /proxy for the ON/OFF panel, /proxy on|off, "
                                    "/proxy clear, /proxy export to save the live list, or "
                                    "/proxychk to add proxies.")
        elif command == "/apis":
            uid = event.sender_id
            arg = text.split(maxsplit=2)
            if len(arg) == 3 and arg[1].lower() == "remove" and arg[2].lower() in SERVICE_KEYWORDS:
                service = SERVICE_KEYWORDS[arg[2].lower()]
                if self.clear_api(uid, service):
                    await event.respond(f"Custom {SERVICE_LABELS[service]} API removed. Future links use the built-in "
                                        "API; queued jobs are unchanged.")
                else:
                    await event.respond(f"No custom {SERVICE_LABELS[service]} API to remove.")
            elif len(arg) > 1:
                await event.respond("Use /apis to list the APIs, or /apis remove yt|tera|daily|rumble|mega|ig to restore one "
                                    "built-in API.")
            else:
                await event.respond("\n".join(["Configured APIs (for your account):", *self.api_lines(uid),
                                                "Change with /changeapi yt|tera|daily|rumble|mega|ig <URL>; "
                                                "restore with /changeapi yt|tera|daily|rumble|mega|ig reset."]),
                                  parse_mode=None)
        elif command == "/cancel" or command.startswith("/cancel_"):
            uid = event.sender_id
            if command.startswith("/cancel_") and not re.fullmatch(r"/cancel_[a-f0-9]{8}", command):
                await event.respond("Invalid job command. Send /cancel to see your current job IDs.")
                return
            target = command[8:] if command.startswith("/cancel_") else text.partition(" ")[2].strip()
            owned = self.user_jobs(uid)
            if target == "proxy":
                task = self.proxy_checks.get(uid)
                if not task:
                    await event.respond("No proxy check is running.")
                else:
                    task.cancel()
                    await event.respond("Proxy check cancellation requested.")
            elif target == "lookup" and uid in self.lookups:
                self.lookups[uid].cancel()
                await event.respond("Link lookup cancellation requested.")
            elif target:
                job = self.jobs.get(target)
                if not job or job.user_id != uid or not job.task:
                    await event.respond("No matching active/queued job of yours. Send /cancel for your job IDs.")
                else:
                    job.task.cancel()
                    await event.respond(f"Cancellation requested for {target} ({job.state}).")
            elif owned:
                lines = ["Choose a job to stop (running or queued):"]
                for job in owned:
                    lines.append(f"• {job.token} · {job.state} · {job.title[:50] or job.source.kind} "
                                 f"[{', '.join(job.selected)}] — /cancel_{job.token}")
                if uid in self.lookups:
                    lines.append("/cancel lookup — stop the current link lookup")
                if uid in self.proxy_checks:
                    lines.append("/cancel proxy — stop the running proxy check")
                status = "\n".join(lines)
                await event.respond(status, parse_mode=None, formatting_entities=cancel_entities(status))
            else:
                task = self.lookups.get(uid)
                if task:
                    task.cancel()
                selection = self.selections.pop(uid, None)
                range_request = self.pending_ranges.pop(uid, None)
                pending_api = uid in self.awaiting_api
                pending_proxy = uid in self.awaiting_proxy
                self.awaiting_api.discard(uid)
                self.awaiting_proxy.discard(uid)
                await event.respond("Lookup cancellation requested." if task else
                                    "Selection cleared." if selection or range_request or pending_api
                                    or pending_proxy else "Nothing to cancel.")
        elif command:
            await event.respond("Unknown command. Send /help for instructions.")
        else:
            document = getattr(getattr(event, "message", None), "document", None)
            if event.sender_id in self.awaiting_proxy and (document is not None or not is_media_link(text)):
                self.awaiting_proxy.discard(event.sender_id)
                if document is not None:
                    await self.on_proxy_file(event)
                else:
                    await self.on_proxy_text(event, text)
            elif event.sender_id in self.awaiting_api:
                if is_media_link(text):
                    self.awaiting_api.discard(event.sender_id)
                    self.pending_ranges.pop(event.sender_id, None)
                    await self.on_link(event)
                else:
                    service = self.awaiting_service.get(event.sender_id, "youtube")
                    try:
                        self.set_api(event.sender_id, text, service)
                    except ValueError as exc:
                        await event.respond(f"{exc} Send an API URL, or /changeapi reset.")
                    else:
                        await event.respond(f"Custom {SERVICE_LABELS[service]} API saved for your future links. "
                                            "/changeapi reset restores the built-in API.")
            elif event.sender_id in self.pending_ranges and not text.startswith("https://"):
                await self.on_range(event)
            else:
                self.pending_ranges.pop(event.sender_id, None)
                await self.on_link(event)

    @staticmethod
    def mega_choices(files: tuple[MegaFile, ...]) -> tuple[str, ...]:
        """MEGA files come as they are: the choice is the kind of file."""
        order = ("Video", "Audio", "File")
        found = {video_choice(file) for file in files}
        return tuple(kind for kind in order if kind in found)

    def mega_waiter(self, notice):
        """A callback that keeps the user informed while the MEGA API wakes up."""
        async def waited(attempt: int, delay: int, attempts: int) -> None:
            await self.safe_edit(notice, mega_wait_text(attempt, delay, attempts))

        return waited

    async def read_mega_links(self, uid: int, links: tuple[str, ...], endpoint: str,
                              on_wait=None) -> tuple[MegaListing, list[str]]:
        """Read several MEGA links at once and merge them into one listing.

        One link the API cannot read never sinks the batch: the other links are
        still listed, and the prompt names the ones that were skipped.
        """
        session = self.http_for(uid)
        results = await asyncio.gather(
            *(fetch_mega(session, link, endpoint, on_wait=on_wait) for link in links),
            return_exceptions=True)
        listings: list[MegaListing] = []
        labels: list[str] = []
        broken: list[str] = []
        for position, result in enumerate(results, 1):
            if isinstance(result, MegaListing) and result.files:
                listings.append(result)
                labels.append(str(position))  # The pasted link's own number.
                continue
            reason = (str(result) if isinstance(result, MegaError) and str(result)
                      else "the API could not read it" if not isinstance(result, BaseException)
                      else type(result).__name__)
            broken.append(f"link {position} ({reason})")
        if not listings:
            raise MegaError("None of those MEGA links could be read. " + "; ".join(broken[:3]) + ".")
        return merge_listings(listings, tuple(labels)), broken

    async def read_social_links(self, uid: int, links: tuple[str, ...], endpoint: str
                                ) -> tuple[SocialBatch, list[str]]:
        """Read several Instagram/Facebook links at once and offer them as one list.

        The API is slow, so the links are read in parallel rather than one after
        another; a link it cannot read is named in the prompt and the rest stay.
        """
        session = self.http_for(uid)
        results = await asyncio.gather(*(fetch_social(session, link, endpoint) for link in links),
                                       return_exceptions=True)
        slots: list[SocialMedia | None] = []
        broken: list[str] = []
        for position, result in enumerate(results, 1):
            if isinstance(result, SocialMedia):
                slots.append(result)
                continue
            slots.append(None)  # Keeps this link's number for the options that did work.
            reason = (str(result) if isinstance(result, SocialError) and str(result)
                      else "the API could not read it" if not isinstance(result, BaseException)
                      else type(result).__name__)
            broken.append(f"link {position} ({reason})")
        if not any(slots):
            raise SocialError("None of those links could be read. " + "; ".join(broken[:3]) + ".")
        return SocialBatch(tuple(slots)), broken

    @staticmethod
    def social_prompt(media: "SocialMedia | SocialBatch", choices: tuple[SocialChoice, ...],
                      broken: list[str]) -> str:
        """The lookup message: what was found, what can be picked, what failed."""
        if isinstance(media, SocialBatch):
            lines = [f"📸 {media.title}:"]
            lines += [f"· {position}. {item.platform}: {item.title} — {len(item.options())} option(s)"
                      for position, item in media.present()]
        else:
            lines = [f"📸 {media.platform}: {media.title}"]
        lines += [f"· {choice.label} ({choice.item.kind}, {choice.item.format})"
                  for choice in choices]
        if isinstance(media, SocialBatch) and media.dropped:
            lines.append(f"… {media.dropped} more option(s) do not fit here; send fewer links "
                         "and every option appears.")
        text = "\n".join(lines) + DownloaderBot.mega_note(broken)
        return text + "\nChoose one or more, then tap Done:"

    @staticmethod
    def mega_note(broken: list[str]) -> str:
        """One warning line for the links a multi-link paste could not read."""
        if not broken:
            return ""
        return f"\n⚠️ Skipped {len(broken)} link(s): " + "; ".join(broken[:2]) + "."

    def listing_choices(self, files: tuple[TeraboxFile, ...]) -> tuple[str, ...]:
        return tuple(sorted({item.quality for item in files},
                            key=lambda q: (q == "Original", q == "MP3", int(q[:-1]) if q.endswith("p") and q[:-1].isdigit() else 0)))

    async def on_range(self, event) -> None:
        request = self.pending_ranges.get(event.sender_id)
        if not request or time.monotonic() - request.created > SESSION_SECONDS:
            self.pending_ranges.pop(event.sender_id, None)
            await event.respond("Range request expired. Send the link again.")
            return
        match = re.fullmatch(r"\s*(\d+)\s*[-–]\s*(\d+)\s*", event.raw_text or "")
        if not match:
            await event.respond("Send a range like 1-29 or 200-300 (inclusive), or /cancel.")
            return
        start, end = map(int, match.groups())
        if not (1 <= start <= end <= request.count):
            await event.respond(f"Choose a range within 1-{request.count}.")
            return
        if end - start + 1 > self.playlist_limit:
            await event.respond(f"Choose up to {self.playlist_limit} entries per job to protect Koyeb free.")
            return
        if len(self.selections) >= MAX_SELECTIONS:
            await event.respond("Too many open selections right now. Try again later.")
            return
        if isinstance(request.listing, MegaListing):
            files = tuple(request.listing.files[start - 1:end])
            choices = self.mega_choices(files) or ("Video",)
        else:
            files = (tuple(file for index in range(start - 1, end)
                           for file in request.listing.options_for(index)) if request.listing else ())
            choices = (self.listing_choices(files) if files else
                       tuple(f"{q}p" for q in VIDEO_QUALITIES))
        selection = Selection(secrets.token_hex(4), request.source, choices, request.peer,
                              api_endpoint=request.api_endpoint, start=start, end=end,
                              listing=request.listing)
        self.pending_ranges.pop(event.sender_id, None)
        self.selections[event.sender_id] = selection
        await event.respond(f"Entries {start}-{end} of {request.count}. Pick formats and tap Done. "
                            "Unavailable formats will be skipped per item.",
                            buttons=self.keyboard(selection))

    def keyboard(self, selection: Selection):
        def label(key: str) -> str:
            if key == AUDIO:
                return "MP3 audio"
            if selection.source.kind == "playlist" and key in ("1440p", "2160p"):
                return key + (" (2K)" if key == "1440p" else " (4K)")
            return key

        buttons = [Button.inline(("✅ " if key in selection.selected else "▫️ ") + label(key),
                                 data=f"s:{selection.token}:t:{key}".encode()) for key in selection.choices]
        rows = [buttons[i:i + 2] for i in range(0, len(buttons), 2)]
        rows.append([Button.inline("Done ▶️", data=f"s:{selection.token}:done".encode()),
                     Button.inline("Cancel", data=f"s:{selection.token}:cancel".encode())])
        return rows

    async def on_link(self, event) -> None:
        uid = event.sender_id
        now = time.monotonic()
        for owner, selection in list(self.selections.items()):
            if now - selection.created > SESSION_SECONDS:
                self.selections.pop(owner, None)
        for owner, request in list(self.pending_ranges.items()):
            if now - request.created > SESSION_SECONDS:
                self.pending_ranges.pop(owner, None)
        if uid in self.lookups:
            await event.respond("Finish or /cancel your current link lookup first.")
            return
        if len(self.lookups) >= 2:
            await event.respond("The link lookup slots are busy. Try again shortly.")
            return
        mode = self.modes.get(uid, "youtube")
        try:
            source = (Source("terabox", parse_terabox_source(event.raw_text)) if mode == "terabox"
                      else Source("dailymotion", parse_dailymotion_source(event.raw_text)) if mode == "dailymotion"
                      else Source("rumble", parse_rumble_source(event.raw_text)) if mode == "rumble"
                      else Source("mega", mega_source(event.raw_text)) if mode == "mega"
                      else Source("parciv", parse_parciv_source(event.raw_text)) if mode == "parciv"
                      else Source("social", social_source(event.raw_text)) if mode == "social"
                      else parse_source(event.raw_text))
        except ValueError as exc:
            if crunchyroll_link(event.raw_text or ""):
                await event.respond(CRUNCHYROLL_NOTICE, parse_mode=None)
                return
            # A pasted proxy list is the most common mistake here; point at /proxychk.
            proxies, _ = parse_proxies(event.raw_text or "", 10)
            if len(proxies) >= 2:
                await event.respond("That looks like a proxy list. Send /proxychk to test it "
                                    "and add it to your pool, then /proxy to switch it on.")
            else:
                await event.respond(str(exc))
            return
        self.selections.pop(uid, None)
        self.pending_ranges.pop(uid, None)
        if len(self.selections) + len(self.pending_ranges) >= MAX_SELECTIONS:
            await event.respond("Too many open selections right now. Try again later.")
            return
        peer = await event.get_input_chat()
        self.lookups[uid] = asyncio.current_task()
        notice = None
        try:
            if source.kind == "playlist":
                notice = await event.respond("Counting playlist videos (flat discovery, no downloads)…")
                count = await asyncio.wait_for(
                    self.youtube_task(uid, playlist_count, source.url, MAX_DISCOVER), timeout=180)
                if self.modes.get(uid, "youtube") != mode:
                    await notice.edit("Mode changed. Send your link again.")
                    return
                self.pending_ranges[uid] = RangeRequest(source, peer, count, self.api_for(uid))
                await notice.edit(f"Playlist reports {count} videos. Send an inclusive range like 1-29 or "
                                  f"200-300 (within 1-{count}, up to {self.playlist_limit} per job).")
                return
            if source.kind == "rumble":
                notice = await event.respond("Checking Rumble qualities…")
                endpoint = self.api_for(uid, "rumble")
                try:
                    proxy = self.proxy_for(uid, "rumble")
                    video = await fetch_rumble(self.http_for(uid), source.url, endpoint,
                                               proxy=proxy.url if proxy else None)
                except RumbleError as exc:
                    await notice.edit(str(exc))
                    return
                if self.modes.get(uid, "youtube") != mode:
                    await notice.edit("Mode changed. Send your link again.")
                    return
                if len(self.selections) >= MAX_SELECTIONS:
                    await notice.edit("Too many open selections right now. Try again later.")
                    return
                choices = tuple(sorted(video.streams, key=lambda q: int(q[:-1])))
                selection = Selection(secrets.token_hex(4), source, choices, peer, video, api_endpoint=endpoint)
                self.selections[uid] = selection
                await notice.edit(f"{video.title}\nChoose one or more available formats, then tap Done:",
                                  buttons=self.keyboard(selection))
                return
            if source.kind == "dailymotion":
                notice = await event.respond("Checking Dailymotion qualities…")
                proxy = self.proxy_for(uid, "dailymotion")
                try:
                    info = await fetch_dailymotion(self.http_for(uid), source.url,
                                                   self.api_for(uid, "dailymotion"),
                                                   proxy=proxy.url if proxy else None)
                except DailymotionError as exc:
                    await notice.edit(str(exc))
                    return
                if self.modes.get(uid, "youtube") != mode:
                    await notice.edit("Mode changed. Send your link again.")
                    return
                selection = Selection(secrets.token_hex(4), source,
                                      tuple(fmt.label for fmt in reversed(info.formats)), peer, info,
                                      api_endpoint=self.api_for(uid, "dailymotion"))
                self.selections[uid] = selection
                await notice.edit(f"{info.title}\nChoose one or more qualities, then tap Done:",
                                  buttons=self.keyboard(selection))
                return
            if source.kind == "terabox":
                notice = await event.respond("Checking Terabox files and available qualities…")
                listing = await fetch_terabox(self.http_for(uid), source.url,
                                              self.api_for(uid, "terabox"))
                if self.modes.get(uid, "youtube") != mode:
                    await notice.edit("Mode changed. Send your link again.")
                    return
                count = len(listing.files)
                if count > 1:
                    folder = Source("terabox-folder", source.url)
                    self.pending_ranges[uid] = RangeRequest(folder, peer, count,
                                                            api_endpoint=self.api_for(uid, "terabox"), listing=listing)
                    extra = (f" API reports {listing.reported_total} files but provided only {count} links; "
                             "the bot can process only provided files." if listing.reported_total > count else "")
                    await notice.edit(f"Terabox folder: {count} available files.{extra} Send an inclusive range "
                                      f"like 1-{min(count, 29)} (up to {self.playlist_limit} per job).")
                else:
                    item = listing.files[0]
                    selection = Selection(secrets.token_hex(4), source,
                                          self.listing_choices(listing.options_for(0)), peer,
                                          listing=listing, api_endpoint=self.api_for(uid, "terabox"))
                    self.selections[uid] = selection
                    partial = (f"API reports {listing.reported_total} files but provided only one usable link. "
                               if listing.reported_total > 1 else "")
                    await notice.edit(f"{partial}{item.filename} · "
                                      f"{human_size(item.size) if item.size else 'size unknown'}\n"
                                      "Choose a format, then Done:", buttons=self.keyboard(selection))
                return
            if source.kind == "mega":
                links = find_mega_links(event.raw_text or "", MAX_MEGA_LINKS + 1)
                if len(links) > MAX_MEGA_LINKS:
                    await event.respond(f"Send at most {MAX_MEGA_LINKS} MEGA links at a time.")
                    return
                endpoint = self.api_for(uid, "mega")
                if len(links) > 1:
                    notice = await event.respond(f"Reading {len(links)} MEGA links through the API…")
                    listing, broken = await self.read_mega_links(uid, links, endpoint,
                                                                 on_wait=self.mega_waiter(notice))
                else:
                    notice = await event.respond("Reading this MEGA link through the API…")
                    listing, broken = await fetch_mega(self.http_for(uid), source.url, endpoint,
                                                       on_wait=self.mega_waiter(notice)), []
                if self.modes.get(uid, "youtube") != mode:
                    await notice.edit("Mode changed. Send your link again.")
                    return
                if not listing.files:
                    # An empty listing is an API failure, not an empty folder:
                    # there is nothing to show and nothing to pick.
                    raise MegaError("This MEGA link has no files the bot can download.")
                count = len(listing.files)
                if count > 1:
                    folder = Source("mega", source.url)
                    self.pending_ranges[uid] = RangeRequest(folder, peer, count,
                                                            api_endpoint=endpoint,
                                                            listing=listing)
                    names = listing.names(15)
                    extra = (f"\n(The API sent {count} of {listing.reported_files} files; "
                             "that is what the bot can take.)" if listing.truncated else "")
                    where = f"{len(links)} MEGA links" if len(links) > 1 else "MEGA folder"
                    await notice.edit(
                        f"🗂 {where}: {count} files, {len(listing.folders)} subfolders."
                        + self.mega_note(broken) + "\n"
                        + "\n".join(names) + extra
                        + f"\n\nSend an inclusive range like 1-{min(count, 5)} "
                          f"(up to {self.playlist_limit} per job), or /cancel.",
                        parse_mode=None)
                else:
                    item = listing.files[0]
                    selection = Selection(secrets.token_hex(4), source,
                                          self.mega_choices((item,)), peer, listing=listing,
                                          api_endpoint=endpoint)
                    self.selections[uid] = selection
                    await notice.edit(self.mega_note(broken) + f"{item.path}\n"
                                      f"{human_size(item.size) if item.size else 'size unknown'}"
                                      f" · {video_choice(item)}\nChoose it, then tap Done:",
                                      buttons=self.keyboard(selection), parse_mode=None)
                return
            if source.kind == "parciv":
                notice = await event.respond("Reading this apitera link…")
                await self.parciv_link_flow(event, uid, peer, source.url, notice=notice)
                return
            if source.kind == "social":
                links = find_social_links(event.raw_text or "", MAX_SOCIAL_LINKS + 1)
                if len(links) > MAX_SOCIAL_LINKS:
                    await event.respond(f"Send at most {MAX_SOCIAL_LINKS} Instagram/Facebook "
                                        "links at a time.")
                    return
                endpoint = self.api_for(uid, "social")
                if len(links) > 1:
                    notice = await event.respond(f"Reading {len(links)} Instagram/Facebook links… "
                                                 "this API is slow, so it can take a few minutes. "
                                                 "/cancel to stop.")
                    media, broken = await self.read_social_links(uid, links, endpoint)
                else:
                    notice = await event.respond("Reading this Instagram/Facebook link… this API is "
                                                 "slow, so it can take a minute. /cancel to stop.")
                    media, broken = await fetch_social(self.http_for(uid), source.url, endpoint), []
                if self.modes.get(uid, "youtube") != mode:
                    await notice.edit("Mode changed. Send your link again.")
                    return
                choices = social_choices(media)
                selection = Selection(secrets.token_hex(4), source,
                                      tuple(choice.label for choice in choices), peer, social=media,
                                      api_endpoint=endpoint)
                self.selections[uid] = selection
                await notice.edit(self.social_prompt(media, choices, broken),
                                  buttons=self.keyboard(selection), parse_mode=None)
                return
            notice = await event.respond("Checking available qualities… This API may take 1–2 minutes. /cancel to stop.")
            http = self.http_for(uid)
            proxy = self.proxy_for(uid)
            title_task = asyncio.create_task(
                get_youtube_title(http, source.url, proxy.url if proxy else None))
            try:
                endpoint = self.api_for(uid)
                video = await fetch_video(http, source.url, endpoint)
                video = replace(video, title=choose_title(await title_task, video.title, source.url))
            finally:
                if not title_task.done():
                    title_task.cancel()
                await asyncio.gather(title_task, return_exceptions=True)
            if self.modes.get(uid, "youtube") != mode:
                await notice.edit("Mode changed. Send your link again.")
                return
            choices = tuple(sorted((q for q in video.streams if q != AUDIO), key=lambda q: int(q[:-1])))
            if AUDIO in video.streams:
                choices += (AUDIO,)
            if len(self.selections) >= MAX_SELECTIONS:
                await notice.edit("Too many open selections right now. Try again later.")
                return
            selection = Selection(secrets.token_hex(4), source, choices, peer, video,
                                  api_endpoint=endpoint)
            self.selections[uid] = selection
            await notice.edit(f"{video.title}\nChoose one or more available formats, then tap Done:",
                              buttons=self.keyboard(selection))
        except asyncio.CancelledError:
            if notice:
                await self.safe_edit(notice, "Lookup cancelled.")
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, TypeError) as exc:
            LOG.warning("Link lookup failed for user %s: %s", uid, type(exc).__name__)
            if notice:
                # Our own providers explain themselves ("private post", "no files");
                # a raw library error would only confuse, so it stays generic.
                reason = (str(exc) if isinstance(exc, (MegaError, SocialError, ParcivError, ValueError))
                          and str(exc) and len(str(exc)) < 200 else "")
                await self.safe_edit(notice, f"Could not load this link. {reason}" if reason else
                                     "Could not load this link. The API or source may be "
                                     "unavailable; try later.")
        finally:
            self.lookups.pop(uid, None)

    async def safe_edit(self, message, text: str, **kwargs) -> None:
        try:
            await message.edit(text, **kwargs)
        except RPCError:
            LOG.warning("Could not update a Telegram message", exc_info=True)

    # ------------------------------------------------- apitera commands

    async def handle_auto(self, event, text: str) -> None:
        """`/auto` — the automatic apitera watcher: on, off, now, sections, limit, every."""
        uid = event.sender_id
        arg = text.partition(" ")[2].strip()
        head, _, tail = arg.partition(" ")
        head, tail = head.lower(), tail.strip()
        if not arg:
            token = self.apitera_token(uid)
            on = self.auto_enabled.get(uid, False)
            buttons = [[Button.inline("⏸ Turn off" if on else "▶️ Turn on",
                                      data=f"a:{token}:{'off' if on else 'on'}".encode())],
                       [Button.inline("🔎 Check now", data=f"a:{token}:now".encode()),
                        Button.inline("📄 Latest list", data=f"a:{token}:latest".encode())]]
            await event.respond(self.auto_status_text(uid), buttons=buttons, parse_mode=None)
            await event.respond("Turn it on to upload new apitera titles automatically. "
                                "`/auto sections movies,anime` picks the sections, "
                                "`/auto limit 3` the items per section, `/auto every 15m` the pace. "
                                "`/auto now` checks once; `/latest` lists page 1 by hand.",
                                parse_mode=None)
            return
        if head in ("on", "off", "yes", "no", "enable", "disable"):
            turn_on = head in ("on", "yes", "enable")
            if turn_on:
                # Remember where the uploads go: this chat.
                self.auto_peers[uid] = await event.get_input_chat()
                self.auto_enabled[uid] = True
                self.start_auto_watch(uid)
            else:
                self.stop_auto_watch(uid)
            await event.respond(self.auto_status_text(uid), parse_mode=None)
            return
        if head in ("now", "check", "run"):
            self.auto_peers[uid] = await event.get_input_chat()
            notice = await event.respond("Checking the apitera catalogue…")
            try:
                result = await self.run_auto_check(uid, notice=notice, note="Manual check (/auto now).")
            except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, OSError, RPCError) as exc:
                await self.safe_edit(notice, f"Could not check the catalogue ({type(exc).__name__}).")
                return
            if "Queued" not in result:
                await self.safe_edit(notice, result)
            return
        if head == "sections":
            if not tail or tail.lower() in ("default", "all", "reset"):
                self.auto_sections[uid] = AUTO_SECTIONS
                await event.respond(f"Automatic sections: {sections_text(AUTO_SECTIONS)}.")
                return
            try:
                sections = parse_sections(tail)
            except ParcivError as exc:
                await event.respond(str(exc))
                return
            self.auto_sections[uid] = sections
            await event.respond(f"Automatic sections: {sections_text(sections)}.")
            return
        if head == "limit":
            if not tail:
                await event.respond(f"Automatic limit: {limit_text(self.auto_limits.get(uid, self.auto_limit_default))}. "
                                    f"Use /auto limit <1-{MAX_AUTO_LIMIT}>.")
                return
            try:
                self.auto_limits[uid] = parse_limit(tail)
            except ValueError as exc:
                await event.respond(f"{exc} Use /auto limit <1-{MAX_AUTO_LIMIT}>.")
                return
            await event.respond(f"Automatic limit: {limit_text(self.auto_limits[uid])}.")
            return
        if head in ("every", "interval", "pace"):
            if not tail:
                await event.respond(f"Automatic pace: every {interval_text(int(self.auto_interval))}. "
                                    "Use /auto every 15m.")
                return
            try:
                self.auto_interval = float(parse_interval(tail))
            except ValueError as exc:
                await event.respond(f"{exc} Use /auto every 15m.")
                return
            if self.auto_enabled.get(uid):
                self.stop_auto_watch(uid)
                self.auto_enabled[uid] = True
                self.start_auto_watch(uid)
            await event.respond(f"Automatic pace: every {interval_text(int(self.auto_interval))}.")
            return
        await event.respond("Use /auto, /auto on|off, /auto now, /auto sections movies,anime, "
                            f"/auto limit <1-{MAX_AUTO_LIMIT}>, or /auto every 15m.")

    async def handle_latest(self, event, text: str) -> None:
        """`/latest [sections] [count] [page=2]` — browse page 1 of the catalogue by hand."""
        uid = event.sender_id
        sections: tuple[str, ...] = ()
        count = self.auto_limit_default
        page = 1
        try:
            for part in text.split()[1:]:
                low = part.strip().lower()
                if low.isdigit():
                    count = max(1, min(MAX_AUTO_LIMIT, int(low)))
                elif re.fullmatch(r"page=?-?\d{0,2}", low.replace(" ", "")):
                    page = max(1, min(50, int(re.sub(r"\D", "", low) or 1)))
                elif low.startswith("page") and low[4:].isdigit():
                    page = max(1, min(50, int(low[4:])))
                else:
                    sections += tuple(section for section in parse_sections(part)
                                      if section not in sections)
        except (ValueError, ParcivError) as exc:
            await event.respond(str(exc))
            return
        sections = normalise_sections(sections)
        peer = await event.get_input_chat()
        self.auto_peers[uid] = peer
        notice = await event.respond(f"Reading {sections_text(sections)}…")
        try:
            await self.show_latest(uid, peer, notice, sections, page, count)
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, OSError) as exc:
            LOG.warning("apitera /latest failed: %s", type(exc).__name__)
            await self.safe_edit(notice, f"Could not read the apitera catalogue "
                                         f"({type(exc).__name__}). Try again in a minute.")

    async def show_latest(self, uid: int, peer, notice, sections: tuple[str, ...], page: int,
                          count: int) -> None:
        """Read the catalogue and edit `notice` into the listing with its buttons."""
        api = ParcivApi(self.api_for(uid, "parciv"))
        pages, problems = await self.read_catalogue(uid, api, sections, count, page)
        items = tuple(item for page_items in pages for item in page_items.items)
        if not items:
            reason = "; ".join(problems[:2]) or "the API answered nothing"
            await self.safe_edit(notice, f"No catalogue entries came back: {reason}.")
            return
        keys = await self.dm_titles(peer)
        token = self.apitera_token(uid)
        self.latest_panels[uid] = LatestPanel(peer, getattr(notice, "id", 0), token, items,
                                              sections, page)
        await self.safe_edit(notice, self.latest_panel_text(uid, items, keys, sections, page),
                             buttons=self.latest_buttons(uid, token, items, keys), parse_mode=None)

    def apitera_token(self, uid: int) -> str:
        """The button token for this user's apitera panels.

        /auto, /latest, /search and a listing link all hand out the same one, so a
        panel opened a moment ago keeps working after another one opens: tapping an
        older panel's button must never answer "expired".
        """
        token = self.auto_tokens.get(uid)
        if not token:
            token = secrets.token_hex(4)
            self.auto_tokens[uid] = token
        return token

    def latest_panel_text(self, uid: int, items: tuple[ParcivItem, ...], keys: set[str],
                          sections: tuple[str, ...], page: int) -> str:
        fresh = new_items(items, keys)
        on = self.auto_enabled.get(uid, False)
        lines = [f"🔎 apitera — page {page} of {sections_text(sections)}",
                 "✅ = already in your DM · ⬇️ = not uploaded yet", ""]
        lines += listing_lines(CataloguePage("", page, items), keys, len(items))
        lines += ["", f"⬇️ {len(fresh)} of {len(items)} listed title(s) are new. Every quality of a "
                      "title is uploaded, named with its language and subtitle tags."]
        lines.append(f"🤖 Automatic: {'ON' if on else 'OFF'} · every "
                     f"{interval_text(int(self.auto_interval))} · "
                     f"{limit_text(self.auto_limits.get(uid, self.auto_limit_default))}")
        return "\n".join(lines)

    def latest_buttons(self, uid: int, token: str, items: tuple[ParcivItem, ...],
                       keys: set[str]) -> list:
        fresh = len(new_items(items, keys))
        on = self.auto_enabled.get(uid, False)
        return [[Button.inline(f"⬇️ Download new ({fresh})", data=f"a:{token}:go".encode()),
                 Button.inline(f"⬇️ Download all ({len(items)})", data=f"a:{token}:all".encode())],
                [Button.inline("🔁 Refresh", data=f"a:{token}:latest".encode()),
                 Button.inline("⏸ Auto off" if on else "▶️ Auto on",
                               data=f"a:{token}:{'off' if on else 'on'}".encode())]]

    async def handle_search(self, event, text: str) -> None:
        """`/search [sections] <words>` — find a title in the apitera catalogues."""
        uid = event.sender_id
        sections, words = parse_search(text)
        if not words:
            await event.respond("Use /search <words> to look for a title in every apitera catalogue "
                                "(Moviebox movies, tv, tvshows, animation, kids, recommend, Anime, "
                                "Donghua). Narrow it with /search movies,anime <words>, or add "
                                "sections=movies,anime.")
            return
        peer = await event.get_input_chat()
        self.auto_peers[uid] = peer
        notice = await event.respond(f"🔎 Searching {sections_text(sections)} for {words}…")
        try:
            await self.show_search(uid, peer, notice, words, sections)
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, OSError) as exc:
            LOG.warning("apitera /search failed: %s", type(exc).__name__)
            await self.safe_edit(notice, f"Could not search the apitera catalogue "
                                         f"({type(exc).__name__}). Try again in a minute.")

    async def search_family(self, uid: int, api: ParcivApi, base: str, sections: tuple[str, ...],
                            words: str) -> tuple[tuple[ParcivItem, ...], str]:
        """One family's search: its own search call, else its catalogue pages.

        A search call that answers with matches is believed; anything else (a
        refused call, an answer this API does not serve, no match at all) falls
        back to reading that family's catalogue pages and filtering them here.
        """
        session = self.http_for(uid)
        answered = False  # Whether this family has a search call at all.
        try:
            payload = await fetch_parciv_json(session, search_url_for(base, words))
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, OSError) as exc:
            LOG.info("apitera search call refused for %s (%s); scanning its pages",
                     sections_text(sections), type(exc).__name__)
        else:
            answered = True
            found = tuple(item for item in parse_catalogue(payload, sections[0], api)
                          if matches_words(item.title, words))
            if found:
                return found[:SEARCH_LIMIT], ""
        reads = await asyncio.gather(
            *(fetch_parciv_catalogue(session, api, section, page)
              for section in sections for page in range(1, SEARCH_PAGES + 1)),
            return_exceptions=True)
        matches: list[ParcivItem] = []
        seen: set[str] = set()
        failed = 0
        for result in reads:
            if not isinstance(result, CataloguePage):
                failed += 1
                LOG.info("apitera catalogue read failed (%s)",
                         type(result).__name__ if not isinstance(result, BaseException) else str(result))
                continue
            for item in result.items:
                key = item.subject_id or item.title
                if key in seen or not matches_words(item.title, words):
                    continue
                seen.add(key)
                matches.append(item)
        if not matches and failed == len(reads):
            raise ParcivError(f"{sections_text(sections)} could not be read")
        note = ""
        if not answered:  # Only worth saying when there was no search call to answer.
            note = (f"{sections_text(sections)}: the first {SEARCH_PAGES} page(s) were scanned "
                    "instead of a search call")
            if failed:
                note += f" ({failed} read(s) failed)"
        return tuple(matches[:SEARCH_LIMIT]), note

    async def search_catalogue(self, uid: int, api: ParcivApi, sections: tuple[str, ...],
                               words: str) -> tuple[tuple[ParcivItem, ...], tuple[str, ...]]:
        """Search every named family once, in parallel, and report what failed.

        The six Moviebox sections share one family, so they cost one search call;
        they are still scanned separately when that family has no search call. A
        title listed in several families (Moviebox lists some films in more than
        one) is kept once.
        """
        groups: dict[str, list[str]] = {}
        for section in sections:
            groups.setdefault(family_base(api, section), []).append(section)
        results = await asyncio.gather(
            *(self.search_family(uid, api, base, tuple(group), words)
              for base, group in groups.items()),
            return_exceptions=True)
        items: list[ParcivItem] = []
        notes: list[str] = []
        seen: set[str] = set()
        for (base, group), result in zip(groups.items(), results):
            if not isinstance(result, tuple):
                reason = str(result) if isinstance(result, ParcivError) else type(result).__name__
                notes.append(f"{sections_text(tuple(group))}: {reason}")
                LOG.warning("apitera search failed for %s: %s", base, type(result).__name__)
                continue
            found, note = result
            if note:
                notes.append(note)
            for item in found:
                key = item.subject_id or item.title
                if key in seen:
                    continue
                seen.add(key)
                items.append(item)
        return tuple(items[:SEARCH_LIMIT]), tuple(notes)

    async def show_listing_url(self, uid: int, peer, notice, url: str, section: str) -> None:
        """List any apitera catalogue-style URL: a family endpoint or a search link.

        The API has more endpoints than the ones `/latest` polls (Anime and
        Donghua each have a dozen), so any of them can be opened by sending its
        link: this reads it, shows what it listed and offers the same buttons.
        """
        api = ParcivApi(self.api_for(uid, "parciv"))
        session = self.http_for(uid)
        parts = urlsplit(url)
        query = dict(pair.split("=", 1) for pair in parts.query.split("&") if "=" in pair)
        page = max(1, min(50, int(re.sub(r"\D", "", query.get("page", "1")) or 1)))
        payload = await fetch_parciv_json(session, url)
        items = parse_catalogue(payload, section, api)
        if not items:
            raise ParcivError("That apitera URL listed no items.")
        keys = await self.dm_titles(peer)
        token = self.apitera_token(uid)
        self.latest_panels[uid] = LatestPanel(peer, getattr(notice, "id", 0), token, items,
                                              (section,), page, url=url)
        words = unquote(query.get("query") or query.get("q") or query.get("search") or "").strip()
        if words:  # A search link: keep the matches, but never empty the API's own answer.
            items = tuple(item for item in items if matches_words(item.title, words)) or items
        text = (self.search_panel_text(uid, items, keys, words, (section,)) if words
                else self.latest_panel_text(uid, items, keys, (section,), page))
        await self.safe_edit(notice, text, buttons=self.latest_buttons(uid, token, items, keys),
                             parse_mode=None)

    async def show_search(self, uid: int, peer, notice, words: str,
                          sections: tuple[str, ...]) -> None:
        """Edit `notice` into the search matches with the same buttons as /latest."""
        api = ParcivApi(self.api_for(uid, "parciv"))
        items, notes = await self.search_catalogue(uid, api, sections, words)
        if not items:
            reason = "; ".join(notes[:2]) or "nothing that matches came back"
            await self.safe_edit(notice, f"🔎 Nothing matched {words!r} in "
                                         f"{sections_text(sections)}: {reason}.")
            return
        keys = await self.dm_titles(peer)
        token = self.apitera_token(uid)
        self.latest_panels[uid] = LatestPanel(peer, getattr(notice, "id", 0), token, items,
                                              sections, 1, words=words)
        await self.safe_edit(notice, self.search_panel_text(uid, items, keys, words, sections,
                                                            notes),
                             buttons=self.latest_buttons(uid, token, items, keys), parse_mode=None)

    def search_panel_text(self, uid: int, items: tuple[ParcivItem, ...], keys: set[str],
                          words: str, sections: tuple[str, ...],
                          notes: tuple[str, ...] = ()) -> str:
        """The /search reply: the matches, what the DM holds and the auto state."""
        fresh = new_items(items, keys)
        on = self.auto_enabled.get(uid, False)
        scope = "every catalogue" if len(sections) == len(SEARCH_SECTIONS) else sections_text(sections)
        lines = [f"🔎 apitera search — {words} · {scope}",
                 "✅ = already in your DM · ⬇️ = not uploaded yet", ""]
        lines += listing_lines(CataloguePage("", 1, items), keys, len(items))
        lines += ["", f"⬇️ {len(fresh)} of {len(items)} match(es) are new. Every quality of a "
                      "title is uploaded, named with its language and subtitle tags."]
        if notes:
            lines.append("⚠️ " + " · ".join(notes[:3]))
        lines.append(f"🤖 Automatic: {'ON' if on else 'OFF'} · every "
                     f"{interval_text(int(self.auto_interval))} · "
                     f"{limit_text(self.auto_limits.get(uid, self.auto_limit_default))}")
        return "\n".join(lines)

    async def parciv_link_flow(self, event, uid: int, peer, link: str, notice=None) -> None:
        """A hand-sent apitera link: a catalogue page lists, one item offers its qualities."""
        api = ParcivApi(self.api_for(uid, "parciv"))
        parts = urlsplit(link)
        path = parts.path.rstrip("/").lower()
        query = dict(pair.split("=", 1) for pair in parts.query.split("&") if "=" in pair)
        section = next((name for name in PARCIV_STREAM_SECTIONS if path.startswith(f"/{name}")), "")
        if section and (path.endswith("/post") or query.get("slug")):
            item = ParcivItem(subject_id=query.get("id") or query.get("slug", ""), name="",
                              section=section, post_url=link)
            await self.parciv_item_selection(event, uid, peer, api, item, notice=notice)
            return
        if path.endswith(("/latest", "/videos", "/trending")) and section:
            notice = notice or await event.respond(f"Reading {sections_text((section,))}…")
            await self.show_latest(uid, peer, notice, (section,), 1, self.auto_limit_default)
            return
        listed = next((name for name in PARCIV_SECTIONS if path.endswith(f"/{name}")), "")
        if listed and not query.get("id"):
            page = max(1, min(50, int(re.sub(r"\D", "", query.get("page", "1")) or 1)))
            notice = notice or await event.respond(f"Reading {sections_text((listed,))}…")
            await self.show_latest(uid, peer, notice, (listed,), page, self.auto_limit_default)
            return
        subject_id = query.get("id") or query.get("subjectId") or query.get("subject_id") or ""
        if not subject_id:
            match = re.search(r"[?&](?:id|subjectId)=([\w-]+)", link) or re.search(r"/(\d{6,})", link)
            subject_id = match.group(1) if match else ""
        if not subject_id:
            # Any other catalogue-style link of these families: Anime and Donghua each
            # have a dozen endpoints, and any of them can be opened this way.
            family = next((name for name in PARCIV_STREAM_SECTIONS
                           if path.startswith(f"/{name}/")), "")
            if not family and not path.startswith("/moviebox/"):
                raise ParcivError("That apitera URL names no item. Send /latest to browse, or a "
                                  "/moviebox/stream?id=… link.")
            notice = notice or await event.respond("Reading that apitera listing…")
            try:
                await self.show_listing_url(uid, peer, notice, link, family or "movies")
            except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, OSError) as exc:
                LOG.warning("apitera listing link failed: %s", type(exc).__name__)
                await self.safe_edit(notice, f"That apitera URL listed nothing "
                                             f"({type(exc).__name__}).")
            return
        await self.parciv_item_selection(event, uid, peer, api,
                                         ParcivItem(subject_id=subject_id, name="", section="movies"),
                                         notice=notice)

    async def parciv_item_selection(self, event, uid: int, peer, api: ParcivApi,
                                    item: ParcivItem, notice=None) -> None:
        """Read one item's media and offer its qualities as toggle buttons."""
        notice = notice or await event.respond("Reading this apitera item…")
        session = self.http_for(uid)
        if item.subject_id and not item.post_url:
            try:
                item = await fetch_parciv_item(session, api, item)
            except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, OSError) as exc:
                LOG.info("apitera detail lookup failed (%s); using the stream title",
                         type(exc).__name__)
        media = await fetch_parciv_media(session, api, item)
        sources = media.labelled_sources()
        if not sources:
            raise ParcivError("This apitera item offers no downloadable quality.")
        # The batch carries what was just read, so the job's title is the real one.
        item = replace(item, name=media.title or item.name, poster=media.poster or item.poster,
                       episode=media.episode or item.episode, langs=media.langs or item.langs,
                       summary=media.summary or item.summary)
        batch = ParcivBatch(items=(item,), check_dm=False, source_url=api.endpoint)
        selection = Selection(secrets.token_hex(4), Source("parciv", api.endpoint),
                              tuple(source.label for source in sources), peer, parciv=batch,
                              api_endpoint=self.api_for(uid, "parciv"))
        self.selections[uid] = selection
        lines = [f"🎬 {media.title}"]
        if media.episode:
            lines.append(f"· {media.episode}")
        if media.langs:
            lines.append("· Audio: " + ", ".join(media.langs))
        if media.subtitles:
            lines.append("· Subtitles: " + ", ".join(media.subtitles))
        lines += [f"· {source.label} ({source.codec or 'video'}{', DASH' if source.dash else ''}"
                  f"{', ' + human_size(source.size) if source.size else ''})" for source in sources]
        lines.append("Choose one or more, then tap Done:")
        await notice.edit("\n".join(lines), buttons=self.keyboard(selection), parse_mode=None)

    async def handle_parciv(self, event, text: str) -> None:
        """`/parciv <link|id>` — read one apitera item, or browse a catalogue link."""
        uid = event.sender_id
        arg = text.partition(" ")[2].strip()
        if not arg:
            await event.respond("Use /parciv <apitera link or subject id>, or send the link in "
                                "apitera mode. /latest browses the catalogue; /auto turns the "
                                "automatic uploads on and off.")
            return
        peer = await event.get_input_chat()
        self.auto_peers[uid] = peer
        if arg.isdigit():
            item = ParcivItem(subject_id=arg, name="", section="movies")
        else:
            try:
                link = parse_parciv_source(arg)
            except ValueError as exc:
                await event.respond(str(exc))
                return
            notice = await event.respond("Reading this apitera link…")
            try:
                await self.parciv_link_flow(event, uid, peer, link, notice=notice)
            except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, OSError) as exc:
                LOG.warning("apitera link failed: %s", type(exc).__name__)
                reason = str(exc) if isinstance(exc, ParcivError) and str(exc) else type(exc).__name__
                await self.safe_edit(notice, f"Could not read that apitera link ({reason}).")
            return
        api = ParcivApi(self.api_for(uid, "parciv"))
        try:
            await self.parciv_item_selection(event, uid, peer, api, item)
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, OSError) as exc:
            LOG.warning("apitera item failed: %s", type(exc).__name__)
            reason = str(exc) if isinstance(exc, ParcivError) and str(exc) else type(exc).__name__
            await event.respond(f"Could not read that apitera item ({reason}).")

    async def auto_action(self, event, uid: int, action: str) -> None:
        """The buttons on the /auto panel and on a /latest listing."""
        if action in ("on", "off"):
            if action == "on":
                self.auto_peers[uid] = await event.get_input_chat()
                self.auto_enabled[uid] = True
                self.start_auto_watch(uid)
            else:
                self.stop_auto_watch(uid)
            await event.answer("Automatic uploads ON" if action == "on" else "Automatic uploads OFF")
            await event.edit(self.auto_status_text(uid), buttons=self.latest_buttons(
                uid, self.auto_tokens.get(uid, ""), self.latest_panels[uid].items if uid in self.latest_panels else (),
                set()))
            return
        if action == "now":
            self.auto_peers[uid] = self.auto_peers.get(uid) or await event.get_input_chat()
            await event.answer("Checking now…")
            try:
                result = await self.run_auto_check(uid, note="Manual check (button).")
            except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, OSError) as exc:
                await event.edit(f"Could not check the catalogue ({type(exc).__name__}).")
                return
            if "Queued" not in result:
                await event.edit(result)
            return
        panel = self.latest_panels.get(uid)
        if panel is None:
            await event.answer("That listing expired. Send /latest again.", alert=True)
            return
        if action == "latest":
            self.auto_peers[uid] = panel.peer
            if getattr(panel, "url", ""):  # A link panel: Refresh re-reads that link.
                await event.answer("Refreshing…")
                await self.show_listing_url(uid, panel.peer, event, panel.url, panel.sections[0])
                return
            if panel.words:  # A search panel: Refresh repeats the search.
                await event.answer("Searching again…")
                await self.show_search(uid, panel.peer, event, panel.words, panel.sections)
                return
            await event.answer("Refreshing…")
            await self.show_latest(uid, panel.peer, event, panel.sections, panel.page,
                                   self.auto_limit_default)
            return
        if action in ("go", "all"):
            keys = await self.dm_titles(panel.peer) if action == "go" else set()
            items = new_items(panel.items, keys) if action == "go" else panel.items
            if not items:
                await event.answer("Nothing new to download.", alert=True)
                return
            batch = ParcivBatch(items=items, check_dm=True, source_url=self.api_for(uid, "parciv"),
                                sections=panel.sections)
            await event.answer(f"Queued {len(items)} title(s)")
            await self.queue_parciv_job(uid, panel.peer, batch, notice=event,
                                        note="From the /latest listing.")

    async def callback(self, event) -> None:
        uid = event.sender_id
        parts = (event.data or b"").decode("ascii", errors="ignore").split(":")
        if parts[0] == "d":
            dashboard = self.dashboards.get(uid)
            if (not event.is_private or len(parts) != 3 or not dashboard
                    or dashboard.token != parts[1] or parts[2] != "refresh"
                    or event.message_id != dashboard.message_id):
                await event.answer("Status message expired. Send /status again.", alert=True)
                return
            now = time.monotonic()
            if now < dashboard.edit_flood_until:
                await event.answer("Telegram rate limit: try again shortly.")
            elif now - dashboard.last_manual < MANUAL_REFRESH_SECONDS:
                await event.answer("Just refreshed. Please wait a moment.")
            else:
                dashboard.last_manual = now
                await event.answer("Refreshing now")
                await self.render_dashboard(uid, force=True)
            return
        if parts[0] == "p":
            # The token ties the buttons to the panel this user opened last.
            if (not event.is_private or len(parts) != 3 or self.proxy_tokens.get(uid) != parts[1]
                    or parts[2] not in ("on", "off", "status", "recheck", "purge", "clear",
                                        "export")):
                await event.answer("Proxy panel expired. Send /proxy again.", alert=True)
                return
            await self.proxy_action(event, uid, parts[2])
            return
        if parts[0] == "a":
            if (not event.is_private or len(parts) != 3 or self.auto_tokens.get(uid) != parts[1]
                    or parts[2] not in ("on", "off", "now", "latest", "go", "all")):
                await event.answer("These apitera buttons expired. Send /auto or /latest again.",
                                   alert=True)
                return
            await self.auto_action(event, uid, parts[2])
            return
        if parts[0] == "m":
            if (not event.is_private or len(parts) != 3 or self.mode_tokens.get(uid) != parts[1]
                    or parts[2] not in MODE_LABELS):
                await event.answer("Mode selector expired. Send /change again.", alert=True)
                return
            mode = parts[2]
            self.modes[uid] = mode
            self.mode_tokens.pop(uid, None)
            self.awaiting_api.discard(uid)
            self.pending_ranges.pop(uid, None)
            self.selections.pop(uid, None)
            label = MODE_LABELS[mode]
            await event.answer(f"{label} selected")
            await event.edit(f"{label} mode is active. Send a {label} link.")
            return
        if parts[0] == "j":
            job = self.jobs.get(parts[1]) if len(parts) == 3 else None
            if (not event.is_private or not job or job.user_id != uid):
                await event.answer("This job has finished or expired.", alert=True)
                return
            if parts[2] == "cancel":
                if job.task:
                    job.task.cancel()
                await event.answer("Cancellation requested")
                return
            if parts[2] == "refresh":
                now = time.monotonic()
                if now < job.edit_flood_until:
                    await event.answer("Telegram rate limit: try again shortly.")
                    return
                if now - job.last_manual < MANUAL_REFRESH_SECONDS:
                    await event.answer("Just refreshed. Please wait a moment.")
                    return
                job.last_manual = now
                await event.answer("Refreshing now")
                if uid in self.dashboards:
                    await self.render_dashboard(uid, force=True)
                elif event.message_id == job.message_id:
                    await self.render(job, force=True)  # Resets the next automatic refresh.
                else:
                    # A /status reply has its own Refresh button.
                    job.next_edit = now + REPORT_SECONDS
                    try:
                        text = self.format_job(job)
                        await event.edit(text, buttons=self.job_buttons(job), parse_mode=None,
                                         formatting_entities=cancel_entities(text))
                    except FloodWaitError as exc:
                        job.edit_flood_until = time.monotonic() + exc.seconds + 3
                        job.next_edit = job.edit_flood_until
                    except MessageNotModifiedError:
                        pass
                    except RPCError:
                        LOG.warning("Could not refresh job status", exc_info=True)
                return
            await event.answer("Invalid button.", alert=True)
            return
        selection = self.selections.get(uid)
        if (not event.is_private or len(parts) < 3 or not selection or parts[1] != selection.token
                or time.monotonic() - selection.created > SESSION_SECONDS):
            await event.answer("This selection expired. Send the link again.", alert=True)
            if selection and time.monotonic() - selection.created > SESSION_SECONDS:
                self.selections.pop(uid, None)
            return
        action = parts[2]
        if action == "cancel":
            self.selections.pop(uid, None)
            await event.answer()
            await event.edit("Selection cancelled.")
            return
        if action == "t" and len(parts) == 4 and parts[3] in selection.choices:
            key = parts[3]
            if key in selection.selected:
                selection.selected.remove(key)
            else:
                selection.selected.add(key)
            await event.answer()
            try:
                await event.edit(buttons=self.keyboard(selection))
            except MessageNotModifiedError:
                pass  # A repeated tap can produce identical buttons; nothing to update.
            return
        if action != "done" or len(parts) != 3:
            await event.answer("Invalid button.", alert=True)
            return
        if not selection.selected:
            await event.answer("Select at least one quality first.", alert=True)
            return
        if len(self.jobs) >= MAX_WAITING + 1:
            await event.answer("Queue is full (one active, four waiting). Try later.", alert=True)
            return
        selected = tuple(key for key in selection.choices if key in selection.selected)
        parciv = replace(selection.parciv, qualities=selected) if selection.parciv else None
        job = Job(uid, selection.peer, event.message_id, selection.source, selected, selection.video,
                  api_endpoint=selection.api_endpoint, start=selection.start, end=selection.end,
                  listing=selection.listing, social=selection.social, parciv=parciv)
        job.title = (selection.video.title if selection.video else
                     selection.social.title if selection.social else
                     selection.parciv.title if selection.parciv else
                     selection.listing.files[job.start - 1].title if selection.listing else "")
        job.detail = f"Queued: {', '.join(selected)}. Waiting for the download slot."
        self.selections.pop(uid, None)
        self.jobs[job.token] = job
        job.task = asyncio.create_task(self.run_job(job))
        job.task.add_done_callback(lambda task: self.on_job_done(job, task))
        try:
            await event.answer("Added to queue")
        except RPCError:
            LOG.warning("Could not acknowledge the selection; starting the queued job", exc_info=True)
        # Replace the old dashboard/selection panel, not just edit it. The
        # task starts immediately (and can be cancelled even during status
        # publishing); queued jobs appear in the newly published dashboard.
        try:
            published = await self.replace_dashboard(
                uid, job.peer, lambda text, **kwargs: self.client.send_message(job.peer, text, **kwargs))
        except (RPCError, OSError):
            LOG.warning("Could not post combined status; using the existing job panel", exc_info=True)
            published = False
        if not published:
            await self.render(job, force=True)

    async def archive_job(self, job: Job) -> None:
        async with self.turn:
            if self.jobs.get(job.token) is job:
                self.jobs.pop(job.token, None)  # Completed/cancelled jobs leave the status.
            self.turn.notify_all()
        await self.render_dashboard(job.user_id, force=True)

    def on_job_done(self, job: Job, task: asyncio.Task) -> None:
        # Cancelling a task before its first event-loop step skips run_job's
        # finally block. Retire that orphaned queued job as well.
        if task.cancelled() and self.jobs.get(job.token) is job:
            asyncio.create_task(self.cleanup_unstarted_job(job))

    async def cleanup_unstarted_job(self, job: Job) -> None:
        if self.jobs.get(job.token) is job:
            job.state = "finished"
            job.detail = "Cancelled before transfer started."
            await self.archive_job(job)

    async def progress(self, job: Job, text: str, *, force: bool = False) -> None:
        job.detail = text
        await self.render(job, force=force)

    async def wait_turn(self, job: Job) -> None:
        async with self.turn:
            while True:
                queued = next((candidate for candidate in self.jobs.values()
                               if candidate.state == "queued"), None)
                running = any(candidate.state == "running" for candidate in self.jobs.values())
                if queued is job and not running:
                    job.state = "running"
                    return
                await self.turn.wait()

    async def fresh_terabox_file(self, job: Job, item: TeraboxFile) -> TeraboxFile:
        listing = job.listing
        if listing and listing.expires_at and listing.expires_at <= time.time() + 120:
            listing = await fetch_terabox(self.http_for(job.user_id), job.source.url,
                                          job.api_endpoint)
            job.listing = listing
            if listing.expires_at and listing.expires_at <= time.time():
                raise ValueError("Terabox API returned expired download links.")
        if listing:
            available = (file for index in range(len(listing.files))
                         for file in listing.options_for(index))
            match = next((file for file in available
                          if file.file_id == item.file_id and file.quality == item.quality), None)
            if not match:
                available = (file for index in range(len(listing.files))
                             for file in listing.options_for(index))
                match = next((file for file in available
                              if file.filename == item.filename and file.quality == item.quality), None)
            if match:
                return match
            raise ValueError("Selected file/quality is no longer available from the Terabox API.")
        return item

    async def run_job(self, job: Job) -> None:
        sent = missing = too_large = unknown_size = failed = 0
        skip_reasons: list[str] = []  # Shown in the final summary, not only in the logs.
        job.reporter = asyncio.create_task(self.report_loop(job))
        try:
            await self.progress(job, f"Queued: {', '.join(job.selected)}. Use /status or /cancel.", force=True)
            await self.wait_turn(job)
            async with self.slot:
                await self.progress(job, "Starting…")
                if job.source.kind == "dailymotion":
                    await self.run_dailymotion(job)
                    return
                if job.parciv:
                    await self.run_parciv(job)
                    return
                if job.source.kind == "social":
                    await self.run_social(job)
                    return
                if job.source.kind == "mega":
                    await self.run_mega(job)
                    return
                if job.source.kind == "playlist":
                    await self.progress(job, f"Reading playlist entries {job.start}-{job.end}…")
                    entries = await asyncio.wait_for(
                        self.youtube_task(job.user_id, extract_playlist_range, job.source.url,
                                          job.start, job.end), timeout=180)
                    LOG.info("Playlist range %s-%s: %d of %d entries readable",
                             job.start, job.end, len(entries), job.end - job.start + 1)
                elif job.source.kind.startswith("terabox"):
                    initial_listing = job.listing
                    entries = list(initial_listing.files[job.start - 1:job.end]) if initial_listing else []
                else:
                    entries = [PlaylistEntry(job.source.url, job.video.title if job.video else "")]
                if not entries:
                    raise ValueError("No available entries in this range.")
                job.item_total = job.end if job.source.kind in ("playlist", "terabox-folder") else len(entries)
                # Rumble has its own API response shape; both the first lookup and refreshes must use it.
                lookup = fetch_rumble if job.source.kind == "rumble" else fetch_video
                for offset, entry in enumerate(entries):
                    if job.source.kind == "playlist":
                        index = entry.position or job.start + offset
                    elif job.source.kind == "terabox-folder":
                        index = job.start + offset
                    else:
                        index = offset + 1
                    job.item_index = index
                    job.current_url = (f"{job.source.url}#{entry.file_id}" if isinstance(entry, TeraboxFile) else entry.url)
                    job.quality = ""
                    job.title = entry.title
                    job.total_bytes = job.downloaded = job.uploaded = 0
                    if isinstance(entry, TeraboxFile):
                        options = {file.quality: file for file in initial_listing.options_for(job.start - 1 + offset)}
                        for quality in job.selected:
                            if quality not in options:
                                missing += 1
                                continue
                            try:
                                item = await self.fresh_terabox_file(job, options[quality])
                                job.current_url = f"{job.source.url}#{item.file_id}:{item.quality}"
                                video = Video(item.title, {item.quality: item.url}, thumbnail=item.thumbnail)
                                job.title = video.title
                                job.quality = item.quality
                                job.total_bytes = job.downloaded = job.uploaded = 0
                                job.speed = 0.0
                                job.sample_bytes, job.sample_at = 0, time.monotonic()
                                await self.progress(job, "Streaming Terabox → Telegram.")
                                await self.send_stream(job, video, item.quality, item.url, index, job.item_total, item)
                                sent += 1
                            except FileTooLarge:
                                too_large += 1
                            except UnknownLength:
                                unknown_size += 1
                            except (aiohttp.ClientError, RPCError, asyncio.TimeoutError, ValueError, OSError) as exc:
                                LOG.warning("Terabox transfer failed for item %s, quality %s: %s",
                                            index, quality, type(exc).__name__)
                                failed += 1
                        continue
                    await self.progress(job, f"Checking formats (API may take 1–2 minutes). Sent {sent} so far.")
                    if offset and job.source.kind == "playlist":
                        await asyncio.sleep(LOOKUP_GAP_SECONDS)
                    http = self.http_for(job.user_id)
                    proxy_url = self.proxy_for(job.user_id)
                    try:
                        video = job.video if job.video else await lookup(http, entry.url, job.api_endpoint)
                        youtube_name = (entry.title
                                        or await get_youtube_title(http, entry.url,
                                                                   proxy_url and proxy_url.url))
                        video = replace(video, title=choose_title(youtube_name, video.title, entry.url))
                        job.title = video.title
                    except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, TypeError) as exc:
                        status = getattr(exc, "status", "")
                        LOG.warning("Lookup failed for item %s (%s%s)", index, type(exc).__name__,
                                    f", HTTP {status}" if status else "")
                        reason = f"HTTP {status}" if status else (
                            str(exc)[:60] if isinstance(exc, ValueError) and str(exc) else type(exc).__name__)
                        skip_reasons.append(f"item {index}: lookup {reason}")
                        failed += len(job.selected)
                        continue
                    for quality in job.selected:
                        stream = video.streams.get(quality)
                        if not stream:
                            missing += 1
                            continue
                        job.quality = quality
                        job.total_bytes = job.downloaded = job.uploaded = 0
                        job.speed = 0.0
                        job.sample_bytes, job.sample_at = 0, time.monotonic()
                        await self.progress(job, f"Streaming provider → Telegram. Sent {sent} so far.")
                        refreshed = False
                        while True:
                            try:
                                await self.send_stream(job, video, quality, stream, index, job.item_total)
                                sent += 1
                            except FileTooLarge:
                                too_large += 1
                            except UnknownLength:
                                unknown_size += 1
                            except aiohttp.ClientResponseError as exc:
                                # The provider refused the link before any bytes were sent:
                                # ask the API once for a fresh link, then give up on this quality.
                                if exc.status in REFRESH_STATUSES and not job.downloaded and not refreshed:
                                    refreshed = True
                                    LOG.info("Link refused for item %s (HTTP %s); requesting a fresh link",
                                             index, exc.status)
                                    try:
                                        fresh = await lookup(self.http_for(job.user_id), entry.url,
                                                             job.api_endpoint)
                                        fresh_stream = fresh.streams.get(quality)
                                    except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, TypeError):
                                        fresh_stream = None
                                    if fresh_stream:
                                        video, stream = replace(fresh, title=video.title), fresh_stream
                                        continue
                                LOG.warning("Transfer failed at item %s, quality %s: HTTP %s",
                                            index, quality, exc.status)
                                skip_reasons.append(f"item {index} {quality}: HTTP {exc.status}")
                                failed += 1
                            except (aiohttp.ClientError, RPCError, asyncio.TimeoutError, ValueError, OSError) as exc:
                                LOG.warning("Transfer failed at item %s, quality %s: %s", index, quality, exc)
                                skip_reasons.append(f"item {index} {quality}: {type(exc).__name__}")
                                failed += 1
                            break
                job.state = "finished"
                detail = f" ({'; '.join(skip_reasons[:6])})" if skip_reasons else ""
                await self.progress(job, f"Finished. Sent {sent}; unavailable {missing}; over size limit {too_large}; "
                                         f"missing size header (>64 MiB) {unknown_size}; failed {failed}{detail}.",
                                    force=True)
        except asyncio.CancelledError:
            job.state = "finished"
            job.rate_limit_until = 0
            await self.progress(job, "Cancelled. Already uploaded files remain in the chat.", force=True)
        except (ValueError, asyncio.TimeoutError) as exc:
            job.state = "finished"
            await self.progress(job, f"Could not load the selected entries: {exc}", force=True)
        except Exception:
            job.state = "finished"
            LOG.exception("Unexpected job failure")
            await self.progress(job, "Job failed unexpectedly. Please try again later.", force=True)
        finally:
            if job.reporter:
                job.reporter.cancel()
                await asyncio.gather(job.reporter, return_exceptions=True)
            await self.archive_job(job)

    async def run_mega(self, job: Job) -> None:
        """One MEGA file or a folder range: stream each file straight to Telegram.

        Files are downloaded through the API URL the listing handed over, so
        proxy mode and the tool limits apply exactly as they do everywhere else.
        """
        listing = job.listing
        if not isinstance(listing, MegaListing):
            raise MegaError("The MEGA listing is missing; send the link again.")
        multi = job.start > 1 or job.end > job.start
        files = listing.files[job.start - 1:job.end] if multi else listing.files[:1]
        if not files:
            raise MegaError("No files in that range.")
        wanted = [file for file in files if video_choice(file) in job.selected]
        oversized = [file for file in wanted if file.size and file.size > self.max_bytes]
        if oversized:
            LOG.info("MEGA job %s: %d of %d selected file(s) exceed the %d MiB limit", job.token,
                     len(oversized), len(wanted), self.max_bytes // 1048576)
        sent = too_large = failed = 0
        skip_reasons: list[str] = []
        for offset, file in enumerate(files):
            choice = video_choice(file)
            if choice not in job.selected:
                continue
            index = job.start + offset if multi else offset + 1
            job.item_index = index
            job.item_total = job.end if multi else len(files)
            job.current_url = f"{job.source.url}#{file.file_id}:{choice}"
            job.title = file.title
            job.quality = choice
            job.total_bytes = job.downloaded = job.uploaded = 0
            job.speed = 0.0
            job.sample_bytes, job.sample_at = 0, time.monotonic()
            if file.size > self.max_bytes:
                too_large += 1
                continue
            video = Video(file.title, {choice: file.url}, thumbnail="")
            await self.progress(job, f"Downloading MEGA → Telegram ({file.path}).")
            urls = file.alternates()
            # Every URL is tried once; when they all fail without a byte moving the
            # whole host is probably asleep, so the first URL gets one more chance
            # after a wait instead of the file being counted as failed.
            attempts = [(url, 0) for url in urls]
            if urls:
                attempts.append((urls[0], MEGA_DOWNLOAD_WAIT))
            for position, (url, wait) in enumerate(attempts):
                try:
                    if wait:
                        await self.progress(
                            job, f"The MEGA host is not answering; waiting {wait}s and retrying…")
                        await asyncio.sleep(wait)
                    await self.send_stream(job, video, choice, url, index, job.item_total, file)
                    sent += 1
                    break
                except FileTooLarge:
                    too_large += 1
                    break
                except UnknownLength:
                    skip_reasons.append(f"{file.filename}: unknown size")
                    failed += 1
                    break
                except (aiohttp.ClientError, RPCError, asyncio.TimeoutError, ValueError,
                        OSError) as exc:
                    # The API's download route can refuse a link while its streaming
                    # route still works, so the other URLs for the same file are tried.
                    if position + 1 < len(attempts) and not job.downloaded:
                        LOG.info("MEGA URL refused for %s (%s); trying the next one",
                                 file.filename, type(exc).__name__)
                        continue
                    LOG.warning("MEGA transfer failed for %s: %s", file.filename, type(exc).__name__)
                    skip_reasons.append(f"{file.filename}: {type(exc).__name__}")
                    failed += 1
                    break
        summary = f"✅ Sent {sent} file(s) from MEGA."
        if too_large:
            summary += f" {too_large} over the size limit."
        if failed:
            summary += f" {failed} failed."
        if skip_reasons:
            summary += " " + " · ".join(skip_reasons[:3])
        await self.progress(job, summary, force=True)

    async def run_social(self, job: Job) -> None:
        """Instagram/Facebook: download the chosen media straight from the CDN."""
        media = job.social
        if not isinstance(media, (SocialMedia, SocialBatch)):
            raise SocialError("The Instagram/Facebook lookup is missing; send the link again.")
        options = social_choices(media)
        sent = failed = 0
        reasons: list[str] = []
        job.item_total = len(job.selected)
        for offset, label in enumerate(job.selected):
            choice = next((option for option in options if option.label == label), None)
            if choice is None:
                failed += 1
                continue
            item = choice.item
            job.item_index = offset + 1
            job.quality = item.label
            job.title = choice.media.title
            job.current_url = item.url
            job.total_bytes = job.downloaded = job.uploaded = 0
            job.speed = 0.0
            job.sample_bytes, job.sample_at = 0, time.monotonic()
            await self.progress(job, f"Downloading {item.label} → Telegram.")
            try:
                await self.send_social(job, choice.media, item)
                sent += 1
            except (aiohttp.ClientError, RPCError, asyncio.TimeoutError, ValueError,
                    OSError) as exc:
                LOG.warning("Instagram/Facebook transfer failed (%s): %s", item.label,
                            type(exc).__name__)
                reasons.append(f"{item.label}: {type(exc).__name__}")
                failed += 1
        summary = f"✅ Sent {sent} item(s)." if sent else "No media could be sent."
        if failed:
            summary += f" {failed} failed ({' · '.join(reasons[:3])})."
        await self.progress(job, summary, force=True)

    async def send_social(self, job: Job, media: SocialMedia, item: SocialDownload) -> None:
        """Send one Instagram/Facebook file: a photo, a video or a document.

        The CDN URLs are signed for a browser, so the requests carry a browser
        user agent and the platform's referer. Images are bounded downloads,
        videos stream through the same MTProto pipeline as every other service.
        """
        filename = media_filename(media.title, item, job.item_index, job.item_total)
        headers = {"User-Agent": BROWSER_UA, "Referer": f"https://www.{'instagram.com' if media.platform == 'Instagram' else 'facebook.com'}/"}
        session = self.http_for(job.user_id)
        thumb_data = thumb_w = thumb_h = None
        if not item.is_video:
            # Images are small: one bounded read, then send (or refuse) it.
            data = await download_social_image(session, item.url, headers, MAX_SOCIAL_IMAGE_BYTES)
            if item.is_image:
                buffer = io.BytesIO(data)
                buffer.name = filename
                sent = await self.client.send_file(job.peer, buffer, caption=media.title,
                                                   parse_mode=None, force_document=False)
                LOG.info("Sent %s image (%d bytes)", media.platform, len(data))
                return sent
            buffer = io.BytesIO(data)
            buffer.name = filename
            return await self.client.send_file(job.peer, buffer, caption=media.title,
                                               parse_mode=None, force_document=True)
        probe = MediaProbe(item.suffix)
        def on_total(count: int) -> None:
            job.total_bytes = count
        def on_download(count: int) -> None:
            job.downloaded += count
        def on_upload(done: int, total: int) -> None:
            job.uploaded = done
            job.total_bytes = total
        def on_flood(seconds: int) -> None:
            job.rate_limit_until = time.monotonic() + seconds if seconds else 0
        uploaded = await upload_stream(self.client, session, item.url, filename, self.max_bytes,
                                       on_total=on_total, on_download=on_download,
                                       on_upload=on_upload, on_flood=on_flood,
                                       on_data=probe.add, in_flight=self.in_flight,
                                       headers=headers)
        info = await probe.inspect()
        if media.thumbnail:
            cover = await thumbnail_jpeg(session, media.thumbnail)
            if cover:
                thumb_data, thumb_w, thumb_h = cover
        if not thumb_data:
            frame = await probe.preview_jpeg()
            if frame:
                thumb_data, thumb_w, thumb_h = frame
        thumb_file = io.BytesIO(thumb_data) if thumb_data else None
        if thumb_file:
            thumb_file.name = "preview.jpg"
        attributes = [types.DocumentAttributeVideo(duration=info.duration,
                                                   w=info.width or thumb_w or 16,
                                                   h=info.height or thumb_h or 9,
                                                   supports_streaming=True)]
        await self.progress(job, "Preparing preview and sending file to chat.")
        try:
            sent = await self.client.send_file(job.peer, uploaded, caption=media.title,
                                               parse_mode=None, mime_type="video/mp4",
                                               attributes=attributes, supports_streaming=True,
                                               thumb=thumb_file)
        except (PhotoInvalidDimensionsError, PhotoInvalidError, VideoContentTypeInvalidError):
            thumb_file = None
            sent = await self.client.send_file(job.peer, uploaded, caption=media.title,
                                               parse_mode=None, mime_type="video/mp4",
                                               attributes=attributes, supports_streaming=True,
                                               force_document=True)
        LOG.info("Sent %s video (%d bytes, cover %s)", media.platform, job.total_bytes,
                 "yes" if thumb_data else "no")
        return sent

    # ------------------------------------------------------------- apitera

    async def dm_titles(self, peer, limit: int = DM_SCAN_LIMIT) -> set[str]:
        """The titles this DM already holds, so an automatic run uploads nothing twice."""
        try:
            messages = [message async for message in self.client.iter_messages(peer, limit=limit)]
        except (RPCError, OSError, ValueError, TypeError) as exc:
            # Without the history the check cannot run; the job still uploads what it found.
            LOG.warning("Could not read the DM history for the apitera check: %s", type(exc).__name__)
            return set()
        keys = past_titles(messages)
        LOG.info("apitera: %d title form(s) already in this chat", len(keys))
        return keys

    def parciv_busy(self, uid: int) -> bool:
        """True while this user has an apitera batch queued or running."""
        return any(job.user_id == uid and job.parciv for job in self.jobs.values())

    async def read_catalogue(self, uid: int, api: ParcivApi, sections: tuple[str, ...],
                             limit: int, page: int = 1) -> tuple[list[CataloguePage], list[str]]:
        """Read page 1 of each section; a failing section is reported, not fatal."""
        session = self.http_for(uid)
        results = await asyncio.gather(
            *(fetch_parciv_catalogue(session, api, section, page) for section in sections),
            return_exceptions=True)
        pages: list[CataloguePage] = []
        problems: list[str] = []
        for section, result in zip(sections, results):
            if isinstance(result, CataloguePage):
                if result.items:
                    pages.append(CataloguePage(result.section, result.page,
                                               result.items[:limit], result.url))
                else:
                    problems.append(f"{sections_text((section,))} listed nothing")
                continue
            reason = str(result) if isinstance(result, (ParcivError, ValueError)) and str(result) \
                else type(result).__name__
            problems.append(f"{sections_text((section,))}: {reason}")
            LOG.warning("apitera catalogue read failed for %s: %s", section, type(result).__name__)
        return pages, problems

    async def queue_parciv_job(self, uid: int, peer, batch: ParcivBatch, *,
                               notice=None, note: str = "") -> Job | None:
        """Put one apitera batch into the download queue and open its status message.

        The batch is a normal job, so /status, /cancel and the one-transfer-at-a-time
        rule apply exactly as they do for a hand-sent link.
        """
        if len(self.jobs) >= MAX_WAITING + 1:
            text = "Queue is full (one active, four waiting). Try again later."
            LOG.info("apitera: %s", text)
            if notice is not None:
                await self.safe_edit(notice, text)
            else:
                await self.client.send_message(peer, text)
            return None
        if notice is not None:
            # A Message carries `.id`; a button click carries the message it came
            # from in `.message_id` (its own `.id` is the query id, not a message).
            message_id = getattr(notice, "message_id", 0) or getattr(notice, "id", 0)
        else:
            sent = await self.client.send_message(peer, f"🤖 apitera: {batch.plan_line()}")
            message_id = sent.id
        job = Job(uid, peer, message_id, Source("parciv", batch.source_url), ("All",), None,
                  parciv=batch, api_endpoint=self.api_for(uid, "parciv"))
        job.title = batch.title
        job.detail = f"Queued: {batch.plan_line()}. {note}".strip()
        self.jobs[job.token] = job
        job.task = asyncio.create_task(self.run_job(job))
        job.task.add_done_callback(lambda task: self.on_job_done(job, task))
        await self.render(job, force=True)
        return job

    async def run_parciv(self, job: Job) -> None:
        """apitera: upload every quality of every item that is not in the DM yet.

        One item at a time: read its detail (the canonical name) and its stream
        answer, then download each quality to a temporary file and send it as a
        streamable video with the API's poster as the cover. Files above the bot's
        size limit are reported instead of being downloaded.
        """
        batch = job.parciv
        if not isinstance(batch, ParcivBatch):
            raise ParcivError("The apitera batch is missing; send the link again.")
        uid = job.user_id
        api = ParcivApi(parciv_endpoint(job.api_endpoint))
        dm_keys = await self.dm_titles(job.peer) if batch.check_dm else set()
        sent = skipped = failed = too_large = 0
        reasons: list[str] = []
        job.item_total = len(batch.items)
        for position, item in enumerate(batch.items, 1):
            job.item_index = position
            job.title = item.title
            job.quality = ""
            job.total_bytes = job.downloaded = job.uploaded = 0
            job.speed = 0.0
            job.sample_bytes, job.sample_at = 0, time.monotonic()
            await self.progress(job, f"Reading {item.title} ({item.kind})…")
            try:
                media = await self.parciv_media(uid, api, item)
            except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, OSError) as exc:
                failed += 1
                reasons.append(f"{item.title}: {type(exc).__name__}")
                LOG.warning("apitera lookup failed for %s: %s", item.title, type(exc).__name__)
                continue
            if batch.check_dm and already_uploaded(media.title, dm_keys):
                skipped += 1
                LOG.info("apitera: %s is already in this chat", media.title)
                await self.progress(job, f"Already in your DM: {media.title}. Skipping.")
                continue
            job.title = media.title
            sources = [source for source in media.labelled_sources()
                       if not batch.qualities or source.label in batch.qualities]
            if not sources:
                failed += 1
                reasons.append(f"{item.title}: no matching quality")
                continue
            for source in sources:
                job.quality = source.label
                job.total_bytes = source.size
                job.downloaded = job.uploaded = 0
                job.speed = 0.0
                job.sample_bytes, job.sample_at = 0, time.monotonic()
                if source.size and source.size > self.max_bytes:
                    too_large += 1
                    LOG.info("apitera %s %s: %d bytes is over the limit", media.title, source.label,
                             source.size)
                    continue
                tmpdir = tempfile.mkdtemp(prefix="ytdown-parciv-")
                stop = threading.Event()
                try:
                    if source.size and shutil.disk_usage(tmpdir).free < source.size * 1.2:
                        raise ParcivError("not enough free disk space on this host")
                    proxy = self.proxy_for(uid, "parciv")
                    await self.progress(job, f"Downloading {media.title} {source.label}…")

                    def on_bytes(done: int, total: int, size: int = source.size) -> None:
                        job.downloaded = done
                        job.total_bytes = total or size

                    try:
                        path = await asyncio.to_thread(
                            download_parciv, source.url, tmpdir, on_bytes, stop,
                            proxy=proxy.url if proxy else None, headers=source.headers,
                            label=source.label, api=api, section=item.section,
                            dash=source.dash,
                            duration=media.duration or source.duration, title=media.title)
                    except asyncio.CancelledError:
                        stop.set()  # Stops the worker thread at its next progress callback.
                        raise
                    size = os.path.getsize(path)
                    if size > self.max_bytes:
                        too_large += 1
                        continue
                    await self.send_parciv_file(job, media, source, path, tmpdir)
                    sent += 1
                    if batch.check_dm:
                        dm_keys |= title_variants(media.title)
                except ParcivError as exc:
                    failed += 1
                    reasons.append(f"{media.title} {source.label}: {exc}")
                    LOG.warning("apitera transfer failed for %s %s: %s", media.title,
                                source.label, exc)
                except (aiohttp.ClientError, RPCError, asyncio.TimeoutError, ValueError,
                        OSError) as exc:
                    failed += 1
                    reasons.append(f"{media.title} {source.label}: {type(exc).__name__}")
                    LOG.warning("apitera upload failed for %s %s: %s", media.title, source.label,
                                type(exc).__name__)
                finally:
                    shutil.rmtree(tmpdir, ignore_errors=True)
        summary = f"✅ Uploaded {sent} file(s) from apitera."
        if skipped:
            summary += f" {skipped} title(s) were already in your DM."
        if too_large:
            summary += f" {too_large} file(s) are over the size limit."
        if failed:
            summary += f" {failed} failed ({' · '.join(reasons[:3])})."
        await self.progress(job, summary, force=True)

    async def parciv_media(self, uid: int, api: ParcivApi, item: ParcivItem) -> ParcivMedia:
        """One item's media, with the detail call first so the name is the API's own."""
        session = self.http_for(uid)
        try:
            item = await fetch_parciv_item(session, api, item)
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, OSError) as exc:
            # The catalogue name is good enough to upload under; the stream call is next.
            LOG.info("apitera detail for %s failed (%s); using the catalogue name", item.title,
                     type(exc).__name__)
        return await fetch_parciv_media(session, api, item)

    async def send_parciv_file(self, job: Job, media: ParcivMedia, source: ParcivSource,
                               path: str, tmpdir: str) -> None:
        """Send one downloaded quality as a streamable video, titled the way it was asked for.

        The name carries the API's title, its audio language tags and its subtitle
        language before the quality: "PAW Patrol_ The Dino Movie [Hindi] [Eng Sub]
        [360p].mp4", from a file already on disk (DASH streams need ffmpeg to mux).
        """
        title = media.display_title(source)
        # Brackets stay: the language tags are part of the name the user asked for.
        safe = re.sub(r"[^\w .\[\]-]", "_", title, flags=re.UNICODE).strip(" .") or "video"
        suffix = Path(path).suffix.lower() or ".mp4"
        final = os.path.join(tmpdir, media_name(safe, source.label, 100) + suffix)
        os.replace(path, final)
        duration = media.duration or source.duration
        thumb = (await thumbnail_jpeg(self.http_for(job.user_id), media.poster)
                 if media.poster else None)
        thumb_file = None
        if thumb:
            thumb_file = io.BytesIO(thumb[0])
            thumb_file.name = "preview.jpg"
        job.total_bytes = os.path.getsize(final)
        job.uploaded = 0

        def on_upload(done: int, total: int) -> None:
            job.uploaded = done
            job.total_bytes = total

        await self.progress(job, "Uploading to Telegram…")
        message = await self.client.send_file(
            job.peer, final, caption=media_name(title, source.label, 750), parse_mode=None,
            mime_type="video/webm" if suffix == ".webm" else "video/mp4", supports_streaming=True,
            thumb=thumb_file,
            attributes=[types.DocumentAttributeVideo(duration=duration, w=source.width or 1280,
                                                     h=source.height or 720, supports_streaming=True)],
            progress_callback=on_upload)
        LOG.info("Sent apitera %s %s (%d bytes, cover %s)", media.title, source.label,
                 job.total_bytes, "yes" if thumb else "no")
        return message

    async def run_auto_check(self, uid: int, *, notice=None, note: str = "",
                             announce: bool = False) -> str:
        """Read the catalogue and queue one batch for the titles the DM does not hold.

        With `announce` (the watcher) the check opens one message of its own: when
        a batch is queued that message becomes the batch's usual download status
        ("🎬 title, Video 1/3, progress…"), and when there is nothing to do it is
        removed again, so a check every 15 minutes leaves no clutter.
        """
        if self.parciv_busy(uid):
            return "An apitera batch is already queued or running. /cancel stops it."
        peer = self.auto_peers.get(uid)
        if peer is None:
            return "Send /auto on first, so the bot knows where to upload."
        sections = normalise_sections(self.auto_sections.get(uid, ()))
        limit = self.auto_limits.get(uid, self.auto_limit_default)
        if announce and notice is None:
            notice = await self.client.send_message(
                peer, f"🔎 Automatic apitera check: reading {sections_text(sections)}…")
        api = ParcivApi(self.api_for(uid, "parciv"))
        pages, problems = await self.read_catalogue(uid, api, sections, limit)
        items = tuple(item for page in pages for item in page.items)
        if not items:
            reason = "; ".join(problems[:2]) or "no entries"
            result = f"Nothing to upload: {reason}."
        else:
            keys = await self.dm_titles(peer)
            fresh = new_items(items, keys)
            if not fresh:
                result = f"Nothing new: all {len(items)} listed title(s) are already in your DM."
            else:
                batch = ParcivBatch(items=fresh, check_dm=True, source_url=api.endpoint,
                                    sections=sections)
                job = await self.queue_parciv_job(uid, peer, batch, notice=notice, note=note)
                result = (f"Queued {len(fresh)} new title(s) from {sections_text(sections)} "
                          f"({len(items) - len(fresh)} already in your DM).") if job else (
                    "The queue is full; the next automatic check will retry.")
        if notice is not None and "Queued" not in result:
            if announce and result.startswith("Nothing"):
                await self.drop_notice(notice)  # A clean chat beats a "nothing" every 15 minutes.
            else:
                await self.safe_edit(notice, result)
        return result

    async def drop_notice(self, notice) -> None:
        """Remove one of the bot's own messages; failing to is harmless."""
        try:
            await notice.delete()
        except (RPCError, AttributeError, TypeError, ValueError) as exc:
            LOG.info("Could not remove an apitera notice: %s", type(exc).__name__)

    async def auto_watch(self, uid: int) -> None:
        """The watcher behind `/auto on`: check, upload what is missing, wait, repeat."""
        LOG.info("apitera auto watch started for user %s (every %s)", uid,
                 interval_text(int(self.auto_interval)))
        first = True  # The first check runs straight away; /auto on should do something.
        while True:
            if not first:
                await asyncio.sleep(self.auto_interval)
            first = False
            if not self.auto_enabled.get(uid) or uid in self.auto_running:
                continue
            self.auto_running.add(uid)
            try:
                self.auto_last[uid] = time.monotonic()
                message = await self.run_auto_check(uid, note="Automatic check.", announce=True)
                LOG.info("apitera automatic check for user %s: %s", uid, message)
            except asyncio.CancelledError:
                raise
            except Exception:
                LOG.warning("apitera automatic check failed for user %s", uid, exc_info=True)
            finally:
                self.auto_running.discard(uid)

    def auto_status_text(self, uid: int) -> str:
        on = self.auto_enabled.get(uid, False)
        sections = normalise_sections(self.auto_sections.get(uid, ()))
        limit = self.auto_limits.get(uid, self.auto_limit_default)
        last = self.auto_last.get(uid)
        lines = [f"🤖 apitera auto: {'ON' if on else 'OFF'}",
                 f"Sections: {sections_text(sections)} (page 1 each)",
                 f"Pace: every {interval_text(int(self.auto_interval))}, {limit_text(limit)}",
                 "Uploads: every quality of each title, named with its language and subtitle tags",
                 "DM check: titles already in this chat are skipped",
                 f"API: {self.api_for(uid, 'parciv')}"]
        lines.append(f"Last check: {int(time.monotonic() - last)}s ago" if last else "Last check: not yet")
        return "\n".join(lines)

    def start_auto_watch(self, uid: int) -> None:
        task = self.auto_tasks.get(uid)
        if task and not task.done():
            return
        self.auto_tasks[uid] = asyncio.create_task(self.auto_watch(uid))

    def stop_auto_watch(self, uid: int) -> None:
        self.auto_enabled[uid] = False
        task = self.auto_tasks.pop(uid, None)
        if task:
            task.cancel()

    async def run_dailymotion(self, job: Job) -> None:
        """One Dailymotion video: HLS variant -> temporary MP4 (yt-dlp + ffmpeg) -> upload from disk."""
        info = job.video
        if not isinstance(info, DailymotionInfo):
            raise DailymotionError("The Dailymotion lookup is missing; send the link again.")
        if not ffmpeg_ready():
            raise DailymotionError("Dailymotion downloads need ffmpeg on this host (the Koyeb Docker image includes it).")
        formats = {fmt.label: fmt for fmt in info.formats}
        sent = missing = too_large = failed = 0
        reasons: list[str] = []
        job.item_index, job.item_total = 1, 1
        job.title = info.title
        job.current_url = job.source.url
        for quality in job.selected:
            fmt = formats.get(quality)
            if fmt is None:
                missing += 1
                continue
            job.quality = quality
            job.total_bytes = job.downloaded = job.uploaded = 0
            job.speed = 0.0
            job.sample_bytes, job.sample_at = 0, time.monotonic()
            estimate = estimate_bytes(fmt, info.duration)
            if estimate > self.max_bytes:
                too_large += 1
                continue
            tmpdir = tempfile.mkdtemp(prefix="ytdown-dm-")
            stop = threading.Event()
            try:
                if estimate and shutil.disk_usage(tmpdir).free < estimate * 1.2:
                    raise DailymotionError("Not enough free disk space on this host for this quality.")
                await self.progress(job, f"Downloading the Dailymotion {quality} stream…")

                def on_bytes(done: int, total: int) -> None:
                    job.downloaded = done
                    job.total_bytes = total or estimate

                try:
                    # The pool proxy is passed only when proxy mode is on, so a direct
                    # download keeps exactly the old yt-dlp options.
                    proxy = self.proxy_for(job.user_id, "dailymotion")
                    route = {"proxy": proxy.url} if proxy else {}
                    path = await asyncio.to_thread(
                        download_dailymotion, fmt.url, tmpdir, on_bytes, stop,
                        watch_url=job.source.url, height=fmt.height, media=info.media,
                        protocol=fmt.protocol, format_id=fmt.format_id, duration=info.duration,
                        title=info.title, **route)
                except asyncio.CancelledError:
                    stop.set()  # Stops the worker thread at its next progress callback.
                    raise
                size = os.path.getsize(path)
                if size > self.max_bytes:
                    too_large += 1
                    continue
                safe_title = re.sub(r"[^\w .-]", "_", info.title[:100], flags=re.UNICODE).strip(" .") or "video"
                final = os.path.join(tmpdir, media_name(safe_title, quality, 100) + ".mp4")
                os.replace(path, final)
                thumb = (await thumbnail_jpeg(self.http_for(job.user_id), info.poster)
                         if info.poster else None)
                thumb_file = None
                if thumb:
                    thumb_file = io.BytesIO(thumb[0])
                    thumb_file.name = "preview.jpg"
                job.total_bytes = size
                job.uploaded = 0

                def on_upload(done: int, total: int) -> None:
                    job.uploaded = done
                    job.total_bytes = total

                await self.progress(job, "Uploading to Telegram…")
                await self.client.send_file(
                    job.peer, final, caption=media_name(info.title, quality, 750), parse_mode=None,
                    mime_type="video/mp4", supports_streaming=True, thumb=thumb_file,
                    attributes=[types.DocumentAttributeVideo(duration=info.duration, w=fmt.width, h=fmt.height,
                                                             supports_streaming=True)],
                    progress_callback=on_upload)
                sent += 1
                LOG.info("Sent Dailymotion %s (%d bytes, cover %s)", quality, size, "yes" if thumb else "no")
            except DailymotionError as exc:
                failed += 1
                reasons.append(f"{quality}: {exc}")
                LOG.warning("Dailymotion %s skipped: %s", quality, exc)
            except (DownloadError, OSError, RPCError, aiohttp.ClientError, asyncio.TimeoutError, ValueError) as exc:
                failed += 1
                status = re.search(r"HTTP Error \d{3}", str(exc))
                reason = status.group(0) if status else type(exc).__name__
                reasons.append(f"{quality}: {reason}")
                LOG.warning("Dailymotion %s failed: %s", quality, reason)
            finally:
                shutil.rmtree(tmpdir, ignore_errors=True)
        job.state = "finished"
        detail = f" ({'; '.join(reasons)})" if reasons else ""
        await self.progress(job, f"Finished. Sent {sent}; unavailable {missing}; over size limit {too_large}; "
                                 f"failed {failed}{detail}.", force=True)

    async def video_thumbnail(self, job: Job, video: Video) -> tuple[bytes | None, int, int]:
        source = job.current_url or job.source.url
        if job.thumbnail_id == source:
            return job.thumbnail, *job.thumb_size
        if job.source.kind.startswith("terabox"):
            # Prefer the file URL supplied by the API, then its parent listing
            # thumbnail. Both go through the same JPEG/redirect/blank checks as YouTube.
            urls = list(dict.fromkeys(url for url in (video.thumbnail, *job.thumbnail_urls) if url))
        elif job.source.kind == "rumble":
            # Rumble only offers the image its API supplies; no YouTube-style fallbacks.
            urls = [video.thumbnail] if video.thumbnail else []
        else:
            video_id = source.rsplit("=", 1)[-1]
            # Prefer the API's supplied thumbnail; YouTube sizes are fallbacks.
            urls = [video.thumbnail] if video.thumbnail else []
            # HQ is reliably present; maxres can be a valid but blank placeholder.
            for candidate in (f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg",
                              f"https://i.ytimg.com/vi/{video_id}/maxresdefault.jpg"):
                if candidate not in urls:
                    urls.append(candidate)
        thumb = None
        for image_url in urls:
            thumb = await thumbnail_jpeg(self.http_for(job.user_id), image_url)
            if thumb:
                LOG.info("Prepared %s thumbnail from %s (%d bytes, %dx%d)",
                         job.source.kind, urlsplit(image_url).hostname,
                         len(thumb[0]), thumb[1], thumb[2])
                break
        if not thumb:
            LOG.info("No usable API/site thumbnail for %s", job.source.kind)
        job.thumbnail_id = source
        job.thumbnail = thumb[0] if thumb else None
        job.thumb_size = thumb[1:] if thumb else (0, 0)
        return job.thumbnail, *job.thumb_size

    async def returned_still(self, message) -> bytes | None:
        """Bounded download of Telegram's returned still-image thumbnail (None if missing)."""
        document = getattr(message, "document", None)
        if not isinstance(document, types.Document):
            return None
        stills = [thumb for thumb in (document.thumbs or [])
                  if isinstance(thumb, (types.PhotoSize, types.PhotoCachedSize))]
        if not stills:
            return None
        best = max(stills, key=lambda thumb: thumb.w * thumb.h)
        if isinstance(best, types.PhotoCachedSize):  # Telegram already inlined these bytes.
            data = bytes(best.bytes)
            return data if 0 < len(data) <= THUMB_LIMIT else None
        if best.size > THUMB_LIMIT:
            return None
        location = types.InputDocumentFileLocation(id=document.id, access_hash=document.access_hash,
                                                   file_reference=document.file_reference,
                                                   thumb_size=best.type)
        data = bytearray()
        async for chunk in self.client.iter_download(location, file_size=best.size, request_size=THUMB_REQUEST):
            data.extend(chunk)
            if len(data) > THUMB_LIMIT:
                return None
        return bytes(data) or None

    async def ensure_cover(self, job: Job, message, cover: bytes | None) -> None:
        """Reply to the delivered video with our JPEG if Telegram's still is missing or white.

        Failures are logged only. The video itself is never re-sent here.
        """
        if message is None:
            return  # Nothing was returned to inspect.
        try:
            still = await asyncio.wait_for(self.returned_still(message), timeout=COVER_CHECK_SECONDS)
            if still and await asyncio.to_thread(usable_still, still):
                return
        except Exception as exc:
            LOG.info("Could not read Telegram's returned preview: %s", type(exc).__name__)
        if not cover:
            LOG.info("Telegram's preview is missing or white and no prepared cover exists")
            return
        try:
            photo = io.BytesIO(cover)
            photo.name = "cover.jpg"
            await self.client.send_file(job.peer, photo, reply_to=message.id, force_document=False)
            LOG.info("Sent a separate cover photo for the video")
        except Exception:
            LOG.warning("Could not send the cover photo; the video remains in the chat", exc_info=True)

    async def send_stream(self, job: Job, video: Video, quality: str, url: str, index: int, total: int,
                          source_file: TeraboxFile | MegaFile | None = None) -> None:
        suffix = (Path(source_file.filename).suffix.lower() if source_file else
                  ".mp3" if quality == AUDIO else ".mp4")
        # ".mp4.ts", ".part" and friends exist in the wild; Telegram needs a sane one.
        suffix = suffix if re.fullmatch(r"\.[a-z0-9]{1,8}", suffix or "") else (
            ".mp3" if quality == AUDIO else ".mp4")
        suffix = suffix if re.fullmatch(r"\.[a-z0-9]{1,8}", suffix) else ".bin"
        audio = quality.lower() == "mp3" or bool(source_file and source_file.mime.startswith("audio/"))
        video_file = source_file.video if source_file else not audio
        streamable = video_file and suffix == ".mp4"
        job.thumbnail_urls = source_file.thumbnail_urls if source_file else ()
        # YouTube only: subtitle markers move after the quality, and playlist
        # filenames start with "02-39". Terabox keeps its original names and captions.
        youtube = source_file is None
        clean_title, subtitle_tag = split_subtitle_tag(video.title) if youtube else (video.title, "")
        safe_title = re.sub(r"[^\w .-]", "_", clean_title, flags=re.UNICODE).strip(" .") or "video"
        file_prefix = ""
        if youtube and job.source.kind == "playlist" and total > 1:
            width = max(2, len(str(total)))
            file_prefix = f"{index:0{width}d}-{total:0{width}d} "
        filename = file_prefix + media_name(safe_title, quality, 100 - len(file_prefix), subtitle_tag) + suffix
        if youtube:
            caption = media_label(video.title, quality, 750)  # No position in YouTube captions.
        else:
            prefix = f"{index}/{total} · " if total > 1 else ""
            caption = prefix + media_name(video.title, quality, 750 - len(prefix))
        def on_total(count: int) -> None:
            job.total_bytes = count

        def on_download(count: int) -> None:
            job.downloaded += count

        def on_upload(done: int, total: int) -> None:
            job.uploaded = done
            job.total_bytes = total

        def on_flood(seconds: int) -> None:
            job.rate_limit_until = time.monotonic() + seconds if seconds else 0

        probe = MediaProbe(suffix) if video_file else None
        thumb_task = asyncio.create_task(self.video_thumbnail(job, video)) if probe else None
        try:
            uploaded = await upload_stream(self.client, self.http_for(job.user_id), url, filename,
                                           self.max_bytes,
                                           on_total=on_total, on_download=on_download,
                                           on_upload=on_upload, on_flood=on_flood,
                                           on_data=probe.add if probe else None,
                                           in_flight=self.in_flight,
                                           expected_size=source_file.size if source_file else 0)
        except BaseException:
            if thumb_task:
                thumb_task.cancel()
                await asyncio.gather(thumb_task, return_exceptions=True)
            raise
        await self.progress(job, "Preparing preview and sending file to chat.")
        try:
            thumb_data, thumb_w, thumb_h = await thumb_task if thumb_task else (None, 0, 0)
        except Exception as exc:
            LOG.warning("Thumbnail unavailable: %s", type(exc).__name__)
            thumb_data, thumb_w, thumb_h = None, 0, 0
        info = await probe.inspect() if probe else None
        if video_file and not thumb_data and probe:
            # Terabox often omits or expires preview URLs, and YouTube playlist items
            # have no API thumbnail. Use one decoded frame from the bounded sample
            # already captured while streaming; no second full download.
            fallback = await probe.preview_jpeg()
            if fallback:
                thumb_data, thumb_w, thumb_h = fallback
                LOG.info("Prepared %s frame thumbnail (%d bytes, %dx%d)",
                         "Terabox" if source_file else job.source.kind,
                         len(thumb_data), thumb_w, thumb_h)
        if video_file and not thumb_data:
            LOG.warning("No usable thumbnail for %s; Telegram may show the video's own first frame",
                        job.source.kind)
        if audio:
            attributes = [types.DocumentAttributeAudio(duration=video.duration,
                                                       title=(media_label(video.title, quality, 64) if youtube else
                                                              media_name(video.title, quality, 64)))]
        elif video_file:
            # ffprobe inspects the actual MP4 head/tail. API duration and image
            # dimensions are fallbacks only if its metadata cannot be parsed.
            attributes = [types.DocumentAttributeVideo(duration=info.duration or video.duration,
                                                       w=info.width or thumb_w or 16,
                                                       h=info.height or thumb_h or 9,
                                                       supports_streaming=streamable)]
        else:
            attributes = None

        async def send_with_preview(*, document: bool = False, with_thumb: bool = True):
            thumb_file = None
            if thumb_data and with_thumb:
                thumb_file = io.BytesIO(thumb_data)
                thumb_file.name = "preview.jpg"
            # MP4s stay inline (streamable video) unless Telegram refuses them as video.
            return await self.client.send_file(job.peer, uploaded, caption=caption, parse_mode=None,
                                               mime_type=(source_file.mime if source_file else
                                                          "audio/mpeg" if audio else "video/mp4"),
                                               attributes=attributes,
                                               supports_streaming=streamable and not document,
                                               force_document=document or (
                                                   source_file is not None and not streamable and not audio),
                                               thumb=thumb_file)

        async def deliver():
            # These retries only follow a rejected send, so the video was not delivered yet.
            # The uploaded handle is reused; bytes are never streamed again.
            try:
                return await send_with_preview()
            except (PhotoInvalidDimensionsError, PhotoInvalidError):
                LOG.warning("Telegram rejected the prepared thumbnail for %s", job.source.kind)
                return await send_with_preview(with_thumb=False)
            except VideoContentTypeInvalidError:
                if audio:
                    raise
                # Last resort: Telegram refused the MP4 as video, so send the same handle as a document.
                try:
                    return await send_with_preview(document=True)
                except (PhotoInvalidDimensionsError, PhotoInvalidError):
                    return await send_with_preview(document=True, with_thumb=False)

        sent_message = None
        for attempt in range(3):
            try:
                sent_message = await deliver()
                job.rate_limit_until = 0
                break
            except FloodWaitError as exc:
                if attempt == 2 or exc.seconds > 300:
                    raise
                job.rate_limit_until = time.monotonic() + exc.seconds + 1
                await asyncio.sleep(exc.seconds + 1)
        # The video is delivered now. YouTube cover problems must never re-send it.
        # Terabox thumbnails keep their existing behaviour (no reply cover).
        if youtube and video_file:
            await self.ensure_cover(job, sent_message, thumb_data)


async def start_health(runner: web.AppRunner, port: int) -> bool:
    """Optional /healthz endpoint (Koyeb uses it). Hosts without public ports, such as BotKeep, can skip it."""
    try:
        await web.TCPSite(runner, "0.0.0.0", port).start()
        LOG.info("Health check listening on port %s", port)
        return True
    except OSError as exc:
        LOG.warning("Health check not started on port %s (%s); the bot keeps running.", port, exc)
        return False


def check_media_tools() -> None:
    """ffmpeg/ffprobe add durations and frame thumbnails; without them the bot falls back to API data."""
    missing = [tool for tool in ("ffmpeg", "ffprobe") if not shutil.which(tool)]
    if missing:
        LOG.warning("%s not found: durations come from the API, frame thumbnails are skipped, and "
                    "Dailymotion downloads are unavailable. Docker images include ffmpeg; BotKeep may not.",
                    ", ".join(missing))


async def serve(token: str, api_id: int, api_hash: str) -> None:
    # MemorySession is not a StringSession and writes no authentication file to disk.
    client = TelegramClient(MemorySession(), api_id, api_hash, receive_updates=True)
    timeout = aiohttp.ClientTimeout(total=240, connect=20)
    connector = aiohttp.TCPConnector(resolver=PublicResolver())
    headers = {"User-Agent": "ytdown-telegram-bot/1.0"}
    async with aiohttp.ClientSession(timeout=timeout, connector=connector,
                                     headers=headers) as http:
        service = DownloaderBot(client, http, timeout=timeout, headers=headers)
        service.register()
        health = web.Application()
        health.router.add_get("/healthz", lambda request: web.json_response(
            {"status": "ok" if client.is_connected() else "disconnected"},
            status=200 if client.is_connected() else 503))
        runner = web.AppRunner(health)
        await runner.setup()
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for name in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(name, stop.set)
            except NotImplementedError:
                pass
        try:
            await client.start(bot_token=token)
            await start_health(runner, env_int("PORT", 8000, 1, 65535))
            LOG.info("MTProto bot connected")
            await stop.wait()
        finally:
            for task in list(service.lookups.values()) + [j.task for j in service.jobs.values() if j.task]:
                task.cancel()
            await asyncio.gather(*list(service.lookups.values()),
                                 *(j.task for j in service.jobs.values() if j.task), return_exceptions=True)
            await service.close()
            await client.disconnect()
            await runner.cleanup()


def main() -> None:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    token = os.getenv("BOT_TOKEN")
    api_hash = os.getenv("API_HASH")
    try:
        api_id = int(os.getenv("API_ID", ""))
    except ValueError:
        api_id = 0
    if not token or not api_id or not api_hash:
        raise SystemExit("Set BOT_TOKEN, API_ID, and API_HASH as environment variables.")
    check_media_tools()
    asyncio.run(serve(token, api_id, api_hash))
