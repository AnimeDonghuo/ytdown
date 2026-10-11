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
import signal
import tempfile
import time
from dataclasses import dataclass, field, replace
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit

import aiohttp
from aiohttp import web
from telethon import Button, TelegramClient, events, helpers
from telethon.errors import (FloodWaitError, MessageNotModifiedError, PhotoInvalidDimensionsError,
                             PhotoInvalidError, RPCError, VideoContentTypeInvalidError)
from telethon.sessions import MemorySession
from telethon.tl import types
from telethon.tl.functions.upload import SaveBigFilePartRequest, SaveFilePartRequest

from .core import (AUDIO, VIDEO_QUALITIES, PlaylistEntry, Source, Video, extract_playlist_range,
                   extract_title, normalize_video, parse_source, playlist_count, public_https_url, youtube_title)
from .terabox import (TERABOX_API_URL, TeraboxFile, TeraboxListing, normalize_terabox,
                      parse_terabox_source)
from .metrics import ContainerMetrics, human_size
from .media import MediaProbe, thumbnail_jpeg

LOG = logging.getLogger(__name__)
API_URL = "https://samra-youtube-api.antideploy.com/"
MAX_WAITING = 4
MAX_SELECTIONS = 100
SESSION_SECONDS = 1800
# Telegram clients limit uploads to 4,000 parts of 512 KiB (2,000 MiB).
# 2,047 MiB would need 4,094 parts and can fail near the end of an upload.
MAX_MT_MB = 2000
PART_BYTES = 512 * 1024
MAX_PARTS = 4000
SPOOL_MB = 64  # For rare providers that do not send Content-Length.
REPORT_SECONDS = 12  # Automatic edits: keep well below Telegram flood limits.
MANUAL_REFRESH_SECONDS = 2
MAX_RANGE_VIDEOS = 150
MAX_DISCOVER = 5000
MAX_DASHBOARDS = 100
MAX_COMPLETED_USERS = 200


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


class FileTooLarge(Exception):
    pass


class UnknownLength(Exception):
    pass


def media_name(title: str, quality: str, max_length: int) -> str:
    """Keep the [quality] suffix even when a long video title is shortened."""
    suffix = f" [{quality.upper() if quality == AUDIO else quality}]"
    return (title[:max_length - len(suffix)].rstrip() or "YouTube video") + suffix


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
    listing: TeraboxListing | None = None


@dataclass
class RangeRequest:
    source: Source
    peer: object
    count: int
    api_endpoint: str = API_URL
    listing: TeraboxListing | None = None
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
    thumb_size: tuple[int, int] = (0, 0)
    current_url: str = ""
    start: int = 1
    end: int = 1
    listing: TeraboxListing | None = None


def normalize_api_endpoint(raw: str) -> str:
    """Allow public HTTPS APIs; replace any url= placeholder with a safe query param."""
    raw = raw.strip()
    if len(raw) > 500 or not public_https_url(raw):
        raise ValueError("API must be a public HTTPS URL (no localhost or private IP).")
    parts = urlsplit(raw)
    if parts.hostname.lower().endswith(("youtube.com", "youtu.be")):
        raise ValueError("Send an API endpoint, not a YouTube video link.")
    params = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True) if k.lower() != "url"]
    return urlunsplit((parts.scheme, parts.netloc, parts.path or "/", urlencode(params), ""))


async def fetch_json_api(session: aiohttp.ClientSession, api_endpoint: str, url: str,
                         max_bytes: int = 2_000_000):
    endpoint, params = api_endpoint, {"url": url}
    for _ in range(5):
        if not public_https_url(endpoint):
            raise ValueError("API redirected to an unsafe address.")
        async with session.get(endpoint, params=params, allow_redirects=False,
                               timeout=aiohttp.ClientTimeout(total=240, connect=20)) as response:
            if response.status in (301, 302, 303, 307, 308):
                location = response.headers.get("Location")
                if not location:
                    raise ValueError("API redirect has no destination.")
                endpoint = urljoin(str(response.url), location)
                params = None  # The Location already contains its own query.
                continue
            response.raise_for_status()
            if response.content_length and response.content_length > max_bytes:
                raise ValueError("The link provider returned too much data.")
            data = await response.content.read(max_bytes + 1)
            if len(data) > max_bytes:
                raise ValueError("The link provider returned too much data.")
            return json.loads(data)
    raise ValueError("Too many API redirects.")


async def fetch_video(session: aiohttp.ClientSession, url: str, api_endpoint: str = API_URL) -> Video:
    return normalize_video(await fetch_json_api(session, api_endpoint, url))


async def fetch_terabox(session: aiohttp.ClientSession, url: str) -> TeraboxListing:
    return normalize_terabox(await fetch_json_api(session, TERABOX_API_URL, url, 4_000_000))


async def get_youtube_title(session: aiohttp.ClientSession, url: str) -> str:
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
        return await asyncio.wait_for(asyncio.to_thread(extract_title, url), timeout=35) or ""
    except Exception as exc:
        LOG.warning("YouTube metadata lookup failed for %s: %s", video_id, exc)
    return ""


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
                        expected_size: int = 0):
    """Download from CDN while uploading MTProto parts. Never store a 2 GB file on Koyeb.

Pipelines bounded saveBigFilePart/saveFilePart requests from the HTTP stream,
    with MD5 for small files; no unbounded buffering.
    """
    for _ in range(5):
        if not public_https_url(url):
            raise ValueError("The provider supplied an unsafe download link.")
        async with session.get(url, allow_redirects=False, auto_decompress=False,
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
    def __init__(self, client: TelegramClient, session: aiohttp.ClientSession):
        self.client = client
        self.http = session
        self.selections: dict[int, Selection] = {}
        self.lookups: dict[int, asyncio.Task] = {}
        self.jobs: dict[str, Job] = {}
        self.api_overrides: dict[int, str] = {}
        self.awaiting_api: set[int] = set()
        self.modes: dict[int, str] = {}
        self.mode_tokens: dict[int, str] = {}
        self.pending_ranges: dict[int, RangeRequest] = {}
        self.dashboards: dict[int, Dashboard] = {}
        self.status_lock = asyncio.Lock()  # Serialize /status replacements.
        # Keep only a few completed status snapshots per user for one hour.
        self.completed: dict[int, list[tuple[float, str]]] = {}
        self.slot = asyncio.Semaphore(1)
        self.turn = asyncio.Condition()  # Preserve enqueue order even if status edits are slow.
        self.max_bytes = env_int("MAX_FILE_MB", MAX_MT_MB, 1, MAX_MT_MB) * 1024 * 1024
        self.playlist_limit = env_int("MAX_RANGE_VIDEOS", MAX_RANGE_VIDEOS, 1, MAX_RANGE_VIDEOS)
        self.in_flight = env_int("UPLOAD_IN_FLIGHT", 2, 1, 3)
        self.metrics = ContainerMetrics()

    def user_jobs(self, uid: int) -> list[Job]:
        return [job for job in self.jobs.values() if job.user_id == uid]

    def api_for(self, uid: int) -> str:
        return self.api_overrides.get(uid, API_URL)

    def set_api(self, uid: int, raw: str) -> str:
        endpoint = normalize_api_endpoint(raw)
        self.api_overrides[uid] = endpoint
        self.awaiting_api.discard(uid)
        return endpoint

    def register(self) -> None:
        self.client.add_event_handler(self.on_message, events.NewMessage(incoming=True))
        self.client.add_event_handler(self.callback, events.CallbackQuery(pattern=rb"^(s|j|m|d):"))

    def job_buttons(self, job: Job):
        if job.state == "finished":
            return None
        return [[Button.inline("🔄 Refresh", data=f"j:{job.token}:refresh".encode()),
                 Button.inline("✖ Cancel", data=f"j:{job.token}:cancel".encode())]]

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
        title = job.title[:95] or ("Playlist" if job.source.kind == "playlist" else
                                  "Terabox files" if job.source.kind.startswith("terabox") else "YouTube video")
        quality = f" [{job.quality}]" if job.quality else ""
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
                f"🆔 {job.token} · /cancel {job.token}"
                + (f"\n📊 {self.metrics.describe()}\nTasks in bot: {len(self.jobs)}" if include_metrics else ""))

    def dashboard_buttons(self, uid: int, dashboard: Dashboard):
        rows = [[Button.inline("🔄 Refresh all", data=f"d:{dashboard.token}:refresh".encode())]]
        rows.extend([Button.inline(f"✖ Cancel {job.token}", data=f"j:{job.token}:cancel".encode())]
                    for job in self.user_jobs(uid))
        return rows

    def format_dashboard(self, uid: int) -> str:
        active = self.user_jobs(uid)
        now = time.monotonic()
        history = [(when, text) for when, text in self.completed.get(uid, []) if now - when < 3600]
        if history:
            self.completed[uid] = history
        else:
            self.completed.pop(uid, None)
        panels = [self.format_job(job, include_metrics=False) for job in active]
        panels += [text for _, text in reversed(history[-2:])]
        if not panels:
            panels = ["No active or recent transfers. Send a link to begin."]
        body = "📋 Your downloads\n\n" + "\n\n──────────\n\n".join(panels)
        if uid in self.lookups:
            body += "\n\n🔎 Looking up a link…"
        body += (f"\n\n📊 {self.metrics.describe()}\n"
                 f"Active/queued: {len(active)} · /cancel lists job IDs")
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
                await self.client.edit_message(dashboard.peer, dashboard.message_id,
                                               self.format_dashboard(uid), parse_mode=None,
                                               buttons=self.dashboard_buttons(uid, dashboard))
            except MessageNotModifiedError:
                pass
            except FloodWaitError as exc:
                dashboard.edit_flood_until = time.monotonic() + exc.seconds + 3
                dashboard.next_edit = dashboard.edit_flood_until
            except RPCError:
                LOG.warning("Could not update dashboard for %s; will retry", uid, exc_info=True)

    async def show_dashboard(self, event) -> None:
        async with self.status_lock:
            await self._show_dashboard_locked(event)

    async def _show_dashboard_locked(self, event) -> None:
        uid = event.sender_id
        if uid not in self.dashboards and len(self.dashboards) >= MAX_DASHBOARDS:
            idle = [(owner, panel) for owner, panel in self.dashboards.items()
                    if not self.user_jobs(owner)]
            if not idle:
                await event.respond("Too many live status dashboards. Please try again later.")
                return
            oldest = min(idle, key=lambda item: item[1].created)[0]
            self.dashboards.pop(oldest, None)
        old = self.dashboards.get(uid)
        dashboard = Dashboard(await event.get_input_chat(), 0)
        message = await event.respond(self.format_dashboard(uid), parse_mode=None,
                                      buttons=self.dashboard_buttons(uid, dashboard))
        dashboard.message_id = message.id
        self.dashboards[uid] = dashboard
        ids = [job.message_id for job in self.user_jobs(uid) if job.message_id != message.id]
        if old and old.message_id != message.id:
            ids.append(old.message_id)
        if ids:
            try:
                await self.client.delete_messages(dashboard.peer, list(set(ids)))
            except RPCError:
                LOG.warning("Could not remove old status panels", exc_info=True)

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
                await self.client.edit_message(job.peer, job.message_id, self.format_job(job),
                                               buttons=self.job_buttons(job), parse_mode=None)
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


    async def on_message(self, event) -> None:
        if not event.is_private:
            await event.respond("Please use this bot in a private chat.")
            return
        text = (event.raw_text or "").strip()
        command = text.split(maxsplit=1)[0].split("@", 1)[0].lower() if text.startswith("/") else ""
        if command in ("/start", "/help"):
            await event.respond(
                "Choose YouTube or Terabox with /change, then send a link for that service. "
                "Pick one or several available qualities and tap Done. Playlists/folders first show "
                "their file count; send a range such as 1-29 or 200-300, then select qualities. "
                "Send another link while a job runs to queue it. Transfers stay sequential.\n\n"
                "A watch link containing list= is treated as a playlist; remove it for just one video.\n\n"
                "/change — YouTube / Terabox buttons\n"
                "/status or /queue — one live status for running, queued and recent jobs\n"
                "/cancel — list job IDs; /cancel <job-id> stops one\n"
                "/apis — numbered API list; /apis remove 3 resets a custom API\n"
                "/changeapi — set your YouTube API; /changeapi reset restores default\n"
                "/limits — size and queue limits\n/help — these instructions\n\n"
                "Only download media you have permission to use.")
        elif command == "/limits":
            await event.respond(
                f"MTProto limit: {self.max_bytes // 1024 // 1024} MiB per file (up to 4,000 parts). "
                f"YouTube playlist: up to {MAX_DISCOVER} entries, {self.playlist_limit} per selected range. "
                f"Terabox folders: up to 500 listed files. One job at a time, "
                f"{self.in_flight} concurrent upload parts; up to {MAX_WAITING} waiting jobs. "
                "Files with neither Content-Length nor API size are limited to 64 MiB. "
                "Jobs/selections are not restored after a restart.")
        elif command == "/change":
            uid = event.sender_id
            token = secrets.token_hex(4)
            self.mode_tokens[uid] = token
            active = self.modes.get(uid, "youtube")
            await event.respond(f"Current downloader: {active.title()}. Choose a service for future links:",
                                buttons=[[Button.inline("▶️ YouTube", data=f"m:{token}:youtube".encode()),
                                          Button.inline("📁 Terabox", data=f"m:{token}:terabox".encode())]])
        elif command == "/changeapi":
            uid = event.sender_id
            arg = text.partition(" ")[2].strip()
            if arg.lower() in ("reset", "remove", "default"):
                self.api_overrides.pop(uid, None)
                self.awaiting_api.discard(uid)
                await event.respond("API reset to the built-in default. Existing queued jobs keep their chosen API.")
            elif arg.lower() == "show":
                await event.respond("Custom API active." if uid in self.api_overrides else "Using the built-in API.")
            elif not arg:
                self.awaiting_api.add(uid)
                await event.respond("Send your public HTTPS API URL now (for example https://example.com/?url=). "
                                    "Use /changeapi reset to restore the default, or /cancel to stop.")
            else:
                try:
                    self.set_api(uid, arg)
                except ValueError as exc:
                    await event.respond(str(exc))
                else:
                    await event.respond("Custom API saved for your future links. /changeapi reset restores the default.")
        elif command in ("/status", "/queue"):
            await self.show_dashboard(event)
        elif command == "/apis":
            uid = event.sender_id
            arg = text.split(maxsplit=2)
            if len(arg) == 3 and arg[1].lower() == "remove" and arg[2] == "3":
                if self.api_overrides.pop(uid, None):
                    self.awaiting_api.discard(uid)
                    await event.respond("Custom YouTube API #3 removed. Future links use the default; queued jobs are unchanged.")
                else:
                    await event.respond("No custom API #3 to remove.")
            elif len(arg) == 3 and arg[1].lower() == "remove" and arg[2] in ("1", "2"):
                await event.respond("Built-in APIs #1 and #2 cannot be removed. Use /apis remove 3 for a custom API.")
            elif len(arg) > 1:
                await event.respond("Use /apis to list providers or /apis remove 3 to reset your custom YouTube API.")
            else:
                custom = self.api_overrides.get(uid)
                lines = ["Configured APIs (for your account):",
                         f"1. YouTube default: {API_URL}" + (" (inactive)" if custom else " (active)"),
                         f"2. Terabox: {TERABOX_API_URL} (built-in)"]
                if custom:
                    lines.append(f"3. YouTube custom: {custom} (active; /apis remove 3)")
                lines.append("Built-in APIs cannot be removed. /changeapi sets a custom YouTube API.")
                await event.respond("\n".join(lines), parse_mode=None)
        elif command == "/cancel":
            uid = event.sender_id
            target = text.partition(" ")[2].strip()
            owned = self.user_jobs(uid)
            if target == "lookup" and uid in self.lookups:
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
                                 f"[{', '.join(job.selected)}] — /cancel {job.token}")
                if uid in self.lookups:
                    lines.append("/cancel lookup — stop the current link lookup")
                buttons = [[Button.inline(f"✖ Cancel {job.token}",
                                          data=f"j:{job.token}:cancel".encode())] for job in owned]
                await event.respond("\n".join(lines), buttons=buttons, parse_mode=None)
            else:
                task = self.lookups.get(uid)
                if task:
                    task.cancel()
                selection = self.selections.pop(uid, None)
                range_request = self.pending_ranges.pop(uid, None)
                pending_api = uid in self.awaiting_api
                self.awaiting_api.discard(uid)
                await event.respond("Lookup cancellation requested." if task else
                                    "Selection cleared." if selection or range_request or pending_api else
                                    "Nothing to cancel.")
        elif command:
            await event.respond("Unknown command. Send /help for instructions.")
        else:
            if event.sender_id in self.awaiting_api:
                try:
                    self.set_api(event.sender_id, text)
                except ValueError as exc:
                    await event.respond(f"{exc} Send an API URL, or /changeapi reset.")
                else:
                    await event.respond("Custom API saved for your future links. /changeapi reset restores the default.")
            elif event.sender_id in self.pending_ranges and not text.startswith("https://"):
                await self.on_range(event)
            else:
                self.pending_ranges.pop(event.sender_id, None)
                await self.on_link(event)

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
                      else parse_source(event.raw_text))
        except ValueError as exc:
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
                    asyncio.to_thread(playlist_count, source.url, MAX_DISCOVER), timeout=180)
                if self.modes.get(uid, "youtube") != mode:
                    await notice.edit("Mode changed. Send your link again.")
                    return
                self.pending_ranges[uid] = RangeRequest(source, peer, count, self.api_for(uid))
                await notice.edit(f"Playlist reports {count} videos. Send an inclusive range like 1-29 or "
                                  f"200-300 (within 1-{count}, up to {self.playlist_limit} per job).")
                return
            if source.kind == "terabox":
                notice = await event.respond("Checking Terabox files and available qualities…")
                listing = await fetch_terabox(self.http, source.url)
                if self.modes.get(uid, "youtube") != mode:
                    await notice.edit("Mode changed. Send your link again.")
                    return
                count = len(listing.files)
                if count > 1:
                    folder = Source("terabox-folder", source.url)
                    self.pending_ranges[uid] = RangeRequest(folder, peer, count, listing=listing)
                    extra = (f" API reports {listing.reported_total} files but provided only {count} links; "
                             "the bot can process only provided files." if listing.reported_total > count else "")
                    await notice.edit(f"Terabox folder: {count} available files.{extra} Send an inclusive range "
                                      f"like 1-{min(count, 29)} (up to {self.playlist_limit} per job).")
                else:
                    item = listing.files[0]
                    selection = Selection(secrets.token_hex(4), source,
                                          self.listing_choices(listing.options_for(0)), peer,
                                          listing=listing)
                    self.selections[uid] = selection
                    partial = (f"API reports {listing.reported_total} files but provided only one usable link. "
                               if listing.reported_total > 1 else "")
                    await notice.edit(f"{partial}{item.filename} · "
                                      f"{human_size(item.size) if item.size else 'size unknown'}\n"
                                      "Choose a format, then Done:", buttons=self.keyboard(selection))
                return
            notice = await event.respond("Checking available qualities… This API may take 1–2 minutes. /cancel to stop.")
            title_task = asyncio.create_task(get_youtube_title(self.http, source.url))
            try:
                endpoint = self.api_for(uid)
                video = await fetch_video(self.http, source.url, endpoint)
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
                await self.safe_edit(notice, "Could not load this link. The API or source may be unavailable; try later.")
        finally:
            self.lookups.pop(uid, None)

    async def safe_edit(self, message, text: str) -> None:
        try:
            await message.edit(text)
        except RPCError:
            LOG.warning("Could not update a Telegram message", exc_info=True)

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
        if parts[0] == "m":
            if (not event.is_private or len(parts) != 3 or self.mode_tokens.get(uid) != parts[1]
                    or parts[2] not in ("youtube", "terabox")):
                await event.answer("Mode selector expired. Send /change again.", alert=True)
                return
            mode = parts[2]
            self.modes[uid] = mode
            self.mode_tokens.pop(uid, None)
            self.pending_ranges.pop(uid, None)
            self.selections.pop(uid, None)
            await event.answer(f"{mode.title()} selected")
            await event.edit(f"{mode.title()} mode is active. Send a {mode.title()} link.")
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
                        await event.edit(self.format_job(job), buttons=self.job_buttons(job), parse_mode=None)
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
            await event.edit(buttons=self.keyboard(selection))
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
        job = Job(uid, selection.peer, event.message_id, selection.source, selected, selection.video,
                  api_endpoint=selection.api_endpoint, start=selection.start, end=selection.end,
                  listing=selection.listing)
        self.selections.pop(uid, None)
        self.jobs[job.token] = job
        job.task = asyncio.create_task(self.run_job(job))
        await event.answer("Added to queue")
        if uid in self.dashboards:
            try:
                await self.client.delete_messages(job.peer, [job.message_id])
            except RPCError:
                LOG.warning("Could not remove the selection panel", exc_info=True)
            await self.render_dashboard(uid, force=True)

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
            listing = await fetch_terabox(self.http, job.source.url)
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
        job.reporter = asyncio.create_task(self.report_loop(job))
        try:
            await self.progress(job, f"Queued: {', '.join(job.selected)}. Use /status or /cancel.", force=True)
            await self.wait_turn(job)
            async with self.slot:
                await self.progress(job, "Starting…")
                if job.source.kind == "playlist":
                    await self.progress(job, f"Reading playlist entries {job.start}-{job.end}…")
                    entries = await asyncio.wait_for(
                        asyncio.to_thread(extract_playlist_range, job.source.url, job.start, job.end),
                        timeout=180)
                elif job.source.kind.startswith("terabox"):
                    initial_listing = job.listing
                    entries = list(initial_listing.files[job.start - 1:job.end]) if initial_listing else []
                else:
                    entries = [PlaylistEntry(job.source.url, job.video.title if job.video else "")]
                if not entries:
                    raise ValueError("No available entries in this range.")
                job.item_total = job.end if job.source.kind in ("playlist", "terabox-folder") else len(entries)
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
                    try:
                        video = job.video if job.video else await fetch_video(self.http, entry.url, job.api_endpoint)
                        youtube_name = entry.title or await get_youtube_title(self.http, entry.url)
                        video = replace(video, title=choose_title(youtube_name, video.title, entry.url))
                        job.title = video.title
                    except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, TypeError) as exc:
                        LOG.warning("Lookup failed for playlist item %s: %s", index, type(exc).__name__)
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
                        try:
                            await self.send_stream(job, video, quality, stream, index, job.item_total)
                            sent += 1
                        except FileTooLarge:
                            too_large += 1
                        except UnknownLength:
                            unknown_size += 1
                        except (aiohttp.ClientError, RPCError, asyncio.TimeoutError, ValueError, OSError) as exc:
                            LOG.warning("Transfer failed at video %s, quality %s: %s", index, quality, exc)
                            failed += 1
                job.state = "finished"
                await self.progress(job, f"Finished. Sent {sent}; unavailable {missing}; over size limit {too_large}; "
                                         f"missing size header (>64 MiB) {unknown_size}; failed {failed}.", force=True)
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
            async with self.turn:
                if self.jobs.get(job.token) is job:
                    if job.state == "finished":
                        if job.user_id not in self.completed and len(self.completed) >= MAX_COMPLETED_USERS:
                            oldest = min(self.completed, key=lambda uid: self.completed[uid][-1][0])
                            self.completed.pop(oldest, None)
                        history = self.completed.setdefault(job.user_id, [])
                        history.append((time.monotonic(), self.format_job(job, include_metrics=False)))
                        self.completed[job.user_id] = history[-2:]
                    self.jobs.pop(job.token, None)
                self.turn.notify_all()
            await self.render_dashboard(job.user_id, force=True)

    async def video_thumbnail(self, job: Job, video: Video) -> tuple[bytes | None, int, int]:
        source = job.current_url or job.source.url
        if job.thumbnail_id == source:
            return job.thumbnail, *job.thumb_size
        if job.source.kind.startswith("terabox"):
            urls = [video.thumbnail] if video.thumbnail else []
        else:
            video_id = source.rsplit("=", 1)[-1]
            # Prefer the API's supplied thumbnail; YouTube sizes are fallbacks.
            urls = [video.thumbnail] if video.thumbnail else []
            for candidate in (f"https://i.ytimg.com/vi/{video_id}/maxresdefault.jpg",
                              f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg"):
                if candidate not in urls:
                    urls.append(candidate)
        thumb = None
        for image_url in urls:
            thumb = await thumbnail_jpeg(self.http, image_url)
            if thumb:
                break
        job.thumbnail_id = source
        job.thumbnail = thumb[0] if thumb else None
        job.thumb_size = thumb[1:] if thumb else (0, 0)
        return job.thumbnail, *job.thumb_size

    async def send_stream(self, job: Job, video: Video, quality: str, url: str, index: int, total: int,
                          source_file: TeraboxFile | None = None) -> None:
        suffix = (Path(source_file.filename).suffix.lower() if source_file else
                  ".mp3" if quality == AUDIO else ".mp4")
        suffix = suffix if re.fullmatch(r"\.[a-z0-9]{1,8}", suffix) else ".bin"
        audio = quality.lower() == "mp3" or bool(source_file and source_file.mime.startswith("audio/"))
        video_file = source_file.video if source_file else not audio
        streamable = video_file and suffix == ".mp4"
        safe_title = re.sub(r"[^\w .-]", "_", video.title, flags=re.UNICODE).strip(" .") or "video"
        filename = media_name(safe_title, quality, 100) + suffix
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
            uploaded = await upload_stream(self.client, self.http, url, filename, self.max_bytes,
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
        if audio:
            attributes = [types.DocumentAttributeAudio(duration=video.duration,
                                                       title=media_name(video.title, quality, 64))]
        elif video_file:
            # ffprobe inspects the actual MP4 head/tail. API duration and image
            # dimensions are fallbacks only if its metadata cannot be parsed.
            attributes = [types.DocumentAttributeVideo(duration=info.duration or video.duration,
                                                       w=info.width or thumb_w or 16,
                                                       h=info.height or thumb_h or 9,
                                                       supports_streaming=streamable)]
        else:
            attributes = None

        async def send_with_preview(*, document: bool = False, with_thumb: bool = True) -> None:
            thumb_file = None
            if thumb_data and with_thumb:
                thumb_file = io.BytesIO(thumb_data)
                thumb_file.name = "preview.jpg"
            await self.client.send_file(job.peer, uploaded, caption=caption, parse_mode=None,
                                        mime_type=(source_file.mime if source_file else
                                                   "audio/mpeg" if audio else "video/mp4"),
                                        attributes=attributes, supports_streaming=streamable and not document,
                                        force_document=document or (source_file is not None and not streamable and not audio),
                                        thumb=thumb_file)

        async def deliver() -> None:
            try:
                await send_with_preview()
            except (PhotoInvalidDimensionsError, PhotoInvalidError):
                await send_with_preview(with_thumb=False)
            except VideoContentTypeInvalidError:
                if audio:
                    raise
                # Reuse the handle if Telegram rejects the MP4 as streamable.
                try:
                    await send_with_preview(document=True)
                except (PhotoInvalidDimensionsError, PhotoInvalidError):
                    await send_with_preview(document=True, with_thumb=False)

        for attempt in range(3):
            try:
                await deliver()
                job.rate_limit_until = 0
                return
            except FloodWaitError as exc:
                if attempt == 2 or exc.seconds > 300:
                    raise
                job.rate_limit_until = time.monotonic() + exc.seconds + 1
                await asyncio.sleep(exc.seconds + 1)
        # All retries either returned or raised above.


async def serve(token: str, api_id: int, api_hash: str) -> None:
    # MemorySession is not a StringSession and writes no authentication file to disk.
    client = TelegramClient(MemorySession(), api_id, api_hash, receive_updates=True)
    timeout = aiohttp.ClientTimeout(total=240, connect=20)
    connector = aiohttp.TCPConnector(resolver=PublicResolver())
    async with aiohttp.ClientSession(timeout=timeout, connector=connector,
                                     headers={"User-Agent": "ytdown-telegram-bot/1.0"}) as http:
        service = DownloaderBot(client, http)
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
            site = web.TCPSite(runner, "0.0.0.0", env_int("PORT", 8000, 1, 65535))
            await site.start()
            LOG.info("MTProto bot connected; health check ready")
            await stop.wait()
        finally:
            for task in list(service.lookups.values()) + [j.task for j in service.jobs.values() if j.task]:
                task.cancel()
            await asyncio.gather(*list(service.lookups.values()),
                                 *(j.task for j in service.jobs.values() if j.task), return_exceptions=True)
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
    asyncio.run(serve(token, api_id, api_hash))
