"""MTProto Telegram bot with sequential, bounded-memory media transfers."""
from __future__ import annotations

import asyncio
import hashlib
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
from urllib.parse import urljoin

import aiohttp
from aiohttp import web
from telethon import Button, TelegramClient, events, helpers
from telethon.errors import FloodWaitError, MessageNotModifiedError, RPCError, VideoContentTypeInvalidError
from telethon.sessions import MemorySession
from telethon.tl import types
from telethon.tl.functions.upload import SaveBigFilePartRequest, SaveFilePartRequest

from .core import (AUDIO, VIDEO_QUALITIES, PlaylistEntry, Source, Video, extract_playlist,
                   extract_title, normalize_video, parse_source, public_https_url, youtube_title)

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
MIN_EDIT_SECONDS = 8


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


def human_size(count: float) -> str:
    for unit in ("B", "KiB", "MiB", "GiB"):
        if count < 1024 or unit == "GiB":
            return f"{count:.1f} {unit}" if unit != "B" else f"{count:.0f} B"
        count /= 1024
    return "0 B"


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


async def fetch_video(session: aiohttp.ClientSession, url: str) -> Video:
    async with session.get(API_URL, params={"url": url}, timeout=aiohttp.ClientTimeout(total=240, connect=20)) as response:
        response.raise_for_status()
        if response.content_length and response.content_length > 2_000_000:
            raise ValueError("The link provider returned too much data.")
        data = await response.content.read(2_000_001)
        if len(data) > 2_000_000:
            raise ValueError("The link provider returned too much data.")
        return normalize_video(json.loads(data))


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

    def __init__(self, response: aiohttp.ClientResponse, size: int, on_download=None):
        self.content = response.content
        self.remaining = size
        self.on_download = on_download

    async def read(self, count: int) -> bytes:
        size = min(count, self.remaining)
        if not size:
            return b""
        data = await self.content.readexactly(size)
        self.remaining -= len(data)
        if self.on_download:
            self.on_download(len(data))
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
                        on_upload=None, on_flood=None, in_flight: int = 2):
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
            size = response.content_length
            if size is not None:
                if size > min(max_bytes, MAX_PARTS * PART_BYTES):
                    raise FileTooLarge()
                if size == 0:
                    raise ValueError("The download link returned an empty file.")
                if on_total:
                    on_total(size)
                reader = HTTPReader(response, size, on_download)
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
        self.jobs: dict[int, Job] = {}
        self.slot = asyncio.Semaphore(1)
        self.max_bytes = env_int("MAX_FILE_MB", MAX_MT_MB, 1, MAX_MT_MB) * 1024 * 1024
        self.playlist_limit = env_int("MAX_PLAYLIST_VIDEOS", 25, 1, 50)
        self.in_flight = env_int("UPLOAD_IN_FLIGHT", 2, 1, 3)
        self._cpu_wall = time.monotonic()
        self._cpu_process = time.process_time()
        self._cpu_percent = 0.0

    def resource_line(self) -> str:
        # Sample no more than every 5 seconds; CPU is per-process, not Koyeb's host metric.
        now = time.monotonic()
        if now - self._cpu_wall >= 5:
            current = time.process_time()
            self._cpu_percent = max(0, (current - self._cpu_process) / (now - self._cpu_wall) * 100)
            self._cpu_wall, self._cpu_process = now, current
        try:
            rss_pages = int(Path("/proc/self/statm").read_text().split()[1])
            rss = human_size(rss_pages * os.sysconf("SC_PAGE_SIZE"))
            return f"Process CPU: {self._cpu_percent:.0f}% · RAM: {rss}"
        except (OSError, ValueError, IndexError):
            return f"Process CPU: {self._cpu_percent:.0f}%"

    def register(self) -> None:
        self.client.add_event_handler(self.on_message, events.NewMessage(incoming=True))
        self.client.add_event_handler(self.callback, events.CallbackQuery(pattern=rb"^(s|j):"))

    def job_buttons(self, job: Job):
        if job.state == "finished":
            return None
        return [[Button.inline("🔄 Refresh", data=f"j:{job.token}:refresh".encode()),
                 Button.inline("✖ Cancel", data=f"j:{job.token}:cancel".encode())]]

    def format_job(self, job: Job) -> str:
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
        queue = f" (position {waiting.index(job) + 1})" if job in waiting else ""
        item = f"{job.item_index}/{job.item_total}" if job.item_total else "—"
        title = job.title[:95] or ("Playlist" if job.source.kind == "playlist" else "YouTube video")
        quality = f" [{job.quality}]" if job.quality else ""
        rate = (f"Rate limited until {human_eta(job.rate_limit_until - now)}"
                if job.rate_limit_until > now else job.detail)
        return (f"🎬 {title}{quality}\n"
                f"👤 {job.user_id} · Video {item}\n"
                f"[{('●' * filled) + ('○' * (12 - filled))}] {ratio * 100:.1f}%\n"
                f"↑ Uploaded: {human_size(job.uploaded)} / {human_size(total) if total else '—'}\n"
                f"↓ Downloaded: {human_size(job.downloaded)}\n"
                f"Speed: {human_size(job.speed)}/s · ETA: {eta}\n"
                f"Status: {job.state}{queue} · {rate}\n"
                f"Tasks: {len(self.jobs)} · {self.resource_line()}\n"
                f"/cancel or buttons below")

    async def render(self, job: Job, *, force: bool = False) -> None:
        async with job.edit_lock:
            now = time.monotonic()
            if not force and now < job.next_edit:
                return
            job.next_edit = now + REPORT_SECONDS
            try:
                await self.client.edit_message(job.peer, job.message_id, self.format_job(job),
                                               buttons=self.job_buttons(job), parse_mode=None)
            except MessageNotModifiedError:
                pass
            except FloodWaitError as exc:
                job.next_edit = time.monotonic() + exc.seconds + 3
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
                "Send a YouTube video, Shorts, or public playlist link. Choose one or several qualities, then tap Done. "
                "Files are transferred sequentially. Watch the live progress message or tap Refresh; "
                "playlist qualities unavailable for a video are skipped.\n\n"
                "A watch link containing list= is treated as a playlist; remove it for just one video.\n\n"
                "/status — current job\n/cancel — stop a lookup or job\n/limits — size and queue limits\n/help — these instructions\n\n"
                "Only download media you have permission to use.")
        elif command == "/limits":
            await event.respond(
                f"MTProto limit: {self.max_bytes // 1024 // 1024} MiB per file (up to 4,000 parts). "
                f"Playlist: first {self.playlist_limit} entries. One job at a time, "
                f"{self.in_flight} concurrent upload parts; up to {MAX_WAITING} waiting jobs. "
                "Files without Content-Length are limited to 64 MiB. Jobs/selections are not restored after a restart.")
        elif command == "/status":
            uid = event.sender_id
            job = self.jobs.get(uid)
            text = (self.format_job(job) if job else
                    "Looking up video qualities; the provider may take a few minutes." if uid in self.lookups else
                    "Choose qualities and tap Done, or send a new link." if uid in self.selections else
                    "No active job. Send a YouTube link to begin.")
            await event.respond(text, buttons=self.job_buttons(job) if job else None, parse_mode=None)
        elif command == "/cancel":
            uid = event.sender_id
            task = self.lookups.get(uid)
            job = self.jobs.get(uid)
            if task:
                task.cancel()
            if job and job.task:
                job.task.cancel()
            selection = self.selections.pop(uid, None)
            await event.respond("Cancellation requested." if task or job else
                                "Selection cleared." if selection else "Nothing to cancel.")
        elif command:
            await event.respond("Unknown command. Send /help for instructions.")
        else:
            await self.on_link(event)

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
        if uid in self.jobs or uid in self.lookups:
            await event.respond("Finish or /cancel your current request first.")
            return
        if len(self.lookups) >= 2:
            await event.respond("The provider lookup slots are busy. Try again shortly.")
            return
        try:
            source = parse_source(event.raw_text)
        except ValueError as exc:
            await event.respond(str(exc))
            return
        self.selections.pop(uid, None)
        if len(self.selections) >= MAX_SELECTIONS:
            await event.respond("Too many open selections right now. Try again later.")
            return
        peer = await event.get_input_chat()
        if source.kind == "playlist":
            selection = Selection(secrets.token_hex(4), source, tuple(f"{q}p" for q in VIDEO_QUALITIES), peer)
            self.selections[uid] = selection
            await event.respond(
                f"Playlist: choose qualities from 360p to 4K, then tap Done. "
                f"The first {self.playlist_limit} entries will be checked individually against the API; "
                "only available qualities will be sent.", buttons=self.keyboard(selection))
            return
        self.lookups[uid] = asyncio.current_task()
        notice = None
        try:
            notice = await event.respond("Checking available qualities… This API may take 1–2 minutes. /cancel to stop.")
            title_task = asyncio.create_task(get_youtube_title(self.http, source.url))
            try:
                video = await fetch_video(self.http, source.url)
                video = replace(video, title=choose_title(await title_task, video.title, source.url))
            finally:
                if not title_task.done():
                    title_task.cancel()
                await asyncio.gather(title_task, return_exceptions=True)
            choices = tuple(sorted((q for q in video.streams if q != AUDIO), key=lambda q: int(q[:-1])))
            if AUDIO in video.streams:
                choices += (AUDIO,)
            if len(self.selections) >= MAX_SELECTIONS:
                await notice.edit("Too many open selections right now. Try again later.")
                return
            selection = Selection(secrets.token_hex(4), source, choices, peer, video)
            self.selections[uid] = selection
            await notice.edit(f"{video.title}\nChoose one or more available formats, then tap Done:",
                              buttons=self.keyboard(selection))
        except asyncio.CancelledError:
            if notice:
                await self.safe_edit(notice, "Lookup cancelled.")
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, TypeError) as exc:
            LOG.warning("Video lookup failed: %s", exc)
            if notice:
                await self.safe_edit(notice, "Could not load this video. The API may be down or slow; try again later.")
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
        if parts[0] == "j":
            job = self.jobs.get(uid)
            if (not event.is_private or len(parts) != 3 or not job or parts[1] != job.token):
                await event.answer("This job has finished or expired.", alert=True)
                return
            if parts[2] == "cancel":
                if job.task:
                    job.task.cancel()
                await event.answer("Cancellation requested")
                return
            if parts[2] == "refresh":
                now = time.monotonic()
                if now < job.next_edit:
                    await event.answer(f"Next update in {int(job.next_edit - now) + 1}s")
                    return
                await event.answer("Updating status")
                if event.message_id == job.message_id:
                    await self.render(job)
                else:
                    # A /status reply has its own Refresh button.
                    job.next_edit = now + REPORT_SECONDS
                    try:
                        await event.edit(self.format_job(job), buttons=self.job_buttons(job), parse_mode=None)
                    except FloodWaitError as exc:
                        job.next_edit = time.monotonic() + exc.seconds + 3
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
        if uid in self.jobs or len(self.jobs) >= MAX_WAITING + 1:
            await event.answer("The queue is full or you already have a job. Try later.", alert=True)
            return
        selected = tuple(key for key in selection.choices if key in selection.selected)
        job = Job(uid, selection.peer, event.message_id, selection.source, selected, selection.video)
        self.selections.pop(uid, None)
        self.jobs[uid] = job
        job.task = asyncio.create_task(self.run_job(job))
        await event.answer("Added to queue")

    async def progress(self, job: Job, text: str, *, force: bool = False) -> None:
        job.detail = text
        await self.render(job, force=force)

    async def run_job(self, job: Job) -> None:
        sent = missing = too_large = unknown_size = failed = 0
        job.reporter = asyncio.create_task(self.report_loop(job))
        try:
            await self.progress(job, f"Queued: {', '.join(job.selected)}. Use /status or /cancel.", force=True)
            async with self.slot:
                job.state = "running"
                await self.progress(job, "Starting…")
                if job.source.kind == "playlist":
                    await self.progress(job, "Reading playlist entries…")
                    entries = await asyncio.wait_for(
                        asyncio.to_thread(extract_playlist, job.source.url, self.playlist_limit), timeout=180)
                else:
                    entries = [PlaylistEntry(job.source.url, job.video.title if job.video else "")]
                job.item_total = len(entries)
                for index, entry in enumerate(entries, 1):
                    job.item_index = index
                    job.quality = ""
                    job.title = entry.title
                    job.total_bytes = job.downloaded = job.uploaded = 0
                    await self.progress(job, f"Checking formats (API may take 1–2 minutes). Sent {sent} so far.")
                    try:
                        video = job.video if job.video else await fetch_video(self.http, entry.url)
                        youtube_name = entry.title or await get_youtube_title(self.http, entry.url)
                        video = replace(video, title=choose_title(youtube_name, video.title, entry.url))
                        job.title = video.title
                    except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, TypeError) as exc:
                        LOG.warning("Lookup failed for playlist item %s: %s", index, exc)
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
                            await self.send_stream(job, video, quality, stream, index, len(entries))
                            sent += 1
                        except FileTooLarge:
                            too_large += 1
                        except UnknownLength:
                            LOG.warning("Unknown-size download exceeds spool limit for video %s", index)
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
            await self.progress(job, f"Could not read the playlist: {exc}", force=True)
        except Exception:
            job.state = "finished"
            LOG.exception("Unexpected job failure")
            await self.progress(job, "Job failed unexpectedly. Please try again later.", force=True)
        finally:
            if job.reporter:
                job.reporter.cancel()
                await asyncio.gather(job.reporter, return_exceptions=True)
            if self.jobs.get(job.user_id) is job:
                self.jobs.pop(job.user_id, None)

    async def send_stream(self, job: Job, video: Video, quality: str, url: str, index: int, total: int) -> None:
        suffix = ".mp3" if quality == AUDIO else ".mp4"
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

        uploaded = await upload_stream(self.client, self.http, url, filename, self.max_bytes,
                                       on_total=on_total, on_download=on_download,
                                       on_upload=on_upload, on_flood=on_flood,
                                       in_flight=self.in_flight)
        await self.progress(job, "Finalizing: sending uploaded file to chat.")
        attributes = ([types.DocumentAttributeAudio(duration=0, title=media_name(video.title, quality, 64))]
                      if quality == AUDIO else None)
        async def deliver() -> None:
            try:
                await self.client.send_file(job.peer, uploaded, caption=caption, parse_mode=None,
                                            mime_type="audio/mpeg" if quality == AUDIO else "video/mp4",
                                            attributes=attributes, supports_streaming=quality != AUDIO)
            except VideoContentTypeInvalidError:
                if quality == AUDIO:
                    raise
                # Reuse the same MTProto handle if Telegram rejects streaming.
                await self.client.send_file(job.peer, uploaded, caption=caption, parse_mode=None,
                                            mime_type="video/mp4", force_document=True)

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
