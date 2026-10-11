"""Polling bot. One download/upload at a time, with a bounded waiting queue."""
from __future__ import annotations

import asyncio
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
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, InputFile, Update
from telegram.error import TelegramError
from telegram.ext import Application, ApplicationBuilder, CallbackQueryHandler, CommandHandler, ContextTypes, MessageHandler, filters

from .core import (AUDIO, VIDEO_QUALITIES, PlaylistEntry, Source, Video, extract_playlist,
                   extract_title, normalize_video, parse_source, public_https_url, youtube_title)

LOG = logging.getLogger(__name__)
API_URL = "https://samra-youtube-api.antideploy.com/"
MAX_WAITING = 4
MAX_SELECTIONS = 100
SESSION_SECONDS = 1800


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


def media_name(title: str, quality: str, max_length: int) -> str:
    """Keep the [quality] suffix even when a long video title is shortened."""
    suffix = f" [{quality.upper() if quality == AUDIO else quality}]"
    return (title[:max_length - len(suffix)].rstrip() or "YouTube video") + suffix


@dataclass
class Selection:
    token: str
    source: Source
    choices: tuple[str, ...]
    video: Video | None = None
    selected: set[str] = field(default_factory=set)
    created: float = field(default_factory=time.monotonic)


@dataclass
class Job:
    user_id: int
    chat_id: int
    message_id: int
    source: Source
    selected: tuple[str, ...]
    video: Video | None
    state: str = "queued"
    detail: str = "Waiting for the download slot."
    task: asyncio.Task | None = None


async def fetch_video(session: aiohttp.ClientSession, url: str) -> Video:
    # The URL is normalized from a validated YouTube ID; aiohttp encodes its query parameter.
    async with session.get(API_URL, params={"url": url}, timeout=aiohttp.ClientTimeout(total=240, connect=20)) as response:
        response.raise_for_status()
        # A misbehaving provider must not consume unbounded memory.
        if response.content_length and response.content_length > 2_000_000:
            raise ValueError("The link provider returned too much data.")
        data = await response.content.read(2_000_001)
        if len(data) > 2_000_000:
            raise ValueError("The link provider returned too much data.")
        return normalize_video(json.loads(data))


async def get_youtube_title(session: aiohttp.ClientSession, url: str) -> str:
    """Resolve the real YouTube title independently of the link provider."""
    video_id = url.rsplit("=", 1)[-1]  # url is our normalized watch URL.
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
        LOG.info("YouTube oEmbed title lookup failed for %s: %s", video_id, exc)
    try:
        title = await asyncio.wait_for(asyncio.to_thread(extract_title, url), timeout=35)
        if title:
            return title
    except Exception as exc:  # yt-dlp metadata may be unavailable from some hosting regions.
        LOG.warning("YouTube metadata lookup failed for %s: %s", video_id, exc)
    # Let the caller use the API title if neither YouTube metadata source worked.
    return ""


def choose_title(youtube: str, provider: str, url: str) -> str:
    """YouTube title first, then API title, then the video ID."""
    return youtube or provider or f"YouTube video {url.rsplit('=', 1)[-1]}"


async def download(session: aiohttp.ClientSession, url: str, path: Path, max_bytes: int) -> None:
    """Stream to disk, enforce size during transfer, validate every redirect."""
    for _ in range(5):
        if not public_https_url(url):
            raise ValueError("The provider supplied an unsafe download link.")
        async with session.get(url, allow_redirects=False, timeout=aiohttp.ClientTimeout(total=600, connect=20, sock_read=60)) as response:
            if response.status in (301, 302, 303, 307, 308):
                location = response.headers.get("Location")
                if not location:
                    raise ValueError("Download redirect has no destination.")
                url = urljoin(url, location)
                continue
            response.raise_for_status()
            if response.content_type in ("text/html", "application/json", "text/plain"):
                raise ValueError("The download link returned a page instead of media.")
            if response.content_length and response.content_length > max_bytes:
                raise FileTooLarge()
            size = 0
            with path.open("wb") as output:
                async for chunk in response.content.iter_chunked(128 * 1024):
                    size += len(chunk)
                    if size > max_bytes:
                        raise FileTooLarge()
                    output.write(chunk)
            if not size:
                raise ValueError("The download link returned an empty file.")
            return
    raise ValueError("Too many download redirects.")


class DownloaderBot:
    def __init__(self, application: Application, session: aiohttp.ClientSession):
        self.app = application
        self.http = session
        self.selections: dict[int, Selection] = {}
        self.lookups: dict[int, asyncio.Task] = {}
        self.jobs: dict[int, Job] = {}
        self.slot = asyncio.Semaphore(1)
        self.max_bytes = env_int("MAX_FILE_MB", 47, 1, 47) * 1024 * 1024
        self.playlist_limit = env_int("MAX_PLAYLIST_VIDEOS", 25, 1, 50)

    def register(self) -> None:
        self.app.add_handler(CommandHandler(("start", "help"), self.help))
        self.app.add_handler(CommandHandler("limits", self.limits))
        self.app.add_handler(CommandHandler("status", self.status))
        self.app.add_handler(CommandHandler("cancel", self.cancel))
        self.app.add_handler(CallbackQueryHandler(self.callback, pattern=r"^s:"))
        self.app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.on_link))

    async def private(self, update: Update) -> bool:
        if update.effective_chat and update.effective_chat.type == "private":
            return True
        if update.effective_message:
            await update.effective_message.reply_text("Please use this bot in a private chat.")
        return False

    async def help(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self.private(update):
            return
        await update.effective_message.reply_text(
            "Send a YouTube video, Shorts, or public playlist link. Pick one or several qualities, then tap Done. "
            "The bot downloads and uploads each chosen quality in order.\n\n"
            "For a playlist, choose qualities first; unavailable qualities are skipped per video. "
            "A watch link containing list= is treated as a playlist. Remove list= for just one video.\n\n"
            "/status — current job\n/cancel — stop a lookup or job\n/limits — resource limits\n/help — these instructions\n\n"
            "Only download media you have permission to use."
        )

    async def limits(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self.private(update):
            return
        await update.effective_message.reply_text(
            f"Max file: {self.max_bytes // 1024 // 1024} MiB (Telegram bots cannot upload larger files here). "
            f"Playlist: first {self.playlist_limit} entries. One download/upload globally at a time; "
            f"up to {MAX_WAITING} waiting jobs. API lookups can take 1–2 minutes or longer. "
            "Jobs and selections are not restored after a restart."
        )

    async def status(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self.private(update):
            return
        uid = update.effective_user.id
        job = self.jobs.get(uid)
        if job:
            text = f"Job {job.state}: {job.detail}"
        elif uid in self.lookups:
            text = "Looking up video qualities; the provider may take a few minutes."
        elif uid in self.selections:
            text = "Choose qualities and tap Done, or send a new link."
        else:
            text = "No active job. Send a YouTube link to begin."
        await update.effective_message.reply_text(text)

    async def cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self.private(update):
            return
        uid = update.effective_user.id
        task = self.lookups.get(uid)
        job = self.jobs.get(uid)
        if task:
            task.cancel()
        if job and job.task:
            job.task.cancel()
        selection = self.selections.pop(uid, None)
        await update.effective_message.reply_text(
            "Cancellation requested." if task or job else "Selection cleared." if selection else "Nothing to cancel."
        )

    def keyboard(self, selection: Selection) -> InlineKeyboardMarkup:
        def label(key: str) -> str:
            if key == AUDIO:
                return "MP3 audio"
            if selection.source.kind == "playlist" and key in ("1440p", "2160p"):
                return key + (" (2K)" if key == "1440p" else " (4K)")
            return key

        buttons = [InlineKeyboardButton(
            ("✅ " if key in selection.selected else "▫️ ") + label(key),
            callback_data=f"s:{selection.token}:t:{key}",
        ) for key in selection.choices]
        rows = [buttons[i:i + 2] for i in range(0, len(buttons), 2)]
        rows.append([
            InlineKeyboardButton("Done ▶️", callback_data=f"s:{selection.token}:done"),
            InlineKeyboardButton("Cancel", callback_data=f"s:{selection.token}:cancel"),
        ])
        return InlineKeyboardMarkup(rows)

    async def on_link(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self.private(update):
            return
        uid = update.effective_user.id
        now = time.monotonic()
        for owner, selection in list(self.selections.items()):
            if now - selection.created > SESSION_SECONDS:
                self.selections.pop(owner, None)
        if uid in self.jobs or uid in self.lookups:
            await update.effective_message.reply_text("Finish or /cancel your current request first.")
            return
        if len(self.lookups) >= 2:
            await update.effective_message.reply_text("The provider lookup slots are busy. Try again shortly.")
            return
        try:
            source = parse_source(update.effective_message.text)
        except ValueError as exc:
            await update.effective_message.reply_text(str(exc))
            return
        self.selections.pop(uid, None)
        if len(self.selections) >= MAX_SELECTIONS:
            await update.effective_message.reply_text("Too many open selections right now. Try again later.")
            return
        if source.kind == "playlist":
            selection = Selection(secrets.token_hex(4), source, tuple(f"{q}p" for q in VIDEO_QUALITIES))
            self.selections[uid] = selection
            await update.effective_message.reply_text(
                f"Playlist: choose qualities from 360p to 4K, then tap Done. "
                f"The first {self.playlist_limit} entries will be checked individually against the API; "
                "only available qualities will be sent.", reply_markup=self.keyboard(selection)
            )
            return
        self.lookups[uid] = asyncio.current_task()
        notice = None
        try:
            notice = await update.effective_message.reply_text("Checking available qualities… This API may take 1–2 minutes. /cancel to stop.")
            title_task = asyncio.create_task(get_youtube_title(self.http, source.url))
            try:
                video = await fetch_video(self.http, source.url)
                video = replace(video, title=choose_title(await title_task, video.title, source.url))
            finally:
                if not title_task.done():
                    title_task.cancel()
                await asyncio.gather(title_task, return_exceptions=True)
            # The API's downloads list, not availableQuality, determines what we can send.
            choices = tuple(sorted((q for q in video.streams if q != AUDIO), key=lambda q: int(q[:-1])))
            if AUDIO in video.streams:
                choices += (AUDIO,)
            if len(self.selections) >= MAX_SELECTIONS:
                await notice.edit_text("Too many open selections right now. Try again later.")
                return
            selection = Selection(secrets.token_hex(4), source, choices, video)
            self.selections[uid] = selection
            await notice.edit_text(f"{video.title}\nChoose one or more available formats, then tap Done:", reply_markup=self.keyboard(selection))
        except asyncio.CancelledError:
            if notice:
                await self.safe_notice(notice, "Lookup cancelled.")
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, TypeError) as exc:
            LOG.warning("Video lookup failed: %s", exc)
            if notice:
                await self.safe_notice(notice, "Could not load this video. The API may be down or slow; try again later.")
        finally:
            self.lookups.pop(uid, None)

    async def safe_notice(self, message, text: str) -> None:
        try:
            await message.edit_text(text)
        except TelegramError:
            LOG.warning("Could not update a Telegram message", exc_info=True)

    async def callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        query = update.callback_query
        if not query or not query.message or query.message.chat.type != "private":
            if query:
                await query.answer("Use this bot in a private chat.", show_alert=True)
            return
        uid = query.from_user.id
        parts = (query.data or "").split(":")
        selection = self.selections.get(uid)
        if (len(parts) < 3 or not selection or parts[1] != selection.token
                or time.monotonic() - selection.created > SESSION_SECONDS):
            await query.answer("This selection expired. Send the link again.", show_alert=True)
            if selection and time.monotonic() - selection.created > SESSION_SECONDS:
                self.selections.pop(uid, None)
            return
        action = parts[2]
        if action == "cancel":
            self.selections.pop(uid, None)
            await query.answer()
            await query.edit_message_text("Selection cancelled.")
            return
        if action == "t" and len(parts) == 4 and parts[3] in selection.choices:
            key = parts[3]
            if key in selection.selected:
                selection.selected.remove(key)
            else:
                selection.selected.add(key)
            await query.answer()
            await query.edit_message_reply_markup(reply_markup=self.keyboard(selection))
            return
        if action != "done" or len(parts) != 3:
            await query.answer("Invalid button.", show_alert=True)
            return
        if not selection.selected:
            await query.answer("Select at least one quality first.", show_alert=True)
            return
        if uid in self.jobs or len(self.jobs) >= MAX_WAITING + 1:
            await query.answer("The queue is full or you already have a job. Try later.", show_alert=True)
            return
        selected = tuple(key for key in selection.choices if key in selection.selected)
        job = Job(uid, query.message.chat_id, query.message.message_id, selection.source, selected, selection.video)
        self.selections.pop(uid, None)
        self.jobs[uid] = job
        job.task = asyncio.create_task(self.run_job(job))
        await query.answer("Added to queue")

    async def progress(self, job: Job, text: str) -> None:
        job.detail = text
        try:
            await self.app.bot.edit_message_text(text, chat_id=job.chat_id, message_id=job.message_id)
        except TelegramError:
            LOG.warning("Could not update job progress", exc_info=True)

    async def run_job(self, job: Job) -> None:
        sent = missing = too_large = failed = 0
        try:
            await self.progress(job, f"Queued: {', '.join(job.selected)}. Use /status or /cancel.")
            async with self.slot:
                job.state = "running"
                await self.progress(job, "Starting…")
                if job.source.kind == "playlist":
                    await self.progress(job, "Reading playlist entries…")
                    entries = await asyncio.wait_for(
                        asyncio.to_thread(extract_playlist, job.source.url, self.playlist_limit), timeout=180
                    )
                else:
                    entries = [PlaylistEntry(job.source.url, job.video.title if job.video else "")]
                for index, entry in enumerate(entries, 1):
                    await self.progress(job, f"Video {index}/{len(entries)}: checking formats… Sent {sent} so far.")
                    try:
                        video = job.video if job.video else await fetch_video(self.http, entry.url)
                        # Playlist entries carry YouTube's own title. Only fetch metadata
                        # separately when YouTube omitted the entry title.
                        youtube_name = entry.title or await get_youtube_title(self.http, entry.url)
                        video = replace(video, title=choose_title(youtube_name, video.title, entry.url))
                    except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, TypeError) as exc:
                        LOG.warning("Lookup failed for playlist item %s: %s", index, exc)
                        failed += len(job.selected)
                        continue
                    for quality in job.selected:
                        stream = video.streams.get(quality)
                        if not stream:
                            missing += 1
                            continue
                        await self.progress(job, f"Video {index}/{len(entries)}: downloading/uploading {quality}… Sent {sent} so far.")
                        try:
                            await self.send_stream(job, video, quality, stream, index, len(entries))
                            sent += 1
                        except FileTooLarge:
                            too_large += 1
                        except (aiohttp.ClientError, TelegramError, asyncio.TimeoutError, ValueError, OSError) as exc:
                            LOG.warning("Transfer failed at video %s, quality %s: %s", index, quality, exc)
                            failed += 1
                await self.progress(job, f"Finished. Sent {sent}; unavailable {missing}; over size limit {too_large}; failed {failed}.")
        except asyncio.CancelledError:
            await self.progress(job, "Cancelled. Already uploaded files remain in the chat.")
        except (ValueError, asyncio.TimeoutError) as exc:
            await self.progress(job, f"Could not read the playlist: {exc}")
        except Exception:
            LOG.exception("Unexpected job failure")
            await self.progress(job, "Job failed unexpectedly. Please try again later.")
        finally:
            if self.jobs.get(job.user_id) is job:
                self.jobs.pop(job.user_id, None)

    async def send_stream(self, job: Job, video: Video, quality: str, url: str, index: int, total: int) -> None:
        suffix = ".mp3" if quality == AUDIO else ".mp4"
        # Use one temporary file per transfer; it is removed even after errors/cancellation.
        with tempfile.TemporaryDirectory(prefix="ytdown-") as directory:
            path = Path(directory) / ("audio.mp3" if quality == AUDIO else "video.mp4")
            await download(self.http, url, path, self.max_bytes)
            safe_title = re.sub(r"[^\w .-]", "_", video.title, flags=re.UNICODE).strip(" .") or "video"
            filename = media_name(safe_title, quality, 100) + suffix
            prefix = f"{index}/{total} · " if total > 1 else ""
            caption = prefix + media_name(video.title, quality, 750 - len(prefix))
            # read_file_handle=False lets httpx stream the file instead of loading ~47 MiB into RAM.
            with path.open("rb") as handle:
                file = InputFile(handle, filename=filename, read_file_handle=False)
                if quality == AUDIO:
                    await self.app.bot.send_audio(job.chat_id, audio=file, caption=caption,
                                                  title=media_name(video.title, quality, 64),
                                                  read_timeout=300, write_timeout=300, connect_timeout=30, pool_timeout=30)
                else:
                    await self.app.bot.send_video(job.chat_id, video=file, caption=caption, supports_streaming=True,
                                                  read_timeout=300, write_timeout=300, connect_timeout=30, pool_timeout=30)


async def serve(token: str) -> None:
    application = ApplicationBuilder().token(token).concurrent_updates(16).build()
    timeout = aiohttp.ClientTimeout(total=240, connect=20)
    connector = aiohttp.TCPConnector(resolver=PublicResolver())
    async with aiohttp.ClientSession(timeout=timeout, connector=connector,
                                     headers={"User-Agent": "ytdown-telegram-bot/1.0"}) as http:
        service = DownloaderBot(application, http)
        service.register()
        health = web.Application()
        health.router.add_get("/healthz", lambda request: web.json_response({"status": "ok"}))
        runner = web.AppRunner(health)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", env_int("PORT", 8000, 1, 65535))
        await site.start()
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for name in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(name, stop.set)
            except NotImplementedError:  # Windows
                pass
        try:
            async with application:
                await application.start()
                await application.updater.start_polling(drop_pending_updates=True)
                LOG.info("Bot polling; health check ready")
                await stop.wait()
                await application.updater.stop()
                for task in list(service.lookups.values()) + [j.task for j in service.jobs.values() if j.task]:
                    task.cancel()
                await asyncio.gather(*list(service.lookups.values()),
                                     *(j.task for j in service.jobs.values() if j.task), return_exceptions=True)
                await application.stop()
        finally:
            await runner.cleanup()


def main() -> None:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    token = os.getenv("BOT_TOKEN")
    if not token:
        raise SystemExit("Set BOT_TOKEN to your Telegram bot token (environment variable).")
    asyncio.run(serve(token))
