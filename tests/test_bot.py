import asyncio
import io
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from ytdown.bot import (DownloaderBot, FileTooLarge, HTTPReader, Job, MAX_MT_MB, MAX_PARTS,
                        PART_BYTES, Selection, UnknownLength, choose_title, get_youtube_title,
                        media_label, media_name, split_subtitle_tag, upload_parts, upload_stream)
from ytdown.core import PlaylistEntry, Source, Video
from ytdown.media import MediaInfo

VIDEO_URL = "https://www.youtube.com/watch?v=3SY1i5CB03Q"
CDN_URL = "https://cdn.example.com/video.mp4"


def jpeg_image(color=(30, 110, 170), size=(320, 180)) -> bytes:
    """A JPEG that is not a blank placeholder (a red stripe on the left unless white)."""
    from PIL import Image, ImageDraw
    image = Image.new("RGB", size, color)
    if color != (255, 255, 255):
        ImageDraw.Draw(image).rectangle((0, 0, size[0] // 3, size[1]), fill=(200, 40, 40))
    out = io.BytesIO()
    image.save(out, format="JPEG", quality=80)
    return out.getvalue()


GOOD_STILL = jpeg_image()
WHITE_STILL = jpeg_image((255, 255, 255))
COVER = jpeg_image((12, 200, 90))


def returned_video(*thumbs, message_id: int = 7):
    """The Message-like object Telethon returns after a successful send_file."""
    from telethon.tl import types
    document = types.Document(id=1, access_hash=2, file_reference=b"", date=None, mime_type="video/mp4",
                              size=10, dc_id=1, attributes=[], thumbs=list(thumbs))
    return SimpleNamespace(id=message_id, document=document)


def cached_still(data: bytes):
    from telethon.tl import types
    return types.PhotoCachedSize(type="m", w=320, h=180, bytes=data)


class FakeContent:
    def __init__(self, data: bytes):
        self.data = data
        self.pos = 0

    async def read(self, count=-1):
        count = len(self.data) - self.pos if count < 0 else count
        data = self.data[self.pos:self.pos + count]
        self.pos += len(data)
        return data

    async def readexactly(self, count):
        data = await self.read(count)
        if len(data) != count:
            raise asyncio.IncompleteReadError(data, count)
        return data

    async def iter_chunked(self, count):
        while True:
            chunk = await self.read(count)
            if not chunk:
                break
            yield chunk


class FakeResponse:
    status = 200
    content_type = "video/mp4"
    headers = {}

    def __init__(self, data=b"", length=None):
        self.content_length = length
        self.content = FakeContent(data)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass

    def raise_for_status(self):
        pass


class PipelineTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.service = DownloaderBot(MagicMock(), MagicMock())
        self.service.progress = AsyncMock()
        self.sent = []

        async def fake_send(job, video, quality, url, index, total):
            self.sent.append((index, quality, url, video.title))

        self.service.send_stream = fake_send

    async def test_playlist_uses_youtube_title_and_skips_missing_quality(self):
        job = Job(1, "peer", 2, Source("playlist", "https://www.youtube.com/playlist?list=PL12345678901"),
                  ("360p", "720p", "2160p"), None)
        self.service.jobs[job.token] = job
        videos = [Video("Wrong API title", {"720p": CDN_URL}),
                  Video("Another wrong API title", {"360p": CDN_URL, "2160p": CDN_URL})]
        entries = [PlaylistEntry(VIDEO_URL, "YouTube title one"),
                   PlaylistEntry("https://www.youtube.com/watch?v=abcdefghijk", "YouTube title two")]
        with patch("ytdown.bot.extract_playlist_range", return_value=entries), \
             patch("ytdown.bot.fetch_video", new=AsyncMock(side_effect=videos)) as fetch, \
             patch("ytdown.bot.get_youtube_title", new=AsyncMock()) as metadata:
            await self.service.run_job(job)
        self.assertEqual(fetch.await_count, 2)
        metadata.assert_not_awaited()
        self.assertEqual([(item[1], item[3]) for item in self.sent],
                         [("720p", "YouTube title one"), ("360p", "YouTube title two"),
                          ("2160p", "YouTube title two")])
        self.assertIn("unavailable 3", self.service.progress.call_args.args[1])
        self.assertNotIn(job.token, self.service.jobs)

    async def test_playlist_falls_back_to_api_name(self):
        job = Job(1, "peer", 2, Source("playlist", "url"), ("480p",), None)
        self.service.jobs[job.token] = job
        with patch("ytdown.bot.extract_playlist_range", return_value=[PlaylistEntry(VIDEO_URL, "")]), \
             patch("ytdown.bot.fetch_video", new=AsyncMock(return_value=Video("API name", {"480p": CDN_URL}))), \
             patch("ytdown.bot.get_youtube_title", new=AsyncMock(return_value="")) as metadata:
            await self.service.run_job(job)
        metadata.assert_awaited_once()
        self.assertEqual(self.sent[0][3], "API name")

    async def test_queue_is_fifo_even_if_first_status_edit_is_slow(self):
        first = Job(1, "peer1", 1, Source("video", VIDEO_URL), ("480p",), Video("first", {"480p": CDN_URL}))
        second = Job(1, "peer1", 2, Source("video", VIDEO_URL), ("480p",), Video("second", {"480p": CDN_URL}))
        self.service.jobs[first.token] = first
        self.service.jobs[second.token] = second
        async def slow_first(job, text, **kwargs):
            if job is first and text.startswith("Queued"):
                await asyncio.sleep(0.02)
        self.service.progress = slow_first
        await asyncio.wait_for(asyncio.gather(self.service.run_job(first), self.service.run_job(second)), 1)
        self.assertEqual([item[3] for item in self.sent], ["first", "second"])

    async def test_cancelled_waiting_job_unblocks_next_job(self):
        active = Job(1, "peer", 1, Source("video", VIDEO_URL), ("480p",), None, state="running")
        first = Job(1, "peer", 2, Source("video", VIDEO_URL), ("480p",), Video("first", {"480p": CDN_URL}))
        second = Job(1, "peer", 3, Source("video", VIDEO_URL), ("480p",), Video("second", {"480p": CDN_URL}))
        for job in (active, first, second):
            self.service.jobs[job.token] = job
        one = asyncio.create_task(self.service.run_job(first))
        two = asyncio.create_task(self.service.run_job(second))
        await asyncio.sleep(0)
        one.cancel()
        await one
        async with self.service.turn:
            self.service.jobs.pop(active.token)
            self.service.turn.notify_all()
        await asyncio.wait_for(two, 1)
        self.assertEqual([item[3] for item in self.sent], ["second"])

    async def test_single_uses_cached_video(self):
        video = Video("first", {"360p": CDN_URL, "mp3": CDN_URL})
        job = Job(1, "peer", 2, Source("video", VIDEO_URL), ("360p", "mp3"), video)
        self.service.jobs[job.token] = job
        with patch("ytdown.bot.fetch_video", new=AsyncMock()) as fetch:
            await self.service.run_job(job)
        fetch.assert_not_awaited()
        self.assertEqual([item[1] for item in self.sent], ["360p", "mp3"])


class StreamTests(unittest.IsolatedAsyncioTestCase):
    async def test_known_size_streams_in_parts_not_a_whole_file(self):
        import hashlib
        from telethon.tl import types
        from telethon.tl.functions.upload import SaveFilePartRequest
        session = MagicMock()
        session.get.return_value = FakeResponse(b"abcdef", length=6)
        client = AsyncMock(return_value=True)
        downloaded, uploaded = [], []
        handle = await upload_stream(client, session, CDN_URL, "video.mp4", 100,
                                     on_download=downloaded.append,
                                     on_upload=lambda done, total: uploaded.append(done))
        self.assertIsInstance(handle, types.InputFile)
        self.assertEqual(handle.md5_checksum, hashlib.md5(b"abcdef").hexdigest())
        self.assertIsInstance(client.call_args.args[0], SaveFilePartRequest)
        self.assertEqual(client.call_args.args[0].bytes, b"abcdef")
        self.assertEqual(downloaded, [6])
        self.assertEqual(uploaded, [6])

    async def test_telethon_uploads_big_file_with_multipart_requests(self):
        from telethon import TelegramClient
        from telethon.sessions import MemorySession
        from telethon.tl import types
        from telethon.tl.functions.upload import SaveBigFilePartRequest

        class OfflineClient(TelegramClient):
            def __init__(self):
                super().__init__(MemorySession(), 12345, "0" * 32)
                self.requests = []

            async def __call__(self, request, *args, **kwargs):
                self.requests.append(request)
                return True

        client = OfflineClient()
        size = 10 * 1024 * 1024 + 1
        response = FakeResponse(b"x" * size, length=size)
        session = MagicMock()
        session.get.return_value = response
        handle = await upload_stream(client, session, CDN_URL, "video.mp4", size)
        self.assertIsInstance(handle, types.InputFileBig)
        self.assertEqual(handle.parts, 21)
        self.assertEqual(len(client.requests), 21)
        self.assertTrue(all(isinstance(r, SaveBigFilePartRequest) and len(r.bytes) <= 512 * 1024
                            for r in client.requests))

    async def test_near_2gb_part_boundary_checked_before_upload(self):
        self.assertEqual(MAX_MT_MB, 2000)
        self.assertEqual(MAX_MT_MB * 1024 * 1024 // PART_BYTES, MAX_PARTS)
        session = MagicMock()
        client = AsyncMock(return_value=True)
        session.get.return_value = FakeResponse(length=MAX_PARTS * PART_BYTES + 1)
        with self.assertRaises(FileTooLarge):
            await upload_stream(client, session, CDN_URL, "big.mp4", MAX_MT_MB * 1024 * 1024)
        client.assert_not_awaited()
        # Exactly 4,000 parts passes the early size check (body is absent in
        # this offline test, so streaming then fails with an incomplete read).
        session.get.return_value = FakeResponse(length=MAX_PARTS * PART_BYTES)
        with self.assertRaises(asyncio.IncompleteReadError):
            await upload_stream(client, session, CDN_URL, "big.mp4", MAX_MT_MB * 1024 * 1024)
        with self.assertRaises(FileTooLarge):
            await upload_parts(client, HTTPReader(FakeResponse(), MAX_PARTS * PART_BYTES + 1),
                               MAX_PARTS * PART_BYTES + 1, "big.mp4", 2)

    async def test_size_cap_redirect_and_wrong_length(self):
        session = MagicMock()
        session.get.return_value = FakeResponse(b"abc", length=101)
        with self.assertRaises(FileTooLarge):
            await upload_stream(MagicMock(), session, CDN_URL, "video.mp4", 100)
        response = FakeResponse()
        response.status = 302
        response.headers = {"Location": "https://127.0.0.1/private"}
        session.get.return_value = response
        with self.assertRaises(ValueError):
            await upload_stream(MagicMock(), session, CDN_URL, "video.mp4", 100)
        self.assertEqual(session.get.call_count, 2)
        session.get.return_value = FakeResponse(b"abc", length=4)
        client = AsyncMock(return_value=True)
        with self.assertRaises(asyncio.IncompleteReadError):
            await upload_stream(client, session, CDN_URL, "video.mp4", 100)

    async def test_unknown_length_spools_only_small_files(self):
        session = MagicMock()
        session.get.return_value = FakeResponse(b"small")
        client = MagicMock()
        async def upload(path, **kwargs):
            from pathlib import Path
            self.assertEqual(Path(path).read_bytes(), b"small")
            return "uploaded"
        client.upload_file = upload
        self.assertEqual(await upload_stream(client, session, CDN_URL, "video.mp4", 100), "uploaded")
        session.get.return_value = FakeResponse(b"too big")
        with patch("ytdown.bot.SPOOL_MB", 0), self.assertRaises(UnknownLength):
            await upload_stream(client, session, CDN_URL, "video.mp4", 100)

    async def test_filename_and_caption_on_mtproto_send(self):
        client = MagicMock()
        client.send_file = AsyncMock(return_value=returned_video(cached_still(GOOD_STILL)))
        client.edit_message = AsyncMock()
        service = DownloaderBot(client, MagicMock())
        video = Video("A real YouTube video title", {"480p": CDN_URL})
        job = Job(1, "peer", 2, Source("playlist", "https://www.youtube.com/playlist?list=PL12345678901"),
                  ("480p",), video)
        service.video_thumbnail = AsyncMock(return_value=(b"jpeg", 320, 180))
        with patch("ytdown.bot.upload_stream", new=AsyncMock(return_value="uploaded")) as upload, \
             patch("ytdown.bot.MediaProbe.inspect", new=AsyncMock(return_value=MediaInfo(884, 1080, 1920))):
            await service.send_stream(job, video, "480p", CDN_URL, 1, 3)
            await service.send_stream(job, video, "mp3", CDN_URL, 2, 3)
        # Playlist-style multi-item jobs use zero-padded "01-03" filenames.
        self.assertEqual(upload.await_args_list[0].args[3], "01-03 A real YouTube video title [480p].mp4")
        self.assertEqual(upload.await_args_list[1].args[3], "02-03 A real YouTube video title [MP3].mp3")
        self.assertEqual(client.send_file.await_args_list[0].kwargs["caption"],
                         "A real YouTube video title [480p]")
        self.assertEqual(client.send_file.await_args_list[1].kwargs["mime_type"], "audio/mpeg")
        attr = client.send_file.await_args_list[0].kwargs["attributes"][0]
        self.assertEqual((attr.duration, attr.w, attr.h), (884, 1080, 1920))
        self.assertEqual(client.send_file.await_args_list[0].kwargs["thumb"].name, "preview.jpg")
        self.assertTrue(media_name("X" * 250, "480p", 80).endswith(" [480p]"))
        self.assertEqual(client.send_file.await_count, 2)  # Two videos; no unexpected cover photos.

    async def test_api_duration_and_thumbnail_shape_if_probe_unavailable(self):
        client = MagicMock()
        client.edit_message = AsyncMock()
        client.send_file = AsyncMock(return_value=returned_video(cached_still(GOOD_STILL)))
        service = DownloaderBot(client, MagicMock())
        service.video_thumbnail = AsyncMock(return_value=(b"jpeg", 320, 180))
        video = Video("Video", {"480p": CDN_URL}, duration=884)
        job = Job(1, "peer", 2, Source("video", VIDEO_URL), ("480p",), video)
        with patch("ytdown.bot.upload_stream", new=AsyncMock(return_value="handle")), \
             patch("ytdown.bot.MediaProbe.inspect", new=AsyncMock(return_value=MediaInfo())):
            await service.send_stream(job, video, "480p", CDN_URL, 1, 1)
        attr = client.send_file.call_args.kwargs["attributes"][0]
        self.assertEqual((attr.duration, attr.w, attr.h), (884, 320, 180))

    async def test_send_flood_wait_reuses_uploaded_handle(self):
        from telethon.errors import FloodWaitError
        client = MagicMock()
        client.edit_message = AsyncMock()
        client.send_file = AsyncMock(side_effect=[FloodWaitError(None, capture=3), None])
        service = DownloaderBot(client, MagicMock())
        video = Video("YouTube title", {"480p": CDN_URL})
        job = Job(1, "peer", 2, Source("video", VIDEO_URL), ("480p",), video)
        service.video_thumbnail = AsyncMock(return_value=(None, 0, 0))
        with patch("ytdown.bot.upload_stream", new=AsyncMock(return_value="uploaded once")) as upload, \
             patch("ytdown.bot.MediaProbe.inspect", new=AsyncMock(return_value=MediaInfo())), \
             patch("ytdown.bot.asyncio.sleep", new=AsyncMock()) as sleeper:
            await service.send_stream(job, video, "480p", CDN_URL, 1, 1)
        upload.assert_awaited_once()
        self.assertEqual(client.send_file.await_count, 2)
        self.assertEqual(client.send_file.await_args_list[1].args[1], "uploaded once")
        sleeper.assert_awaited_once_with(4)

    async def test_invalid_streamable_video_reuses_upload_as_document(self):
        from telethon.errors import VideoContentTypeInvalidError
        client = MagicMock()
        client.send_file = AsyncMock(side_effect=[VideoContentTypeInvalidError(None), None])
        client.edit_message = AsyncMock()
        service = DownloaderBot(client, MagicMock())
        video = Video("YouTube title", {"480p": CDN_URL})
        job = Job(1, "peer", 2, Source("video", VIDEO_URL), ("480p",), video)
        service.video_thumbnail = AsyncMock(return_value=(None, 0, 0))
        with patch("ytdown.bot.upload_stream", new=AsyncMock(return_value="same handle")) as upload, \
             patch("ytdown.bot.MediaProbe.inspect", new=AsyncMock(return_value=MediaInfo())):
            await service.send_stream(job, video, "480p", CDN_URL, 1, 1)
        upload.assert_awaited_once()
        self.assertEqual(client.send_file.await_count, 2)
        self.assertEqual(client.send_file.await_args_list[1].args[1], "same handle")
        self.assertTrue(client.send_file.await_args_list[1].kwargs["force_document"])


class SelectionTests(unittest.IsolatedAsyncioTestCase):
    async def test_buttons_toggle_and_done(self):
        client = MagicMock()
        client.send_message = AsyncMock(return_value=SimpleNamespace(id=101))
        client.delete_messages = AsyncMock()
        service = DownloaderBot(client, MagicMock())
        service.run_job = AsyncMock()
        source = Source("playlist", "https://www.youtube.com/playlist?list=PL12345678901")
        choice = Selection("abcd1234", source, ("360p", "720p", "2160p"), "peer")
        service.selections[7] = choice
        event = SimpleNamespace(data=b"", sender_id=7, is_private=True, message_id=99,
                                answer=AsyncMock(), edit=AsyncMock())
        for key in ("360p", "2160p"):
            event.data = f"s:abcd1234:t:{key}".encode()
            await service.callback(event)
        self.assertEqual(choice.selected, {"360p", "2160p"})
        self.assertIn("(4K)", service.keyboard(choice)[1][0].text)
        event.data = b"s:abcd1234:done"
        await service.callback(event)
        job = service.user_jobs(7)[0]
        self.assertEqual(job.selected, ("360p", "2160p"))
        await job.task
        event.data = b"s:abcd1234:t:720p"
        await service.callback(event)
        self.assertEqual(event.answer.call_args.args[0], "This selection expired. Send the link again.")

    async def test_single_youtube_title_then_api_fallback(self):
        service = DownloaderBot(MagicMock(), MagicMock())
        notice = SimpleNamespace(edit=AsyncMock())
        event = SimpleNamespace(sender_id=7, raw_text="https://youtu.be/3SY1i5CB03Q",
                                get_input_chat=AsyncMock(return_value="peer"),
                                respond=AsyncMock(return_value=notice))
        with patch("ytdown.bot.fetch_video", new=AsyncMock(return_value=Video("Wrong API title", {"480p": CDN_URL}))), \
             patch("ytdown.bot.get_youtube_title", new=AsyncMock(return_value="Actual YouTube title")):
            await service.on_link(event)
        self.assertEqual(service.selections[7].video.title, "Actual YouTube title")
        with patch("ytdown.bot.fetch_video", new=AsyncMock(return_value=Video("API fallback", {"480p": CDN_URL}))), \
             patch("ytdown.bot.get_youtube_title", new=AsyncMock(return_value="")):
            await service.on_link(event)
        self.assertEqual(service.selections[7].video.title, "API fallback")

    async def test_resolver_filters_private_dns(self):
        from ytdown.bot import PublicResolver
        resolver = PublicResolver()
        resolver.inner.resolve = AsyncMock(return_value=[{"host": "127.0.0.1"}, {"host": "1.1.1.1"}])
        self.assertEqual(await resolver.resolve("cdn.example.com"), [{"host": "1.1.1.1"}])
        resolver.inner.resolve = AsyncMock(return_value=[{"host": "10.0.0.2"}])
        with self.assertRaises(OSError):
            await resolver.resolve("cdn.example.com")
        await resolver.close()

    async def test_oembed_and_yt_dlp_backup(self):
        response = FakeResponse(b'{"title":"Real title"}')
        session = MagicMock()
        session.get.return_value = response
        with patch("ytdown.bot.extract_title") as fallback:
            self.assertEqual(await get_youtube_title(session, VIDEO_URL), "Real title")
        fallback.assert_not_called()
        session.get.return_value = FakeResponse(b'{"title":"NA"}')
        with patch("ytdown.bot.extract_title", return_value="Backup title") as fallback:
            self.assertEqual(await get_youtube_title(session, VIDEO_URL), "Backup title")
        fallback.assert_called_once()
        session.get.return_value = FakeResponse(b'{"title":"NA"}')
        with patch("ytdown.bot.extract_title", side_effect=ValueError("Unavailable")):
            self.assertEqual(await get_youtube_title(session, VIDEO_URL), "")
        self.assertEqual(choose_title("", "API title", VIDEO_URL), "API title")
        self.assertEqual(choose_title("", "", VIDEO_URL), "YouTube video 3SY1i5CB03Q")


class LiveStatusTests(unittest.IsolatedAsyncioTestCase):
    async def test_progress_bar_eta_queue_refresh_and_edit_throttling(self):
        client = MagicMock()
        client.edit_message = AsyncMock()
        service = DownloaderBot(client, MagicMock())
        job = Job(7, "peer", 33, Source("video", VIDEO_URL), ("480p",), None)
        service.jobs[job.token] = job
        self.assertIn("Queue #1", service.format_job(job))
        job.state = "running"
        job.title = "YouTube title"
        job.quality = "480p"
        job.item_index = job.item_total = 1
        job.total_bytes = 1024 * 1024
        job.downloaded = 700 * 1024
        job.uploaded = 512 * 1024
        job.sample_at -= 10
        await service.render(job, force=True)
        rendered = client.edit_message.call_args.args[2]
        self.assertIn("50.0%", rendered)
        self.assertIn("Telegram: 512.0 KiB / 1.0 MiB", rendered)
        self.assertIn("ETA ", rendered)
        self.assertIn("YouTube title [480p]", rendered)
        self.assertEqual(len(client.edit_message.call_args.kwargs["buttons"][0]), 1)
        self.assertIn(f"/cancel_{job.token}", rendered)
        await service.render(job)
        self.assertEqual(client.edit_message.await_count, 1)  # No flood of status edits.
        event = SimpleNamespace(data=f"j:{job.token}:refresh".encode(), sender_id=7,
                                is_private=True, message_id=33, answer=AsyncMock())
        await service.callback(event)
        self.assertEqual(client.edit_message.await_count, 2)  # Manual refresh is immediate.
        self.assertIn("Refreshing now", event.answer.call_args.args[0])
        self.assertGreater(job.next_edit, asyncio.get_running_loop().time())
        await service.callback(event)
        self.assertIn("Just refreshed", event.answer.call_args.args[0])

    async def test_flood_wait_delays_next_edit_without_crashing_transfer(self):
        from telethon.errors import FloodWaitError
        client = MagicMock()
        client.edit_message = AsyncMock(side_effect=FloodWaitError(None, capture=90))
        service = DownloaderBot(client, MagicMock())
        job = Job(7, "peer", 33, Source("video", VIDEO_URL), ("480p",), None)
        await service.render(job, force=True)
        self.assertGreater(job.next_edit - asyncio.get_running_loop().time(), 85)

    async def test_bounded_big_upload_can_pipeline_two_parts(self):
        from telethon.tl import types
        data = b"x" * (10 * 1024 * 1024 + 1)
        session = MagicMock()
        session.get.return_value = FakeResponse(data, length=len(data))
        class DelayedClient:
            def __init__(self):
                self.active = 0
                self.peak = 0
                self.count = 0
            async def __call__(self, request):
                self.active += 1
                self.peak = max(self.peak, self.active)
                await asyncio.sleep(0)
                self.active -= 1
                self.count += 1
                return True
        client = DelayedClient()
        downloads = []
        uploads = []
        handle = await upload_stream(client, session, CDN_URL, "video.mp4", len(data),
                                     on_download=downloads.append,
                                     on_upload=lambda n, total: uploads.append(n), in_flight=2)
        self.assertIsInstance(handle, types.InputFileBig)
        self.assertEqual(client.count, 21)
        self.assertEqual(client.peak, 2)
        self.assertEqual(sum(downloads), len(data))
        self.assertEqual(uploads[-1], len(data))

    async def test_mtproto_flood_wait_retries_part_without_reuploading_entire_file(self):
        from telethon.errors import FloodWaitError
        size = 10 * 1024 * 1024 + 1
        session = MagicMock()
        session.get.return_value = FakeResponse(b"x" * size, length=size)
        class RateLimitedClient:
            def __init__(self):
                self.attempts = {}
            async def __call__(self, request):
                index = request.file_part
                self.attempts[index] = self.attempts.get(index, 0) + 1
                if index == 0 and self.attempts[index] == 1:
                    raise FloodWaitError(None, capture=2)
                return True
        client = RateLimitedClient()
        waits = []
        with patch("ytdown.bot.asyncio.sleep", new=AsyncMock()) as sleeper:
            await upload_stream(client, session, CDN_URL, "video.mp4", size, in_flight=2,
                                on_flood=waits.append)
        self.assertEqual(client.attempts[0], 2)
        self.assertTrue(all(count == 1 for part, count in client.attempts.items() if part != 0))
        self.assertEqual(waits, [2, 0])
        sleeper.assert_awaited_once_with(3)


class QueueAndAPISettingsTests(unittest.IsolatedAsyncioTestCase):
    async def test_multiple_links_from_same_user_can_queue_and_cancel_individually(self):
        client = MagicMock()
        client.delete_messages = AsyncMock()
        client.send_message = AsyncMock(return_value=SimpleNamespace(id=54))
        service = DownloaderBot(client, MagicMock())
        service.run_job = AsyncMock()
        existing = Job(7, "peer", 10, Source("video", VIDEO_URL), ("480p",), None, state="running")
        service.jobs[existing.token] = existing
        notice = SimpleNamespace(edit=AsyncMock())
        event = SimpleNamespace(sender_id=7, raw_text=VIDEO_URL, is_private=True,
                                get_input_chat=AsyncMock(return_value="peer"),
                                respond=AsyncMock(return_value=notice))
        with patch("ytdown.bot.fetch_video", new=AsyncMock(return_value=Video("API title", {"480p": CDN_URL}))), \
             patch("ytdown.bot.get_youtube_title", new=AsyncMock(return_value="YouTube title")):
            await service.on_link(event)
        self.assertIsNotNone(service.selections.get(7))
        selection = service.selections[7]
        selection.selected.add("480p")
        click = SimpleNamespace(sender_id=7, data=f"s:{selection.token}:done".encode(),
                                is_private=True, message_id=11, answer=AsyncMock())
        await service.callback(click)
        self.assertEqual(len(service.user_jobs(7)), 2)
        event.raw_text = "/queue"
        event.respond.return_value = SimpleNamespace(id=55)
        await service.on_message(event)
        self.assertIn("Your downloads", event.respond.call_args.args[0])
        self.assertIn("Queue #1", event.respond.call_args.args[0])
        self.assertIn(existing.token, event.respond.call_args.args[0])
        second = next(j for j in service.user_jobs(7) if j is not existing)
        await second.task
        wrong_user = SimpleNamespace(sender_id=8, data=f"j:{second.token}:cancel".encode(),
                                     is_private=True, message_id=11, answer=AsyncMock())
        await service.callback(wrong_user)
        self.assertIn("expired", wrong_user.answer.call_args.args[0])
        # Cancel by exact job ID must not cancel the first job.
        second.task = asyncio.create_task(asyncio.sleep(300))
        event.raw_text = f"/cancel {second.token}"
        await service.on_message(event)
        self.assertTrue(second.task.cancelled() or second.task.cancelling())
        self.assertIn(existing.token, service.jobs)
        await asyncio.gather(second.task, return_exceptions=True)

    async def test_full_queue_does_not_discard_selection(self):
        service = DownloaderBot(MagicMock(), MagicMock())
        service.run_job = AsyncMock()
        for i in range(5):
            job = Job(i, f"peer{i}", i, Source("video", VIDEO_URL), ("480p",), None)
            service.jobs[job.token] = job
        selection = Selection("abcd1234", Source("video", VIDEO_URL), ("480p",), "peer", selected={"480p"})
        service.selections[7] = selection
        event = SimpleNamespace(sender_id=7, data=b"s:abcd1234:done", is_private=True,
                                message_id=77, answer=AsyncMock())
        await service.callback(event)
        self.assertIn("Queue is full", event.answer.call_args.args[0])
        self.assertIs(service.selections[7], selection)

    async def test_api_redirect_to_private_address_is_rejected(self):
        from ytdown.bot import fetch_video
        response = FakeResponse()
        response.status = 302
        response.url = "https://api.example.com/?url=video"
        response.headers = {"Location": "http://127.0.0.1/internal"}
        session = MagicMock()
        session.get.return_value = response
        with self.assertRaises(ValueError):
            await fetch_video(session, VIDEO_URL, "https://api.example.com/")
        self.assertEqual(session.get.call_count, 1)

    async def test_changeapi_per_user_then_reset(self):
        from ytdown.bot import API_URL, normalize_api_endpoint
        endpoint = normalize_api_endpoint("https://api.example.com/download?key=hello&url=")
        self.assertEqual(endpoint, "https://api.example.com/download?key=hello")
        for bad in ("http://api.example.com/?url=", "https://127.0.0.1/?url=",
                    "https://www.youtube.com/watch?v=3SY1i5CB03Q"):
            with self.assertRaises(ValueError):
                normalize_api_endpoint(bad)
        service = DownloaderBot(MagicMock(), MagicMock())
        event = SimpleNamespace(sender_id=7, is_private=True, raw_text="/changeapi",
                                respond=AsyncMock(), get_input_chat=AsyncMock(return_value="peer"))
        await service.on_message(event)
        self.assertIn(7, service.awaiting_api)
        event.raw_text = "https://api.example.com/download?key=hello&url="
        await service.on_message(event)
        self.assertEqual(service.api_for(7), endpoint)
        self.assertEqual(service.api_for(8), API_URL)
        notice = SimpleNamespace(edit=AsyncMock())
        event.respond = AsyncMock(return_value=notice)
        event.raw_text = VIDEO_URL
        with patch("ytdown.bot.fetch_video", new=AsyncMock(return_value=Video("API", {"480p": CDN_URL}))) as fetch, \
             patch("ytdown.bot.get_youtube_title", new=AsyncMock(return_value="YouTube")):
            await service.on_message(event)
        self.assertEqual(fetch.call_args.args[2], endpoint)
        self.assertEqual(service.selections[7].api_endpoint, endpoint)
        event.raw_text = "/changeapi reset"
        await service.on_message(event)
        self.assertEqual(service.api_for(7), API_URL)
        self.assertEqual(service.selections[7].api_endpoint, endpoint)  # Existing selections keep their API.


class LabelTests(unittest.TestCase):
    def test_subtitle_marker_moves_after_quality(self):
        self.assertEqual(split_subtitle_tag("One Way or Another Episode 15 [MultiSub]"),
                         ("One Way or Another Episode 15", "[MultiSub]"))
        self.assertEqual(split_subtitle_tag("One Way or Another - Episode 15 (EngSub)"),
                         ("One Way or Another - Episode 15", "[EngSub]"))
        self.assertEqual(split_subtitle_tag("Show Eng Sub 2 [engsub]")[1], "[EngSub]")
        self.assertEqual(split_subtitle_tag("Plain title"), ("Plain title", ""))
        self.assertEqual(split_subtitle_tag("Both [EngSub] [MultiSub]")[1], "[MultiSub]")
        self.assertEqual(split_subtitle_tag("[MultiSub]"), ("", "[MultiSub]"))

    def test_quality_and_subtitle_label_has_no_playlist_number(self):
        self.assertEqual(media_label("One Way or Another Episode 15 [MultiSub]", "360p", 750),
                         "One Way or Another Episode 15 [360p] [MultiSub]")
        self.assertEqual(media_label("Episode 15 (EngSub)", "mp3", 750), "Episode 15 [MP3] [EngSub]")
        self.assertEqual(media_label("Plain", "720p", 750), "Plain [720p]")

    def test_long_title_keeps_quality_and_subtitle(self):
        label = media_label("X" * 900 + " [MultiSub]", "1080p", 750)
        self.assertLessEqual(len(label), 750)
        self.assertTrue(label.startswith("X"))
        self.assertTrue(label.endswith(" [1080p] [MultiSub]"))
        self.assertTrue(media_name("X" * 250, "480p", 80, "[EngSub]").endswith(" [480p] [EngSub]"))

    def test_status_title_has_no_playlist_number(self):
        service = DownloaderBot(MagicMock(), MagicMock())
        job = Job(1, "peer", 2, Source("playlist", "url"), ("360p",), None, state="running",
                  title="One Way or Another Episode 15 [MultiSub]", quality="360p",
                  item_index=2, item_total=39)
        service.jobs[job.token] = job
        text = service.format_job(job, include_metrics=False)
        self.assertTrue(text.startswith("🎬 One Way or Another Episode 15 [360p] [MultiSub]\n"), text)
        self.assertNotIn("2/39", text.split("\n")[0])
        single = Job(1, "peer", 3, Source("video", VIDEO_URL), ("mp3",), None, state="running",
                     title="Solo [EngSub]", quality="mp3", item_index=1, item_total=1)
        self.assertTrue(service.format_job(single, include_metrics=False).startswith(
            "🎬 Solo [MP3] [EngSub]\n"))


class PlaylistLabelTests(unittest.IsolatedAsyncioTestCase):
    async def test_playlist_filename_caption_and_inline_options(self):
        client = MagicMock()
        client.edit_message = AsyncMock()
        client.send_file = AsyncMock(return_value=returned_video(cached_still(GOOD_STILL)))
        service = DownloaderBot(client, MagicMock())
        service.video_thumbnail = AsyncMock(return_value=(COVER, 320, 180))
        video = Video("One Way or Another Episode 15 [MultiSub]", {"360p": CDN_URL})
        job = Job(1, "peer", 2, Source("playlist", "url"), ("360p",), None, item_total=39)
        with patch("ytdown.bot.upload_stream", new=AsyncMock(return_value="handle")) as upload, \
             patch("ytdown.bot.MediaProbe.inspect", new=AsyncMock(return_value=MediaInfo(600, 640, 360))):
            await service.send_stream(job, video, "360p", CDN_URL, 2, 39)
        self.assertEqual(upload.await_args.args[3], "02-39 One Way or Another Episode 15 [360p] [MultiSub].mp4")
        options = client.send_file.await_args.kwargs
        self.assertEqual(options["caption"], "One Way or Another Episode 15 [360p] [MultiSub]")
        # Playable inline: streamable MP4, not forced to a document.
        self.assertFalse(options["force_document"])
        self.assertTrue(options["supports_streaming"])
        self.assertEqual(options["mime_type"], "video/mp4")
        self.assertEqual(client.send_file.await_count, 1)

    async def test_three_digit_playlist_index_is_padded_to_total_width(self):
        client = MagicMock()
        client.edit_message = AsyncMock()
        client.send_file = AsyncMock(return_value=returned_video(cached_still(GOOD_STILL)))
        service = DownloaderBot(client, MagicMock())
        service.video_thumbnail = AsyncMock(return_value=(None, 0, 0))
        video = Video("Episode", {"480p": CDN_URL})
        job = Job(1, "peer", 2, Source("playlist", "url"), ("480p",), None, item_total=150)
        with patch("ytdown.bot.upload_stream", new=AsyncMock(return_value="handle")) as upload, \
             patch("ytdown.bot.MediaProbe.inspect", new=AsyncMock(return_value=MediaInfo())):
            await service.send_stream(job, video, "480p", CDN_URL, 2, 150)
        self.assertEqual(upload.await_args.args[3], "002-150 Episode [480p].mp4")
        self.assertEqual(client.send_file.await_args.kwargs["caption"], "Episode [480p]")

    async def test_single_video_has_no_position_prefix(self):
        client = MagicMock()
        client.edit_message = AsyncMock()
        client.send_file = AsyncMock(return_value=returned_video(cached_still(GOOD_STILL)))
        service = DownloaderBot(client, MagicMock())
        service.video_thumbnail = AsyncMock(return_value=(None, 0, 0))
        video = Video("Solo [EngSub]", {"480p": CDN_URL})
        job = Job(1, "peer", 2, Source("video", VIDEO_URL), ("480p",), video)
        with patch("ytdown.bot.upload_stream", new=AsyncMock(return_value="handle")) as upload, \
             patch("ytdown.bot.MediaProbe.inspect", new=AsyncMock(return_value=MediaInfo())):
            await service.send_stream(job, video, "480p", CDN_URL, 1, 1)
        self.assertEqual(upload.await_args.args[3], "Solo [480p] [EngSub].mp4")
        self.assertEqual(client.send_file.await_args.kwargs["caption"], "Solo [480p] [EngSub]")


class TeraboxUnchangedTests(unittest.IsolatedAsyncioTestCase):
    """Terabox keeps its original filenames, captions, and single preview send."""

    async def test_folder_item_keeps_original_names_and_no_reply_cover(self):
        from ytdown.terabox import TeraboxFile
        client = MagicMock()
        client.edit_message = AsyncMock()
        # A returned message with no still: YouTube would add a cover photo, Terabox must not.
        client.send_file = AsyncMock(return_value=SimpleNamespace(id=9))
        service = DownloaderBot(client, MagicMock())
        service.video_thumbnail = AsyncMock(return_value=(COVER, 320, 180))
        item = TeraboxFile("1", "Movie Part.mkv", "1080p", CDN_URL, "", "video/x-matroska", 20 * 1024 * 1024)
        job = Job(7, "peer", 2, Source("terabox-folder", "https://terasharefile.com/s/abc"), ("1080p",), None,
                  item_total=39)
        with patch("ytdown.bot.upload_stream", new=AsyncMock(return_value="handle")) as upload, \
             patch("ytdown.bot.MediaProbe.inspect", new=AsyncMock(return_value=MediaInfo(120, 1920, 1080))):
            await service.send_stream(job, Video(item.title, {}), "1080p", CDN_URL, 2, 39, item)
        self.assertEqual(upload.await_args.args[3], "Movie Part [1080p].mkv")
        self.assertEqual(client.send_file.await_count, 1)
        self.assertEqual(client.send_file.await_args.kwargs["caption"], "2/39 · Movie Part [1080p]")
        self.assertTrue(client.send_file.await_args.kwargs["force_document"])

    async def test_terabox_subtitle_text_is_not_rearranged(self):
        from ytdown.terabox import TeraboxFile
        client = MagicMock()
        client.edit_message = AsyncMock()
        client.send_file = AsyncMock(return_value=SimpleNamespace(id=9))
        service = DownloaderBot(client, MagicMock())
        service.video_thumbnail = AsyncMock(return_value=(None, 0, 0))
        item = TeraboxFile("1", "Track [EngSub].mp3", "MP3", CDN_URL, "", "audio/mpeg", 1000)
        job = Job(7, "peer", 2, Source("terabox", "https://terasharefile.com/s/abc"), ("MP3",), None)
        with patch("ytdown.bot.upload_stream", new=AsyncMock(return_value="handle")) as upload, \
             patch("ytdown.bot.MediaProbe.inspect", new=AsyncMock(return_value=MediaInfo())):
            await service.send_stream(job, Video(item.title, {}), "MP3", CDN_URL, 1, 1, item)
        self.assertEqual(upload.await_args.args[3], "Track _EngSub_ [MP3].mp3")


class CoverTests(unittest.IsolatedAsyncioTestCase):
    """After a video is delivered, Telegram's returned still is checked; a missing or white one gets a reply cover."""

    def make_service(self, *, iter_download=None, send_results=None):
        client = MagicMock()
        client.edit_message = AsyncMock()
        client.send_file = AsyncMock(side_effect=send_results)
        if iter_download is not None:
            client.iter_download = iter_download
        service = DownloaderBot(client, MagicMock())
        service.video_thumbnail = AsyncMock(return_value=(COVER, 320, 180))
        self.job = Job(1, "peer", 2, Source("video", VIDEO_URL), ("360p",), None)
        self.video = Video("Cover test", {"360p": CDN_URL})
        return service

    async def stream(self, service, quality="360p", url=CDN_URL, *, mime_probe=True):
        video = self.video if quality != "mp3" else Video("Cover test", {"mp3": CDN_URL})
        job = self.job if quality != "mp3" else Job(1, "peer", 2, Source("video", VIDEO_URL), ("mp3",), None)
        with patch("ytdown.bot.upload_stream", new=AsyncMock(return_value="handle")) as upload, \
             patch("ytdown.bot.MediaProbe.inspect", new=AsyncMock(return_value=MediaInfo(60, 640, 360))):
            await service.send_stream(job, video, quality, url, 1, 1)
        return upload

    async def test_healthy_returned_still_sends_no_cover(self):
        service = self.make_service(send_results=[returned_video(cached_still(GOOD_STILL))])
        upload = await self.stream(service)
        upload.assert_awaited_once()
        self.assertEqual(service.client.send_file.await_count, 1)

    async def test_missing_still_gets_reply_photo_without_reuploading_video(self):
        service = self.make_service(send_results=[returned_video(), SimpleNamespace(id=9)])
        upload = await self.stream(service)
        upload.assert_awaited_once()  # The video bytes were uploaded only once.
        self.assertEqual(service.client.send_file.await_count, 2)
        video_call, cover_call = service.client.send_file.await_args_list
        self.assertIs(video_call.args[1], "handle")
        self.assertEqual(cover_call.kwargs["reply_to"], 7)
        self.assertFalse(cover_call.kwargs["force_document"])
        photo = cover_call.args[1]
        self.assertEqual(photo.name, "cover.jpg")
        self.assertEqual(photo.getvalue(), COVER)

    async def test_healthy_downloaded_still_is_checked_without_cover(self):
        from telethon.tl import types
        requested = []

        async def download(location, file_size=None, request_size=None):
            requested.append((location.thumb_size, file_size, request_size))
            for start in range(0, len(GOOD_STILL), 4096):
                yield GOOD_STILL[start:start + 4096]

        still = types.PhotoSize(type="m", w=320, h=180, size=len(GOOD_STILL))
        service = self.make_service(iter_download=download, send_results=[returned_video(still)])
        await self.stream(service)
        self.assertEqual(requested, [("m", len(GOOD_STILL), 64 * 1024)])
        self.assertEqual(service.client.send_file.await_count, 1)

    async def test_stripped_placeholder_only_counts_as_missing(self):
        from telethon.tl import types
        stripped = types.PhotoStrippedSize(type="i", bytes=b"\x01\x02")
        service = self.make_service(send_results=[returned_video(stripped), SimpleNamespace(id=9)])
        await self.stream(service)
        self.assertEqual(service.client.send_file.await_count, 2)

    async def test_white_still_is_replaced_with_cover(self):
        service = self.make_service(send_results=[returned_video(cached_still(WHITE_STILL)), None])
        await self.stream(service)
        self.assertEqual(service.client.send_file.await_count, 2)
        self.assertEqual(service.client.send_file.await_args.kwargs["reply_to"], 7)

    async def test_unreadable_downloaded_still_counts_as_missing(self):
        from telethon.tl import types

        def broken_download(*args, **kwargs):
            raise ConnectionError("thumbnail download dropped")

        still = types.PhotoSize(type="m", w=320, h=180, size=4000)
        service = self.make_service(iter_download=broken_download,
                                    send_results=[returned_video(still), SimpleNamespace(id=9)])
        await self.stream(service)
        self.assertEqual(service.client.send_file.await_count, 2)

    async def test_oversized_returned_still_download_is_bounded(self):
        from telethon.tl import types
        chunks_read = []

        async def endless(location, file_size=None, request_size=None):
            while True:  # A misbehaving server could stream forever.
                chunks_read.append(1)
                yield b"\x00" * request_size

        still = types.PhotoSize(type="m", w=320, h=180, size=4000)
        service = self.make_service(iter_download=endless,
                                    send_results=[returned_video(still), SimpleNamespace(id=9)])
        await self.stream(service)
        self.assertLessEqual(len(chunks_read), 5)  # Stops right after the 256 KiB cap.
        self.assertEqual(service.client.send_file.await_count, 2)  # Treated as missing.

    async def test_cover_failure_never_resends_the_video(self):
        from telethon.errors import RPCError
        service = self.make_service(send_results=[returned_video(), RPCError(None, "PHOTO_SAVE_FILE_INVALID", 400)])
        upload = await self.stream(service)  # Must not raise.
        upload.assert_awaited_once()
        self.assertEqual(service.client.send_file.await_count, 2)
        self.assertNotIn("handle", [call.args[1] for call in service.client.send_file.await_args_list[1:]])

    async def test_rejected_video_thumb_resends_only_the_failed_send_then_checks_cover(self):
        from telethon.errors import PhotoInvalidDimensionsError
        service = self.make_service(send_results=[PhotoInvalidDimensionsError(None),
                                                  returned_video(), SimpleNamespace(id=9)])
        upload = await self.stream(service)
        upload.assert_awaited_once()
        self.assertEqual(service.client.send_file.await_count, 3)
        self.assertIsNotNone(service.client.send_file.await_args_list[0].kwargs["thumb"])
        self.assertIsNone(service.client.send_file.await_args_list[1].kwargs["thumb"])
        self.assertEqual(service.client.send_file.await_args_list[2].kwargs["reply_to"], 7)

    async def test_audio_never_gets_a_cover(self):
        service = self.make_service(send_results=[returned_video()])
        await self.stream(service, quality="mp3", url=CDN_URL)
        self.assertEqual(service.client.send_file.await_count, 1)

    async def test_no_prepared_cover_leaves_video_alone(self):
        service = self.make_service(send_results=[returned_video()])
        service.video_thumbnail = AsyncMock(return_value=(None, 0, 0))
        await self.stream(service)
        self.assertEqual(service.client.send_file.await_count, 1)


class FrameThumbnailTests(unittest.IsolatedAsyncioTestCase):
    """YouTube items with no API/YouTube still use a frame from the bounded sample."""

    async def run_item(self, *, kind, thumbnail, frame):
        client = MagicMock()
        client.edit_message = AsyncMock()
        client.send_file = AsyncMock(return_value=returned_video(cached_still(GOOD_STILL)))
        service = DownloaderBot(client, MagicMock())
        service.video_thumbnail = AsyncMock(return_value=thumbnail)
        job = Job(1, "peer", 2, Source(kind, "https://www.youtube.com/playlist?list=PL12345678901"
                                       if kind == "playlist" else VIDEO_URL), ("360p",), None, item_total=39)
        video = Video("Episode 15", {"360p": CDN_URL})
        with patch("ytdown.bot.upload_stream", new=AsyncMock(return_value="handle")), \
             patch("ytdown.bot.MediaProbe.inspect", new=AsyncMock(return_value=MediaInfo(60, 640, 360))), \
             patch("ytdown.bot.MediaProbe.preview_jpeg", new=AsyncMock(return_value=frame)) as preview:
            await service.send_stream(job, video, "360p", CDN_URL, 2, 39)
        return client, preview

    async def test_playlist_item_without_api_thumbnail_uses_video_frame(self):
        frame = (COVER, 320, 180)
        client, preview = await self.run_item(kind="playlist", thumbnail=(None, 0, 0), frame=frame)
        preview.assert_awaited_once()
        self.assertEqual(client.send_file.await_args.kwargs["thumb"].read(), COVER)

    async def test_api_thumbnail_is_preferred_over_frame(self):
        client, preview = await self.run_item(kind="playlist", thumbnail=(GOOD_STILL, 320, 180),
                                              frame=(COVER, 320, 180))
        preview.assert_not_awaited()
        self.assertEqual(client.send_file.await_args.kwargs["thumb"].read(), GOOD_STILL)

    async def test_no_frame_still_sends_video_without_thumbnail(self):
        client, preview = await self.run_item(kind="playlist", thumbnail=(None, 0, 0), frame=None)
        preview.assert_awaited_once()
        self.assertIsNone(client.send_file.await_args.kwargs["thumb"])
        self.assertEqual(client.send_file.await_count, 1)


class ToggleTests(unittest.IsolatedAsyncioTestCase):
    async def test_repeated_toggle_with_identical_buttons_does_not_raise(self):
        from telethon.errors import MessageNotModifiedError
        service = DownloaderBot(MagicMock(), MagicMock())
        selection = Selection("abcd1234", Source("video", VIDEO_URL), ("360p", "720p"), "peer",
                              Video("T", {"360p": CDN_URL, "720p": CDN_URL}))
        service.selections[5] = selection
        event = SimpleNamespace(sender_id=5, is_private=True, data=b"s:abcd1234:t:360p",
                                answer=AsyncMock(), edit=AsyncMock(side_effect=MessageNotModifiedError(None)))
        await service.callback(event)  # Must not propagate the Telegram error.
        event.edit.assert_awaited_once()
        self.assertEqual(selection.selected, {"360p"})
