import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from ytdown.bot import (DownloaderBot, FileTooLarge, HTTPReader, Job, Selection, UnknownLength,
                        choose_title, get_youtube_title, media_name, upload_stream)
from ytdown.core import PlaylistEntry, Source, Video

VIDEO_URL = "https://www.youtube.com/watch?v=3SY1i5CB03Q"
CDN_URL = "https://cdn.example.com/video.mp4"


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
        self.service.jobs[1] = job
        videos = [Video("Wrong API title", {"720p": CDN_URL}),
                  Video("Another wrong API title", {"360p": CDN_URL, "2160p": CDN_URL})]
        entries = [PlaylistEntry(VIDEO_URL, "YouTube title one"),
                   PlaylistEntry("https://www.youtube.com/watch?v=abcdefghijk", "YouTube title two")]
        with patch("ytdown.bot.extract_playlist", return_value=entries), \
             patch("ytdown.bot.fetch_video", new=AsyncMock(side_effect=videos)) as fetch, \
             patch("ytdown.bot.get_youtube_title", new=AsyncMock()) as metadata:
            await self.service.run_job(job)
        self.assertEqual(fetch.await_count, 2)
        metadata.assert_not_awaited()
        self.assertEqual([(item[1], item[3]) for item in self.sent],
                         [("720p", "YouTube title one"), ("360p", "YouTube title two"),
                          ("2160p", "YouTube title two")])
        self.assertIn("unavailable 3", self.service.progress.call_args.args[1])
        self.assertNotIn(1, self.service.jobs)

    async def test_playlist_falls_back_to_api_name(self):
        job = Job(1, "peer", 2, Source("playlist", "url"), ("480p",), None)
        with patch("ytdown.bot.extract_playlist", return_value=[PlaylistEntry(VIDEO_URL, "")]), \
             patch("ytdown.bot.fetch_video", new=AsyncMock(return_value=Video("API name", {"480p": CDN_URL}))), \
             patch("ytdown.bot.get_youtube_title", new=AsyncMock(return_value="")) as metadata:
            await self.service.run_job(job)
        metadata.assert_awaited_once()
        self.assertEqual(self.sent[0][3], "API name")

    async def test_single_uses_cached_video(self):
        video = Video("first", {"360p": CDN_URL, "mp3": CDN_URL})
        job = Job(1, "peer", 2, Source("video", VIDEO_URL), ("360p", "mp3"), video)
        with patch("ytdown.bot.fetch_video", new=AsyncMock()) as fetch:
            await self.service.run_job(job)
        fetch.assert_not_awaited()
        self.assertEqual([item[1] for item in self.sent], ["360p", "mp3"])


class StreamTests(unittest.IsolatedAsyncioTestCase):
    async def test_known_size_streams_in_parts_not_a_whole_file(self):
        session = MagicMock()
        session.get.return_value = FakeResponse(b"abcdef", length=6)
        client = MagicMock()
        pieces = []

        async def upload(reader, *, file_size, file_name, part_size_kb):
            self.assertIsInstance(reader, HTTPReader)
            self.assertEqual((file_size, file_name, part_size_kb), (6, "video.mp4", 512))
            pieces.append(await reader.read(4))
            pieces.append(await reader.read(4))
            return "uploaded handle"

        client.upload_file = upload
        handle = await upload_stream(client, session, CDN_URL, "video.mp4", 100)
        self.assertEqual(handle, "uploaded handle")
        self.assertEqual(pieces, [b"abcd", b"ef"])

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
        async def upload(reader, **kwargs):
            await reader.read(4)
        client = MagicMock()
        client.upload_file = upload
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
        client.send_file = AsyncMock()
        service = DownloaderBot(client, MagicMock())
        video = Video("A real YouTube video title", {"480p": CDN_URL})
        job = Job(1, "peer", 2, Source("video", VIDEO_URL), ("480p",), video)
        with patch("ytdown.bot.upload_stream", new=AsyncMock(return_value="uploaded")) as upload:
            await service.send_stream(job, video, "480p", CDN_URL, 1, 3)
            await service.send_stream(job, video, "mp3", CDN_URL, 2, 3)
        self.assertEqual(upload.await_args_list[0].args[3], "A real YouTube video title [480p].mp4")
        self.assertEqual(upload.await_args_list[1].args[3], "A real YouTube video title [MP3].mp3")
        self.assertEqual(client.send_file.await_args_list[0].kwargs["caption"],
                         "1/3 · A real YouTube video title [480p]")
        self.assertEqual(client.send_file.await_args_list[1].kwargs["mime_type"], "audio/mpeg")
        self.assertTrue(media_name("X" * 250, "480p", 80).endswith(" [480p]"))

    async def test_invalid_streamable_video_reuses_upload_as_document(self):
        from telethon.errors import VideoContentTypeInvalidError
        client = MagicMock()
        client.send_file = AsyncMock(side_effect=[VideoContentTypeInvalidError(None), None])
        service = DownloaderBot(client, MagicMock())
        video = Video("YouTube title", {"480p": CDN_URL})
        job = Job(1, "peer", 2, Source("video", VIDEO_URL), ("480p",), video)
        with patch("ytdown.bot.upload_stream", new=AsyncMock(return_value="same handle")) as upload:
            await service.send_stream(job, video, "480p", CDN_URL, 1, 1)
        upload.assert_awaited_once()
        self.assertEqual(client.send_file.await_count, 2)
        self.assertEqual(client.send_file.await_args_list[1].args[1], "same handle")
        self.assertTrue(client.send_file.await_args_list[1].kwargs["force_document"])


class SelectionTests(unittest.IsolatedAsyncioTestCase):
    async def test_buttons_toggle_and_done(self):
        service = DownloaderBot(MagicMock(), MagicMock())
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
        self.assertEqual(service.jobs[7].selected, ("360p", "2160p"))
        await service.jobs[7].task
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
