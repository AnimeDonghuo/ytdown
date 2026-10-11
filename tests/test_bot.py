import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from ytdown.bot import DownloaderBot, Job, Selection, choose_title, download, FileTooLarge, media_name, get_youtube_title
from ytdown.core import PlaylistEntry, Source, Video


class PipelineTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.service = DownloaderBot(MagicMock(), MagicMock())
        self.service.progress = AsyncMock()
        self.sent = []
        async def fake_send(job, video, quality, url, index, total):
            self.sent.append((index, quality, url, video.title))
        self.service.send_stream = fake_send

    async def test_playlist_skips_missing_formats_and_uploads_in_order(self):
        source = Source("playlist", "https://www.youtube.com/playlist?list=PL12345678901")
        job = Job(1, 1, 2, source, ("360p", "720p", "2160p"), None)
        self.service.jobs[1] = job
        videos = [Video("Wrong API title", {"720p": "https://cdn.example.com/a"}),
                  Video("Another wrong API title", {"360p": "https://cdn.example.com/b", "2160p": "https://cdn.example.com/c"})]
        entries = [PlaylistEntry("url1", "YouTube title one"), PlaylistEntry("url2", "YouTube title two")]
        with patch("ytdown.bot.extract_playlist", return_value=entries), \
             patch("ytdown.bot.fetch_video", new=AsyncMock(side_effect=videos)) as fetch, \
             patch("ytdown.bot.get_youtube_title", new=AsyncMock()) as metadata:
            await self.service.run_job(job)
        self.assertEqual(fetch.await_count, 2)
        metadata.assert_not_awaited()  # Flat playlist titles do not need extra requests.
        self.assertEqual(self.sent, [(1, "720p", "https://cdn.example.com/a", "YouTube title one"),
                                     (2, "360p", "https://cdn.example.com/b", "YouTube title two"),
                                     (2, "2160p", "https://cdn.example.com/c", "YouTube title two")])
        self.assertIn("unavailable 3", self.service.progress.call_args.args[1])
        self.assertNotIn(1, self.service.jobs)

    async def test_playlist_falls_back_to_api_name_when_youtube_title_is_missing(self):
        job = Job(1, 1, 2, Source("playlist", "https://www.youtube.com/playlist?list=PL12345678901"),
                  ("480p",), None)
        entry = PlaylistEntry("https://www.youtube.com/watch?v=3SY1i5CB03Q", "")
        with patch("ytdown.bot.extract_playlist", return_value=[entry]), \
             patch("ytdown.bot.fetch_video", new=AsyncMock(return_value=Video(
                 "API name for playlist video", {"480p": "https://cdn.example.com/v.mp4"}))), \
             patch("ytdown.bot.get_youtube_title", new=AsyncMock(return_value="")) as metadata:
            await self.service.run_job(job)
        metadata.assert_awaited_once()
        self.assertEqual(self.sent[0][3], "API name for playlist video")

    async def test_single_uses_cached_video_without_second_api_call(self):
        video = Video("first", {"360p": "https://cdn.example.com/a", "mp3": "https://cdn.example.com/b"})
        job = Job(1, 1, 2, Source("video", "https://www.youtube.com/watch?v=3SY1i5CB03Q"),
                  ("360p", "mp3"), video)
        with patch("ytdown.bot.fetch_video", new=AsyncMock()) as fetch:
            await self.service.run_job(job)
        fetch.assert_not_awaited()
        self.assertEqual([item[1] for item in self.sent], ["360p", "mp3"])
        self.assertEqual([item[3] for item in self.sent], ["first", "first"])


class FakeResponse:
    status = 200
    content_type = "video/mp4"
    def __init__(self, chunks, length=None):
        self.content_length = length
        self.content = self
        self.chunks = chunks
    async def __aenter__(self):
        return self
    async def __aexit__(self, *args):
        pass
    def raise_for_status(self):
        pass
    async def iter_chunked(self, n):
        for chunk in self.chunks:
            yield chunk


class DownloadTests(unittest.IsolatedAsyncioTestCase):
    async def test_size_is_checked_even_without_content_length(self):
        from pathlib import Path
        from tempfile import TemporaryDirectory
        with TemporaryDirectory() as folder:
            session = MagicMock()
            session.get.return_value = FakeResponse([b"abc", b"def"])
            with self.assertRaises(FileTooLarge):
                await download(session, "https://cdn.example.com/v.mp4", Path(folder) / "file", 5)

    async def test_rejects_redirect_to_local_network(self):
        from pathlib import Path
        from tempfile import TemporaryDirectory
        response = FakeResponse([])
        response.status = 302
        response.headers = {"Location": "https://127.0.0.1/private"}
        with TemporaryDirectory() as folder:
            session = MagicMock()
            session.get.return_value = response
            with self.assertRaises(ValueError):
                await download(session, "https://cdn.example.com/v.mp4", Path(folder) / "file", 50)
            self.assertEqual(session.get.call_count, 1)


class SelectionTests(unittest.IsolatedAsyncioTestCase):
    async def test_multiple_buttons_toggle_then_done(self):
        from types import SimpleNamespace
        service = DownloaderBot(MagicMock(), MagicMock())
        service.run_job = AsyncMock()
        source = Source("playlist", "https://www.youtube.com/playlist?list=PL12345678901")
        choice = Selection("abcd1234", source, ("360p", "720p", "2160p"))
        service.selections[7] = choice
        query = SimpleNamespace(
            data="", from_user=SimpleNamespace(id=7),
            message=SimpleNamespace(chat=SimpleNamespace(type="private"), chat_id=7, message_id=99),
            answer=AsyncMock(), edit_message_reply_markup=AsyncMock(), edit_message_text=AsyncMock(),
        )
        update = SimpleNamespace(callback_query=query)
        for key in ("360p", "2160p"):
            query.data = f"s:abcd1234:t:{key}"
            await service.callback(update, None)
        self.assertEqual(choice.selected, {"360p", "2160p"})
        self.assertIn("(4K)", service.keyboard(choice).inline_keyboard[1][0].text)
        query.data = "s:abcd1234:done"
        await service.callback(update, None)
        self.assertEqual(service.jobs[7].selected, ("360p", "2160p"))
        await service.jobs[7].task
        query.data = "s:abcd1234:t:720p"
        await service.callback(update, None)
        self.assertEqual(query.answer.call_args.args[0], "This selection expired. Send the link again.")

    async def test_resolver_filters_private_dns_records(self):
        from ytdown.bot import PublicResolver
        resolver = PublicResolver()
        resolver.inner.resolve = AsyncMock(return_value=[{"host": "127.0.0.1"}, {"host": "1.1.1.1"}])
        self.assertEqual(await resolver.resolve("cdn.example.com"), [{"host": "1.1.1.1"}])
        resolver.inner.resolve = AsyncMock(return_value=[{"host": "10.0.0.2"}])
        with self.assertRaises(OSError):
            await resolver.resolve("cdn.example.com")
        await resolver.close()


class TitleTests(unittest.IsolatedAsyncioTestCase):
    async def test_single_selection_uses_youtube_metadata_not_provider_title(self):
        from types import SimpleNamespace
        service = DownloaderBot(MagicMock(), MagicMock())
        notice = SimpleNamespace(edit_text=AsyncMock())
        message = SimpleNamespace(text="https://youtu.be/3SY1i5CB03Q", reply_text=AsyncMock(return_value=notice))
        update = SimpleNamespace(effective_chat=SimpleNamespace(type="private"),
                                 effective_user=SimpleNamespace(id=7), effective_message=message)
        with patch("ytdown.bot.fetch_video", new=AsyncMock(return_value=Video(
                "Incorrect API title", {"480p": "https://cdn.example.com/video.mp4"}))), \
             patch("ytdown.bot.get_youtube_title", new=AsyncMock(return_value="Actual YouTube title")):
            await service.on_link(update, None)
        self.assertEqual(service.selections[7].video.title, "Actual YouTube title")
        self.assertIn("Actual YouTube title", notice.edit_text.call_args.args[0])
        self.assertNotIn("Incorrect API title", notice.edit_text.call_args.args[0])
        with patch("ytdown.bot.fetch_video", new=AsyncMock(return_value=Video(
                "API fallback title", {"480p": "https://cdn.example.com/video.mp4"}))), \
             patch("ytdown.bot.get_youtube_title", new=AsyncMock(return_value="")):
            await service.on_link(update, None)
        self.assertEqual(service.selections[7].video.title, "API fallback title")
        self.assertIn("API fallback title", notice.edit_text.call_args.args[0])

    async def test_oembed_returns_youtube_title_and_yt_dlp_is_backup(self):
        from types import SimpleNamespace
        response = FakeResponse([])
        response.content.read = AsyncMock(return_value=b'{"title":"Real title"}')
        session = MagicMock()
        session.get.return_value = response
        with patch("ytdown.bot.extract_title") as fallback:
            title = await get_youtube_title(session, "https://www.youtube.com/watch?v=3SY1i5CB03Q")
        self.assertEqual(title, "Real title")
        fallback.assert_not_called()
        response.content.read = AsyncMock(return_value=b'{"title":"NA"}')
        with patch("ytdown.bot.extract_title", return_value="Backup YouTube title") as fallback:
            title = await get_youtube_title(session, "https://www.youtube.com/watch?v=3SY1i5CB03Q")
        self.assertEqual(title, "Backup YouTube title")
        fallback.assert_called_once()
        with patch("ytdown.bot.extract_title", side_effect=ValueError("YouTube unavailable")):
            title = await get_youtube_title(session, "https://www.youtube.com/watch?v=3SY1i5CB03Q")
        self.assertEqual(title, "")
        url = "https://www.youtube.com/watch?v=3SY1i5CB03Q"
        self.assertEqual(choose_title(title, "Title from API", url), "Title from API")
        self.assertEqual(choose_title(title, "", url), "YouTube video 3SY1i5CB03Q")

    async def test_upload_caption_filename_and_audio_title_end_with_quality(self):
        from pathlib import Path
        from tempfile import TemporaryDirectory
        service = DownloaderBot(MagicMock(), MagicMock())
        video = Video("A real YouTube video title", {"480p": "https://cdn.example.com/v.mp4"})
        job = Job(1, 1, 2, Source("video", "url"), ("480p",), video)
        seen = []
        async def fake_download(session, url, path, max_bytes):
            path.write_bytes(b"media")
        async def send_video(chat, video, **kwargs):
            seen.append((video.filename, kwargs["caption"], video.input_file_content.read()))
        async def send_audio(chat, audio, **kwargs):
            seen.append((audio.filename, kwargs["caption"], kwargs["title"]))
        service.app.bot.send_video = send_video
        service.app.bot.send_audio = send_audio
        with patch("ytdown.bot.download", new=fake_download):
            await service.send_stream(job, video, "480p", "https://cdn.example.com/v.mp4", 1, 3)
            await service.send_stream(job, video, "mp3", "https://cdn.example.com/a.mp3", 2, 3)
        self.assertEqual(seen[0], ("A real YouTube video title [480p].mp4",
                                   "1/3 · A real YouTube video title [480p]", b"media"))
        self.assertEqual(seen[1], ("A real YouTube video title [MP3].mp3",
                                   "2/3 · A real YouTube video title [MP3]",
                                   "A real YouTube video title [MP3]"))
        self.assertTrue(media_name("X" * 250, "480p", 80).endswith(" [480p]"))
