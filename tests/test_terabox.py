import asyncio
import io
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from ytdown.bot import DownloaderBot, Job, RangeRequest
from ytdown.core import Source, Video
from ytdown.terabox import (TeraboxFile, TeraboxListing, normalize_terabox,
                            parse_terabox_source)

SHARE = "https://www.terabox.com/s/12345"
CDN = "https://cdn.example.com/file.mkv"


class TeraboxNormalizationTests(unittest.TestCase):
    def test_allowed_share_hosts_only(self):
        self.assertEqual(parse_terabox_source(SHARE), SHARE)
        for url in ("https://www.youtube.com/watch?v=3SY1i5CB03Q",
                    "https://terabox.com.evil.com/s/12345", "http://terabox.com/s/1",
                    "https://127.0.0.1/s/1"):
            with self.assertRaises(ValueError):
                parse_terabox_source(url)

    def test_single_file_keeps_extension_mime_size_and_thumbnail(self):
        listing = normalize_terabox({"success": True, "total_files": 1,
                                     "data": {"name": "Movie.mkv", "quality": "1080p",
                                              "download_url": CDN,
                                              "thumbnail": "https://cdn.example.com/cover.jpg"}})
        self.assertEqual(len(listing.files), 1)
        self.assertEqual(listing.files[0].mime, "video/x-matroska")
        self.assertEqual(listing.files[0].quality, "1080p")
        self.assertEqual(listing.files[0].title, "Movie")
        self.assertTrue(listing.files[0].video)
        unnamed_ext = normalize_terabox({"success": True, "data": {"files": [
            {"id": "B", "filename": "clip", "extension": ".webm",
             "download_url": CDN, "size_bytes": "4096", "quality": "4K"}]}})
        self.assertEqual(unnamed_ext.files[0].filename, "clip.webm")
        self.assertEqual(unnamed_ext.files[0].size, 4096)
        self.assertEqual(unnamed_ext.files[0].quality, "2160p")

    def test_file_thumbnail_falls_back_to_parent_thumbnail(self):
        listing = normalize_terabox({"success": True, "data": {
            "thumbnail": {"url": "https://cdn.example.com/parent.jpg"},
            "files": [{"id": "A", "filename": "film.mp4", "quality": "480p",
                       "thumbnail_url": "https://cdn.example.com/file.jpg",
                       "download_url": CDN}]}})
        item = listing.files[0]
        self.assertEqual(item.thumbnail, "https://cdn.example.com/file.jpg")
        self.assertEqual(item.thumbnail_urls, ("https://cdn.example.com/file.jpg",
                                               "https://cdn.example.com/parent.jpg"))

    def test_multiple_qualities_for_one_file_are_not_counted_as_folder(self):
        listing = normalize_terabox({"success": True, "total_files": 1,
                                     "data": {"files": [
                                         {"id": "A", "filename": "film.mp4", "quality": "480p",
                                          "download_url": "https://cdn.example.com/480.mp4"},
                                         {"id": "A", "filename": "film.mp4", "quality": "720p",
                                          "download_url": "https://cdn.example.com/720.mp4"}]}})
        self.assertEqual(len(listing.files), 1)
        self.assertEqual({v.quality for v in listing.options_for(0)}, {"480p", "720p"})
        parent = normalize_terabox({"success": True, "data": {
            "quality": "1080p", "download_url": "https://cdn.example.com/1080.mp4",
            "files": [{"id": "A", "filename": "film.mp4", "quality": "480p",
                       "download_url": "https://cdn.example.com/480.mp4"}]}})
        self.assertEqual({v.quality for v in parent.options_for(0)}, {"480p", "1080p"})

    def test_folder_uses_only_returned_public_links(self):
        listing = normalize_terabox({"success": True, "total_files": 250,
                                     "data": {"name": "folder", "files": [
                                         {"id": "A", "filename": "clip.mp4", "size_bytes": 1234,
                                          "quality": "480p", "mime": "video/mp4", "stream_url": CDN},
                                         {"id": "B", "filename": "other.avi", "quality": "720p",
                                          "download_url": "http://127.0.0.1/private"}]}})
        self.assertEqual(listing.reported_total, 250)
        self.assertEqual(len(listing.files), 1)
        self.assertEqual(listing.files[0].size, 1234)
        with self.assertRaises(ValueError):
            normalize_terabox({"success": False, "data": {}})


class ModeAndFolderTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.client = MagicMock()
        self.service = DownloaderBot(self.client, MagicMock())
        self.notice = SimpleNamespace(edit=AsyncMock())
        self.event = SimpleNamespace(sender_id=7, raw_text="/change", is_private=True,
                                     respond=AsyncMock(return_value=self.notice),
                                     get_input_chat=AsyncMock(return_value="peer"))

    async def test_change_mode_rejects_other_service_links(self):
        await self.service.on_message(self.event)
        token = self.service.mode_tokens[7]
        click = SimpleNamespace(sender_id=7, is_private=True, data=f"m:{token}:terabox".encode(),
                                answer=AsyncMock(), edit=AsyncMock())
        await self.service.callback(click)
        self.assertEqual(self.service.modes[7], "terabox")
        self.event.raw_text = "https://www.youtube.com/watch?v=3SY1i5CB03Q"
        await self.service.on_message(self.event)
        self.assertIn("only Terabox", self.event.respond.call_args.args[0])
        token = self.service.mode_tokens[7] = "another"
        click.data = f"m:{token}:youtube".encode()
        await self.service.callback(click)
        self.event.raw_text = SHARE
        await self.service.on_message(self.event)
        self.assertIn("YouTube", self.event.respond.call_args.args[0])

    async def test_folder_range_then_sequential_transfer_only_available_quality(self):
        listing = TeraboxListing((
            TeraboxFile("1", "one.mp4", "480p", CDN, "", "video/mp4", 100),
            TeraboxFile("2", "two.mkv", "720p", CDN, "", "video/x-matroska", 200),
            TeraboxFile("3", "three.mp4", "480p", CDN, "", "video/mp4", 300)),
            reported_total=300)
        self.service.modes[7] = "terabox"
        self.event.raw_text = SHARE
        with patch("ytdown.bot.fetch_terabox", new=AsyncMock(return_value=listing)):
            await self.service.on_link(self.event)
        self.assertIn("3 available files", self.notice.edit.call_args.args[0])
        self.assertIn("300 files", self.notice.edit.call_args.args[0])
        self.event.raw_text = "2-3"
        await self.service.on_message(self.event)
        selection = self.service.selections[7]
        self.assertEqual((selection.start, selection.end), (2, 3))
        self.assertEqual(selection.choices, ("480p", "720p"))
        job = Job(7, "peer", 2, selection.source, ("480p", "720p"), None,
                  start=2, end=3, listing=listing)
        self.service.jobs[job.token] = job
        self.service.progress = AsyncMock()
        calls = []
        async def send(job, video, quality, url, index, total, source_file=None):
            calls.append((quality, index, total, source_file.filename))
        self.service.send_stream = send
        await self.service.run_job(job)
        self.assertEqual(calls, [("720p", 2, 3, "two.mkv"), ("480p", 3, 3, "three.mp4")])

    async def test_single_terabox_file_offers_each_returned_quality(self):
        listing = normalize_terabox({"success": True, "data": {"files": [
            {"id": "A", "filename": "film.mp4", "quality": "480p", "download_url": CDN},
            {"id": "A", "filename": "film.mp4", "quality": "720p", "download_url": CDN}]}})
        self.service.modes[7] = "terabox"
        self.event.raw_text = SHARE
        with patch("ytdown.bot.fetch_terabox", new=AsyncMock(return_value=listing)):
            await self.service.on_link(self.event)
        selection = self.service.selections[7]
        self.assertEqual(selection.choices, ("480p", "720p"))
        job = Job(7, "peer", 2, selection.source, selection.choices, None, listing=listing)
        self.service.jobs[job.token] = job
        self.service.progress = AsyncMock()
        transfers = []
        async def send(job, video, quality, url, index, total, source_file=None):
            transfers.append(quality)
        self.service.send_stream = send
        await self.service.run_job(job)
        self.assertEqual(transfers, ["480p", "720p"])

    async def test_youtube_playlist_reports_large_count_before_range_quality(self):
        self.event.raw_text = "https://www.youtube.com/playlist?list=PL12345678901"
        with patch("ytdown.bot.playlist_count", return_value=320):
            await self.service.on_link(self.event)
        self.assertIn("320 videos", self.notice.edit.call_args.args[0])
        self.event.raw_text = "200-300"
        await self.service.on_message(self.event)
        self.assertEqual((self.service.selections[7].start, self.service.selections[7].end), (200, 300))
        self.assertEqual(self.service.selections[7].choices[0], "360p")

    async def test_terabox_file_preview_falls_back_to_parent_api_url(self):
        job = Job(7, "peer", 2, Source("terabox", SHARE), ("480p",), None)
        job.thumbnail_urls = ("https://cdn.example.com/file.jpg", "https://cdn.example.com/parent.jpg")
        thumb = (b"jpeg", 320, 180)
        with patch("ytdown.bot.thumbnail_jpeg", new=AsyncMock(side_effect=[None, thumb])) as fetch:
            self.assertEqual(await self.service.video_thumbnail(job, Video(
                "Movie", {}, thumbnail=job.thumbnail_urls[0])), thumb)
        self.assertEqual([call.args[1] for call in fetch.await_args_list], list(job.thumbnail_urls))

    async def test_real_blank_api_image_falls_back_to_usable_youtube_hq(self):
        from PIL import Image, ImageDraw
        def image_bytes(blank):
            image = Image.new("RGB", (320, 180), "white" if blank else "red")
            if blank:
                ImageDraw.Draw(image).rectangle((100, 62, 220, 118), fill="red")
            data = io.BytesIO()
            image.save(data, format="JPEG", quality=80)
            return data.getvalue()
        def response(data):
            result = MagicMock()
            result.__aenter__ = AsyncMock(return_value=result)
            result.__aexit__ = AsyncMock()
            result.status, result.content_length = 200, len(data)
            result.content.read = AsyncMock(return_value=data)
            return result
        source = Source("video", "https://www.youtube.com/watch?v=3SY1i5CB03Q")
        job = Job(7, "peer", 2, source, ("480p",), None)
        session = MagicMock()
        session.get.side_effect = [response(image_bytes(True)), response(image_bytes(False))]
        self.service.http = session
        jpeg, width, height = await self.service.video_thumbnail(job, Video(
            "", {}, thumbnail="https://cdn.example.com/blank.jpg"))
        self.assertLessEqual(len(jpeg), 18 * 1024)
        self.assertEqual((width, height), (320, 180))
        self.assertEqual(session.get.call_count, 2)

    async def test_youtube_blank_api_thumbnail_falls_back_to_hq(self):
        source = Source("video", "https://www.youtube.com/watch?v=3SY1i5CB03Q")
        job = Job(7, "peer", 2, source, ("480p",), None)
        api_thumbnail = "https://cdn.example.com/white.jpg"
        with patch("ytdown.bot.thumbnail_jpeg", new=AsyncMock(
                side_effect=[None, (b"hq", 320, 180)])) as thumb:
            self.assertEqual(await self.service.video_thumbnail(job, Video(
                "", {}, thumbnail=api_thumbnail)), (b"hq", 320, 180))
        self.assertEqual([call.args[1] for call in thumb.await_args_list],
                         [api_thumbnail, "https://i.ytimg.com/vi/3SY1i5CB03Q/hqdefault.jpg"])

    async def test_api_thumbnail_is_preferred_not_youtube_fallback(self):
        source = Source("video", "https://www.youtube.com/watch?v=3SY1i5CB03Q")
        job = Job(7, "peer", 2, source, ("480p",), None)
        api_thumbnail = "https://cdn.example.com/api-thumb.jpg"
        with patch("ytdown.bot.thumbnail_jpeg", new=AsyncMock(return_value=(b"image", 320, 180))) as thumb:
            self.assertEqual(await self.service.video_thumbnail(job, Video("", {}, thumbnail=api_thumbnail)),
                             (b"image", 320, 180))
        thumb.assert_awaited_once_with(self.service.http, api_thumbnail)


class StreamMetadataTests(unittest.IsolatedAsyncioTestCase):
    async def test_terabox_mkv_keeps_container_and_size_and_duration(self):
        from ytdown.media import MediaInfo
        from tests.test_bot import GOOD_STILL, cached_still, returned_video
        service = DownloaderBot(MagicMock(), MagicMock())
        service.client.send_file = AsyncMock(return_value=returned_video(cached_still(GOOD_STILL)))
        service.progress = AsyncMock()
        job = Job(7, "peer", 2, Source("terabox", SHARE), ("1080p",), None)
        item = TeraboxFile("1", "movie.mkv", "1080p", CDN,
                           "https://cdn.example.com/cover.jpg", "video/x-matroska", 20 * 1024 * 1024)
        with patch("ytdown.bot.upload_stream", new=AsyncMock(return_value=object())) as upload, \
             patch("ytdown.bot.MediaProbe") as probe, \
             patch.object(service, "video_thumbnail", new=AsyncMock(return_value=(b"jpeg", 320, 180))):
            probe.return_value.inspect = AsyncMock(return_value=MediaInfo(120, 1920, 1080))
            await service.send_stream(job, Video(item.title, {}, 0, item.thumbnail),
                                      item.quality, item.url, 1, 1, item)
        self.assertEqual(upload.call_args.kwargs["expected_size"], item.size)
        self.assertTrue(upload.call_args.args[3].endswith(".mkv"))
        options = service.client.send_file.call_args.kwargs
        self.assertEqual(options["mime_type"], "video/x-matroska")
        self.assertTrue(options["force_document"])
        self.assertFalse(options["supports_streaming"])
        self.assertEqual(options["attributes"][0].duration, 120)
        self.assertEqual((options["attributes"][0].w, options["attributes"][0].h), (1920, 1080))
        self.assertEqual(options["thumb"].read(), b"jpeg")

    async def test_terabox_missing_api_thumbnail_uses_frame_preview(self):
        from ytdown.media import MediaInfo
        from tests.test_bot import GOOD_STILL, cached_still, returned_video
        service = DownloaderBot(MagicMock(), MagicMock())
        service.client.send_file = AsyncMock(return_value=returned_video(cached_still(GOOD_STILL)))
        service.progress = AsyncMock()
        item = TeraboxFile("1", "film.mkv", "480p", CDN, "", "video/x-matroska", 1024)
        job = Job(7, "peer", 2, Source("terabox", SHARE), ("480p",), None)
        with patch("ytdown.bot.upload_stream", new=AsyncMock(return_value=object())), \
             patch.object(service, "video_thumbnail", new=AsyncMock(return_value=(None, 0, 0))), \
             patch("ytdown.bot.MediaProbe.inspect", new=AsyncMock(return_value=MediaInfo())), \
             patch("ytdown.bot.MediaProbe.preview_jpeg", new=AsyncMock(
                 return_value=(b"frame", 320, 180))) as frame:
            await service.send_stream(job, Video("Film", {}, 0), "480p", CDN, 1, 1, item)
        frame.assert_awaited_once()
        self.assertEqual(service.client.send_file.call_args.kwargs["thumb"].read(), b"frame")

    async def test_known_api_size_allows_transfer_without_content_length(self):
        from ytdown.bot import upload_stream
        from tests.test_bot import FakeResponse
        content = b"x" * 2048
        session = MagicMock()
        session.get.return_value = FakeResponse(content, length=None)
        client = AsyncMock(return_value=True)
        await upload_stream(client, session, CDN, "movie.mkv", 4096, expected_size=len(content))
        self.assertEqual(client.await_count, 1)

    async def test_api_duration_supports_colon_delimited_time(self):
        from ytdown.core import normalize_video
        payload = {"success": True, "title": "Test", "duration": "1:02:03",
                   "thumbnail": "https://cdn.example.com/cover.jpg",
                   "downloads": [{"url": CDN, "quality": "480p", "format": "mp4", "type": "video"}]}
        video = normalize_video(payload)
        self.assertEqual(video.duration, 3723)
        self.assertEqual(video.thumbnail, payload["thumbnail"])
