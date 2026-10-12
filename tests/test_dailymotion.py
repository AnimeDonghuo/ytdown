import os
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
from yt_dlp.utils import DownloadError

from ytdown.bot import DownloaderBot, Job, MODE_LABELS, is_media_link
from ytdown.core import Source
from ytdown.dailymotion import (DailymotionError, DailymotionInfo, DMFormat, estimate_bytes,
                                normalize_dailymotion, parse_dailymotion_source, parse_master)

MASTER = """#EXTM3U
#EXT-X-STREAM-INF:BANDWIDTH=900000,RESOLUTION=640x360,CODECS="avc1.64001f,mp4a.40.2"
https://cdn.example.com/a/360.m3u8?sec=1
#EXT-X-STREAM-INF:BANDWIDTH=2500000,RESOLUTION=1280x720
720/index.m3u8
#EXT-X-STREAM-INF:BANDWIDTH=2400000,RESOLUTION=1280x720
720-low/index.m3u8
#EXT-X-STREAM-INF:BANDWIDTH=100,RESOLUTION=bad
http://insecure.example.com/x.m3u8
"""
WATCH = "https://www.dailymotion.com/video/x7tgad0"
FORMAT_720 = DMFormat("720p", "https://cdn.example.com/720/index.m3u8", 1280, 720, 2500000)


class ParsingTests(unittest.TestCase):
    def test_accepts_common_dailymotion_forms(self):
        for link in ("https://dai.ly/x7tgad0",
                     "https://www.dailymotion.com/video/x7tgad0?playlist=x1abcde",
                     "https://dailymotion.com/embed/video/x7tgad0",
                     "https://geo.dailymotion.com/player/xhm3k.html?video=x7tgad0"):
            self.assertEqual(parse_dailymotion_source(link), WATCH, link)

    def test_accepts_newer_case_sensitive_ids(self):
        self.assertEqual(parse_dailymotion_source("https://dai.ly/k5wsnkZKZa86T2LnlRY"),
                         "https://www.dailymotion.com/video/k5wsnkZKZa86T2LnlRY")
        self.assertEqual(parse_dailymotion_source("https://dailymotion.com/video/k5oZV9z1IBqgOEKNzhI"),
                         "https://www.dailymotion.com/video/k5oZV9z1IBqgOEKNzhI")

    def test_rejects_other_links(self):
        for link in ("https://www.youtube.com/watch?v=3SY1i5CB03Q",
                     "http://www.dailymotion.com/video/x7tgad0",
                     "https://www.dailymotion.com/playlist/x6abcde",
                     "https://www.dailymotion.com/user/someone",
                     "https://evil.example.com/video/x7tgad0",
                     "https://localhost/video/x7tgad0",
                     "https://www.dailymotion.com/video/" + "x" * 3000):
            with self.assertRaises(ValueError, msg=link):
                parse_dailymotion_source(link)

    def test_media_link_detection_includes_dailymotion(self):
        self.assertTrue(is_media_link("https://dai.ly/x7tgad0"))
        self.assertFalse(is_media_link("https://example.com/not-media"))
        self.assertEqual(MODE_LABELS["dailymotion"], "Dailymotion")

    def test_normalize_api_response(self):
        title, duration, poster, manifest = normalize_dailymotion({
            "success": True, "title": " Midnight Sun ", "duration": "87",
            "poster": "https://s2.dmcdn.net/v/x/x1080", "master_m3u8": "https://cdn.example.com/m.m3u8"})
        self.assertEqual((title, duration, poster, manifest),
                         ("Midnight Sun", 87, "https://s2.dmcdn.net/v/x/x1080", "https://cdn.example.com/m.m3u8"))
        _, duration, poster, _ = normalize_dailymotion({
            "success": True, "master_m3u8": "https://cdn.example.com/m.m3u8", "duration": "oops",
            "poster": "http://insecure.example.com/p.jpg"})
        self.assertEqual((duration, poster), (0, ""))

    def test_normalize_failures_are_user_facing(self):
        with self.assertRaisesRegex(DailymotionError, "private video"):
            normalize_dailymotion({"success": False, "error": "private video"})
        with self.assertRaises(DailymotionError):
            normalize_dailymotion({"success": True, "master_m3u8": "http://cdn.example.com/m.m3u8"})
        self.assertIsInstance(DailymotionError("x"), ValueError)

    def test_master_playlist_keeps_best_bandwidth_per_height(self):
        formats = parse_master(MASTER, "https://cdn.example.com/master.m3u8")
        self.assertEqual([(f.label, f.width, f.height, f.bandwidth) for f in formats],
                         [("720p", 1280, 720, 2500000), ("360p", 640, 360, 900000)])
        self.assertEqual(formats[0].url, "https://cdn.example.com/720/index.m3u8")
        self.assertEqual(formats[1].url, "https://cdn.example.com/a/360.m3u8?sec=1")

    def test_size_estimate_uses_peak_bandwidth(self):
        self.assertEqual(estimate_bytes(FORMAT_720, 80), 2500000 // 8 * 80)
        self.assertEqual(estimate_bytes(DMFormat("360p", "https://x.example.com/a", 0, 360, 0), 80), 0)


class BotLookupTests(unittest.IsolatedAsyncioTestCase):
    def make_service(self):
        client = MagicMock()
        service = DownloaderBot(client, MagicMock())
        service.progress = AsyncMock()
        return service

    async def test_lookup_offers_qualities_lowest_first(self):
        service = self.make_service()
        service.modes[7] = "dailymotion"
        info = DailymotionInfo("Midnight Sun", 87, "", (FORMAT_720, DMFormat("360p", "https://c.example.com/a", 640, 360, 900000)))
        notice = SimpleNamespace(edit=AsyncMock())
        event = SimpleNamespace(sender_id=7, raw_text="https://dai.ly/x7tgad0",
                                get_input_chat=AsyncMock(return_value="peer"),
                                respond=AsyncMock(return_value=notice))
        with patch("ytdown.bot.fetch_dailymotion", new=AsyncMock(return_value=info)) as fetch:
            await service.on_link(event)
        fetch.assert_awaited_once()
        selection = service.selections[7]
        self.assertEqual(selection.source, Source("dailymotion", WATCH))
        self.assertEqual(selection.choices, ("360p", "720p"))
        self.assertIn("Midnight Sun", notice.edit.call_args.args[0])

    async def test_lookup_failure_shows_the_reason(self):
        service = self.make_service()
        service.modes[7] = "dailymotion"
        notice = SimpleNamespace(edit=AsyncMock())
        event = SimpleNamespace(sender_id=7, raw_text="https://dai.ly/x7tgad0",
                                get_input_chat=AsyncMock(return_value="peer"),
                                respond=AsyncMock(return_value=notice))
        with patch("ytdown.bot.fetch_dailymotion", new=AsyncMock(side_effect=DailymotionError("Private video."))):
            await service.on_link(event)
        notice.edit.assert_awaited_once_with("Private video.")
        self.assertNotIn(7, service.selections)

    async def test_mode_button_selects_dailymotion(self):
        service = self.make_service()
        service.mode_tokens[7] = "tok1"
        event = SimpleNamespace(data=b"m:tok1:dailymotion", sender_id=7, is_private=True,
                                answer=AsyncMock(), edit=AsyncMock())
        await service.callback(event)
        self.assertEqual(service.modes[7], "dailymotion")
        self.assertIn("Dailymotion mode is active", event.edit.call_args.args[0])


class BotDownloadTests(unittest.IsolatedAsyncioTestCase):
    def make_service(self, download_side_effect=None):
        client = MagicMock()
        client.send_file = AsyncMock(side_effect=self.record_send)
        service = DownloaderBot(client, MagicMock())
        service.progress = AsyncMock()
        self.sent = []
        self.tmpdirs = []
        self.download = MagicMock(side_effect=download_side_effect or self.fake_download)  # Runs in a thread.
        return service

    def record_send(self, peer, path, **kwargs):
        self.sent.append((os.path.basename(path), os.path.exists(path), kwargs))
        return SimpleNamespace(id=1)

    def fake_download(self, url, out_dir, on_bytes, stop):
        self.tmpdirs.append(out_dir)
        path = os.path.join(out_dir, "stream.mp4")
        with open(path, "wb") as handle:
            handle.write(b"x" * 1000)
        on_bytes(1000, 1000)
        return path

    def job(self, service, info=None, selected=("720p",)):
        info = info or DailymotionInfo("Midnight Sun | Iceland", 87, "", (FORMAT_720,))
        job = Job(7, "peer", 2, Source("dailymotion", WATCH), selected, info)
        service.jobs[job.token] = job
        return job

    async def test_sends_streamable_mp4_and_removes_temp_files(self):
        service = self.make_service()
        job = self.job(service)
        with patch("ytdown.bot.download_dailymotion", new=self.download), \
             patch("ytdown.bot.ffmpeg_ready", return_value=True), \
             patch("ytdown.bot.thumbnail_jpeg", new=AsyncMock(return_value=None)):
            await service.run_job(job)
        self.assertEqual(len(self.sent), 1)
        name, existed, kwargs = self.sent[0]
        self.assertTrue(existed)
        self.assertEqual(name, "Midnight Sun _ Iceland [720p].mp4")
        self.assertTrue(kwargs["supports_streaming"])
        self.assertEqual(kwargs["caption"], "Midnight Sun | Iceland [720p]")
        attributes = kwargs["attributes"][0]
        self.assertEqual((attributes.w, attributes.h, attributes.duration, attributes.supports_streaming),
                         (1280, 720, 87, True))
        self.assertFalse(os.path.exists(self.tmpdirs[0]))
        self.assertIn("Sent 1", service.progress.call_args.args[1])

    async def test_cover_is_sent_when_available(self):
        service = self.make_service()
        job = self.job(service, DailymotionInfo("Clip", 10, "https://s2.dmcdn.net/v/x/x1080", (FORMAT_720,)))
        cover = (b"\xff\xd8jpeg", 320, 180)
        with patch("ytdown.bot.download_dailymotion", new=self.download), \
             patch("ytdown.bot.ffmpeg_ready", return_value=True), \
             patch("ytdown.bot.thumbnail_jpeg", new=AsyncMock(return_value=cover)):
            await service.run_job(job)
        thumb = self.sent[0][2]["thumb"]
        self.assertEqual(thumb.getvalue(), b"\xff\xd8jpeg")

    async def test_oversize_quality_is_refused_before_download(self):
        service = self.make_service()
        service.max_bytes = 1000
        job = self.job(service)
        with patch("ytdown.bot.download_dailymotion", new=self.download), \
             patch("ytdown.bot.ffmpeg_ready", return_value=True):
            await service.run_job(job)
        self.download.assert_not_called()
        self.assertEqual(self.sent, [])
        self.assertIn("over size limit 1", service.progress.call_args.args[1])

    async def test_missing_ffmpeg_fails_the_job_with_a_clear_message(self):
        service = self.make_service()
        job = self.job(service)
        with patch("ytdown.bot.download_dailymotion", new=self.download), \
             patch("ytdown.bot.ffmpeg_ready", return_value=False):
            await service.run_job(job)
        self.download.assert_not_called()
        self.assertIn("need ffmpeg", service.progress.call_args.args[1])

    async def test_download_error_reports_status_without_url(self):
        def refused(url, out_dir, on_bytes, stop):
            raise DownloadError("ERROR: unable to download video data: HTTP Error 403: Forbidden "
                                "https://cdn.example.com/720/index.m3u8?sec=SECRET")
        service = self.make_service(download_side_effect=refused)
        job = self.job(service)
        with patch("ytdown.bot.download_dailymotion", new=self.download), \
             patch("ytdown.bot.ffmpeg_ready", return_value=True):
            await service.run_job(job)
        self.assertEqual(self.sent, [])
        text = service.progress.call_args.args[1]
        self.assertIn("failed 1 (720p: HTTP Error 403)", text)
        self.assertNotIn("SECRET", text)
        self.assertIn("failed 1", text)


class DailymotionFallbackTests(unittest.IsolatedAsyncioTestCase):
    API = {"success": True, "title": "Clip", "duration": 10, "poster": "",
           "master_m3u8": "https://cdndirector.example.com/m.m3u8",
           "proxy_m3u8": "https://worker.example.workers.dev/api/m3u8?url=x"}

    @staticmethod
    def status_error(status):
        return aiohttp.ClientResponseError(request_info=MagicMock(), history=(), status=status)

    async def fetch(self, *reads, api=None, api_error=None):
        from ytdown.bot import fetch_dailymotion
        api_mock = AsyncMock(side_effect=api_error) if api_error else AsyncMock(return_value=api or self.API)
        with patch("ytdown.bot.fetch_json_api", new=api_mock), \
             patch("ytdown.bot.read_bounded", new=AsyncMock(side_effect=list(reads))) as reader:
            info = await fetch_dailymotion(MagicMock(), WATCH)
        return info, reader

    async def test_refused_cdn_playlist_falls_back_to_worker_proxy(self):
        info, reader = await self.fetch(self.status_error(403), MASTER.encode())
        self.assertEqual([f.label for f in info.formats], ["720p", "360p"])
        self.assertEqual(reader.await_args_list[1].args[1], self.API["proxy_m3u8"])

    async def test_cdn_playlist_is_used_when_it_works(self):
        info, reader = await self.fetch(MASTER.encode())
        self.assertEqual(reader.await_count, 1)
        self.assertEqual(info.title, "Clip")

    async def test_both_refused_names_the_status(self):
        with self.assertRaisesRegex(DailymotionError, "HTTP 403"):
            await self.fetch(self.status_error(403), self.status_error(403))

    async def test_api_status_is_reported(self):
        with self.assertRaisesRegex(DailymotionError, "HTTP 429"):
            await self.fetch(api_error=self.status_error(429))

    async def test_redirected_playlist_is_not_read_as_empty(self):
        from ytdown.dailymotion import read_bounded
        response = SimpleNamespace(status=302, content_length=None, request_info=MagicMock(), history=(),
                                   raise_for_status=lambda: None)
        context = MagicMock()
        context.__aenter__ = AsyncMock(return_value=response)
        context.__aexit__ = AsyncMock(return_value=False)
        session = MagicMock()
        session.get = MagicMock(return_value=context)
        with self.assertRaises(aiohttp.ClientResponseError) as caught:
            await read_bounded(session, "https://cdndirector.example.com/m.m3u8", 1000)
        self.assertEqual(caught.exception.status, 302)


if __name__ == "__main__":
    unittest.main()
