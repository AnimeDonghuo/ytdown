import os
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
from yt_dlp.utils import DownloadError

from ytdown.bot import DownloaderBot, Job, MODE_LABELS, is_media_link
from ytdown.core import Source
from ytdown.dailymotion import (DailymotionError, DailymotionInfo, DMFormat, dm_thumbnail,
                                estimate_bytes, looks_like_video, media_problem, normalize_dailymotion,
                                parse_dailymotion_source, parse_master, stream_info)

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
# 9 KiB with an MP4 box header: what a real (if tiny) download leaves behind.
REAL_MP4 = b"\x00\x00\x00\x18ftypisom" + b"\x00" * (9 * 1024)
# 3.6 KB of playlist text: exactly what the generic extractor saved as "288p.mp4".
MANIFEST_TEXT = b"#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:10\n" + b"segment.ts\n" * 300


class DownloadYDL:
    """yt-dlp stand-in for the download path: every route is handled by the test."""

    options: list[dict] = []
    seen: list[tuple] = []

    def __init__(self, options, plan):
        self.options = options
        self.plan = plan
        DownloadYDL.options.append(options)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def extract_info(self, url, download=False):
        DownloadYDL.seen.append(("url", url))
        self.plan("url", url, download)
        return {"title": "t"}

    def process_ie_result(self, info, download=True):
        DownloadYDL.seen.append(("info", info))
        self.plan("info", info, download)
        return info


def write_media(out_dir, data=REAL_MP4, name="stream.mp4"):
    path = os.path.join(out_dir, name)
    with open(path, "wb") as handle:
        handle.write(data)
    return path


def run_download(plan, out_dir, manifest, **kwargs):
    """Run download_dailymotion with a yt-dlp whose every route follows `plan`."""
    DownloadYDL.options, DownloadYDL.seen = [], []
    with patch("yt_dlp.YoutubeDL", new=lambda options: DownloadYDL(options, plan)), \
         patch("ytdown.dailymotion.ffmpeg_ready", return_value=True):
        from ytdown.dailymotion import download_dailymotion
        import threading
        return download_dailymotion(manifest, out_dir, lambda done, total: None,
                                    threading.Event(), **kwargs)


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

    def fake_download(self, url, out_dir, on_bytes, stop, **kwargs):
        self.tmpdirs.append(out_dir)
        self.download_kwargs = kwargs
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

    async def test_download_gets_the_extraction_and_its_format_details(self):
        service = self.make_service()
        fmt = DMFormat("720p", "https://cdn.example.com/720.m3u8?sec=1", 1280, 720, 2500000,
                       "m3u8_native", "hls-720")
        job = self.job(service, DailymotionInfo("Clip", 87, "", (fmt,), {"id": "x7tgad0"}))
        with patch("ytdown.bot.download_dailymotion", new=self.download), \
             patch("ytdown.bot.ffmpeg_ready", return_value=True), \
             patch("ytdown.bot.thumbnail_jpeg", new=AsyncMock(return_value=None)):
            await service.run_job(job)
        self.assertEqual(self.download_kwargs["media"], {"id": "x7tgad0"})
        self.assertEqual(self.download_kwargs["protocol"], "m3u8_native")
        self.assertEqual(self.download_kwargs["format_id"], "hls-720")
        self.assertEqual(self.download_kwargs["duration"], 87)
        self.assertEqual(self.download_kwargs["title"], "Clip")
        self.assertEqual((self.download_kwargs["watch_url"], self.download_kwargs["height"]),
                         (WATCH, 720))

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
        def refused(url, out_dir, on_bytes, stop, **kwargs):
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
        fallback = MagicMock(side_effect=DailymotionError("no yt-dlp here"))
        with patch("ytdown.bot.fetch_json_api", new=api_mock), \
             patch("ytdown.bot.extract_dailymotion", new=fallback), \
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
        # The API reported no poster, so Dailymotion's own thumbnail endpoint is used.
        self.assertEqual(info.poster, "https://www.dailymotion.com/thumbnail/video/x7tgad0")
        self.assertIsNone(info.media)  # Nothing was extracted on this path.

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


class FakeYDL:
    """Stand-in for yt_dlp.YoutubeDL: records options, returns canned info."""

    calls = []

    def __init__(self, options):
        self.options = options
        FakeYDL.calls.append(options)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def extract_info(self, url, download=False):
        raise AssertionError("not configured")


def ytdlp_patch(info=None, error=None, on_watch=None):
    """Patch yt_dlp.YoutubeDL with a handler for a single extraction."""
    def factory(options):
        ydl = FakeYDL(options)

        def extract_info(url, download=False):
            if error is not None and (on_watch is None or not on_watch(url)):
                raise error
            if on_watch is not None and on_watch(url) and download:
                on_watch(url, options)
            if info is None:
                raise AssertionError("no info configured")
            return info
        ydl.extract_info = extract_info
        return ydl
    return factory


class DailymotionBypassTests(unittest.IsolatedAsyncioTestCase):
    """A refused Worker playlist must not end the job: read Dailymotion directly."""

    YTDLP_INFO = {
        "title": "Straight from Dailymotion",
        "duration": 99,
        "thumbnail": "https://s1.dmcdn.net/v/poster.jpg",
        "formats": [
            {"protocol": "m3u8_native", "height": 720, "width": 1280, "tbr": 2500.0,
             "manifest_url": "https://cdn.example.com/720.m3u8?sec=2"},
            {"protocol": "https", "height": 480, "width": 854, "tbr": 900.0, "ext": "mp4",
             "url": "https://cdn.example.com/480.mp4"},
            {"protocol": "https", "height": 360, "width": 640, "tbr": 500.0, "ext": "mp4",
             "url": "https://cdn.example.com/360.mp4"},
            {"protocol": "http_dash_segments", "height": 1080, "tbr": 5000.0,
             "url": "https://cdn.example.com/dash"},
            {"protocol": "m3u8_native", "vcodec": "none", "height": 480, "tbr": 100.0,
             "manifest_url": "https://cdn.example.com/audio.m3u8"},
            {"protocol": "https", "height": None, "tbr": 10.0, "ext": "mp4",
             "url": "https://cdn.example.com/unknown.mp4"},
        ],
    }

    def test_extract_maps_yt_dlp_formats(self):
        from ytdown.dailymotion import extract_dailymotion
        FakeYDL.calls = []
        with patch("yt_dlp.YoutubeDL", new=ytdlp_patch(info=self.YTDLP_INFO)):
            info = extract_dailymotion(WATCH, proxy="http://1.2.3.4:8080")
        self.assertEqual([f.label for f in info.formats], ["720p", "480p", "360p"])
        self.assertEqual(info.formats[0].url, "https://cdn.example.com/720.m3u8?sec=2")
        self.assertEqual(info.formats[1].url, "https://cdn.example.com/480.mp4")
        self.assertEqual(info.formats[0].height, 720)
        self.assertEqual(info.formats[0].bandwidth, 2_500_000)
        self.assertEqual((info.title, info.duration), ("Straight from Dailymotion", 99))
        self.assertTrue(info.poster.endswith("poster.jpg"))
        self.assertEqual(FakeYDL.calls[0]["proxy"], "http://1.2.3.4:8080")
        self.assertTrue(FakeYDL.calls[0]["noplaylist"])

    def test_extract_keeps_the_extraction_for_the_download(self):
        from ytdown.dailymotion import extract_dailymotion
        info = dict(self.YTDLP_INFO, thumbnails=[{"url": "https://s1.dmcdn.net/v/poster.jpg", "height": 720}])
        info.pop("thumbnail", None)
        with patch("yt_dlp.YoutubeDL", new=ytdlp_patch(info=info)):
            result = extract_dailymotion(WATCH)
        self.assertTrue(result.poster.endswith("poster.jpg"))  # thumbnails, not "thumbnail"
        self.assertIsInstance(result.media, dict)  # The download reuses this extraction.
        self.assertEqual(result.formats[0].protocol, "m3u8_native")

    def test_extract_reports_a_yt_dlp_failure(self):
        from ytdown.dailymotion import extract_dailymotion
        with patch("yt_dlp.YoutubeDL", new=ytdlp_patch(error=DownloadError(
                "ERROR: [dailymotion] x7tgad0: Unable to download webpage HTTP Error 403"))):
            with self.assertRaisesRegex(DailymotionError, "Unable to download webpage"):
                extract_dailymotion(WATCH)

    async def test_fetch_uses_the_bypass_when_both_playlists_are_refused(self):
        from ytdown.bot import fetch_dailymotion
        from ytdown.dailymotion import DailymotionInfo
        api = {"success": True, "title": "API title", "duration": 99, "poster": "",
               "master_m3u8": "https://cdndirector.example.com/m.m3u8",
               "proxy_m3u8": "https://worker.example.workers.dev/api/m3u8?url=x"}
        fresh = DailymotionInfo("Bypass title", 99, "", (FORMAT_720,))
        refused = aiohttp.ClientResponseError(request_info=MagicMock(), history=(), status=403)
        with patch("ytdown.bot.fetch_json_api", new=AsyncMock(return_value=api)), \
             patch("ytdown.bot.read_bounded", new=AsyncMock(side_effect=[refused, refused])), \
             patch("ytdown.bot.extract_dailymotion", new=MagicMock(return_value=fresh)) as bypass:
            info = await fetch_dailymotion(MagicMock(), WATCH, proxy="http://1.2.3.4:8080")
        self.assertEqual([f.label for f in info.formats], ["720p"])
        self.assertEqual(info.title, "API title")  # Metadata still comes from the API.
        self.assertEqual(bypass.call_args.args, (WATCH, "http://1.2.3.4:8080"))

    async def test_fetch_uses_the_bypass_when_the_api_itself_fails(self):
        from ytdown.bot import fetch_dailymotion
        from ytdown.dailymotion import DailymotionInfo
        fresh = DailymotionInfo("Bypass title", 42, "", (FORMAT_720,))
        failure = aiohttp.ClientResponseError(request_info=MagicMock(), history=(), status=500)
        with patch("ytdown.bot.fetch_json_api", new=AsyncMock(side_effect=failure)), \
             patch("ytdown.bot.extract_dailymotion", new=MagicMock(return_value=fresh)) as bypass:
            info = await fetch_dailymotion(MagicMock(), WATCH)
        self.assertEqual((info.title, info.duration), ("Bypass title", 42))
        self.assertEqual(bypass.call_args.args, (WATCH, None))

    async def test_fetch_reports_the_api_reason_when_every_route_fails(self):
        from ytdown.bot import fetch_dailymotion
        refused = aiohttp.ClientResponseError(request_info=MagicMock(), history=(), status=403)
        with patch("ytdown.bot.fetch_json_api", new=AsyncMock(side_effect=refused)), \
             patch("ytdown.bot.extract_dailymotion",
                   new=MagicMock(side_effect=DailymotionError("Dailymotion refused this video"))):
            with self.assertRaisesRegex(DailymotionError, "HTTP 403"):
                await fetch_dailymotion(MagicMock(), WATCH)

    def test_download_repairs_a_refused_manifest_from_the_watch_url(self):
        out_dir = tempfile.mkdtemp()

        def plan(kind, target, download):
            if kind == "url" and target == WATCH:
                write_media(out_dir)
            else:
                raise DownloadError("ERROR: unable to download video data: HTTP Error 403")

        path = run_download(plan, out_dir, manifest="https://cdn.example.com/720.m3u8?sec=expired",
                            watch_url=WATCH, height=720, proxy="http://1.2.3.4:8080")
        self.assertTrue(path.endswith("stream.mp4"))
        self.assertEqual(DownloadYDL.options[1]["format"], "best[height=720]/best[height<=720]/best")
        self.assertEqual(DownloadYDL.options[1]["proxy"], "http://1.2.3.4:8080")


class DailymotionPosterTests(unittest.TestCase):
    """A cover is sent even when the Worker API reports no poster."""

    def test_known_poster_wins_and_the_endpoint_is_the_fallback(self):
        from ytdown.bot import dailymotion_poster
        self.assertEqual(dailymotion_poster(WATCH, "https://s2.dmcdn.net/v/x1080"),
                         "https://s2.dmcdn.net/v/x1080")
        self.assertEqual(dailymotion_poster(WATCH),
                         "https://www.dailymotion.com/thumbnail/video/x7tgad0")
        self.assertEqual(dailymotion_poster("https://www.dailymotion.com/video/" + "x" * 3000), "")

    def test_extraction_thumbnails_are_read(self):
        info = {"title": "Clip", "formats": [], "thumbnails": [
            {"url": "https://s1.dmcdn.net/small.jpg", "height": 240},
            {"url": "https://s1.dmcdn.net/big.jpg", "height": 720}]}
        self.assertEqual(dm_thumbnail(info), "https://s1.dmcdn.net/big.jpg")
        self.assertEqual(dm_thumbnail({"thumbnail": "https://s1.dmcdn.net/one.jpg"}), "https://s1.dmcdn.net/one.jpg")
        self.assertEqual(dm_thumbnail({}), "")


class DailymotionDownloadTests(unittest.TestCase):
    """What actually lands on disk: a real container, or an error."""

    MANIFEST_URL = "https://worker.example.workers.dev/api/m3u8?url=x7tgad0"

    def test_the_playlist_url_is_described_to_yt_dlp_instead_of_guessed(self):
        out_dir = tempfile.mkdtemp()
        seen = []

        def plan(kind, target, download):
            seen.append((kind, target))
            write_media(out_dir)

        path = run_download(plan, out_dir, manifest=self.MANIFEST_URL)
        self.assertTrue(path.endswith("stream.mp4"))
        kind, described = seen[0]
        self.assertEqual(kind, "info")  # Never the generic extractor on a media URL.
        media = described["formats"][0]
        self.assertEqual(media["protocol"], "m3u8_native")
        self.assertEqual(media["url"], self.MANIFEST_URL)
        self.assertEqual(media["http_headers"]["Referer"], "https://www.dailymotion.com/")

    def test_stream_info_marks_hls_and_keeps_the_url(self):
        info = stream_info(self.MANIFEST_URL, height=720, duration=87)
        media = info["formats"][0]
        self.assertEqual(media["protocol"], "m3u8_native")
        self.assertEqual(media["url"], self.MANIFEST_URL)
        self.assertEqual(media["height"], 720)
        self.assertEqual(media["http_headers"]["Referer"], "https://www.dailymotion.com/")
        self.assertEqual(info["duration"], 87)
        plain = stream_info("https://cdn.example.com/v/720.mp4")
        self.assertEqual(plain["formats"][0]["protocol"], "https")

    def test_a_manifest_saved_as_mp4_is_never_returned(self):
        out_dir = tempfile.mkdtemp()

        def plan(kind, target, download):
            write_media(out_dir, data=MANIFEST_TEXT)  # What the generic extractor saved.

        with self.assertRaises(DailymotionError) as caught:
            run_download(plan, out_dir, manifest=self.MANIFEST_URL)
        self.assertIn("is not a video file", str(caught.exception))

    def test_the_watch_page_rescues_a_garbage_playlist_download(self):
        out_dir = tempfile.mkdtemp()

        def plan(kind, target, download):
            write_media(out_dir, data=REAL_MP4 if target == WATCH else MANIFEST_TEXT)

        path = run_download(plan, out_dir, manifest=self.MANIFEST_URL, watch_url=WATCH, height=288)
        self.assertTrue(path.endswith("stream.mp4"))
        self.assertEqual(os.path.getsize(path), len(REAL_MP4))
        self.assertEqual(DownloadYDL.options[1]["format"], "best[height=288]/best[height<=288]/best")

    def test_a_truncated_file_for_a_long_video_is_refused(self):
        out_dir = tempfile.mkdtemp()

        def plan(kind, target, download):
            write_media(out_dir)  # 9 KB of MP4, but the video is 7 minutes long.

        with self.assertRaises(DailymotionError) as caught:
            run_download(plan, out_dir, manifest=self.MANIFEST_URL, duration=420)
        self.assertIn("for a 420s video", str(caught.exception))

    def test_the_extracted_stream_is_tried_first_with_its_own_format(self):
        out_dir = tempfile.mkdtemp()
        media = {"id": "x7tgad0", "title": "Clip",
                 "formats": [{"url": "https://cdn.example.com/720.m3u8?sec=2",
                              "protocol": "m3u8_native", "ext": "mp4", "format_id": "hls-720"}]}

        def plan(kind, target, download):
            write_media(out_dir)

        path = run_download(plan, out_dir, manifest=self.MANIFEST_URL, media=media,
                            format_id="hls-720", height=720)
        self.assertTrue(path.endswith("stream.mp4"))
        self.assertEqual(DownloadYDL.seen[0], ("info", media))
        self.assertEqual(DownloadYDL.options[0]["format"],
                         "hls-720/best[height=720]/best[height<=720]/best")

    def test_every_route_failing_names_why(self):
        out_dir = tempfile.mkdtemp()

        def plan(kind, target, download):
            raise DownloadError("ERROR: 403 Forbidden")

        with self.assertRaises(DailymotionError) as caught:
            run_download(plan, out_dir, manifest=self.MANIFEST_URL, watch_url=WATCH)
        message = str(caught.exception)
        self.assertIn("the playlist URL", message)
        self.assertIn("the watch page: ERROR: 403 Forbidden", message)

    def test_media_problem_reads_the_container(self):
        out_dir = tempfile.mkdtemp()
        good = os.path.join(out_dir, "good.mp4")
        with open(good, "wb") as handle:
            handle.write(REAL_MP4)
        self.assertEqual(media_problem(good, 3), "")
        small = os.path.join(out_dir, "small.mp4")
        tiny = b"\x00\x00\x00\x18ftypisom" + b"x" * 100  # Right magic, far too small.
        with open(small, "wb") as handle:
            handle.write(tiny)
        self.assertEqual(media_problem(small), f"only {len(tiny)} bytes came back")
        text_file = os.path.join(out_dir, "manifest.mp4")
        with open(text_file, "wb") as handle:
            handle.write(MANIFEST_TEXT)
        self.assertIn("is not a video file", media_problem(text_file, 420))
        self.assertIn("no file", media_problem(os.path.join(out_dir, "missing.mp4")))
        self.assertTrue(looks_like_video(b"\x1aE\xdf\xa3\x01\x00"))  # WebM
        self.assertTrue(looks_like_video(b"\x47\x40\x00\x10"))  # MPEG-TS
        self.assertFalse(looks_like_video(b"<!DOCTYPE html>"))


if __name__ == "__main__":
    unittest.main()
