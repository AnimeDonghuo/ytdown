import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp

from ytdown import bot as bot_module
from ytdown.bot import DEFAULT_APIS, DownloaderBot, Job, fetch_rumble, is_media_link
from ytdown.core import Source
from ytdown.rumble import RUMBLE_API_URL, RumbleError, normalize_rumble, parse_rumble_source

LINK = "https://rumble.com/v4t3sz8-playing-some-fallout-76-sleep-token-day.html"
THUMB = "https://hugh.cdn.rumble.cloud/video/s8/1/w/0/9/K/w09Kr.qR4e-small-Playing-some-Fallout-76-Sle.jpg"


def stream(quality, height, real, bandwidth, name):
    return {"quality": quality, "height": height, "width": 1920, "real_height": real,
            "resolution": f"{real}p", "bandwidth_kbps": bandwidth, "type": "video", "format": "mp4",
            "url": f"https://hugh.cdn.rumble.cloud/video/s8/2/w/0/9/K/w09Kr.{name}.rec.mp4"}


# Captured from the live Rumble API (embed v4t3sz8), trimmed to the fields the bot reads.
PAYLOAD = {
    "success": True, "status": True, "embed_id": "v4t3sz8",
    "title": "Playing some Fallout 76| Sleep Token Day", "thumbnail": THUMB, "duration": 10821,
    "hls_master_playlist": None, "audio_stream_link": None,
    "streams": [
        stream("1081p", 1081, 1080, 12043, "aaa"),
        stream("1080p", 1080, 1080, 3994, "haa"),
        stream("720p", 720, 720, 2066, "gaa"),
        stream("480p", 480, 480, 1007, "caa"),
        stream("360p", 360, 360, 639, "baa"),
        {**stream("240p", 240, 360, 203, "oaa"), "width": 640},
    ],
}


class RumbleLinkTests(unittest.TestCase):
    def test_accepts_watch_pages_embeds_and_bare_hosts(self):
        self.assertEqual(parse_rumble_source(LINK), LINK)
        self.assertEqual(parse_rumble_source("rumble.com/v33gu8--missing9-ep02.html"),
                         "https://rumble.com/v33gu8--missing9-ep02.html")
        self.assertEqual(parse_rumble_source("http://www.rumble.com/v2dnuqg-america-first-ep.-1132.html?x=1"),
                         "https://rumble.com/v2dnuqg-america-first-ep.-1132.html")
        self.assertEqual(parse_rumble_source("https://rumble.com/embed/v4t3sz8/"), "https://rumble.com/embed/v4t3sz8/")

    def test_rejects_other_pages_and_hosts(self):
        for bad in ("https://example.com/v4t3sz8.html", "https://rumble.com/c/SomeChannel",
                    "https://rumble.com/user/someone", "https://rumble.com/search/video?q=x",
                    "https://rumble.com/v4t3sz8", "https://user:pass@rumble.com/v4t3sz8-a.html",
                    "https://rumble.com.evil.example/v4t3sz8-a.html", "ftp://rumble.com/v4t3sz8-a.html",
                    "https://rumble.com/v4t3sz8-a.html extra", ""):
            with self.subTest(bad=bad), self.assertRaises(RumbleError):
                parse_rumble_source(bad)

    def test_media_link_detection_includes_rumble(self):
        self.assertTrue(is_media_link(LINK))
        self.assertFalse(is_media_link("https://rumble.com/c/SomeChannel"))


class RumbleNormalizeTests(unittest.TestCase):
    def test_qualities_are_labelled_by_real_height_and_keep_exact_matches(self):
        video = normalize_rumble(PAYLOAD)
        self.assertEqual(sorted(video.streams, key=lambda q: int(q[:-1])), ["360p", "480p", "720p", "1080p"])
        self.assertIn("haa.rec.mp4", video.streams["1080p"])   # the exact 1080 line, not the padded 1081 one
        self.assertIn("baa.rec.mp4", video.streams["360p"])    # the exact 360 line, not the 240-labelled copy
        self.assertEqual(video.title, "Playing some Fallout 76| Sleep Token Day")
        self.assertEqual(video.duration, 10821)
        self.assertEqual(video.thumbnail, THUMB)

    def test_error_answers_and_empty_results_raise(self):
        with self.assertRaisesRegex(RumbleError, "Video is private"):
            normalize_rumble({"success": False, "error": "Video is private"})
        with self.assertRaisesRegex(RumbleError, "could not find"):
            normalize_rumble({"success": False})
        with self.assertRaisesRegex(RumbleError, "no MP4"):
            normalize_rumble({"success": True, "streams": [{**stream("720p", 720, 720, 1, "x"), "format": "webm"}]})
        with self.assertRaisesRegex(RumbleError, "no MP4"):
            normalize_rumble({"success": True, "streams": "not-a-list"})
        with self.assertRaises(RumbleError):
            normalize_rumble(["not", "a", "dict"])

    def test_unsafe_urls_and_bad_heights_are_dropped(self):
        payload = {"success": True, "title": "", "embed_id": "v4t3sz8", "thumbnail": "http://insecure/x.jpg",
                   "duration": -5, "streams": [
                       {**stream("720p", 720, 720, 1, "ok"), "url": "http://insecure/720.mp4"},
                       {**stream("x", 10, 10, 1, "tiny")},
                       {**stream("480p", 480, 480, 1, "good"), "real_height": True},
                       {**stream("360p", 360, 360, 1, "bad"), "type": "audio"}]}
        video = normalize_rumble(payload)
        self.assertEqual(set(video.streams), {"480p"})
        self.assertEqual(video.title, "Rumble video v4t3sz8")
        self.assertEqual((video.duration, video.thumbnail), (0, ""))


class RumbleApiTests(unittest.IsolatedAsyncioTestCase):
    def status_error(self, status):
        return aiohttp.ClientResponseError(request_info=MagicMock(), history=(), status=status)

    async def test_default_endpoint_is_the_rumble_worker(self):
        self.assertEqual(RUMBLE_API_URL, "https://samra-rumble-api.krishnalucky193.workers.dev/")
        self.assertEqual(DEFAULT_APIS["rumble"], RUMBLE_API_URL)

    async def test_transient_api_errors_are_retried_then_succeed(self):
        with patch("ytdown.bot.fetch_json_api", new=AsyncMock(side_effect=[self.status_error(429), PAYLOAD])) as api, \
             patch("ytdown.bot.API_RETRY_DELAYS", [0, 0, 0]):
            video = await fetch_rumble(MagicMock(), LINK, "https://api.example.com/")
        self.assertEqual(api.await_count, 2)
        self.assertIn("1080p", video.streams)

    async def test_permanent_status_is_reported_without_retry_storm(self):
        with patch("ytdown.bot.fetch_json_api", new=AsyncMock(side_effect=self.status_error(403))) as api:
            with self.assertRaisesRegex(RumbleError, "HTTP 403"):
                await fetch_rumble(MagicMock(), LINK)
        self.assertEqual(api.await_count, 1)

    async def test_persistent_server_error_gives_up_with_status(self):
        with patch("ytdown.bot.fetch_json_api", new=AsyncMock(side_effect=self.status_error(500))) as api, \
             patch("ytdown.bot.API_RETRY_DELAYS", [0, 0]):
            with self.assertRaisesRegex(RumbleError, "HTTP 500"):
                await fetch_rumble(MagicMock(), LINK)
        self.assertEqual(api.await_count, 3)

    async def test_unreachable_api_is_a_friendly_error(self):
        with patch("ytdown.bot.fetch_json_api", new=AsyncMock(side_effect=aiohttp.ClientConnectionError())), \
             patch("ytdown.bot.API_RETRY_DELAYS", []):
            with self.assertRaisesRegex(RumbleError, "Could not reach the Rumble API"):
                await fetch_rumble(MagicMock(), LINK)


class RumbleBotTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.service = DownloaderBot(MagicMock(), MagicMock())

    def event(self, text, uid=7):
        notice = SimpleNamespace(edit=AsyncMock())
        return SimpleNamespace(sender_id=uid, is_private=True, raw_text=text, message_id=5,
                               respond=AsyncMock(return_value=notice),
                               get_input_chat=AsyncMock(return_value="peer")), notice

    async def test_change_menu_offers_rumble(self):
        event, _ = self.event("/change")
        await self.service.on_message(event)
        buttons = [b for row in event.respond.call_args.kwargs["buttons"] for b in row]
        self.assertIn(b"rumble", [b.type.data.split(b":")[-1] for b in buttons])

    async def test_rumble_mode_looks_up_and_offers_qualities(self):
        self.service.modes[7] = "rumble"
        event, notice = self.event(LINK)
        with patch("ytdown.bot.fetch_rumble", new=AsyncMock(return_value=normalize_rumble(PAYLOAD))) as fetch:
            await self.service.on_message(event)
        fetch.assert_awaited_once()
        self.assertEqual(fetch.await_args.args[1], LINK)
        selection = self.service.selections[7]
        self.assertEqual(selection.source, Source("rumble", LINK))
        self.assertEqual(selection.choices, ("360p", "480p", "720p", "1080p"))
        self.assertIn("Choose one or more available formats", notice.edit.call_args.args[0])

    async def test_rumble_lookup_error_is_shown_to_user(self):
        self.service.modes[7] = "rumble"
        event, notice = self.event(LINK)
        with patch("ytdown.bot.fetch_rumble", new=AsyncMock(side_effect=RumbleError("Video is private"))):
            await self.service.on_message(event)
        notice.edit.assert_awaited_with("Video is private")
        self.assertNotIn(7, self.service.selections)

    async def test_rumble_link_in_youtube_mode_is_not_a_rumble_lookup(self):
        event, notice = self.event(LINK)
        with patch("ytdown.bot.fetch_rumble", new=AsyncMock()) as fetch, \
             patch("ytdown.bot.get_youtube_title", new=AsyncMock(return_value="")), \
             patch("ytdown.bot.fetch_video", new=AsyncMock(side_effect=ValueError("no"))):
            await self.service.on_message(event)
        fetch.assert_not_awaited()

    async def test_changeapi_stores_a_rumble_api_only(self):
        event, _ = self.event("/changeapi rum https://rumble.example.com/api")
        await self.service.on_message(event)
        self.assertEqual(self.service.api_for(7, "rumble"), "https://rumble.example.com/api")
        self.assertEqual(self.service.api_for(7), bot_module.API_URL)
        self.assertEqual(self.service.api_for(7, "terabox"), DEFAULT_APIS["terabox"])

    async def test_rumble_site_is_not_accepted_as_an_api(self):
        event, _ = self.event("/changeapi rumble https://rumble.com/api")
        await self.service.on_message(event)
        self.assertIn("not a Rumble video link", event.respond.call_args.args[0])
        self.assertEqual(self.service.api_for(7, "rumble"), RUMBLE_API_URL)

    async def test_status_names_rumble_jobs_when_title_missing(self):
        job = Job(7, "peer", 1, Source("rumble", LINK), ("720p",), None)
        job.state = "running"
        self.assertIn("🎬 Rumble video", self.service.format_job(job))

    async def test_thumbnail_uses_only_the_rumble_image(self):
        video = normalize_rumble(PAYLOAD)
        job = Job(7, "peer", 1, Source("rumble", LINK), ("720p",), video)
        with patch("ytdown.bot.thumbnail_jpeg", new=AsyncMock(return_value=None)) as fetch:
            await self.service.video_thumbnail(job, video)
        self.assertEqual([call.args[1] for call in fetch.await_args_list], [THUMB])


    async def test_refused_transfer_asks_the_rumble_api_for_a_fresh_link(self):
        video = normalize_rumble(PAYLOAD)
        fresh = normalize_rumble({**PAYLOAD, "streams": [stream("720p", 720, 720, 1, "fresh")]})
        job = Job(7, "peer", 1, Source("rumble", LINK), ("720p",), video, api_endpoint=RUMBLE_API_URL)
        self.service.jobs[job.token] = job
        self.service.progress = AsyncMock()
        self.service.archive_job = AsyncMock()
        refused = aiohttp.ClientResponseError(request_info=MagicMock(), history=(), status=403)
        with patch.object(self.service, "send_stream", new=AsyncMock(side_effect=[refused, None])) as send, \
             patch("ytdown.bot.fetch_rumble", new=AsyncMock(return_value=fresh)) as fetch_fresh, \
             patch("ytdown.bot.fetch_video", new=AsyncMock(side_effect=AssertionError("YouTube lookup used"))):
            await self.service.run_job(job)
        fetch_fresh.assert_awaited_once()
        self.assertEqual(send.await_count, 2)
        self.assertEqual(send.await_args.args[3], fresh.streams["720p"])
        self.assertIn("Sent 1", self.service.progress.await_args.args[1])


if __name__ == "__main__":
    unittest.main()
