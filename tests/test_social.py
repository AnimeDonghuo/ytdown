"""Instagram/Facebook links: parsing, the tolerant API reader, and the delivery path."""
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp

from ytdown.bot import (BROWSER_UA, DEFAULT_APIS, DownloaderBot, FileTooLarge, Job,
                        MAX_SOCIAL_IMAGE_BYTES,
                        MODE_LABELS, SOCIAL_TIMEOUT, download_social_image, fetch_social,
                        is_media_link, media_filename, normalize_api_endpoint)
from ytdown.core import Source
from ytdown.social import (MAX_SOCIAL_CHOICES, MAX_SOCIAL_LINKS, MAX_SOCIAL_OPTIONS, SOCIAL_API_URL,
                           SocialBatch, SocialDownload, SocialError, SocialMedia, find_social_links,
                           normalize_social, parse_social_source, platform_for, social_choices,
                           social_source)

IG_REEL = "https://www.instagram.com/reel/C8xYzAbCdEf/"
IG_POST = "https://instagram.com/p/C8xYzAbCdEf/"
FB_VIDEO = "https://www.facebook.com/watch/?v=1234567890"
FB_SHORT = "https://fb.watch/xYz1234/"
FB_REEL = "https://www.facebook.com/reel/1234567890"

# A second link's answer: one video and one photo, a different title.
SECOND_PAYLOAD = {
    "status": True,
    "platform": "Facebook",
    "title": "Kitchen reel",
    "thumbnail": "https://video.xx.fbcdn.net/thumb.jpg",
    "downloads": [
        {"quality": "HD", "format": "mp4", "type": "video", "url": "https://video.xx.fbcdn.net/v/hd.mp4"},
        {"quality": "Picture", "format": "jpg", "type": "image", "url": "https://video.xx.fbcdn.net/p.jpg"},
    ],
}

# The shape the DESI worker answers with (trimmed to two files).
PAYLOAD = {
    "status": True,
    "platform": "Instagram",
    "title": "Sunset timelapse over the river",
    "thumbnail": "https://scontent.cdninstagram.com/v/t51/thumb.jpg",
    "duration": "0:24",
    "downloads": [
        {"quality": "HD Video", "format": "mp4", "type": "video",
         "url": "https://scontent.cdninstagram.com/v/t50/hd.mp4"},
        {"quality": "SD Video", "format": "mp4", "type": "video",
         "url": "https://scontent.cdninstagram.com/v/t50/sd.mp4"},
        {"quality": "Cover", "format": "jpg", "type": "image",
         "url": "https://scontent.cdninstagram.com/v/t51/cover.jpg"},
    ],
}


class SocialParsingTests(unittest.TestCase):
    def test_accepts_instagram_and_facebook_links(self):
        for link in (IG_REEL, IG_POST, "https://www.instagram.com/tv/C8xYzAbCdEf/",
                     "https://www.instagram.com/share/reel/xYz1234/",
                     FB_VIDEO, "https://www.facebook.com/reel/1234567890",
                     "https://www.facebook.com/videos/1234567890",
                     "https://m.facebook.com/watch/?v=1234567890",
                     FB_SHORT, "https://www.facebook.com/groups/some.group/posts/1234567890",
                     "https://www.facebook.com/video.php?v=1234567890",
                     "https://instagram.com/stories/someone/1234567890/"):
            self.assertEqual(parse_social_source("  " + link + "  "), link, link)

    def test_rejects_links_that_are_not_posts_or_videos(self):
        for link in ("https://www.instagram.com/", "https://www.instagram.com/someone/",
                     "https://www.facebook.com/", "https://www.facebook.com/profile.php?id=1",
                     "https://www.youtube.com/watch?v=abc", "https://example.com/p/abc",
                     "https://www.instagram.com.evil.example/p/abc",
                     "http://www.instagram.com/p/abc",
                     "https://www.instagram.com/p/" + "a" * 3000):
            with self.assertRaises(ValueError, msg=link):
                parse_social_source(link)

    def test_fb_watch_short_links_have_no_path_keyword(self):
        self.assertEqual(parse_social_source(FB_SHORT), FB_SHORT)
        self.assertEqual(platform_for(FB_SHORT), "Facebook")
        self.assertEqual(platform_for(IG_REEL), "Instagram")

    def test_media_link_detection_includes_social(self):
        self.assertTrue(is_media_link(IG_REEL))
        self.assertTrue(is_media_link(FB_VIDEO))
        self.assertFalse(is_media_link("https://www.instagram.com/someone/"))
        self.assertEqual(MODE_LABELS["social"], "Instagram/Facebook")


class SocialNormalizeTests(unittest.TestCase):
    def test_reads_the_live_api_shape(self):
        media = normalize_social(PAYLOAD, IG_REEL)
        self.assertEqual(media.platform, "Instagram")
        self.assertEqual(media.title, "Sunset timelapse over the river")
        self.assertEqual(media.thumbnail, "https://scontent.cdninstagram.com/v/t51/thumb.jpg")
        self.assertEqual(media.duration, "0:24")
        self.assertEqual([item.label for item in media.downloads],
                         ["HD Video", "SD Video", "Cover"])
        self.assertTrue(media.downloads[0].is_video)
        self.assertTrue(media.downloads[2].is_image)
        self.assertEqual(media.downloads[0].suffix, ".mp4")
        self.assertEqual(media.downloads[2].suffix, ".jpg")

    def test_videos_come_first_and_the_list_is_capped(self):
        payload = {**PAYLOAD, "status": True,
                   "downloads": [{"quality": "Cover", "format": "jpg", "type": "image",
                                  "url": "https://cdn.example.com/cover.jpg"}]
                   + [{"quality": f"V{n}", "format": "mp4", "type": "video",
                       "url": f"https://cdn.example.com/v{n}.mp4"} for n in range(12)]}
        media = normalize_social(payload, IG_REEL)
        options = media.options()
        self.assertEqual(len(options), MAX_SOCIAL_OPTIONS)
        self.assertTrue(all(item.is_video for item in options))  # Videos first.
        self.assertEqual(len(media.downloads), 13)  # Everything is kept, only the buttons are capped.
        self.assertEqual(media.option("V3").url, "https://cdn.example.com/v3.mp4")
        self.assertIsNone(media.option("nope"))

    def test_tolerates_other_response_shapes(self):
        wrapped = {"status": True, "platform": "Facebook", "title": "Clip",
                   "data": {"medias": [{"label": "HD", "ext": "mp4",
                                        "url": "https://video.xx.fbcdn.net/a.mp4"}]}}
        self.assertEqual(normalize_social(wrapped, FB_VIDEO).downloads[0].label, "HD")
        dictionary = {"status": True, "urls": {"HD": "https://cdn.example.com/hd.mp4",
                                               "SD": "https://cdn.example.com/sd.mp4"}}
        self.assertEqual([item.label for item in normalize_social(dictionary, IG_REEL).downloads],
                         ["HD", "SD"])
        bare = {"status": True, "url": "https://cdn.example.com/only.mp4"}
        self.assertEqual(normalize_social(bare, IG_REEL).downloads[0].kind, "video")
        thin = {"success": True, "video_url": "https://cdn.example.com/x.webm",
                "title": "Post", "platform": "Instagram"}
        media = normalize_social(thin, IG_REEL)
        self.assertEqual(media.downloads[0].suffix, ".webm")
        # "Post" says nothing, so the bot shows its own name for the media.
        self.assertEqual(media.title, "Instagram reel")

    def test_a_refused_link_reports_the_apis_own_message(self):
        with self.assertRaises(SocialError) as caught:
            normalize_social({"status": False, "message": "This post is private."}, IG_REEL)
        self.assertIn("private", str(caught.exception))

    def test_no_media_is_an_error_not_an_empty_job(self):
        with self.assertRaises(SocialError):
            normalize_social({"status": True, "downloads": []}, IG_REEL)
        with self.assertRaises(SocialError):
            normalize_social(["not", "a", "dict"], IG_REEL)

    def test_duplicate_labels_are_disambiguated(self):
        payload = {"status": True, "downloads": [
            {"quality": "Video", "type": "video", "url": "https://cdn.example.com/a.mp4"},
            {"quality": "Video", "type": "video", "url": "https://cdn.example.com/b.mp4"}]}
        labels = [item.label for item in normalize_social(payload, IG_REEL).downloads]
        self.assertEqual(len(set(labels)), 2)


class SocialMultiLinkTests(unittest.TestCase):
    def test_finds_every_link_in_one_message(self):
        message = f"look {IG_REEL} and {FB_REEL} — also https://example.com/p/x"
        self.assertEqual(find_social_links(message), (IG_REEL, FB_REEL))
        self.assertEqual(find_social_links(IG_POST + " " + IG_POST), (IG_POST,))
        self.assertEqual(find_social_links("https://www.instagram.com/someone/"), ())
        self.assertEqual(social_source(f"{IG_REEL} {FB_REEL}"), IG_REEL)

    def test_the_whole_paste_is_capped(self):
        many = " ".join(f"https://www.instagram.com/reel/C8xYzAbCdE{n:02d}/" for n in range(8))
        self.assertEqual(len(find_social_links(many)), MAX_SOCIAL_LINKS)
        self.assertEqual(len(find_social_links(many, MAX_SOCIAL_LINKS + 1)), 6)

    def test_a_batch_numbers_every_link_option(self):
        batch = SocialBatch((normalize_social(PAYLOAD, IG_REEL), normalize_social(SECOND_PAYLOAD, FB_REEL)))
        self.assertEqual(batch.platform, "Facebook / Instagram")
        self.assertEqual(batch.title, "2 Instagram/Facebook links")
        self.assertEqual(batch.thumbnail, PAYLOAD["thumbnail"])
        self.assertEqual([choice.label for choice in batch.options()],
                         ["1 · HD Video", "1 · SD Video", "1 · Cover",
                          "2 · HD", "2 · Picture"])
        self.assertEqual(batch.option("2 · Picture").media.title, "Kitchen reel")
        self.assertEqual(batch.option("2 · Picture").item.format, "jpg")
        self.assertIsNone(batch.option("1 · HD"))
        self.assertEqual(batch.dropped, 0)

    def test_the_button_list_is_capped_and_says_how_many_were_dropped(self):
        payload = {"status": True, "downloads": [
            {"quality": f"V{n}", "format": "mp4", "type": "video",
             "url": f"https://cdn.example.com/v{n}.mp4"} for n in range(8)]}
        batch = SocialBatch(tuple(normalize_social(payload, IG_REEL) for _ in range(3)))
        self.assertEqual(len(batch.options()), MAX_SOCIAL_CHOICES)
        self.assertEqual(batch.dropped, 24 - MAX_SOCIAL_CHOICES)

    def test_one_link_looks_the_same_whether_or_not_it_is_a_batch(self):
        media = normalize_social(PAYLOAD, IG_REEL)
        self.assertEqual([choice.label for choice in social_choices(media)],
                         [item.label for item in media.options()])
        self.assertEqual([choice.label for choice in social_choices(SocialBatch((media,)))],
                         [item.label for item in media.options()])

    def test_a_social_link_is_never_taken_as_an_api_endpoint(self):
        for link in (IG_REEL, FB_VIDEO, FB_SHORT):
            with self.assertRaises(ValueError, msg=link):
                normalize_api_endpoint(link)

    def test_a_message_of_social_links_counts_as_media(self):
        self.assertTrue(is_media_link(f"{IG_REEL} {FB_REEL}"))
        self.assertTrue(is_media_link("see " + FB_REEL))


class SocialFlowTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.client = MagicMock()
        self.client.send_message = AsyncMock(return_value=SimpleNamespace(id=11))
        self.client.send_file = AsyncMock(return_value=SimpleNamespace(id=12))
        self.client.edit_message = AsyncMock()
        self.service = DownloaderBot(self.client, MagicMock())
        self.service.progress = AsyncMock()
        self.service.modes[7] = "social"

    def event(self, text, *, uid=7, notice=None):
        return SimpleNamespace(sender_id=uid, raw_text=text, is_private=True, message_id=5,
                               message=SimpleNamespace(document=None, attributes=[]),
                               client=self.client, answer=AsyncMock(),
                               respond=AsyncMock(return_value=notice or SimpleNamespace(
                                   edit=AsyncMock(), respond=AsyncMock())),
                               get_input_chat=AsyncMock(return_value="peer"))

    def media(self):
        return normalize_social(PAYLOAD, IG_REEL)

    async def test_a_link_offers_every_download_the_api_listed(self):
        notice = SimpleNamespace(edit=AsyncMock())
        with patch("ytdown.bot.fetch_social", new=AsyncMock(return_value=self.media())) as fetch:
            await self.service.on_link(self.event(IG_REEL, notice=notice))
        self.assertEqual(fetch.call_args.args[1], IG_REEL)
        self.assertEqual(fetch.call_args.args[2], SOCIAL_API_URL)
        selection = self.service.selections[7]
        self.assertEqual(selection.choices, ("HD Video", "SD Video", "Cover"))
        self.assertIs(selection.social, selection.social)
        text = notice.edit.call_args.args[0]
        self.assertIn("📸 Instagram: Sunset timelapse over the river", text)
        self.assertIn("HD Video (video, mp4)", text)
        self.assertIn("Choose one or more", text)

    async def test_two_links_are_read_together_as_one_list(self):
        notice = SimpleNamespace(edit=AsyncMock())
        media = [normalize_social(PAYLOAD, IG_REEL), normalize_social(SECOND_PAYLOAD, FB_REEL)]
        event = self.event(f"{IG_REEL} {FB_REEL}", notice=notice)
        with patch("ytdown.bot.fetch_social", new=AsyncMock(side_effect=media)) as fetch:
            await self.service.on_link(event)
        self.assertEqual(fetch.await_count, 2)
        self.assertEqual([call.args[1] for call in fetch.await_args_list], [IG_REEL, FB_REEL])
        reported = notice.edit.call_args.args[0]
        self.assertIn("📸 2 Instagram/Facebook links:", reported)
        self.assertIn("· 1. Instagram: Sunset timelapse over the river — 3 option(s)", reported)
        self.assertIn("· 2 · HD (video, mp4)", reported)
        selection = self.service.selections[7]
        self.assertEqual(selection.choices,
                         ("1 · HD Video", "1 · SD Video", "1 · Cover", "2 · HD", "2 · Picture"))
        self.assertIsInstance(selection.social, SocialBatch)

    async def test_a_broken_link_does_not_sink_the_batch(self):
        notice = SimpleNamespace(edit=AsyncMock())
        outcomes = [SocialError("This post is private."), normalize_social(SECOND_PAYLOAD, FB_REEL)]
        event = self.event(f"{IG_REEL} {FB_REEL}", notice=notice)
        with patch("ytdown.bot.fetch_social", new=AsyncMock(side_effect=outcomes)):
            await self.service.on_link(event)
        reported = notice.edit.call_args.args[0]
        self.assertIn("📸 2 Facebook links:", reported)
        self.assertIn("· 2. Facebook: Kitchen reel — 2 option(s)", reported)
        self.assertIn("⚠️ Skipped 1 link(s): link 1 (This post is private.)", reported)
        # The surviving link keeps its own number, so the choices are unambiguous.
        self.assertEqual(self.service.selections[7].choices, ("2 · HD", "2 · Picture"))

    async def test_every_link_failing_is_reported_with_the_reason(self):
        notice = SimpleNamespace(edit=AsyncMock())
        event = self.event(f"{IG_REEL} {FB_REEL}", notice=notice)
        with patch("ytdown.bot.fetch_social",
                   new=AsyncMock(side_effect=[SocialError("private"), SocialError("removed")])):
            await self.service.on_link(event)
        reported = notice.edit.call_args.args[0]
        self.assertIn("Could not load this link. None of those links could be read.", reported)
        self.assertIn("removed", reported)
        self.assertEqual(self.service.selections, {})

    async def test_too_many_links_are_refused_before_any_lookup(self):
        many = " ".join(f"https://www.instagram.com/reel/C8xYzAbCdE{n:02d}/"
                        for n in range(MAX_SOCIAL_LINKS + 1))
        event = self.event(many)
        with patch("ytdown.bot.fetch_social", new=AsyncMock()) as fetch:
            await self.service.on_link(event)
        self.assertIn(f"at most {MAX_SOCIAL_LINKS} Instagram/Facebook links",
                      event.respond.call_args.args[0])
        fetch.assert_not_awaited()

    async def test_a_batch_job_downloads_each_item_from_its_own_link(self):
        media = [normalize_social(PAYLOAD, IG_REEL), normalize_social(SECOND_PAYLOAD, FB_REEL)]
        batch = SocialBatch(tuple(media))
        job = Job(7, "peer", 5, Source("social", IG_REEL), ("1 · SD Video", "2 · Picture"), None,
                  social=batch)
        self.service.jobs[job.token] = job
        sent = []

        async def record(job, media_item, item):
            sent.append((media_item.title, item.label))

        self.service.send_social = AsyncMock(side_effect=record)
        await self.service.run_social(job)
        self.assertEqual(sent, [("Sunset timelapse over the river", "SD Video"),
                                ("Kitchen reel", "Picture")])
        self.assertIn("Sent 2 item(s)", self.service.progress.call_args.args[1])

    async def test_the_slow_api_is_announced_before_it_answers(self):
        notice = SimpleNamespace(edit=AsyncMock())
        with patch("ytdown.bot.fetch_social", new=AsyncMock(return_value=self.media())):
            event = self.event(IG_REEL, notice=notice)
            await self.service.on_link(event)
        first = event.respond.call_args.args[0]
        self.assertIn("slow", first)
        self.assertIn("/cancel", first)

    async def job(self, selected=("HD Video", "SD Video")) -> Job:
        job = Job(7, "peer", 5, Source("social", IG_REEL), selected, None, social=self.media())
        self.service.jobs[job.token] = job
        return job

    async def test_run_social_sends_every_selected_item(self):
        job = await self.job()
        self.service.send_social = AsyncMock()
        await self.service.run_social(job)
        self.assertEqual(self.service.send_social.await_count, 2)
        self.assertEqual([call.args[2].label for call in self.service.send_social.await_args_list],
                         ["HD Video", "SD Video"])
        self.assertEqual(job.item_index, 2)
        self.assertIn("Sent 2 item(s)", self.service.progress.call_args.args[1])

    async def test_a_failed_item_is_counted_with_its_reason(self):
        job = await self.job(selected=("HD Video",))

        async def refuse(*_args, **_kwargs):
            raise ValueError("CDN said 403")

        self.service.send_social = AsyncMock(side_effect=refuse)
        await self.service.run_social(job)
        summary = self.service.progress.call_args.args[1]
        self.assertIn("No media could be sent", summary)
        self.assertIn("ValueError", summary)

    async def test_an_unknown_choice_is_skipped_not_crashed(self):
        job = await self.job(selected=("Gone", "HD Video"))
        self.service.send_social = AsyncMock()
        await self.service.run_social(job)
        self.assertEqual(self.service.send_social.await_count, 1)
        self.assertIn("1 failed", self.service.progress.call_args.args[1])

    async def test_a_missing_lookup_is_reported(self):
        job = await self.job()
        job.social = None
        with self.assertRaises(SocialError):
            await self.service.run_social(job)

    async def test_media_filenames_are_safe_and_numbered(self):
        media = self.media()
        first = media_filename('a/b:c*d?"e', media.downloads[0], 1, 3)
        self.assertTrue(first.startswith("01-03 a_"), first)
        self.assertTrue(first.endswith(" [HD Video].mp4"), first)
        self.assertNotIn("/", first)  # Nothing that could escape a directory.
        single = media_filename("title", media.downloads[0], 1, 1)
        self.assertEqual(single, "title [HD Video].mp4")
        empty = media_filename("", media.downloads[0], 1, 1)
        self.assertTrue(empty.startswith("media [HD Video]"))


class FakeResponse:
    def __init__(self, status=200, body=b"", content_type="video/mp4", headers=None,
                 url="https://cdn.example.com/file.mp4", length=None):
        self.status = status
        self._body = body
        self.content_type = content_type
        self.headers = headers or {}
        self.url = url
        self.content_length = length if length is not None else len(body)

    def raise_for_status(self):
        if self.status >= 400:
            raise aiohttp.ClientResponseError(SimpleNamespace(real_url=self.url), (), status=self.status)

    @property
    def content(self):
        body = self._body

        async def read(_limit=None):
            return body

        return SimpleNamespace(read=read)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        return False


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return self.responses.pop(0)


class SocialDownloadTests(unittest.IsolatedAsyncioTestCase):
    async def test_an_image_is_fetched_with_browser_headers(self):
        session = FakeSession([FakeResponse(body=b"\xff\xd8image")])
        data = await download_social_image(session, "https://cdn.example.com/a.jpg",
                                           {"User-Agent": BROWSER_UA}, 1024)
        self.assertEqual(data, b"\xff\xd8image")
        self.assertEqual(session.calls[0][1]["headers"]["User-Agent"], BROWSER_UA)

    async def test_a_redirect_is_followed_by_hand(self):
        session = FakeSession([FakeResponse(status=302, headers={"Location": "/real.jpg"},
                                            url="https://cdn.example.com/a.jpg"),
                               FakeResponse(body=b"jpeg")])
        data = await download_social_image(session, "https://cdn.example.com/a.jpg", {}, 1024)
        self.assertEqual(data, b"jpeg")
        self.assertEqual(session.calls[1][0], "https://cdn.example.com/real.jpg")

    async def test_a_page_instead_of_a_file_is_refused(self):
        session = FakeSession([FakeResponse(body=b"<html>", content_type="text/html")])
        with self.assertRaises(ValueError):
            await download_social_image(session, "https://cdn.example.com/a.jpg", {}, 1024)

    async def test_an_oversized_image_is_refused(self):
        session = FakeSession([FakeResponse(body=b"x" * 2048)])
        with self.assertRaises(FileTooLarge):
            await download_social_image(session, "https://cdn.example.com/a.jpg", {}, 1024)
        self.assertEqual(MAX_SOCIAL_IMAGE_BYTES, 12 * 1024 * 1024)

    async def test_an_unsafe_link_never_gets_fetched(self):
        session = FakeSession([])
        with self.assertRaises(ValueError):
            await download_social_image(session, "http://127.0.0.1/a.jpg", {}, 1024)
        self.assertEqual(session.calls, [])


class SocialFetchTests(unittest.IsolatedAsyncioTestCase):
    async def test_the_slow_api_gets_a_long_timeout_and_one_retry(self):
        payload = dict(PAYLOAD)
        session = MagicMock()
        side_effects = [aiohttp.ClientConnectionError("dropped"), payload]
        with patch("ytdown.bot.fetch_json_api", new=AsyncMock(side_effect=side_effects)) as call, \
                patch("ytdown.bot.asyncio.sleep", new=AsyncMock()) as sleep:
            media = await fetch_social(session, IG_REEL)
        self.assertEqual(call.await_count, 2)
        self.assertIs(call.await_args.kwargs["timeout"], SOCIAL_TIMEOUT)
        sleep.assert_awaited_once()
        self.assertEqual(media.title, "Sunset timelapse over the river")
        self.assertGreaterEqual(SOCIAL_TIMEOUT.total, 180)

    async def test_an_http_error_is_not_retried(self):
        error = aiohttp.ClientResponseError(SimpleNamespace(real_url=SOCIAL_API_URL), (), status=503)
        with patch("ytdown.bot.fetch_json_api", new=AsyncMock(side_effect=error)) as call:
            with self.assertRaises(aiohttp.ClientResponseError):
                await fetch_social(MagicMock(), IG_REEL)
        self.assertEqual(call.await_count, 1)


class SocialDefaultsTests(unittest.TestCase):
    def test_the_builtin_api_is_the_desi_worker(self):
        self.assertEqual(DEFAULT_APIS["social"], SOCIAL_API_URL)
        self.assertTrue(SOCIAL_API_URL.startswith("https://"))
        self.assertEqual(SocialDownload("HD", "video", "mp4", "https://cdn/x.mp4").suffix, ".mp4")
        self.assertEqual(SocialMedia("Instagram", "t", "").options(), ())


if __name__ == "__main__":
    unittest.main()
