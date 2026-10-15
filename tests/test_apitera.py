"""apitera in the bot: /latest, /search, /auto, /parciv and the upload runner."""
import asyncio
import json
import os
import re
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp

import ytdown.bot as bot
from ytdown.apitera import SEARCH_SECTIONS, CataloguePage, ParcivBatch, parse_search
from ytdown.bot import (DEFAULT_APIS, DownloaderBot, Job, MODE_LABELS, is_media_link,
                        normalize_api_endpoint)
from ytdown.core import Source
from ytdown.parciv import (PARCIV_API_URL, ParcivApi, ParcivError, ParcivItem, ParcivMedia,
                           ParcivSource, parse_catalogue)
from tests.test_parciv import MOVIES

MOVIE_ID = "3920183929835251392"
LINK = f"https://api.parciv.workers.dev/moviebox/stream?id={MOVIE_ID}"


class FakeHistory:
    """What Telethon's iter_messages yields: an async iterator of messages."""

    def __init__(self, messages):
        self.messages = messages

    def __aiter__(self):
        async def generate():
            for message in self.messages:
                yield message

        return generate()


def stored(name: str, caption: str = ""):
    document = SimpleNamespace(attributes=[SimpleNamespace(file_name=name)])
    return SimpleNamespace(document=document, file=SimpleNamespace(name=name), message=caption)


def movie_item(name="PAW Patrol: The Dino Movie", section="movies", subject_id=MOVIE_ID):
    return ParcivItem(subject_id=subject_id, name=name, section=section, year="2026",
                      poster="https://pbcdnw.aoneroom.com/poster.jpg")


def movie_media():
    return ParcivMedia(title="PAW Patrol: The Dino Movie", poster="https://pbcdnw.aoneroom.com/poster.jpg",
                       duration=5800, subtitles=("English",),
                       sources=(ParcivSource("360p", "https://bcdnt.hakunaymatata.com/bt/360.mp4",
                                             size=40 * 1024 * 1024, duration=5800),
                                ParcivSource("1080p", "https://bcdnt.hakunaymatata.com/res/1080.mp4",
                                             size=300 * 1024 * 1024, duration=5800)),
                       subject_id=MOVIE_ID)


class BotCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.client = MagicMock()
        self.client.send_message = AsyncMock(return_value=SimpleNamespace(id=11))
        self.client.send_file = AsyncMock(return_value=SimpleNamespace(id=12))
        self.client.edit_message = AsyncMock()
        self.client.iter_messages = MagicMock(return_value=FakeHistory([]))
        self.client.delete_messages = AsyncMock()
        self.service = DownloaderBot(self.client, MagicMock())
        self.service.progress = AsyncMock()
        self.service.modes[7] = "parciv"

    def event(self, text="", *, uid=7, notice=None):
        return SimpleNamespace(sender_id=uid, raw_text=text, is_private=True, message_id=5,
                               message=SimpleNamespace(document=None, attributes=[]),
                               client=self.client, answer=AsyncMock(),
                               respond=AsyncMock(return_value=notice or SimpleNamespace(
                                   id=21, edit=AsyncMock(), respond=AsyncMock())),
                               get_input_chat=AsyncMock(return_value="peer"))


class LatestTests(BotCase):
    async def test_the_listing_marks_what_the_dm_holds_and_offers_to_download(self):
        self.client.iter_messages = MagicMock(return_value=FakeHistory(
            [stored("Matchbox the Movie (2026) [Hindi] [1080p].mp4")]))
        notice = SimpleNamespace(id=21, edit=AsyncMock())
        with patch("ytdown.bot.fetch_parciv_catalogue",
                   new=AsyncMock(return_value=CataloguePage("movies", 1,
                                                            parse_catalogue(MOVIES, "movies")))):
            await self.service.handle_latest(self.event("/latest movies 3", notice=notice), "/latest movies 3")
        text = notice.edit.call_args.args[0]
        self.assertIn("🔎 apitera — page 1 of Moviebox movies", text)
        self.assertIn("1. ✅ Matchbox the Movie (2026)", text)
        self.assertIn("3. ⬇️ PAW Patrol: The Dino Movie (2026)", text)
        self.assertIn("⬇️ 2 of 3 listed title(s) are new", text)
        buttons = notice.edit.call_args.kwargs["buttons"]
        labels = [button.text for row in buttons for button in row]
        self.assertEqual(labels, ["⬇️ Download new (2)", "⬇️ Download all (3)", "🔁 Refresh",
                                  "▶️ Auto on"])
        panel = self.service.latest_panels[7]
        self.assertEqual(len(panel.items), 3)
        self.assertEqual(panel.sections, ("movies",))

    async def test_a_section_that_answers_nothing_is_reported(self):
        notice = SimpleNamespace(id=21, edit=AsyncMock())
        with patch("ytdown.bot.fetch_parciv_catalogue",
                   new=AsyncMock(return_value=CataloguePage("movies", 1, ()))):
            await self.service.handle_latest(self.event(notice=notice), "/latest movies")
        self.assertIn("No catalogue entries came back", notice.edit.call_args.args[0])

    async def test_an_unknown_section_is_refused_before_any_call(self):
        event = self.event()
        with patch("ytdown.bot.fetch_parciv_catalogue", new=AsyncMock()) as fetch:
            await self.service.handle_latest(event, "/latest netflix")
        self.assertIn("Unknown apitera section", event.respond.call_args.args[0])
        fetch.assert_not_awaited()

    async def test_the_download_new_button_queues_only_what_is_missing(self):
        self.client.iter_messages = MagicMock(return_value=FakeHistory(
            [stored("Matchbox the Movie (2026) [1080p].mp4")]))
        self.service.auto_peers[7] = "peer"
        self.service.latest_panels[7] = SimpleNamespace(
            peer="peer", items=parse_catalogue(MOVIES, "movies"), sections=("movies",), page=1)
        self.service.auto_tokens[7] = "tok"
        self.service.queue_parciv_job = AsyncMock(return_value=None)
        click = SimpleNamespace(sender_id=7, data=b"a:tok:go", is_private=True, message_id=21,
                                answer=AsyncMock(), edit=AsyncMock())
        await self.service.callback(click)
        batch = self.service.queue_parciv_job.call_args.args[2]
        self.assertEqual([item.title for item in batch.items],
                         ["Spider-Man: Hari Baru (2026)", "PAW Patrol: The Dino Movie (2026)"])
        self.assertTrue(batch.check_dm)

    async def test_the_download_all_button_queues_everything_listed(self):
        self.service.auto_peers[7] = "peer"
        self.service.latest_panels[7] = SimpleNamespace(
            peer="peer", items=parse_catalogue(MOVIES, "movies"), sections=("movies",), page=1)
        self.service.auto_tokens[7] = "tok"
        self.service.queue_parciv_job = AsyncMock(return_value=None)
        click = SimpleNamespace(sender_id=7, data=b"a:tok:all", is_private=True, message_id=21,
                                answer=AsyncMock(), edit=AsyncMock())
        await self.service.callback(click)
        self.assertEqual(len(self.service.queue_parciv_job.call_args.args[2].items), 3)

    async def test_an_expired_button_says_so(self):
        click = SimpleNamespace(sender_id=7, data=b"a:gone:now", is_private=True, message_id=21,
                                answer=AsyncMock(), edit=AsyncMock())
        await self.service.callback(click)
        self.assertIn("expired", click.answer.call_args.kwargs["alert"] and "expired"
                      or click.answer.call_args.args[0])


class AutoTests(BotCase):
    async def test_auto_off_by_default_and_on_registers_the_chat(self):
        self.assertFalse(self.service.auto_enabled.get(7, False))
        first = self.event()
        await self.service.handle_auto(first, "/auto")
        self.assertIn("🤖 apitera auto: OFF", first.respond.call_args_list[0].args[0])
        self.assertEqual([button.text for row in first.respond.call_args_list[0].kwargs["buttons"]
                          for button in row], ["▶️ Turn on", "🔎 Check now", "📄 Latest list"])
        await self.service.handle_auto(self.event(), "/auto on")
        self.assertTrue(self.service.auto_enabled[7])
        self.assertEqual(self.service.auto_peers[7], "peer")
        self.assertIn(7, self.service.auto_tasks)
        self.service.stop_auto_watch(7)
        self.assertFalse(self.service.auto_enabled[7])

    async def test_sections_limit_and_pace_are_configurable(self):
        await self.service.handle_auto(self.event(), "/auto sections anime, donghua")
        self.assertEqual(self.service.auto_sections[7], ("anime", "donghua"))
        await self.service.handle_auto(self.event(), "/auto sections default")
        self.assertEqual(self.service.auto_sections[7], ("movies", "anime", "donghua"))
        event = self.event()
        await self.service.handle_auto(event, "/auto sections netflix")
        self.assertIn("Unknown apitera section", event.respond.call_args.args[0])
        await self.service.handle_auto(self.event(), "/auto limit 5")
        self.assertEqual(self.service.auto_limits[7], 5)
        await self.service.handle_auto(self.event(), "/auto every 20m")
        self.assertEqual(int(self.service.auto_interval), 1200)
        status = self.service.auto_status_text(7)
        self.assertIn("Anime, Donghua", status)
        self.assertIn("every 20m", status)
        self.assertIn("5 item(s) per section", status)

    async def test_the_check_queues_only_new_titles(self):
        self.client.iter_messages = MagicMock(return_value=FakeHistory(
            [stored("Matchbox the Movie (2026) [1080p].mp4")]))
        self.service.auto_peers[7] = "peer"
        self.service.auto_sections[7] = ("movies",)
        with patch("ytdown.bot.fetch_parciv_catalogue",
                   new=AsyncMock(return_value=CataloguePage("movies", 1,
                                                            parse_catalogue(MOVIES, "movies")))), \
                patch("ytdown.bot.fetch_parciv_media",
                      new=AsyncMock(side_effect=ParcivError("no stream in this test"))), \
                patch("ytdown.bot.fetch_parciv_item", new=AsyncMock(side_effect=lambda s, a, i: i)):
            result = await self.service.run_auto_check(7)
            self.assertIn("Queued 2 new title(s)", result)
            self.assertEqual(len(self.service.jobs), 1)
            job = next(iter(self.service.jobs.values()))
            # The job runs while the fakes are still in place, so it fails cleanly instead
            # of reaching the mocked session's real HTTP calls.
            await asyncio.gather(job.task, return_exceptions=True)
        self.assertTrue(job.parciv.check_dm)
        self.assertEqual(len(job.parciv.items), 2)
        self.assertEqual(job.source.kind, "parciv")
        self.assertEqual(job.api_endpoint, DEFAULT_APIS["parciv"])

    async def test_nothing_new_is_reported_without_queueing(self):
        self.client.iter_messages = MagicMock(return_value=FakeHistory(
            [stored("Matchbox the Movie (2026) [1080p].mp4"),
             stored("Spider-Man_ Hari Baru (2026) [360p].mp4"),
             stored("PAW Patrol_ The Dino Movie (2026) [Hindi] [360p].mp4")]))
        self.service.auto_peers[7] = "peer"
        self.service.auto_sections[7] = ("movies",)
        with patch("ytdown.bot.fetch_parciv_catalogue",
                   new=AsyncMock(return_value=CataloguePage("movies", 1,
                                                            parse_catalogue(MOVIES, "movies")))):
            result = await self.service.run_auto_check(7)
        self.assertIn("Nothing new", result)
        self.assertEqual(self.service.jobs, {})

    async def test_a_running_batch_blocks_a_second_check(self):
        job = Job(7, "peer", 21, Source("parciv", PARCIV_API_URL), ("All",), None,
                  parciv=ParcivBatch(items=(movie_item(),)))
        self.service.jobs[job.token] = job
        self.assertTrue(self.service.parciv_busy(7))
        self.service.auto_peers[7] = "peer"
        self.assertIn("already queued or running", await self.service.run_auto_check(7))

    async def test_the_watcher_needs_a_chat_to_upload_to(self):
        result = await self.service.run_auto_check(7)
        self.assertIn("Send /auto on first", result)

    async def test_the_watcher_repeats_until_it_is_stopped(self):
        self.service.auto_interval = 1
        self.service.auto_enabled[7] = True
        self.service.auto_peers[7] = "peer"
        self.service.run_auto_check = AsyncMock(return_value="nothing")
        task = asyncio.create_task(self.service.auto_watch(7))
        await asyncio.sleep(1.5)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        self.assertGreaterEqual(self.service.run_auto_check.await_count, 1)
        self.assertIn(7, self.service.auto_last)


POST_PAYLOAD = {"data": {"title": "Kamen Rider Kiva: You Can Also Be Kiva", "duration": 1500,
                          "resources": [{"url": "http://santol.whatbox.ca:29318/Kiva/06-360p.mp4",
                                         "resolution": "360p", "duration": 1500}]}}


class LookupLadderTests(BotCase):
    """An Anime/Donghua item is looked up with every plausible shape until one answers."""

    async def test_the_first_refused_shape_is_followed_by_the_next(self):
        item = ParcivItem(subject_id="kamen-rider-kiva", name="Kamen Rider Kiva", section="anime",
                          extra={"slug": "kamen-rider-kiva-movie"})
        asked = []

        async def answers(session, url, *args, **kwargs):
            asked.append(url)
            if "/post?slug=" in url:
                raise bot.ApiJsonError("answered non-JSON (HTTP 200): <html>Not found</html>",
                                       "", 0)
            return POST_PAYLOAD

        with patch("ytdown.bot.fetch_parciv_json", new=answers):
            media = await bot.fetch_parciv_post(None, ParcivApi(PARCIV_API_URL), item)
        self.assertEqual(media.title, "Kamen Rider Kiva: You Can Also Be Kiva")
        self.assertEqual([source.label for source in media.sources], ["360p"])
        self.assertEqual(asked[0], "https://api.parciv.workers.dev/anime/post?slug=kamen-rider-kiva-movie")
        self.assertEqual(asked[1], "https://api.parciv.workers.dev/anime/post?id=kamen-rider-kiva")

    async def test_every_shape_failing_names_each_reason(self):
        item = ParcivItem(subject_id="dark", name="Dark Machine", section="donghua",
                          extra={"slug": "dark-machine"})

        async def answers(session, url, *args, **kwargs):
            if "slug=" in url:
                return {"message": "no media here"}          # Answers, but without media.
            raise bot.ApiJsonError("answered non-JSON (HTTP 200): <html>nope</html>", "", 0)

        with patch("ytdown.bot.fetch_parciv_json", new=answers):
            with self.assertRaises(ParcivError) as caught:
                await bot.fetch_parciv_post(None, ParcivApi(PARCIV_API_URL), item)
        message = str(caught.exception)
        self.assertIn("no lookup call for Dark Machine answered with media", message)
        self.assertIn("slug: The apitera answer", message)
        self.assertIn("subject id: answered non-JSON (HTTP 200)", message)
        self.assertIn("detail: ", message)

    async def test_a_stream_item_never_waits_for_the_moviebox_detail_call(self):
        item = movie_item(section="donghua")
        with patch("ytdown.bot.fetch_parciv_item", new=AsyncMock()) as detail, \
                patch("ytdown.bot.fetch_parciv_post", new=AsyncMock(return_value=movie_media())):
            media = await self.service.parciv_media(7, ParcivApi(PARCIV_API_URL), item)
        detail.assert_not_awaited()
        self.assertEqual(media.title, "PAW Patrol: The Dino Movie")

    async def test_a_moviebox_item_still_reads_its_detail_first(self):
        item = movie_item()
        with patch("ytdown.bot.fetch_parciv_item", new=AsyncMock(side_effect=lambda s, a, i: i)) as detail, \
                patch("ytdown.bot.fetch_parciv_media", new=AsyncMock(return_value=movie_media())):
            await self.service.parciv_media(7, ParcivApi(PARCIV_API_URL), item)
        self.assertTrue(detail.await_count)


class JsonAnswerTests(unittest.IsolatedAsyncioTestCase):
    """An HTML answer from a wrong call shape must be recognisable, not a bare error."""

    class Response:
        def __init__(self, body, status=200):
            self.body, self.status = body, status
            self.headers = {}
            self.content_length = len(body)
            self.url = "https://api.parciv.workers.dev/anime/post?id=1"

        def raise_for_status(self):
            if self.status >= 400:
                raise aiohttp.ClientResponseError(None, (), status=self.status)

        class Content:
            def __init__(self, body):
                self.body = body

            async def read(self, size):
                return self.body[:size]

        @property
        def content(self):
            return JsonAnswerTests.Response.Content(self.body)

    class Get:
        def __init__(self, response):
            self.response = response

        async def __aenter__(self):
            return self.response

        async def __aexit__(self, *exc):
            return False

    class Session:
        def __init__(self, response):
            self.response = response

        def get(self, *args, **kwargs):
            return JsonAnswerTests.Get(self.response)

    async def test_a_non_json_answer_names_the_status_and_the_body(self):
        session = self.Session(self.Response(b"<html><body>Not Found</body></html>"))
        with self.assertRaises(bot.ApiJsonError) as caught:
            await bot.fetch_json_api(session, "https://api.parciv.workers.dev/anime/post?id=1")
        message = str(caught.exception)
        self.assertIn("HTTP 200", message)
        self.assertIn("Not Found", message)
        self.assertLess(len(message), 200)
        # It is still a JSONDecodeError, so the YouTube retry logic keeps working.
        self.assertIsInstance(caught.exception, json.JSONDecodeError)

    async def test_a_json_answer_is_returned_as_usual(self):
        session = self.Session(self.Response(b'{"data": {"ok": true}}'))
        payload = await bot.fetch_json_api(session, "https://api.parciv.workers.dev/anime/latest")
        self.assertEqual(payload, {"data": {"ok": True}})


class RunParcivTests(BotCase):
    def setUp(self):
        super().setUp()
        self.downloaded = []

        def fake_download(url, out_dir, on_bytes, stop, **kwargs):
            self.downloaded.append((url, kwargs))
            path = os.path.join(out_dir, "stream.mp4")
            with open(path, "wb") as handle:
                handle.write(b"\x00\x00\x00\x18ftypmp42" + b"\x00" * (2 * 1024 * 1024))
            on_bytes(1024, 2048)
            return path

        self.fake_download = fake_download

    async def job(self, batch) -> Job:
        job = Job(7, "peer", 21, Source("parciv", PARCIV_API_URL), ("All",), None, parciv=batch)
        self.service.jobs[job.token] = job
        return job

    async def test_every_quality_is_downloaded_and_sent_with_its_tags(self):
        batch = ParcivBatch(items=(movie_item(),), check_dm=False)
        job = await self.job(batch)
        with patch("ytdown.bot.fetch_parciv_media", new=AsyncMock(return_value=movie_media())), \
                patch("ytdown.bot.fetch_parciv_item", new=AsyncMock(side_effect=lambda s, a, i: i)), \
                patch("ytdown.bot.download_parciv", new=self.fake_download), \
                patch("ytdown.bot.thumbnail_jpeg", new=AsyncMock(return_value=(b"\xff\xd8jpg", 640, 360))):
            await self.service.run_parciv(job)
        self.assertEqual([url for url, _ in self.downloaded],
                         ["https://bcdnt.hakunaymatata.com/bt/360.mp4",
                          "https://bcdnt.hakunaymatata.com/res/1080.mp4"])
        self.assertEqual(self.client.send_file.await_count, 2)
        first = self.client.send_file.call_args_list[0]
        self.assertTrue(first.args[1].endswith("PAW Patrol_ The Dino Movie [English Sub] [360p].mp4"),
                        first.args[1])
        self.assertEqual(first.kwargs["caption"], "PAW Patrol: The Dino Movie [English Sub] [360p]")
        self.assertEqual(first.kwargs["attributes"][0].duration, 5800)
        self.assertEqual(first.kwargs["attributes"][0].h, 360)
        self.assertIn("Uploaded 2 file(s)", self.service.progress.call_args.args[1])

    async def test_only_the_chosen_qualities_are_sent(self):
        batch = ParcivBatch(items=(movie_item(),), qualities=("1080p",), check_dm=False)
        job = await self.job(batch)
        with patch("ytdown.bot.fetch_parciv_media", new=AsyncMock(return_value=movie_media())), \
                patch("ytdown.bot.fetch_parciv_item", new=AsyncMock(side_effect=lambda s, a, i: i)), \
                patch("ytdown.bot.download_parciv", new=self.fake_download), \
                patch("ytdown.bot.thumbnail_jpeg", new=AsyncMock(return_value=(b"\xff\xd8jpg", 640, 360))):
            await self.service.run_parciv(job)
        self.assertEqual([url for url, _ in self.downloaded],
                         ["https://bcdnt.hakunaymatata.com/res/1080.mp4"])

    async def test_a_title_already_in_the_dm_is_skipped(self):
        self.client.iter_messages = MagicMock(return_value=FakeHistory(
            [stored("PAW Patrol_ The Dino Movie [English Sub] [360p].mp4")]))
        batch = ParcivBatch(items=(movie_item(),), check_dm=True)
        job = await self.job(batch)
        with patch("ytdown.bot.fetch_parciv_media", new=AsyncMock(return_value=movie_media())), \
                patch("ytdown.bot.fetch_parciv_item", new=AsyncMock(side_effect=lambda s, a, i: i)), \
                patch("ytdown.bot.download_parciv", new=self.fake_download), \
                patch("ytdown.bot.thumbnail_jpeg", new=AsyncMock(return_value=None)):
            await self.service.run_parciv(job)
        self.assertEqual(self.downloaded, [])
        self.client.send_file.assert_not_awaited()
        summary = self.service.progress.call_args.args[1]
        self.assertIn("1 title(s) were already in your DM", summary)
        self.assertIn("Skipping", [call.args[1] for call in self.service.progress.await_args_list][-2])

    async def test_an_oversized_quality_is_reported_not_downloaded(self):
        media = movie_media()
        big = ParcivSource("2160p", "https://bcdnt.hakunaymatata.com/res/2160.mp4",
                           size=3 * 1024 * 1024 * 1024)
        media = ParcivMedia(media.title, media.poster, media.summary, media.duration, media.episode,
                            media.langs, media.subtitles, (big,), media.subject_id)
        batch = ParcivBatch(items=(movie_item(),), check_dm=False)
        job = await self.job(batch)
        with patch("ytdown.bot.fetch_parciv_media", new=AsyncMock(return_value=media)), \
                patch("ytdown.bot.fetch_parciv_item", new=AsyncMock(side_effect=lambda s, a, i: i)), \
                patch("ytdown.bot.download_parciv", new=self.fake_download):
            await self.service.run_parciv(job)
        self.assertEqual(self.downloaded, [])
        self.assertIn("over the size limit", self.service.progress.call_args.args[1])

    async def test_a_failed_download_is_counted_with_its_reason(self):
        def refuse(*_args, **_kwargs):
            raise ParcivError("360p could not be downloaded (HTTP Error 403: Forbidden).")

        batch = ParcivBatch(items=(movie_item(),), qualities=("360p",), check_dm=False)
        job = await self.job(batch)
        with patch("ytdown.bot.fetch_parciv_media", new=AsyncMock(return_value=movie_media())), \
                patch("ytdown.bot.fetch_parciv_item", new=AsyncMock(side_effect=lambda s, a, i: i)), \
                patch("ytdown.bot.download_parciv", new=refuse):
            await self.service.run_parciv(job)
        summary = self.service.progress.call_args.args[1]
        self.assertIn("Uploaded 0 file(s) from apitera", summary)
        self.assertIn("1 failed", summary)
        self.assertIn("403", summary)

    async def test_a_lookup_failure_does_not_stop_the_other_items(self):
        batch = ParcivBatch(items=(movie_item(name="First"), movie_item(name="Second", subject_id="2")),
                            check_dm=False)
        job = await self.job(batch)
        calls = 0

        async def media(session, api, item):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise ValueError("HTTP 500")
            return movie_media()

        with patch("ytdown.bot.fetch_parciv_media", new=media), \
                patch("ytdown.bot.fetch_parciv_item", new=AsyncMock(side_effect=lambda s, a, i: i)), \
                patch("ytdown.bot.download_parciv", new=self.fake_download), \
                patch("ytdown.bot.thumbnail_jpeg", new=AsyncMock(return_value=None)):
            await self.service.run_parciv(job)
        summary = self.service.progress.call_args.args[1]
        self.assertIn("Uploaded 2 file(s)", summary)
        self.assertIn("1 failed", summary)

    async def test_the_download_is_given_the_api_and_family_for_its_fallback(self):
        batch = ParcivBatch(items=(movie_item(section="donghua"),), qualities=("360p",),
                            check_dm=False)
        job = await self.job(batch)
        calls = []

        def fake_download(url, out_dir, on_bytes, stop, **kwargs):
            calls.append((url, kwargs))
            path = os.path.join(out_dir, "stream.mp4")
            with open(path, "wb") as handle:
                handle.write(b"\x00\x00\x00\x18ftypmp42" + b"\x00" * (2 * 1024 * 1024))
            return path

        with patch("ytdown.bot.fetch_parciv_media", new=AsyncMock(return_value=movie_media())), \
                patch("ytdown.bot.fetch_parciv_item", new=AsyncMock(side_effect=lambda s, a, i: i)), \
                patch("ytdown.bot.download_parciv", new=fake_download), \
                patch("ytdown.bot.thumbnail_jpeg", new=AsyncMock(return_value=None)):
            await self.service.run_parciv(job)
        _, kwargs = calls[0]
        # A Donghua item must leech through the Donghua family, like the API's own URL.
        self.assertEqual(kwargs["section"], "donghua")
        self.assertEqual(kwargs["api"].root, "https://api.parciv.workers.dev")
        self.assertFalse(kwargs["dash"])

    async def test_a_parciv_job_never_leaches_through_another_service_api(self):
        from ytdown.bot import DEFAULT_APIS, parciv_endpoint
        self.assertEqual(parciv_endpoint(DEFAULT_APIS["youtube"]), DEFAULT_APIS["parciv"])
        self.assertEqual(parciv_endpoint(""), DEFAULT_APIS["parciv"])
        own = "https://my-worker.example.workers.dev/moviebox"
        self.assertEqual(parciv_endpoint(own), own)

    async def test_the_proxy_and_cookie_headers_are_passed_to_the_download(self):
        batch = ParcivBatch(items=(movie_item(),), qualities=("360p",), check_dm=False)
        job = await self.job(batch)
        media = ParcivMedia("T", sources=(ParcivSource("360p", "https://bcdnt.hakunaymatata.com/a.mp4",
                                                       headers=(("Cookie", "CloudFront-Policy=x"),)),))
        with patch("ytdown.bot.fetch_parciv_media", new=AsyncMock(return_value=media)), \
                patch("ytdown.bot.fetch_parciv_item", new=AsyncMock(side_effect=lambda s, a, i: i)), \
                patch("ytdown.bot.download_parciv", new=self.fake_download), \
                patch("ytdown.bot.thumbnail_jpeg", new=AsyncMock(return_value=None)), \
                patch.object(self.service, "proxy_for",
                             return_value=SimpleNamespace(url="http://127.0.0.1:8080")):
            await self.service.run_parciv(job)
        self.assertEqual(self.downloaded[0][1]["headers"], (("Cookie", "CloudFront-Policy=x"),))
        self.assertEqual(self.downloaded[0][1]["proxy"], "http://127.0.0.1:8080")


class LinkFlowTests(BotCase):
    async def test_a_stream_link_offers_the_qualities(self):
        notice = SimpleNamespace(id=21, edit=AsyncMock())
        with patch("ytdown.bot.fetch_parciv_media", new=AsyncMock(return_value=movie_media())), \
                patch("ytdown.bot.fetch_parciv_item", new=AsyncMock(side_effect=lambda s, a, i: i)):
            await self.service.on_link(self.event(LINK, notice=notice))
        selection = self.service.selections[7]
        self.assertEqual(selection.choices, ("360p", "1080p"))
        self.assertFalse(selection.parciv.check_dm)  # A hand-sent link is an explicit ask.
        shown = notice.edit.call_args.args[0]
        self.assertIn("🎬 PAW Patrol: The Dino Movie", shown)
        self.assertIn("· Subtitles: English", shown)
        self.assertIn("360p (video", shown)
        self.assertIn("Choose one or more", shown)

    async def test_the_done_button_queues_the_chosen_qualities(self):
        notice = SimpleNamespace(id=21, edit=AsyncMock())
        with patch("ytdown.bot.fetch_parciv_media", new=AsyncMock(return_value=movie_media())), \
                patch("ytdown.bot.fetch_parciv_item", new=AsyncMock(side_effect=lambda s, a, i: i)):
            await self.service.on_link(self.event(LINK, notice=notice))
        selection = self.service.selections[7]
        selection.selected = {"360p"}
        self.service.run_job = AsyncMock()
        click = SimpleNamespace(sender_id=7, data=f"s:{selection.token}:done".encode(),
                                is_private=True, message_id=21, answer=AsyncMock())
        await self.service.callback(click)
        job = self.service.user_jobs(7)[0]
        self.assertEqual(job.parciv.qualities, ("360p",))
        self.assertEqual(job.parciv.items[0].subject_id, MOVIE_ID)
        self.assertEqual(job.title, "PAW Patrol: The Dino Movie")
        await asyncio.gather(job.task, return_exceptions=True)

    async def test_another_family_endpoint_lists_its_items(self):
        notice = SimpleNamespace(id=21, edit=AsyncMock())
        link = "https://api.parciv.workers.dev/anime/popular"
        with patch("ytdown.bot.fetch_parciv_json", new=AsyncMock(return_value=MOVIES)) as fetch:
            await self.service.on_link(self.event(link, notice=notice))
        self.assertEqual(fetch.await_args.args[1], link)
        text = notice.edit.call_args.args[0]
        self.assertIn("🔎 apitera — page 1 of Anime", text)
        panel = self.service.latest_panels[7]
        self.assertEqual(panel.sections, ("anime",))
        self.assertEqual(panel.items[0].section, "anime")
        self.assertEqual(panel.url, link)

    async def test_a_search_link_lists_what_it_found(self):
        notice = SimpleNamespace(id=21, edit=AsyncMock())
        link = "https://api.parciv.workers.dev/moviebox/search?query=paw%20patrol"
        with patch("ytdown.bot.fetch_parciv_json", new=AsyncMock(return_value=MOVIES)):
            await self.service.on_link(self.event(link, notice=notice))
        text = notice.edit.call_args.args[0]
        self.assertIn("🔎 apitera search — paw patrol", text)
        self.assertIn("1. ⬇️ PAW Patrol: The Dino Movie (2026)", text)
        self.assertNotIn("2. ", text)  # The API's extra rows are dropped when the words match some.

    async def test_the_refresh_button_of_a_link_panel_re_reads_the_link(self):
        link = "https://api.parciv.workers.dev/anime/popular"
        self.service.auto_peers[7] = "peer"
        self.service.latest_panels[7] = SimpleNamespace(
            peer="peer", items=(), sections=("anime",), page=1, words="", url=link)
        self.service.auto_tokens[7] = "tok"
        self.service.show_listing_url = AsyncMock()
        click = SimpleNamespace(sender_id=7, data=b"a:tok:latest", is_private=True, message_id=21,
                                answer=AsyncMock(), edit=AsyncMock())
        await self.service.callback(click)
        self.assertEqual(self.service.show_listing_url.call_args.args[3], link)
        self.assertEqual(self.service.show_listing_url.call_args.args[4], "anime")

    async def test_a_catalogue_link_lists_that_section(self):
        with patch("ytdown.bot.fetch_parciv_catalogue",
                   new=AsyncMock(return_value=CataloguePage("tv", 2, parse_catalogue(MOVIES, "tv")))):
            await self.service.on_link(self.event("https://api.parciv.workers.dev/moviebox/tv?page=2"))
        panel = self.service.latest_panels[7]
        self.assertEqual(panel.sections, ("tv",))
        self.assertEqual(panel.page, 2)

    async def test_parciv_mode_refuses_other_links(self):
        event = self.event("https://www.youtube.com/watch?v=x")
        await self.service.on_link(event)
        self.assertNotIn(7, self.service.selections)
        self.assertIn("apitera mode accepts only", event.respond.call_args.args[0])

    async def test_a_link_of_another_site_is_refused_as_a_command(self):
        event = self.event()
        await self.service.handle_parciv(event, "/parciv https://example.com/x")
        self.assertIn("apitera mode accepts only", event.respond.call_args.args[0])

    async def test_the_link_is_media_but_its_api_is_not(self):
        self.assertTrue(is_media_link(LINK))
        self.assertTrue(is_media_link(PARCIV_API_URL))
        self.assertTrue(is_media_link("https://api.parciv.workers.dev/anime/post?slug=x"))
        self.assertFalse(is_media_link("https://example.com/moviebox/movies"))
        with self.assertRaises(ValueError):
            normalize_api_endpoint(LINK)
        self.assertEqual(normalize_api_endpoint("https://api.parciv.workers.dev/moviebox"),
                         "https://api.parciv.workers.dev/moviebox")

    async def test_the_mode_button_exists_and_the_default_api_is_the_apitera_one(self):
        self.assertEqual(MODE_LABELS["parciv"], "apitera")
        self.assertEqual(DEFAULT_APIS["parciv"], PARCIV_API_URL)
        self.assertEqual(ParcivApi(DEFAULT_APIS["parciv"]).moviebox, f"{PARCIV_API_URL}")
        self.assertIn("apitera", self.service.api_lines(7)[-1])
        token = "tok"
        self.service.mode_tokens[7] = token
        click = SimpleNamespace(sender_id=7, data=f"m:{token}:parciv".encode(), is_private=True,
                                message_id=21, answer=AsyncMock(), edit=AsyncMock())
        await self.service.callback(click)
        self.assertEqual(self.service.modes[7], "parciv")


class ParseSearchTests(unittest.TestCase):
    """`/search` line parsing: sections first, the words untouched otherwise."""

    def test_a_bare_query_searches_every_family(self):
        self.assertEqual(parse_search("/search paw patrol"), (SEARCH_SECTIONS, "paw patrol"))

    def test_named_sections_narrow_the_search(self):
        self.assertEqual(parse_search("/search movies paw patrol"), (("movies",), "paw patrol"))
        self.assertEqual(parse_search("/search anime,donghua one piece"),
                         (("anime", "donghua"), "one piece"))

    def test_sections_can_be_named_at_the_end(self):
        self.assertEqual(parse_search("/search paw patrol sections=movies,anime"),
                         (("movies", "anime"), "paw patrol"))

    def test_a_word_that_is_not_a_section_stays_in_the_query(self):
        self.assertEqual(parse_search("/search in the mood for love"),
                         (SEARCH_SECTIONS, "in the mood for love"))
        self.assertEqual(parse_search("/search sections=in the mood for love"),
                         (SEARCH_SECTIONS, "in the mood for love"))

    def test_an_empty_line_asks_for_words(self):
        self.assertEqual(parse_search("/search"), (SEARCH_SECTIONS, ""))
        self.assertEqual(parse_search("/search@some_bot   "), (SEARCH_SECTIONS, ""))


class SearchTests(BotCase):
    """`/search`: the API's own search call, the page fallback and the buttons."""

    async def test_the_search_call_is_asked_per_family_and_the_matches_are_listed(self):
        self.client.iter_messages = MagicMock(return_value=FakeHistory(
            [stored("Matchbox the Movie (2026) [Hindi] [1080p].mp4")]))
        notice = SimpleNamespace(id=21, edit=AsyncMock())
        with patch("ytdown.bot.fetch_parciv_json", new=AsyncMock(return_value=MOVIES)) as fetch:
            await self.service.handle_search(self.event("/search paw patrol", notice=notice),
                                             "/search paw patrol")
        text = notice.edit.call_args.args[0]
        self.assertIn("🔎 apitera search — paw patrol", text)
        self.assertIn("1. ⬇️ PAW Patrol: The Dino Movie (2026)", text)
        self.assertIn("⬇️ 1 of 1 match(es) are new", text)
        buttons = notice.edit.call_args.kwargs["buttons"]
        self.assertEqual([button.text for row in buttons for button in row],
                         ["⬇️ Download new (1)", "⬇️ Download all (1)", "🔁 Refresh", "▶️ Auto on"])
        urls = [call.args[1] for call in fetch.await_args_list]
        # One call per family: the six Moviebox sections share one search call.
        self.assertEqual(sorted(urls),
                         ["https://api.parciv.workers.dev/anime/search?query=paw%20patrol",
                          "https://api.parciv.workers.dev/donghua/search?query=paw%20patrol",
                          "https://api.parciv.workers.dev/moviebox/search?query=paw%20patrol"])
        panel = self.service.latest_panels[7]
        self.assertEqual(panel.words, "paw patrol")
        self.assertEqual([item.title for item in panel.items],
                         ["PAW Patrol: The Dino Movie (2026)"])

    async def test_a_family_without_a_search_call_falls_back_to_its_pages(self):
        notice = SimpleNamespace(id=21, edit=AsyncMock())
        page = CataloguePage("movies", 1, parse_catalogue(MOVIES, "movies"))
        with patch("ytdown.bot.fetch_parciv_json",
                   new=AsyncMock(side_effect=aiohttp.ClientError("HTTP 404"))), \
                patch("ytdown.bot.fetch_parciv_catalogue",
                      new=AsyncMock(return_value=page)) as pages:
            await self.service.handle_search(self.event(notice=notice), "/search movies paw")
        text = notice.edit.call_args.args[0]
        self.assertIn("1. ⬇️ PAW Patrol: The Dino Movie (2026)", text)
        self.assertIn("⚠️", text)
        self.assertIn("scanned", text)
        self.assertEqual(pages.await_count, 2)

    async def test_a_title_listed_in_several_families_is_kept_once(self):
        notice = SimpleNamespace(id=21, edit=AsyncMock())
        page = CataloguePage("movies", 1, parse_catalogue(MOVIES, "movies"))
        with patch("ytdown.bot.fetch_parciv_json",
                   new=AsyncMock(side_effect=aiohttp.ClientError("HTTP 404"))), \
                patch("ytdown.bot.fetch_parciv_catalogue",
                      new=AsyncMock(return_value=page)) as pages:
            await self.service.handle_search(self.event(notice=notice), "/search paw")
        text = notice.edit.call_args.args[0]
        self.assertIn("1. ⬇️ PAW Patrol: The Dino Movie (2026)", text)
        self.assertNotIn("2. ", text)
        # Six Moviebox sections + Anime + Donghua, two pages each.
        self.assertEqual(pages.await_count, 16)

    async def test_a_search_without_words_shows_the_usage(self):
        event = self.event()
        await self.service.handle_search(event, "/search")
        self.assertIn("Use /search <words>", event.respond.call_args.args[0])

    async def test_nothing_matched_is_said_plainly(self):
        notice = SimpleNamespace(id=21, edit=AsyncMock())
        with patch("ytdown.bot.fetch_parciv_json",
                   new=AsyncMock(return_value={"results": []})), \
                patch("ytdown.bot.fetch_parciv_catalogue",
                      new=AsyncMock(return_value=CataloguePage("movies", 1, ()))):
            await self.service.handle_search(self.event(notice=notice), "/search movies zzz")
        self.assertIn("🔎 Nothing matched 'zzz'", notice.edit.call_args.args[0])

    async def test_the_refresh_button_repeats_the_search(self):
        self.service.auto_peers[7] = "peer"
        self.service.latest_panels[7] = SimpleNamespace(
            peer="peer", items=(), sections=("movies",), page=1, words="paw patrol")
        self.service.auto_tokens[7] = "tok"
        self.service.show_search = AsyncMock()
        click = SimpleNamespace(sender_id=7, data=b"a:tok:latest", is_private=True, message_id=21,
                                answer=AsyncMock(), edit=AsyncMock())
        await self.service.callback(click)
        self.assertEqual(click.answer.call_args.args[0], "Searching again…")
        self.assertEqual(self.service.show_search.call_args.args[3], "paw patrol")
        self.assertEqual(self.service.show_search.call_args.args[4], ("movies",))

    async def test_a_search_download_button_queues_the_matches(self):
        self.service.auto_peers[7] = "peer"
        self.service.latest_panels[7] = SimpleNamespace(
            peer="peer", items=parse_catalogue(MOVIES, "movies"), sections=("movies", "anime"),
            page=1, words="paw")
        self.service.auto_tokens[7] = "tok"
        self.service.queue_parciv_job = AsyncMock(return_value=None)
        click = SimpleNamespace(sender_id=7, data=b"a:tok:all", is_private=True, message_id=21,
                                answer=AsyncMock(), edit=AsyncMock())
        await self.service.callback(click)
        batch = self.service.queue_parciv_job.call_args.args[2]
        self.assertEqual(len(batch.items), 3)
        self.assertEqual(batch.sections, ("movies", "anime"))


class ButtonTests(BotCase):
    """Every inline button the bot makes has to reach its callback handler."""

    def test_the_callback_filter_covers_every_button_family(self):
        source = open(os.path.join(os.path.dirname(bot.__file__), "bot.py")).read()
        families = set(re.findall(r'data=f"([a-z]+):', source))
        families |= set(re.findall(r'data=\"([a-z]+):', source))
        self.assertTrue(families)
        pattern = bot.CALLBACK_PATTERN.decode()
        for family in sorted(families):
            self.assertIn(family, pattern, f"{family}: buttons would be ignored by the filter")
        # The filter is only useful if register() really installs it.
        self.assertIn("events.CallbackQuery(pattern=CALLBACK_PATTERN)", source)

    async def test_the_apitera_buttons_are_dispatched(self):
        self.service.auto_tokens[7] = "tok"
        click = SimpleNamespace(sender_id=7, data=b"a:tok:now", is_private=True, message_id=21,
                                answer=AsyncMock(), edit=AsyncMock(),
                                get_input_chat=AsyncMock(return_value="peer"))
        self.service.auto_peers[7] = "peer"
        self.service.run_auto_check = AsyncMock(return_value="Nothing new: 0 listed title(s).")
        await self.service.callback(click)
        self.assertTrue(click.answer.await_count)

    async def test_a_button_opens_the_status_in_the_message_it_sits_on(self):
        batch = ParcivBatch(items=(movie_item(),), check_dm=False)
        click = SimpleNamespace(sender_id=7, id=987654, message_id=21, peer="peer",
                                answer=AsyncMock(), edit=AsyncMock())
        job = await self.service.queue_parciv_job(7, "peer", batch, notice=click)
        self.assertEqual(job.message_id, 21)   # Not the callback's query id.
        self.service.jobs.pop(job.token, None)
        job.task.cancel()
        await asyncio.gather(job.task, return_exceptions=True)


class AutoStatusTests(BotCase):
    """The automatic check shows the normal download status, and one thing at a time."""

    async def test_the_watcher_message_becomes_the_download_status(self):
        sent = SimpleNamespace(id=77, edit=AsyncMock(), delete=AsyncMock())
        self.client.send_message = AsyncMock(return_value=sent)
        self.service.auto_peers[7] = "peer"
        self.service.auto_sections[7] = ("movies",)
        with patch("ytdown.bot.fetch_parciv_catalogue",
                   new=AsyncMock(return_value=CataloguePage("movies", 1,
                                                            parse_catalogue(MOVIES, "movies")))), \
                patch("ytdown.bot.fetch_parciv_media",
                      new=AsyncMock(side_effect=ParcivError("no stream in this test"))), \
                patch("ytdown.bot.fetch_parciv_item", new=AsyncMock(side_effect=lambda s, a, i: i)):
            result = await self.service.run_auto_check(7, note="Automatic check.", announce=True)
            self.assertIn("Queued 3 new title(s)", result)
            self.assertEqual(self.client.send_message.await_args.args[1],
                             "🔎 Automatic apitera check: reading Moviebox movies…")
            job = next(iter(self.service.jobs.values()))
            self.assertEqual(job.message_id, 77)  # The check's message is the job's status.
            await asyncio.gather(job.task, return_exceptions=True)
        # The status card was rendered into that very message.
        texts = [call.args[2] for call in self.client.edit_message.await_args_list]
        self.assertTrue(any(text.startswith("🎬") for text in texts), texts)
        sent.delete.assert_not_awaited()

    async def test_a_check_with_nothing_to_do_removes_its_message(self):
        sent = SimpleNamespace(id=77, edit=AsyncMock(), delete=AsyncMock())
        self.client.send_message = AsyncMock(return_value=sent)
        self.service.auto_peers[7] = "peer"
        self.service.auto_sections[7] = ("movies",)
        with patch("ytdown.bot.fetch_parciv_catalogue",
                   new=AsyncMock(return_value=CataloguePage("movies", 1, parse_catalogue(MOVIES,
                                                                                        "movies")))):
            result = await self.service.run_auto_check(7, note="Automatic check.", announce=True)
        self.assertIn("Queued", result)
        self.service.jobs.clear()

    async def test_the_first_automatic_check_runs_straight_away(self):
        self.service.auto_interval = 3600
        self.service.auto_enabled[7] = True
        self.service.auto_peers[7] = "peer"
        self.service.run_auto_check = AsyncMock(return_value="nothing")
        task = asyncio.create_task(self.service.auto_watch(7))
        await asyncio.sleep(0.05)
        self.assertGreaterEqual(self.service.run_auto_check.await_count, 1)
        self.assertTrue(self.service.run_auto_check.await_args.kwargs["announce"])
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        self.service.auto_enabled[7] = False

    async def test_two_batches_never_download_at_the_same_time(self):
        order = []

        def fake_download(url, out_dir, on_bytes, stop, **kwargs):
            order.append(("start", url))
            path = os.path.join(out_dir, "stream.mp4")
            with open(path, "wb") as handle:
                handle.write(b"\x00\x00\x00\x18ftypmp42" + b"\x00" * (2 * 1024 * 1024))
            order.append(("end", url))
            return path

        async def slow_media(session, api, item):
            order.append(("lookup", item.title))
            return movie_media()

        chunks = (ParcivBatch(items=(movie_item(name="One", subject_id="1"),), check_dm=False,
                              qualities=("360p",)),
                  ParcivBatch(items=(movie_item(name="Two", subject_id="2"),), check_dm=False,
                              qualities=("360p",)))
        with patch("ytdown.bot.fetch_parciv_media", new=slow_media), \
                patch("ytdown.bot.fetch_parciv_item", new=AsyncMock(side_effect=lambda s, a, i: i)), \
                patch("ytdown.bot.download_parciv", new=fake_download), \
                patch("ytdown.bot.thumbnail_jpeg", new=AsyncMock(return_value=None)):
            jobs = [await self.service.queue_parciv_job(7, "peer", batch) for batch in chunks]
            await asyncio.gather(*(job.task for job in jobs), return_exceptions=True)
        self.assertEqual(order[0][0], "lookup")
        self.assertIn("One", order[0][1])
        # Every step of the first batch happens before any step of the second.
        first = max(position for position, (_, name) in enumerate(order) if "One" in name)
        second = min(position for position, (_, name) in enumerate(order) if "Two" in name)
        self.assertLess(first, second)
        self.assertEqual([kind for kind, _ in order].count("lookup"), 2)

    async def test_a_panel_button_keeps_working_after_another_panel_opens(self):
        """Both /auto and /latest hand out the same token, so neither goes stale."""
        notice = SimpleNamespace(id=21, edit=AsyncMock())
        await self.service.handle_auto(self.event(), "/auto")
        token = self.service.auto_tokens[7]
        with patch("ytdown.bot.fetch_parciv_catalogue",
                   new=AsyncMock(return_value=CataloguePage("movies", 1,
                                                            parse_catalogue(MOVIES, "movies")))):
            await self.service.handle_latest(self.event(notice=notice), "/latest movies")
        self.assertEqual(self.service.auto_tokens[7], token)  # Unchanged, not re-minted.
        click = SimpleNamespace(sender_id=7, data=f"a:{token}:now".encode(), is_private=True,
                                message_id=21, answer=AsyncMock(), edit=AsyncMock(),
                                get_input_chat=AsyncMock(return_value="peer"))
        self.service.auto_peers[7] = "peer"
        self.service.run_auto_check = AsyncMock(return_value="Nothing new: 0 listed title(s).")
        await self.service.callback(click)
        self.assertEqual(click.answer.call_args.args[0], "Checking now…")  # Not "expired".

    async def test_a_watcher_check_while_a_batch_runs_queues_nothing(self):
        job = Job(7, "peer", 21, Source("parciv", PARCIV_API_URL), ("All",), None,
                  parciv=ParcivBatch(items=(movie_item(),)))
        self.service.jobs[job.token] = job
        self.service.auto_peers[7] = "peer"
        self.client.send_message = AsyncMock()
        result = await self.service.run_auto_check(7, note="Automatic check.", announce=True)
        self.assertIn("already queued or running", result)
        self.client.send_message.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
