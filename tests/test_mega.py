"""MEGA links: the API's folder listing, the range flow, and the download path."""
import asyncio
import json
import unittest

import aiohttp
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import ytdown.bot as bot
from ytdown.bot import DownloaderBot, Job, MODE_LABELS, is_media_link, normalize_api_endpoint
from ytdown.core import Source
from ytdown.mega import (MAX_MEGA_LINKS, MEGA_API_URL, MegaError, MegaFile, MegaListing, find_mega_links,
                         mega_source, merge_listings, normalize_mega, parse_mega_source, video_choice)

FOLDER_URL = "https://mega.nz/folder/xnlWDCqY#D5WTOPi51IEsiVk0p1yuirAdwUY3ClkZEdLlTjMZvXc"
FILE_URL = "https://mega.nz/file/U69TCIgY#88ai7-MIHIfmMNsmwV3fxUgFNIILUuikIa50HV1KLcc"
OTHER_URL = "https://mega.nz/folder/aaaBBBBB#Zx9-Qkey2ForTheSecondLink00000000000000000"
SECOND_PAYLOAD = {
    "success": True,
    "root_folder_id": "aaaBBBBB",
    "total_files": 2,
    "folders": [],
    "files": [
        {"file_id": "BBB11111", "parent_id": None, "name": "Second link clip.mp4",
         "size_bytes": 4096,
         "download_url": "https://samra-mega-api.onrender.com/api/download?file_id=BBB11111&file_key=k1",
         "stream_proxy": "https://samra-mega-api.onrender.com/api/stream?file_id=BBB11111&file_key=k1",
         "direct_mega_link": "https://mega.nz/file/BBB11111#k1"},
        {"file_id": "BBB22222", "parent_id": None, "name": "Second link notes.txt",
         "size_bytes": 128,
         "download_url": "https://samra-mega-api.onrender.com/api/download?file_id=BBB22222&file_key=k2",
         "stream_proxy": None,
         "direct_mega_link": "https://mega.nz/file/BBB22222#k2"},
    ],
}

# The shape the live API answers with (trimmed to three files and two folders).
PAYLOAD = {
    "success": True,
    "service": "SAMRA MEGA Cloud API",
    "root_folder_id": "xnlWDCqY",
    "target_subfolder_id": None,
    "total_folders": 2,
    "total_files": 3,
    "folders": [
        {"folder_id": "NukXGYhJ", "parent_id": "xq8w2IZQ",
         "name": "Arash Ahadzadeh - UI Design & Figma Mastery"},
        {"folder_id": "Bvlx1aqL", "parent_id": "NukXGYhJ", "name": "02-Module 02 - UI Design Principles"},
    ],
    "files": [
        {"file_id": "omkVyI5J", "parent_id": "Bvlx1aqL", "parent_name": "02-Module 02 - UI Design Principles",
         "name": "01- Intro to Design Principles.mp4.mp4", "file_type": "video", "is_video": True,
         "size_bytes": 20278520, "size_formatted": "19.34 MB", "file_key": "wFfGf6u3",
         "download_url": "https://samra-mega-api.onrender.com/api/download?file_id=omkVyI5J&file_key=wFfGf6u3",
         "stream_proxy": "https://samra-mega-api.onrender.com/api/stream?file_id=omkVyI5J&file_key=wFfGf6u3",
         "direct_mega_link": "https://mega.nz/file/omkVyI5J#wFfGf6u3"},
        {"file_id": "5j8XTA4R", "parent_id": "Bvlx1aqL", "parent_name": "02-Module 02 - UI Design Principles",
         "name": "02- Layout.mp4.ts", "file_type": "video", "is_video": True, "size_bytes": 34033264,
         "download_url": "https://samra-mega-api.onrender.com/api/download?file_id=5j8XTA4R&file_key=kU8hTu6R",
         "stream_proxy": "https://samra-mega-api.onrender.com/api/stream?file_id=5j8XTA4R&file_key=kU8hTu6R",
         "direct_mega_link": "https://mega.nz/file/5j8XTA4R#kU8hTu6R"},
        {"file_id": "kr0X0C6J", "parent_id": "NukXGYhJ", "parent_name": "Arash Ahadzadeh - UI Design & Figma Mastery",
         "name": "Bonus.pdf", "file_type": "document", "is_video": False, "size_bytes": 1888,
         "download_url": "https://samra-mega-api.onrender.com/api/download?file_id=kr0X0C6J&file_key=D5WTOPi5",
         "stream_proxy": None, "direct_mega_link": "https://mega.nz/file/kr0X0C6J#D5WTOPi5"},
    ],
}


class MegaParsingTests(unittest.TestCase):
    def test_accepts_file_and_folder_links(self):
        self.assertEqual(parse_mega_source(FOLDER_URL), FOLDER_URL)
        self.assertEqual(parse_mega_source(FILE_URL), FILE_URL)
        self.assertEqual(parse_mega_source("  https://mega.io/folder/abc123#key  "),
                         "https://mega.io/folder/abc123#key")

    def test_rejects_everything_else(self):
        for link in ("https://mega.nz/folder/xnlWDCqY",  # no # key
                     "https://mega.nz/", "https://mega.nz/folder/",
                     "https://mega.nz/fm/transfer", "https://www.mega.nz.evil.example/folder/abc#k",
                     "http://mega.nz/folder/abc#key", "https://drive.google.com/file/d/abc",
                     "https://mega.nz/folder/" + "a" * 3000):
            with self.assertRaises(ValueError, msg=link):
                parse_mega_source(link)

    def test_media_link_detection_includes_mega(self):
        self.assertTrue(is_media_link(FOLDER_URL))
        self.assertTrue(is_media_link(FILE_URL))
        self.assertFalse(is_media_link("https://example.com/folder/abc#k"))
        self.assertEqual(MODE_LABELS["mega"], "MEGA")


class MegaMultiLinkTests(unittest.TestCase):
    def test_finds_every_link_in_one_message(self):
        message = f"here you go {FOLDER_URL}, plus {FILE_URL} and a stray https://example.com/page"
        self.assertEqual(find_mega_links(message), (FOLDER_URL, FILE_URL))
        self.assertEqual(find_mega_links(FOLDER_URL + "\n" + FOLDER_URL), (FOLDER_URL,))
        self.assertEqual(find_mega_links("no links at all"), ())
        self.assertEqual(mega_source("See " + FOLDER_URL + " and " + FILE_URL), FOLDER_URL)

    def test_the_whole_paste_is_capped(self):
        many = " ".join(f"https://mega.nz/file/Abc{n:04d}#Key{n:04d}" for n in range(12))
        self.assertEqual(len(find_mega_links(many)), MAX_MEGA_LINKS)
        self.assertEqual(len(find_mega_links(many, MAX_MEGA_LINKS + 1)), 11)

    def test_a_merged_listing_keeps_the_files_apart(self):
        merged = merge_listings([normalize_mega(PAYLOAD), normalize_mega(SECOND_PAYLOAD)])
        self.assertTrue(merged.multi)
        self.assertEqual(merged.reported_files, 5)
        self.assertEqual([file.filename for file in merged.files[:3]],
                         ["01- Intro to Design Principles.mp4.mp4", "02- Layout.mp4.ts", "Bonus.pdf"])
        self.assertEqual(merged.files[3].filename, "Second link clip.mp4")
        self.assertTrue(merged.files[0].path.startswith("[1] "))
        self.assertTrue(merged.files[3].path.startswith("[2] "))
        # The numbered list mirrors the paths, so equal file names stay distinct.
        self.assertIn("[2] Second link clip.mp4", merged.names(20)[3])
        # A single link is never labelled, and every file keeps its own URLs.
        self.assertFalse(normalize_mega(PAYLOAD).multi)
        self.assertEqual(merged.files[3].url, "https://samra-mega-api.onrender.com/api/stream?file_id=BBB11111&file_key=k1")

    def test_a_mega_link_is_never_taken_as_an_api_endpoint(self):
        for link in (FOLDER_URL, FILE_URL, "https://mega.co.nz/file/abcDEF12#key"):
            with self.assertRaises(ValueError, msg=link):
                normalize_api_endpoint(link)

    def test_a_message_of_mega_links_counts_as_media(self):
        self.assertTrue(is_media_link(f"{FOLDER_URL} {FILE_URL}"))
        self.assertTrue(is_media_link("See " + FILE_URL))


class MegaNormalizeTests(unittest.TestCase):
    def test_reads_the_live_api_shape(self):
        listing = normalize_mega(PAYLOAD)
        self.assertEqual([file.filename for file in listing.files],
                         ["01- Intro to Design Principles.mp4.mp4", "02- Layout.mp4.ts", "Bonus.pdf"])
        self.assertEqual(listing.reported_files, 3)
        self.assertEqual(len(listing.folders), 2)
        self.assertEqual(listing.folders[1].path,
                         "Arash Ahadzadeh - UI Design & Figma Mastery/02-Module 02 - UI Design Principles")
        first = listing.files[0]
        self.assertEqual(first.path,
                         "Arash Ahadzadeh - UI Design & Figma Mastery/02-Module 02 - UI Design Principles/"
                         "01- Intro to Design Principles.mp4.mp4")
        self.assertEqual(first.size, 20278520)
        self.assertTrue(first.video)
        self.assertEqual(first.suffix, ".mp4")
        self.assertEqual(first.url, first.stream_url)  # Videos stream through the API route.
        self.assertEqual(listing.files[2].url, listing.files[2].download_url)  # Documents download.

    def test_double_extensions_count_as_video(self):
        listing = normalize_mega(PAYLOAD)
        self.assertTrue(listing.files[1].video)  # "02- Layout.mp4.ts" is MPEG-TS.
        # .ts is TypeScript to mimetypes.guess_type; the API's own kinds win.
        self.assertEqual(listing.files[1].mime, "video/mp2t")
        self.assertEqual(listing.files[1].title, "02- Layout")
        self.assertEqual(listing.files[0].title, "01- Intro to Design Principles")

    def test_alternates_deduplicate(self):
        file = normalize_mega(PAYLOAD).files[0]
        self.assertEqual(file.alternates(), (file.stream_url, file.download_url, file.direct_link))
        only_direct = MegaFile("id", "a.pdf", "p", "P", 10, "application/pdf", "", "", "https://mega.nz/file/a#k")
        self.assertEqual(only_direct.alternates(), ("https://mega.nz/file/a#k",))

    def test_names_are_numbered_for_the_range_prompt(self):
        listing = normalize_mega(PAYLOAD)
        self.assertEqual(listing.names(2), ["1. 01- Intro to Design Principles.mp4.mp4",
                                            "2. 02- Layout.mp4.ts", "… and 1 more"])

    def test_every_file_kind_gets_a_choice(self):
        listing = normalize_mega(PAYLOAD)
        self.assertEqual([video_choice(file) for file in listing.files],
                         ["Video", "Video", "File"])

    def test_api_errors_are_reported_with_their_own_message(self):
        with self.assertRaises(MegaError) as caught:
            normalize_mega({"success": False, "error": "Invalid MEGA link or expired key"})
        self.assertIn("Invalid MEGA link", str(caught.exception))
        for payload in ({}, {"success": True, "files": []}, "nonsense"):
            with self.assertRaises(MegaError):
                normalize_mega(payload)

    def test_tolerates_wrapped_and_thin_shapes(self):
        wrapped = normalize_mega({"status": True, "data": {
            "files": [{"name": "clip.mp4", "size": "1024",
                       "direct_mega_link": "https://mega.nz/file/abc#k"}]}})
        self.assertEqual(wrapped.files[0].size, 1024)
        self.assertEqual(wrapped.files[0].url, "https://mega.nz/file/abc#k")
        truncated = normalize_mega({"success": True, "total_files": 900,
                                    "files": PAYLOAD["files"]})
        self.assertEqual(truncated.reported_files, 900)
        self.assertFalse(truncated.truncated)  # Under the cap: nothing was dropped.
        # A listing whose every entry lacks a usable URL is an error, not an empty job.
        self.assertIn("no downloadable files",
                      str(self._error({"success": True, "files": [{"name": "x"}]})))

    @staticmethod
    def _error(payload) -> MegaError:
        try:
            normalize_mega(payload)
        except MegaError as exc:
            return exc
        raise AssertionError("expected a MegaError")


class MegaFlowTests(unittest.IsolatedAsyncioTestCase):
    """The bot side: folder range prompt, selection, and the streaming job."""

    def setUp(self):
        self.client = MagicMock()
        self.client.send_message = AsyncMock(return_value=SimpleNamespace(id=11))
        self.client.send_file = AsyncMock(return_value=SimpleNamespace(id=12))
        self.client.edit_message = AsyncMock()
        self.service = DownloaderBot(self.client, MagicMock())
        self.service.progress = AsyncMock()  # Status edits are not what these tests check.
        self.service.modes[7] = "mega"

    def event(self, text, *, uid=7, notice=None):
        return SimpleNamespace(sender_id=uid, raw_text=text, is_private=True, message_id=5,
                               message=SimpleNamespace(document=None, attributes=[]),
                               client=self.client, answer=AsyncMock(),
                               respond=AsyncMock(return_value=notice or SimpleNamespace(
                                   edit=AsyncMock(), respond=AsyncMock())),
                               get_input_chat=AsyncMock(return_value="peer"))

    def listing(self):
        return normalize_mega(PAYLOAD)

    async def test_a_folder_link_asks_for_a_range_and_lists_the_files(self):
        notice = SimpleNamespace(edit=AsyncMock())
        event = self.event(FOLDER_URL, notice=notice)
        with patch("ytdown.bot.fetch_mega", new=AsyncMock(return_value=self.listing())) as fetch:
            await self.service.on_link(event)
        self.assertEqual(fetch.call_args.args[1], FOLDER_URL)
        self.assertEqual(fetch.call_args.args[2], MEGA_API_URL)
        text = notice.edit.call_args.args[0]
        self.assertIn("MEGA folder: 3 files, 2 subfolders", text)
        self.assertIn("1. 01- Intro to Design Principles.mp4.mp4", text)
        self.assertIn("Send an inclusive range", text)
        request = self.service.pending_ranges[7]
        self.assertEqual(request.count, 3)
        self.assertIsInstance(request.listing, MegaListing)

    async def test_a_range_offers_one_choice_per_file_kind(self):
        with patch("ytdown.bot.fetch_mega", new=AsyncMock(return_value=self.listing())):
            await self.service.on_link(self.event(FOLDER_URL, notice=SimpleNamespace(edit=AsyncMock())))
        range_event = self.event("1-3")
        await self.service.on_range(range_event)
        selection = self.service.selections[7]
        self.assertEqual(selection.choices, ("Video", "File"))
        self.assertEqual((selection.start, selection.end), (1, 3))
        self.assertEqual(selection.listing.files[2].filename, "Bonus.pdf")
        self.assertIn("Entries 1-3 of 3", range_event.respond.call_args.args[0])

    async def test_two_links_are_read_together_as_one_job(self):
        notice = SimpleNamespace(edit=AsyncMock())
        listings = [normalize_mega(PAYLOAD), normalize_mega(SECOND_PAYLOAD)]
        event = self.event(f"{FOLDER_URL} \n {OTHER_URL}", notice=notice)
        with patch("ytdown.bot.fetch_mega", new=AsyncMock(side_effect=listings)) as fetch:
            await self.service.on_link(event)
        self.assertEqual(fetch.await_count, 2)
        self.assertEqual([call.args[1] for call in fetch.await_args_list], [FOLDER_URL, OTHER_URL])
        text = notice.edit.call_args.args[0]
        self.assertIn("2 MEGA links: 5 files", text)
        self.assertNotIn("Skipped", text)
        self.assertEqual(self.service.pending_ranges[7].count, 5)
        self.assertEqual(self.service.selections, {})

    async def test_a_broken_link_does_not_sink_the_batch(self):
        notice = SimpleNamespace(edit=AsyncMock())
        outcomes = [MegaError("This folder is unavailable."), normalize_mega(PAYLOAD)]
        event = self.event(f"{OTHER_URL} {FILE_URL}", notice=notice)
        with patch("ytdown.bot.fetch_mega", new=AsyncMock(side_effect=outcomes)):
            await self.service.on_link(event)
        text = notice.edit.call_args.args[0]
        self.assertIn("2 MEGA links: 3 files", text)
        self.assertIn("⚠️ Skipped 1 link(s): link 1 (This folder is unavailable.)", text)
        self.assertEqual(self.service.pending_ranges[7].count, 3)

    async def test_every_link_failing_is_reported_with_the_reason(self):
        notice = SimpleNamespace(edit=AsyncMock())
        event = self.event(f"{OTHER_URL} {FILE_URL}", notice=notice)
        with patch("ytdown.bot.fetch_mega",
                   new=AsyncMock(side_effect=[MegaError("no such folder"), MegaError("no such file")])):
            await self.service.on_link(event)
        reported = notice.edit.call_args.args[0]
        self.assertIn("Could not load this link. None of those MEGA links could be read.", reported)
        self.assertIn("no such file", reported)
        self.assertEqual(self.service.selections, {})
        self.assertEqual(self.service.pending_ranges, {})

    async def test_too_many_links_are_refused_before_any_lookup(self):
        many = " ".join(f"https://mega.nz/file/Abc{n:04d}#Key{n:04d}" for n in range(MAX_MEGA_LINKS + 1))
        event = self.event(many)
        with patch("ytdown.bot.fetch_mega", new=AsyncMock()) as fetch:
            await self.service.on_link(event)
        self.assertIn(f"at most {MAX_MEGA_LINKS} MEGA links", event.respond.call_args.args[0])
        fetch.assert_not_awaited()

    async def test_a_range_over_two_links_streams_files_from_both(self):
        listings = [normalize_mega(PAYLOAD), normalize_mega(SECOND_PAYLOAD)]
        with patch("ytdown.bot.fetch_mega", new=AsyncMock(side_effect=listings)):
            await self.service.on_link(self.event(f"{FOLDER_URL} {OTHER_URL}"))
        await self.service.on_range(self.event("1-5"))
        selection = self.service.selections[7]
        selection.selected = {"Video", "File"}
        self.service.run_job = AsyncMock()
        click = SimpleNamespace(sender_id=7, data=f"s:{selection.token}:done".encode(),
                                is_private=True, message_id=11, answer=AsyncMock())
        await self.service.callback(click)
        job = self.service.user_jobs(7)[0]
        self.assertEqual(job.start, 1)
        self.assertEqual(job.end, 5)
        urls = []
        self.service.send_stream = AsyncMock(side_effect=lambda *args, **kwargs: urls.append(args[3]))
        await self.service.run_mega(job)
        self.assertEqual(len(urls), 5)
        self.assertIn("api/stream?file_id=omkVyI5J", urls[0])  # First link.
        self.assertIn("api/stream?file_id=BBB11111", urls[3])  # Second link.
        self.assertIn("api/download?file_id=BBB22222", urls[4])

    async def test_a_single_file_link_offers_its_kind(self):
        notice = SimpleNamespace(edit=AsyncMock())
        listing = normalize_mega({**PAYLOAD, "files": [PAYLOAD["files"][0]]})
        with patch("ytdown.bot.fetch_mega", new=AsyncMock(return_value=listing)):
            await self.service.on_link(self.event(FILE_URL, notice=notice))
        selection = self.service.selections[7]
        self.assertEqual(selection.choices, ("Video",))
        self.assertEqual(selection.listing.files[0].filename, "01- Intro to Design Principles.mp4.mp4")
        self.assertIn("Choose it, then tap Done", notice.edit.call_args.args[0])

    async def job(self, selected=("Video", "File"), start=1, end=3) -> Job:
        listing = self.listing()
        job = Job(7, "peer", 5, Source("mega", FOLDER_URL), selected, None,
                  start=start, end=end, listing=listing)
        self.service.jobs[job.token] = job
        return job

    async def test_run_mega_streams_every_selected_kind(self):
        job = await self.job()
        sent = []

        async def fake_send(job, video, quality, url, index, total, source_file=None):
            sent.append((video.title, quality, url, index, total, source_file.filename))

        self.service.send_stream = AsyncMock(side_effect=fake_send)
        await self.service.run_mega(job)
        self.assertEqual([item[1] for item in sent], ["Video", "Video", "File"])
        self.assertEqual([item[3] for item in sent], [1, 2, 3])
        self.assertEqual(sent[0][2], self.listing().files[0].stream_url)
        self.assertEqual(sent[2][2], self.listing().files[2].download_url)
        self.assertIn("Sent 3 file(s) from MEGA", self.service.progress.call_args.args[1])
        self.assertEqual([call.args[2] for call in self.service.send_stream.await_args_list],
                         ["Video", "Video", "File"])

    async def test_only_the_selected_kind_is_downloaded(self):
        job = await self.job(selected=("File",))
        self.service.send_stream = AsyncMock()
        await self.service.run_mega(job)
        self.assertEqual(self.service.send_stream.await_count, 1)
        self.assertEqual(self.service.send_stream.call_args.args[6].filename, "Bonus.pdf")

    async def test_a_refused_url_falls_back_to_the_direct_link_for_the_same_file(self):
        from dataclasses import replace
        listing = self.listing()
        job = await self.job(selected=("File",))
        job.listing = replace(listing, files=(listing.files[2],))
        job.start = job.end = 1
        attempts = []

        async def fake_send(job, video, quality, url, index, total, source_file=None):
            attempts.append(url)
            if len(attempts) == 1:
                raise ValueError("HTTP 403")

        self.service.send_stream = AsyncMock(side_effect=fake_send)
        await self.service.run_mega(job)
        # The API's download route refused the key, so the file's direct MEGA
        # link is tried before the file counts as failed.
        self.assertEqual(attempts, ["https://samra-mega-api.onrender.com/api/download?file_id=kr0X0C6J&file_key=D5WTOPi5",
                                    "https://mega.nz/file/kr0X0C6J#D5WTOPi5"])
        self.assertIn("Sent 1 file(s)", self.service.progress.call_args.args[1])

    async def test_an_oversized_file_is_refused_before_downloading(self):
        from dataclasses import replace
        listing = self.listing()
        job = await self.job(selected=("Video",))
        job.listing = replace(listing, files=(listing.files[0],))
        self.service.max_bytes = 1000
        self.service.send_stream = AsyncMock()
        await self.service.run_mega(job)
        self.service.send_stream.assert_not_called()
        self.assertIn("over the size limit", self.service.progress.call_args.args[1])

    async def test_an_api_failure_is_shown_as_a_message(self):
        notice = SimpleNamespace(edit=AsyncMock())
        with patch("ytdown.bot.fetch_mega", new=AsyncMock(side_effect=MegaError("MEGA API could not resolve this link."))):
            await self.service.on_link(self.event(FOLDER_URL, notice=notice))
        self.assertEqual(self.service.selections, {})
        self.assertIn("Could not load this link", notice.edit.call_args.args[0])

    async def test_the_done_button_queues_a_mega_job(self):
        notice = SimpleNamespace(edit=AsyncMock())
        with patch("ytdown.bot.fetch_mega", new=AsyncMock(return_value=self.listing())):
            await self.service.on_link(self.event(FOLDER_URL, notice=notice))
        await self.service.on_range(self.event("1-3", notice=SimpleNamespace(edit=AsyncMock())))
        selection = self.service.selections[7]
        selection.selected = {"Video"}
        self.service.run_job = AsyncMock()
        click = SimpleNamespace(sender_id=7, data=f"s:{selection.token}:done".encode(),
                                is_private=True, message_id=11, answer=AsyncMock())
        await self.service.callback(click)
        job = self.service.user_jobs(7)[0]
        self.assertEqual(job.selected, ("Video",))
        self.assertEqual(job.title, "01- Intro to Design Principles")
        self.assertIsInstance(job.listing, MegaListing)
        await asyncio.gather(job.task, return_exceptions=True)

    async def test_a_listing_with_no_files_is_reported_not_queued(self):
        from dataclasses import replace
        notice = SimpleNamespace(edit=AsyncMock())
        with patch("ytdown.bot.fetch_mega",
                   new=AsyncMock(return_value=replace(self.listing(), files=()))):
            await self.service.on_link(self.event(FILE_URL, notice=notice))
        self.assertIn("Could not load this link", notice.edit.call_args.args[0])
        self.assertEqual(self.service.selections, {})

    async def test_is_media_link_keeps_mega_out_of_an_api_prompt(self):
        from ytdown.bot import normalize_api_endpoint
        with self.assertRaisesRegex(ValueError, "not a MEGA file"):
            normalize_api_endpoint(FOLDER_URL)


class MegaPatienceTests(unittest.IsolatedAsyncioTestCase):
    """The MEGA API sleeps when idle: the bot waits and says so instead of failing."""

    async def test_a_sleeping_api_is_retried_and_reported(self):
        waits = []
        payload = {"success": True, "files": [
            {"file_id": "AAA11111", "name": "Woke up clip.mp4", "size_bytes": 4096,
             "download_url": "https://samra-mega-api.onrender.com/api/download?file_id=AAA11111",
             "direct_mega_link": "https://mega.nz/file/AAA11111#k1"}]}
        responses = [aiohttp.ClientConnectionError("connection reset"),
                     json.JSONDecodeError("bad json", "", 0), payload]

        async def fake(session, endpoint, url="", max_bytes=0, timeout=None, params=None):
            answer = responses.pop(0)
            if isinstance(answer, BaseException):
                raise answer
            return answer

        async def waited(attempt, delay, attempts):
            waits.append((attempt, delay, attempts))

        with patch("ytdown.bot.fetch_json_api", new=fake), \
                patch("ytdown.bot.asyncio.sleep", new=AsyncMock()) as sleep:
            listing = await bot.fetch_mega(MagicMock(), "https://mega.nz/file/abc#key", on_wait=waited)
        self.assertEqual(listing.files[0].filename, "Woke up clip.mp4")
        self.assertEqual([attempt for attempt, _, _ in waits],
                         [1, 2])  # Two waits, then the answer.
        self.assertEqual(len(bot.MEGA_RETRY_DELAYS), 4)
        self.assertEqual([call.args[0] for call in sleep.await_args_list],
                         list(bot.MEGA_RETRY_DELAYS[:2]))
        self.assertIn("not answering yet", bot.mega_wait_text(2, 10, 5))
        self.assertIn("waiting 10s", bot.mega_wait_text(2, 10, 5))

    async def test_the_last_attempt_hands_the_api_reason_to_the_user(self):
        calls = []

        async def fake(session, endpoint, url="", max_bytes=0, timeout=None, params=None):
            calls.append(url)
            return {"success": False, "error": "MEGA API could not resolve this link."}

        with patch("ytdown.bot.fetch_json_api", new=fake), \
                patch("ytdown.bot.asyncio.sleep", new=AsyncMock()) as sleep:
            with self.assertRaises(MegaError) as caught:
                await bot.fetch_mega(MagicMock(), "https://mega.nz/file/abc#key")
        self.assertIn("could not resolve", str(caught.exception))
        self.assertEqual(sleep.await_count, len(bot.MEGA_RETRY_DELAYS))

    async def test_the_bots_own_refusals_are_not_retried(self):
        """A safety or link check must fail at once, not sleep through the ladder."""
        async def fake(session, endpoint, url="", max_bytes=0, timeout=None, params=None):
            raise ValueError("API redirected to an unsafe address.")

        with patch("ytdown.bot.fetch_json_api", new=fake), \
                patch("ytdown.bot.asyncio.sleep", new=AsyncMock()) as sleep:
            with self.assertRaises(ValueError):
                await bot.fetch_mega(MagicMock(), "https://mega.nz/file/abc#key")
        sleep.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
