"""apitera readers: the catalogue, the stream/post answers, names and title matching."""
import json
import os
import shutil
import tempfile
import threading
import unittest
import urllib.error
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from ytdown.dailymotion import DownloadCancelled
from ytdown.apitera import (already_uploaded, listing_lines, new_items, parse_interval, parse_limit,
                            past_titles, interval_text)
from ytdown.parciv import (AUTO_SECTIONS, PARCIV_API_URL, ParcivApi, ParcivError, ParcivItem,
                            family_base, matches_words, search_url,  # noqa: F401 (used below)
                           download_parciv, download_with_ytdlp, is_parciv_media_endpoint,
                           leech_parciv, leech_url, media_url, message_titles, public_media_url,
                           normalize_section, parse_catalogue, parse_detail, parse_parciv_source,
                           parse_post, parse_sections, parse_stream, pick_media, quality_label,
                           file_title, item_from_dict, json_media_link, leech_link_media,
                           media_file_url, size_of, source_from_dict, title_key, titles_match)

# The real answers, trimmed: /moviebox/movies page 1 and /moviebox/stream?id=….
MOVIES = {"page": 1, "results": [
    {"subjectId": "4183823238805478232", "name": "Matchbox the Movie", "type": 1,
     "poster": "https://pbcdnw.aoneroom.com/image/2026/10/13/28e4d826ed481819a774898ddd252881.jpg",
     "year": "2026", "rating": "5.7", "genre": "Action,Adventure,Comedy", "country": "United States",
     "duration": None, "desc": None},
    {"subjectId": "6026412232966389904", "name": "Spider-Man: Hari Baru", "year": "2026"},
    {"subjectId": "3920183929835251392", "name": "PAW Patrol: The Dino Movie_360P", "year": "2026",
     "desc": "The pups head to the dinosaur park."},
]}
STREAM = {
    "subjectId": "3920183929835251392", "se": 0, "ep": 0,
    "title": "PAW Patrol: The Dino Movie_360P",
    "streams": [{
        "streamId": "7088097462069905984", "resolution": "1080,720,480", "codec": "hevc",
        "format": "DASH",
        "url": "https://sacdn.hakunaymatata.com/dash/3920183929835251392_0_0_1080_h265_258/index_web.mpd",
        "size": "2604346171", "duration": 5800, "se": 0, "ep": 0,
        "signCookie": "CloudFront-Policy=eyJTdGF0ZW1lbnQiO3;CloudFront-Signature=Qlf9bqQF;"
                      "CloudFront-Key-Pair-Id=KMHN1LQ1HEUPL;",
    }],
    "resources": [
        {"url": "https://bcdnt.hakunaymatata.com/bt/3751c85c.mp4?sign=2e90f0b5&amp;t=1791999001",
         "duration": 5800, "resolution": "360p", "codec": "h264", "linkType": "OutsideUrl"},
        {"url": "https://bcdnt.hakunaymatata.com/bt/4269dcd6.mp4?sign=ced48abf&amp;t=1791999105",
         "duration": 5800, "resolution": "480p", "codec": "h264", "linkType": "OutsideUrl"},
        {"url": "https://bcdnt.hakunaymatata.com/resource/928dc686.mp4?sign=c6b83e7c&amp;t=1791999596",
         "duration": 5800, "resolution": "1080p", "codec": "h264", "linkType": "OutsideUrl"},
    ],
    "subtitles": [
        {"lang": "en", "name": None, "indonesian": False},
        {"lang": "es", "name": None},
        {"lang": "fr", "name": None},
        {"lang": "hi", "name": None},
        {"lang": "in_id", "name": None, "indonesian": True},
        {"lang": "ms", "name": None},
        {"lang": "pt", "name": None},
    ],
    "powered_by": "apitera",
}
ANIME_LATEST = {"status": True, "data": {"anime_list": [
    {"title": "One Piece", "slug": "one-piece", "poster": "https://img.example.com/op.jpg",
     "current_episode": 1130, "langs": ["Japanese", "Indonesian Sub"]},
    {"title": "Jujutsu Kaisen", "slug": "jujutsu-kaisen", "animeId": "jj-42", "episode": 3},
]}}
DONGHUA_LATEST = {"status": True, "data": {"list": [
    {"subjectId": "dong-1", "name": "Battle Through the Heavens", "poster": "https://img.example.com/d1.jpg",
     "languages": [{"lang": "in_id"}]},
]}}
ANIME_POST = {"data": {
    "title": "One Piece Episode 1130",
    "poster": "https://img.example.com/op1130.jpg",
    "desc": "Luffy sets sail again.",
    "languages": ["Japanese", "English Sub"],
    "videos": [
        {"quality": "360p", "url": "https://cdn.example.com/op-360.mp4", "size": 42000000},
        {"quality": "480p", "url": "https://cdn.example.com/op-480.mp4"},
        {"quality": "720p", "url": "https://cdn.example.com/op-720.mp4", "size": 130000000},
        {"quality": "1080p", "url": "https://cdn.example.com/op-1080.mp4"},
    ],
    "subtitles": [{"lang": "in_id"}, {"lang": "en"}],
}}


def stored_file(name: str, caption: str = "") -> SimpleNamespace:
    """A DM message that carries one document, the way Telethon hands it over."""
    document = SimpleNamespace(attributes=[SimpleNamespace(file_name=name)])
    return SimpleNamespace(document=document, file=SimpleNamespace(name=name), message=caption)


class CatalogueTests(unittest.TestCase):
    def test_movies_page_reads_the_live_shape(self):
        items = parse_catalogue(MOVIES, "movies", ParcivApi(PARCIV_API_URL))
        self.assertEqual([item.subject_id for item in items],
                         ["4183823238805478232", "6026412232966389904", "3920183929835251392"])
        self.assertEqual(items[0].title, "Matchbox the Movie (2026)")
        self.assertEqual(items[0].poster,
                         "https://pbcdnw.aoneroom.com/image/2026/10/13/28e4d826ed481819a774898ddd252881.jpg")
        # "_360P" is part of the API's own name, never part of the upload name.
        self.assertEqual(items[2].title, "PAW Patrol: The Dino Movie (2026)")
        self.assertEqual(items[2].summary, "The pups head to the dinosaur park.")
        self.assertEqual(items[2].kind, "Moviebox movies")

    def test_a_year_already_in_the_name_is_not_repeated(self):
        payload = {"results": [{"subjectId": "1", "name": "Dune Part Two 2024", "year": "2024"}]}
        self.assertEqual(parse_catalogue(payload, "movies")[0].title, "Dune Part Two 2024")

    def test_anime_latest_uses_its_slugs_for_the_post_call(self):
        api = ParcivApi(PARCIV_API_URL)
        items = parse_catalogue(ANIME_LATEST, "anime", api)
        self.assertEqual([item.subject_id for item in items], ["one-piece", "jj-42"])
        self.assertEqual(items[0].title, "One Piece")
        self.assertEqual(items[0].langs, ("Japanese",))  # "Indonesian Sub" is a subtitle.
        self.assertEqual(items[0].episode, "")
        self.assertEqual(items[1].episode, "Episode 3")
        self.assertEqual(api.post("anime", items[0]),
                         "https://api.parciv.workers.dev/anime/post?slug=one-piece")
        self.assertEqual(items[1].post_url, "https://api.parciv.workers.dev/anime/post?slug=jujutsu-kaisen")

    def test_donghua_latest_reads_its_own_key(self):
        items = parse_catalogue(DONGHUA_LATEST, "donghua", ParcivApi(PARCIV_API_URL))
        self.assertEqual(items[0].subject_id, "dong-1")
        self.assertEqual(items[0].langs, ("Indonesian",))
        self.assertEqual(items[0].kind, "Donghua")

    def test_an_answer_without_entries_is_empty_not_an_error(self):
        self.assertEqual(parse_catalogue({"status": True, "data": {}}, "anime"), ())
        self.assertEqual(parse_catalogue(["nope"], "anime"), ())
        self.assertEqual(parse_catalogue(None, "movies"), ())

    def test_an_entry_without_a_name_or_id_is_skipped(self):
        payload = {"results": [{"subjectId": "1"}, {"name": "No id here"}, {"subjectId": "3", "name": "Fine"}]}
        items = parse_catalogue(payload, "movies")
        self.assertEqual([item.subject_id for item in items], ["3"])

    def test_detail_adds_the_name_and_cover(self):
        api = ParcivApi(PARCIV_API_URL)
        item = parse_catalogue(MOVIES, "movies", api)[1]
        detail = parse_detail({"data": {"subjectId": item.subject_id, "name": "Spider-Man: Hari Baru",
                                        "year": "2026", "poster": "https://img.example.com/sp.jpg",
                                        "desc": "A new day.", "langs": ["Hindi", "English"]}}, item)
        self.assertEqual(detail.title, "Spider-Man: Hari Baru (2026)")
        self.assertEqual(detail.poster, "https://img.example.com/sp.jpg")
        self.assertEqual(detail.summary, "A new day.")
        self.assertEqual(detail.langs, ("Hindi", "English"))

    def test_a_detail_that_is_an_episode_list_changes_nothing(self):
        item = parse_catalogue(MOVIES, "movies", ParcivApi(PARCIV_API_URL))[0]
        self.assertEqual(parse_detail({"data": [{"ep": 3, "title": "Some Episode"}]}, item), item)
        self.assertEqual(parse_detail({"message": "not found"}, item), item)


class StreamTests(unittest.TestCase):
    def media(self):
        return parse_stream(STREAM, "3920183929835251392")[0]

    def test_every_quality_is_listed_with_its_own_link(self):
        media = self.media()
        sources = media.labelled_sources()
        self.assertEqual([source.label for source in sources],
                         ["360p", "480p", "1080p", "1080p DASH"])
        self.assertEqual(sources[0].url, "https://bcdnt.hakunaymatata.com/bt/3751c85c.mp4?sign=2e90f0b5&amp;t=1791999001")
        self.assertEqual(sources[0].size, 0)  # Only the DASH stream reported one.
        self.assertEqual(sources[3].size, 2604346171)
        self.assertTrue(sources[3].dash)
        self.assertFalse(sources[0].dash)
        self.assertEqual((sources[2].width, sources[2].height), (1920, 1080))
        self.assertEqual(sources[3].header_dict["Cookie"],
                         "CloudFront-Policy=eyJTdGF0ZW1lbnQiO3;CloudFront-Signature=Qlf9bqQF;"
                         "CloudFront-Key-Pair-Id=KMHN1LQ1HEUPL;")

    def test_the_title_and_duration_come_from_the_answer(self):
        media = self.media()
        self.assertEqual(media.title, "PAW Patrol: The Dino Movie")
        self.assertEqual(media.duration, 5800)
        self.assertEqual(media.episode, "")
        self.assertEqual(media.subject_id, "3920183929835251392")

    def test_the_subtitle_language_becomes_a_tag(self):
        media = self.media()
        self.assertEqual(media.subtitles,
                         ("English", "Spanish", "French", "Hindi", "Indonesian", "Malay", "Portuguese"))
        self.assertEqual(media.name_tags(), "[English Sub]")
        self.assertEqual(media.display_title(), "PAW Patrol: The Dino Movie [English Sub]")

    def test_audio_languages_come_before_the_subtitle(self):
        payload = dict(STREAM, langs=["Hindi", "Tamil"],
                       subtitles=[{"lang": "en"}, {"lang": "hi"}])
        media = parse_stream(payload)[0]
        self.assertEqual(media.name_tags(), "[Hindi] [Tamil] [English Sub]")
        self.assertEqual(media.display_title(), "PAW Patrol: The Dino Movie [Hindi] [Tamil] [English Sub]")

    def test_a_stream_answer_with_several_episodes_picks_the_right_one(self):
        payload = {"data": [
            {"title": "Show_720P", "se": 1, "ep": 1, "resources": [{"url": "https://cdn.example.com/e1.mp4", "resolution": "720p"}]},
            {"title": "Show_720P", "se": 1, "ep": 2, "resources": [{"url": "https://cdn.example.com/e2.mp4", "resolution": "720p"}]},
        ]}
        media = parse_stream(payload)
        self.assertEqual([entry.episode for entry in media], ["Episode 1", "Episode 2"])
        item = ParcivItem(subject_id="x", name="Show", episode="Episode 2")
        self.assertEqual(pick_media(media, item).sources[0].url, "https://cdn.example.com/e2.mp4")
        self.assertEqual(pick_media(media, ParcivItem("x", "Show")).sources[0].url, "https://cdn.example.com/e1.mp4")

    def test_an_answer_without_media_is_an_error(self):
        with self.assertRaises(ParcivError):
            parse_stream({"subjectId": "1", "streams": [], "resources": []})


class PostTests(unittest.TestCase):
    def test_an_anime_post_becomes_one_media(self):
        api = ParcivApi(PARCIV_API_URL)
        item = parse_catalogue(ANIME_LATEST, "anime", api)[0]
        media = parse_post(ANIME_POST, item)
        self.assertEqual(media.title, "One Piece Episode 1130")
        self.assertEqual(media.poster, "https://img.example.com/op1130.jpg")
        self.assertEqual(media.summary, "Luffy sets sail again.")
        self.assertEqual([source.label for source in media.sources],
                         ["360p", "480p", "720p", "1080p"])
        self.assertEqual(media.sources[0].size, 42000000)
        self.assertEqual(media.langs, ("Japanese",))
        self.assertEqual(media.subtitles, ("Indonesian", "English"))
        self.assertEqual(media.display_title(media.sources[3]),
                         "One Piece Episode 1130 [Japanese] [Indonesian Sub]")

    def test_other_key_names_work_too(self):
        payload = {"result": {"name": "Donghua Ep 12", "downloads": [
            {"label": "SD", "format": "mp4", "url": "https://cdn.example.com/d-sd.mp4"},
            {"label": "HD", "format": "mp4", "url": "https://cdn.example.com/d-hd.mp4"},
        ], "subtitle": [{"lang": "id"}]}}
        media = parse_post(payload)
        self.assertEqual(media.title, "Donghua Ep 12")
        self.assertEqual([source.label for source in media.sources], ["SD", "HD"])
        self.assertEqual(media.subtitles, ("Indonesian",))

    def test_a_post_without_media_is_an_error(self):
        with self.assertRaises(ParcivError):
            parse_post({"data": {"title": "Nothing here"}})


class TitleMatchTests(unittest.TestCase):
    def test_a_catalogue_name_matches_an_uploaded_file(self):
        self.assertTrue(titles_match("PAW Patrol: The Dino Movie",
                                     "PAW Patrol_ The Dino Movie [Hindi] [Eng Sub] [360p].mp4"))
        self.assertTrue(titles_match("Matchbox the Movie", "Matchbox the Movie (2026) [1080p].mp4"))
        self.assertTrue(titles_match("One Piece", "One Piece Episode 1130 [Japanese] [720p].mp4"))
        self.assertFalse(titles_match("Matchbox the Movie", "Spider-Man_ Hari Baru [480p].mp4"))
        self.assertFalse(titles_match("", "PAW Patrol [360p].mp4"))

    def test_a_short_common_start_does_not_match(self):
        self.assertFalse(titles_match("Paw", "Paw Patrol The Movie [360p].mp4"))
        self.assertEqual(title_key("A_ B"), "a b")

    def test_message_titles_reads_the_file_name_and_the_caption(self):
        message = stored_file("One Piece [Hindi] [720p].mp4", "One Piece Episode 1130")
        self.assertEqual(message_titles(message),
                         ("One Piece [Hindi] [720p].mp4", "One Piece Episode 1130"))
        attribute_only = SimpleNamespace(document=SimpleNamespace(attributes=[
            SimpleNamespace(file_name="From Attribute.mkv")]), file=None, message="")
        self.assertEqual(message_titles(attribute_only), ("From Attribute.mkv",))
        self.assertEqual(message_titles(SimpleNamespace(document=None, message="hello")), ("hello",))

    def test_the_dm_index_and_the_check_agree(self):
        keys = past_titles([stored_file("Matchbox the Movie (2026) [1080p].mp4"),
                            SimpleNamespace(document=None, message="One Piece Episode 1130")])
        self.assertTrue(already_uploaded("Matchbox the Movie", keys))
        self.assertTrue(already_uploaded("One Piece", keys))
        self.assertFalse(already_uploaded("Spider-Man: Hari Baru", keys))
        self.assertEqual(len(new_items((ParcivItem("a", "Matchbox the Movie"),
                                        ParcivItem("b", "Spider-Man: Hari Baru")), keys)), 1)


class ApiUrlTests(unittest.TestCase):
    def test_the_endpoint_may_be_the_root_or_the_moviebox_base(self):
        for endpoint in ("https://api.parciv.workers.dev", "https://api.parciv.workers.dev/moviebox",
                         "https://api.parciv.workers.dev/moviebox/"):
            api = ParcivApi(endpoint)
            self.assertEqual(api.section("movies"), "https://api.parciv.workers.dev/moviebox/movies?page=1")
            self.assertEqual(api.section("anime"), "https://api.parciv.workers.dev/anime/latest")
            self.assertEqual(api.stream("42"), "https://api.parciv.workers.dev/moviebox/stream?id=42")
            self.assertEqual(api.detail("42"), "https://api.parciv.workers.dev/moviebox/detail?subjectId=42")

    def test_pages_and_sections_are_validated(self):
        api = ParcivApi(PARCIV_API_URL)
        self.assertTrue(api.section("tv", 3).endswith("/moviebox/tv?page=3"))
        with self.assertRaises(ParcivError):
            api.section("nope")

    def test_section_names_people_type(self):
        self.assertEqual(normalize_section("moviebox/animation"), "animation")
        self.assertEqual(normalize_section("Anime latest"), "anime")
        self.assertEqual(normalize_section("tv-shows"), "tvshows")
        self.assertEqual(parse_sections("anime, donghua movies"), ("anime", "donghua", "movies"))
        with self.assertRaises(ParcivError):
            parse_sections("netflix")
        self.assertEqual(AUTO_SECTIONS, ("movies", "anime", "donghua"))

    def test_media_links_are_accepted_and_others_refused(self):
        for link in (PARCIV_API_URL, "https://api.parciv.workers.dev/moviebox/stream?id=1234",
                     "https://api.parciv.workers.dev/anime/post?slug=one-piece"):
            self.assertEqual(parse_parciv_source(link), link)
        for link in ("https://api.parciv.workers.dev", "https://example.com/moviebox/movies",
                     "http://api.parciv.workers.dev/moviebox/movies", "not a url",
                     "https://api.parciv.workers.dev.evil.example/moviebox/movies",
                     "https://api.parciv.workers.dev/" + "a" * 3000):
            with self.assertRaises(ValueError, msg=link):
                parse_parciv_source(link)

    def test_an_item_stream_link_is_not_an_api_endpoint(self):
        self.assertTrue(is_parciv_media_endpoint("https://api.parciv.workers.dev/moviebox/stream?id=42"))
        self.assertTrue(is_parciv_media_endpoint("https://api.parciv.workers.dev/anime/post?slug=x"))
        self.assertFalse(is_parciv_media_endpoint("https://api.parciv.workers.dev/moviebox/movies?page=1"))
        self.assertFalse(is_parciv_media_endpoint("https://api.parciv.workers.dev/moviebox"))


class HelperTests(unittest.TestCase):
    def test_quality_labels(self):
        self.assertEqual(quality_label("1080,720,480"), "1080p")
        self.assertEqual(quality_label("360p"), "360p")
        self.assertEqual(quality_label(None, "DASH"), "DASH")
        self.assertEqual(quality_label(None, None, "https://cdn/x.mp4"), "Video")

    def test_sizes_are_only_taken_when_they_make_sense(self):
        self.assertEqual(size_of({"size": "2604346171"}), 2604346171)
        self.assertEqual(size_of({"size": "unknown"}), 0)
        self.assertEqual(size_of({"size": "0"}), 0)

    def test_a_listing_marks_what_the_dm_holds(self):
        from ytdown.apitera import CataloguePage
        items = parse_catalogue(MOVIES, "movies", ParcivApi(PARCIV_API_URL))
        keys = past_titles([stored_file("Matchbox the Movie (2026) [1080p].mp4")])
        lines = listing_lines(CataloguePage("movies", 1, items), keys, 10)
        self.assertTrue(lines[0].startswith("1. ✅ Matchbox the Movie (2026)"))
        self.assertTrue(lines[1].startswith("2. ⬇️ Spider-Man: Hari Baru"))
        self.assertEqual(len(lines), 3)

    def test_auto_settings_are_parsed_and_clamped(self):
        self.assertEqual(parse_interval("15m"), 900)
        self.assertEqual(parse_interval("2h"), 7200)
        self.assertEqual(parse_interval("30"), 60)  # Below the one-minute floor.
        self.assertEqual(interval_text(900), "15m")
        with self.assertRaises(ValueError):
            parse_interval("soon")
        self.assertEqual(parse_limit("12"), 10)
        self.assertEqual(parse_limit("3"), 3)
        with self.assertRaises(ValueError):
            parse_limit("many")


class DownloadTests(unittest.TestCase):
    """The yt-dlp route: its options, checks and errors (yt-dlp itself is mocked)."""

    def setUp(self):
        self.out = tempfile.mkdtemp(prefix="ytdown-parciv-test-")
        self.addCleanup(shutil.rmtree, self.out, ignore_errors=True)
        self.progress = []

    def write(self, name="stream.mp4", body=None):
        body = body if body is not None else b"\x00\x00\x00\x18ftypmp42" + b"\x00" * (2 * 1024 * 1024)
        path = os.path.join(self.out, name)
        with open(path, "wb") as handle:
            handle.write(body)
        return path

    def test_a_downloaded_mp4_is_returned_after_being_checked(self):
        fake = MagicMock()

        def download(urls):
            self.write()

        fake.YoutubeDL.return_value.__enter__.return_value.download.side_effect = download
        with patch("ytdown.parciv.yt_dlp", fake):
            path = download_with_ytdlp("https://cdn.example.com/a.mp4", self.out, lambda *a: None,
                                   threading.Event(), headers=(("Cookie", "CloudFront-Policy=x"),),
                                   proxy="http://127.0.0.1:8080", label="360p")
        self.assertTrue(path.endswith("stream.mp4"))
        options = fake.YoutubeDL.call_args.args[0]
        self.assertEqual(options["http_headers"], {"Cookie": "CloudFront-Policy=x"})
        self.assertEqual(options["proxy"], "http://127.0.0.1:8080")
        self.assertEqual(options["merge_output_format"], "mp4")

    def test_a_tiny_file_for_a_long_video_is_refused(self):
        fake = MagicMock()
        fake.YoutubeDL.return_value.__enter__.return_value.download.side_effect = (
            lambda urls: self.write(body=b"\x00\x00\x00\x18ftypmp42" + b"\x00" * 4096))
        with patch("ytdown.parciv.yt_dlp", fake):
            with self.assertRaises(ParcivError) as caught:
                download_with_ytdlp("https://cdn.example.com/a.mp4", self.out, lambda *a: None,
                                threading.Event(), label="360p", duration=5800)
        self.assertIn("only 4108 bytes", str(caught.exception))

    def test_an_error_page_saved_as_mp4_is_refused(self):
        fake = MagicMock()

        def download(urls):
            self.write(body=b"<html>playlist not found</html>" + b"x" * 4096)

        fake.YoutubeDL.return_value.__enter__.return_value.download.side_effect = download
        with patch("ytdown.parciv.yt_dlp", fake):
            with self.assertRaises(ParcivError) as caught:
                download_with_ytdlp("https://cdn.example.com/a.mp4", self.out, lambda *a: None,
                                threading.Event(), label="360p")
        self.assertIn("360p", str(caught.exception))
        self.assertIn("not a video", str(caught.exception))

    def test_a_failed_download_reports_the_reason(self):
        fake = MagicMock()
        fake.YoutubeDL.return_value.__enter__.return_value.download.side_effect = (
            RuntimeError("HTTP Error 403: Forbidden"))
        with patch("ytdown.parciv.yt_dlp", fake):
            with self.assertRaises(ParcivError) as caught:
                download_with_ytdlp("https://cdn.example.com/a.mp4", self.out, lambda *a: None,
                                threading.Event(), label="1080p")
        self.assertIn("1080p", str(caught.exception))
        self.assertIn("403", str(caught.exception))

    def test_a_dash_stream_needs_ffmpeg(self):
        with patch("ytdown.parciv.ffmpeg_ready", return_value=False):
            with self.assertRaises(ParcivError) as caught:
                download_with_ytdlp("https://sacdn.hakunaymatata.com/dash/index_web.mpd", self.out,
                                lambda *a: None, threading.Event(), label="1080p DASH")
        self.assertIn("ffmpeg", str(caught.exception))

    def test_a_cancelled_download_stops_the_worker(self):
        from yt_dlp.utils import DownloadCancelled
        stop = threading.Event()
        stop.set()
        fake = MagicMock()

        def hook(_status):
            raise DownloadCancelled("Cancelled")

        def download(urls):
            fake.YoutubeDL.call_args.args[0]["progress_hooks"][0]({"status": "downloading"})

        fake.YoutubeDL.return_value.__enter__.return_value.download.side_effect = download
        with patch("ytdown.parciv.yt_dlp", fake):
            with self.assertRaises(DownloadCancelled):
                download_with_ytdlp("https://cdn.example.com/a.mp4", self.out, lambda *a: None, stop)
        self.assertTrue(stop.is_set())


class StandInCdn:
    """A tiny stand-in CDN: it answers the routes the tests ask it for."""

    def __init__(self):
        import http.server

        self.seen: list[str] = []
        outer = self

        class Handler(http.server.BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.0"  # One connection per answer keeps the tests quiet.

            def log_message(self, *args):  # Keep the test output quiet.
                pass

            def handle_error(self, *args):  # A cancelled client is not a test failure.
                pass

            def do_GET(self):
                outer.seen.append(self.path)
                if self.path.startswith("/video") or "/stream?" in self.path:
                    body = b"\x00\x00\x00\x18ftypmp42" + b"\x00" * (2 * 1024 * 1024)
                    self.send_response(200)
                    self.send_header("Content-Type", "video/mp4")
                elif self.path.startswith("/page"):
                    body = b"<html>hotlink protection</html>"
                    self.send_response(200)
                    self.send_header("Content-Type", "text/html; charset=utf-8")
                elif self.path.startswith("/api-link"):
                    body = json.dumps({"status": "ok",
                                       "url": outer.url("/video/06-360p.mp4")}).encode()
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                elif self.path.startswith("/api-empty"):
                    body = json.dumps({"status": "error", "message": "no link"}).encode()
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                elif self.path.startswith("/manifest"):
                    body = b"#EXTM3U\n#EXT-X-VERSION:3\n"
                    self.send_response(200)
                    self.send_header("Content-Type", "application/vnd.apple.mpegurl")
                else:
                    body = b"no"
                    self.send_response(403)
                    self.send_header("Content-Type", "text/plain")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def url(self, path: str) -> str:
        return f"http://127.0.0.1:{self.port}{path}"

    def stop(self):
        self.server.shutdown()
        self.server.server_close()


class FakeAria2:
    """A stand-in aria2c process: it writes the file, or fails, exactly as told."""

    def __init__(self, target: str, *, body: bytes = b"", returncode: int = 0, output: str = "",
                 alive_reads: int = 0):
        self.target, self.body, self.returncode, self.output = target, body, returncode, output
        self.alive_reads = alive_reads           # How many polls see it still running.
        self.terminated = False
        self.args: list[str] = []
        self.stdout = None                       # What Popen would hand back.
        if body:
            with open(target, "wb") as handle:
                handle.write(body)

    def poll(self):
        if self.alive_reads > 0:
            self.alive_reads -= 1
            return None
        return self.returncode

    def terminate(self):
        self.terminated = True
        self.returncode = -15

    def kill(self):
        self.terminated = True
        self.returncode = -9

    def wait(self, timeout=None):
        return self.returncode


def fake_popen(factory):
    """Build a Popen replacement that records the command it was handed."""
    calls = []

    def popen(args, **kwargs):
        calls.append(args)
        return factory(args)

    return popen, calls


class PostLookupTests(unittest.TestCase):
    """The Anime/Donghua lookup ladder: every shape of the call, best guess first."""

    def test_a_slug_listing_asks_with_the_slug_first(self):
        api = ParcivApi(PARCIV_API_URL)
        item = ParcivItem(subject_id="kamen-rider-kiva", name="Kamen Rider Kiva", section="anime",
                          extra={"slug": "kamen-rider-kiva-movie"})
        labels = [label for label, _ in api.post_candidates("anime", item)]
        self.assertEqual(labels, ["slug", "subject id", "detail"])
        self.assertEqual(api.post_candidates("anime", item)[0][1],
                         "https://api.parciv.workers.dev/anime/post?slug=kamen-rider-kiva-movie")
        self.assertEqual(api.post("anime", item), api.post_candidates("anime", item)[0][1])

    def test_an_id_listing_asks_with_the_id(self):
        api = ParcivApi(PARCIV_API_URL)
        item = ParcivItem(subject_id="4455", name="Dark Machine", section="donghua",
                          extra={"postId": "9001"})
        self.assertEqual(api.post_candidates("donghua", item)[0],
                         ("id", "https://api.parciv.workers.dev/donghua/post?id=9001"))

    def test_a_link_the_listing_handed_over_is_tried_first(self):
        api = ParcivApi(PARCIV_API_URL)
        link = "https://api.parciv.workers.dev/anime/post?slug=dark-machine"
        item = ParcivItem(subject_id="dark-machine", name="Dark Machine", section="anime",
                          post_url=link, extra={"slug": "dark-machine"})
        candidates = api.post_candidates("anime", item)
        self.assertEqual(candidates[0], ("its own link", link))
        self.assertEqual(len({url for _, url in candidates}), len(candidates))  # No repeats.

    def test_an_api_url_the_listing_gave_is_used_as_the_url_shape(self):
        api = ParcivApi(PARCIV_API_URL)
        own = "https://api.parciv.workers.dev/anime/post?slug=dark-machine"
        item = ParcivItem(subject_id="dark-machine", name="Dark Machine", section="anime",
                          extra={"url": own})
        shapes = dict((label, url) for label, url in api.post_candidates("anime", item))
        self.assertEqual(shapes["api url"],
                         "https://api.parciv.workers.dev/anime/post?url="
                         "https%3A%2F%2Fapi.parciv.workers.dev%2Fanime%2Fpost%3Fslug%3Ddark-machine")

    def test_a_listing_entry_keeps_its_slug_url_and_own_media_link(self):
        raw = {"name": "Dark Machine: The Animation", "id": "4455",
               "slug": "dark-machine-2026",
               "url": "https://api.parciv.workers.dev/anime/post?slug=dark-machine-2026",
               "mediaUrl": "http://santol.whatbox.ca:29318/2026/Donghua/DarkMachine/01-360p.mp4",
               "poster": "https://pbcdnw.aoneroom.com/p.jpg"}
        item = item_from_dict(raw, "donghua", ParcivApi(PARCIV_API_URL))
        self.assertEqual(item.extra["slug"], "dark-machine-2026")
        self.assertEqual(item.extra["url"], raw["url"])
        self.assertEqual(item.extra["media_url"], raw["mediaUrl"])
        # An API-host URL is not media, and a media URL is not a lookup call.
        self.assertNotIn("poster", item.extra)

    def test_a_leech_link_is_read_as_one_file(self):
        source = "http://santol.whatbox.ca:29318/2026/Donghua/KuangWang2/06-360p.mp4"
        self.assertEqual(file_title(source), "KuangWang2 06")
        self.assertEqual(file_title("https://cdn.example.com/movie/PAW Patrol_ The Dino Movie.mp4"),
                         "movie PAW Patrol_ The Dino Movie")
        self.assertEqual(file_title(""), "apitera file")
        self.assertTrue(media_file_url(source))
        self.assertTrue(media_file_url("https://sacdn.hakunaymatata.com/dash/index_web.mpd"))
        self.assertFalse(media_file_url("https://api.parciv.workers.dev/anime/latest"))
        self.assertFalse(media_file_url("http://127.0.0.1:8080/a.mp4"))
        media = leech_link_media(source, ParcivItem(subject_id="", name=file_title(source),
                                                   section="donghua",
                                                   langs=("Japanese",), episode="Episode 6"))
        self.assertEqual(media.title, "KuangWang2 06")
        self.assertEqual(media.episode, "Episode 6")
        self.assertEqual(media.langs, ("Japanese",))
        self.assertEqual([(source_.label, source_.url) for source_ in media.sources],
                         [("360p", source)])

    def test_a_json_answer_naming_a_link_is_read(self):
        self.assertEqual(json_media_link({"status": "ok", "url":
                                          "http://santol.whatbox.ca:29318/x/06-360p.mp4"}),
                         "http://santol.whatbox.ca:29318/x/06-360p.mp4")
        self.assertEqual(json_media_link({"data": {"streams": [
            {"link": "https://cdn.example.com/a.mp4"}]}}), "https://cdn.example.com/a.mp4")
        self.assertEqual(json_media_link({"url": "javascript:alert(1)"}), "")
        self.assertEqual(json_media_link({"message": "no link here"}), "")


class LeechTests(unittest.TestCase):
    """One source, four routes: aria2c, a plain copy, yt-dlp, and the API's leech."""

    def setUp(self):
        self.out = tempfile.mkdtemp(prefix="ytdown-parciv-leech-")
        self.addCleanup(shutil.rmtree, self.out, ignore_errors=True)
        self.cdn = StandInCdn()
        self.addCleanup(self.cdn.stop)
        self.stop = threading.Event()
        self.progress = []
        # These tests exercise the built-in copy unless they say otherwise, so the
        # host's own aria2c can never change what they measure.
        self.aria = patch("ytdown.parciv.aria2_path", return_value="")
        self.aria.start()
        self.addCleanup(self.aria.stop)

    def on_bytes(self, done, total):
        self.progress.append((done, total))

    def test_the_media_urls_allow_the_plain_http_sources_these_cdns_hand_out(self):
        # The exact source shape the user hit: http, a custom port, no TLS.
        self.assertTrue(public_media_url("http://santol.whatbox.ca:29318/2026/Donghua/"
                                         "KuangWang2/06-360p.mp4"))
        self.assertTrue(public_media_url("https://bcdnt.hakunaymatata.com/bt/360.mp4"))
        self.assertFalse(public_media_url("http://127.0.0.1:8080/a.mp4"))
        self.assertFalse(public_media_url("http://localhost/a.mp4"))
        self.assertFalse(public_media_url("http://user:pw@example.com/a.mp4"))
        self.assertFalse(public_media_url("http://192.168.1.5/a.mp4"))
        self.assertFalse(public_media_url("http://[::1]/a.mp4"))
        parsed = source_from_dict({"url": "http://santol.whatbox.ca:29318/x/06-360p.mp4",
                                   "resolution": "360p"})
        self.assertEqual(parsed.url, "http://santol.whatbox.ca:29318/x/06-360p.mp4")
        self.assertEqual(parsed.label, "360p")
        # The API's own calls keep the strict rule: https only.
        self.assertEqual(media_url({"poster": "http://example.com/p.jpg"}), "")

    def test_aria2_is_used_when_the_host_has_it(self):
        target = os.path.join(self.out, "stream.mp4")
        body = b"\x00\x00\x00\x18ftypmp42" + b"\x00" * (2 * 1024 * 1024)
        fake = FakeAria2(target, body=body, alive_reads=1)
        popen, calls = fake_popen(lambda args: fake)
        with patch("ytdown.parciv.aria2_path", return_value="/usr/bin/aria2c"), \
                patch("ytdown.parciv.subprocess.Popen", new=popen), \
                patch("ytdown.parciv.yt_dlp") as ytdlp:
            path = download_parciv(self.cdn.url("/video/360.mp4"), self.out, self.on_bytes, self.stop,
                                   proxy="http://127.0.0.1:9999",
                                   headers=(("Cookie", "CloudFront-Policy=x"),), label="360p")
        self.assertTrue(path.endswith("stream.mp4"))
        self.assertEqual(self.cdn.seen, [])        # aria2c fetched it, not the built-in copy.
        ytdlp.YoutubeDL.assert_not_called()
        command = calls[0]
        self.assertEqual(command[0], "/usr/bin/aria2c")
        self.assertIn("--header=Cookie: CloudFront-Policy=x", command)
        self.assertIn("--all-proxy=http://127.0.0.1:9999", command)
        self.assertIn("--max-connection-per-server=4", command)
        self.assertTrue(command[-1].endswith("/video/360.mp4"))
        self.assertEqual(self.progress[-1], (len(body), 0))  # The bar moves while it runs.

    def test_aria2_failing_falls_back_to_the_built_in_copy(self):
        target = os.path.join(self.out, "stream.mp4")
        fake = FakeAria2(target, returncode=3, output="status=3, error=403 Forbidden")
        popen, calls = fake_popen(lambda args: fake)
        with patch("ytdown.parciv.aria2_path", return_value="/usr/bin/aria2c"), \
                patch("ytdown.parciv.subprocess.Popen", new=popen), \
                patch("ytdown.parciv.yt_dlp") as ytdlp:
            path = download_parciv(self.cdn.url("/video/720.mp4"), self.out, self.on_bytes, self.stop,
                                   label="720p")
        self.assertTrue(path.endswith("stream.mp4"))
        self.assertEqual(self.cdn.seen, ["/video/720.mp4"])  # The copy took over.
        ytdlp.YoutubeDL.assert_not_called()
        self.assertEqual(os.listdir(self.out), ["stream.mp4"])  # No aria2 leftovers.

    def test_aria2_is_terminated_when_the_job_is_cancelled(self):
        target = os.path.join(self.out, "stream.mp4")
        fake = FakeAria2(target, alive_reads=10_000)  # Never finishes on its own.
        popen, _ = fake_popen(lambda args: fake)
        self.stop.set()
        with patch("ytdown.parciv.aria2_path", return_value="/usr/bin/aria2c"), \
                patch("ytdown.parciv.subprocess.Popen", new=popen):
            with self.assertRaises(DownloadCancelled):
                download_parciv(self.cdn.url("/video/1080.mp4"), self.out, self.on_bytes, self.stop)
        self.assertTrue(fake.terminated)
        self.assertEqual(os.listdir(self.out), [])

    def test_every_route_including_aria2_reports_its_reason(self):
        target = os.path.join(self.out, "stream.mp4")
        fake = FakeAria2(target, returncode=3, output="error=403 Forbidden")
        popen, _ = fake_popen(lambda args: fake)
        ytdlp = MagicMock()
        ytdlp.YoutubeDL.return_value.__enter__.return_value.download.side_effect = (
            RuntimeError("Postprocessing: ffmpeg not found"))
        with patch("ytdown.parciv.aria2_path", return_value="/usr/bin/aria2c"), \
                patch("ytdown.parciv.subprocess.Popen", new=popen), \
                patch("ytdown.parciv.yt_dlp", ytdlp):
            with self.assertRaises(ParcivError) as caught:
                download_parciv(self.cdn.url("/refused/06-360p.mp4"), self.out, self.on_bytes,
                                self.stop, label="360p", api=ParcivApi("http://127.0.0.1:1/"),
                                section="donghua")
        message = str(caught.exception)
        self.assertIn("aria2c: aria2c exited 3", message)
        self.assertIn("direct copy: HTTP 403", message)
        self.assertIn("yt-dlp: ", message)
        self.assertIn("via the API: ", message)
        self.assertLess(len(message), 420)

    def test_a_leech_answer_that_is_a_link_is_followed(self):
        # The stand-in CDN lives on 127.0.0.1, which the public-host rule rightly refuses;
        # that rule has its own test, so it is relaxed here to test the link follow itself.
        with patch("ytdown.parciv.public_media_url", lambda value: str(value).startswith("http")):
            path = download_parciv(self.cdn.url("/api-link/06-360p.mp4"), self.out, self.on_bytes,
                                   self.stop, label="360p")
        self.assertEqual(os.path.getsize(path), 2097164)
        # The API answered with JSON naming the file, exactly as a leech bot handles it.
        self.assertEqual(self.cdn.seen, ["/api-link/06-360p.mp4", "/video/06-360p.mp4"])

    def test_a_json_message_without_a_link_is_refused(self):
        with self.assertRaises(ParcivError) as caught:
            leech_parciv(self.cdn.url("/api-empty/06.mp4"), self.out, self.on_bytes, self.stop)
        self.assertIn("JSON message", str(caught.exception))
        self.assertEqual(os.listdir(self.out), [])

    def test_the_api_leech_url_is_built_from_the_family(self):
        api = ParcivApi(PARCIV_API_URL)
        source = "http://santol.whatbox.ca:29318/2026/Donghua/KuangWang2/06-360p.mp4"
        self.assertEqual(leech_url(api, "donghua", source),
                         "https://api.parciv.workers.dev/donghua/stream?url=http%3A%2F%2F"
                         "santol.whatbox.ca%3A29318%2F2026%2FDonghua%2FKuangWang2%2F06-360p.mp4")
        self.assertEqual(leech_url(api, "anime", "https://x.example.com/a.mp4"),
                         "https://api.parciv.workers.dev/anime/stream?url=https%3A%2F%2F"
                         "x.example.com%2Fa.mp4")
        self.assertEqual(leech_url(api, "movies", "https://x.example.com/a.mp4"),
                         "https://api.parciv.workers.dev/moviebox/stream?url=https%3A%2F%2F"
                         "x.example.com%2Fa.mp4")
        # A user's own endpoint keeps its own base.
        own = ParcivApi("https://example.com/api/moviebox")
        self.assertEqual(leech_url(own, "kids", "https://x.example.com/a.mp4"),
                         "https://example.com/api/moviebox/stream?url=https%3A%2F%2F"
                         "x.example.com%2Fa.mp4")

    def test_a_plain_mp4_is_copied_without_yt_dlp(self):
        fake = MagicMock()
        with patch("ytdown.parciv.yt_dlp", fake):
            path = download_parciv(self.cdn.url("/video/360.mp4"), self.out, self.on_bytes, self.stop,
                                   label="360p")
        self.assertTrue(path.endswith("stream.mp4"))
        self.assertEqual(os.path.getsize(path), 2097164)
        fake.YoutubeDL.assert_not_called()          # No post-processor can get in the way.
        self.assertEqual(self.progress[-1], (2097164, 2097164))
        self.assertEqual(self.cdn.seen, ["/video/360.mp4"])

    def test_an_html_answer_is_not_saved_and_yt_dlp_gets_the_next_try(self):
        fake = MagicMock()

        def download(urls):
            with open(os.path.join(self.out, "stream.mp4"), "wb") as handle:
                handle.write(b"\x00\x00\x00\x18ftypmp42" + b"\x00" * (2 * 1024 * 1024))

        fake.YoutubeDL.return_value.__enter__.return_value.download.side_effect = download
        with patch("ytdown.parciv.yt_dlp", fake):
            path = download_parciv(self.cdn.url("/page/360.mp4"), self.out, self.on_bytes, self.stop,
                                   label="360p")
        self.assertTrue(path.endswith("stream.mp4"))
        fake.YoutubeDL.assert_called_once()
        self.assertEqual(self.cdn.seen, ["/page/360.mp4"])

    def test_a_manifest_is_left_to_yt_dlp(self):
        fake = MagicMock()

        def download(urls):
            with open(os.path.join(self.out, "stream.mp4"), "wb") as handle:
                handle.write(b"\x00\x00\x00\x18ftypmp42" + b"\x00" * (2 * 1024 * 1024))

        fake.YoutubeDL.return_value.__enter__.return_value.download.side_effect = download
        with patch("ytdown.parciv.yt_dlp", fake):
            path = download_parciv(self.cdn.url("/manifest/index.m3u8"), self.out, self.on_bytes,
                                   self.stop, label="360p")
        self.assertTrue(path.endswith("stream.mp4"))
        fake.YoutubeDL.assert_called_once()
        self.assertEqual(self.cdn.seen, [])          # The playlist was never copied blindly.

    def test_yt_dlp_failing_hands_the_source_to_the_api(self):
        api = ParcivApi(self.cdn.url("/moviebox"))
        fake = MagicMock()
        fake.YoutubeDL.return_value.__enter__.return_value.download.side_effect = (
            RuntimeError("Postprocessing: ffmpeg not found"))
        with patch("ytdown.parciv.yt_dlp", fake):
            path = download_parciv(self.cdn.url("/refused/06-360p.mp4"), self.out, self.on_bytes,
                                   self.stop, label="360p", api=api, section="donghua")
        self.assertTrue(path.endswith("stream.mp4"))
        self.assertEqual(os.path.getsize(path), 2097164)
        # The direct copy was refused, yt-dlp failed, and the worker's leech route worked.
        self.assertEqual(self.cdn.seen[0], "/refused/06-360p.mp4")
        self.assertEqual(len(self.cdn.seen), 2)
        self.assertTrue(self.cdn.seen[1].startswith("/donghua/stream?url="), self.cdn.seen)
        self.assertIn("refused", self.cdn.seen[1])
        # leech_url encodes the source: the worker must not see it as its own query.
        self.assertIn("url=http%3A%2F%2F127.0.0.1", self.cdn.seen[1])

    def test_every_route_reason_reaches_the_user(self):
        fake = MagicMock()
        fake.YoutubeDL.return_value.__enter__.return_value.download.side_effect = (
            RuntimeError("Postprocessing: ffmpeg not found"))
        dead_api = ParcivApi("http://127.0.0.1:1/")  # Its leech route cannot be reached.
        with patch("ytdown.parciv.yt_dlp", fake):
            with self.assertRaises(ParcivError) as caught:
                download_parciv(self.cdn.url("/refused/06.mp4"), self.out, self.on_bytes, self.stop,
                                label="360p", api=dead_api, section="donghua")
        message = str(caught.exception)
        self.assertIn("360p could not be downloaded", message)
        self.assertIn("direct copy: HTTP 403", message)
        self.assertIn("yt-dlp: ", message)
        self.assertIn("ffmpeg", message)
        self.assertIn("via the API: ", message)
        self.assertEqual(message.count("could not be downloaded"), 1)  # Not nested twice.
        self.assertLess(len(message), 320)

    def test_a_cancelled_copy_stops_without_trying_again(self):
        fake = MagicMock()
        self.stop.set()
        with patch("ytdown.parciv.yt_dlp", fake):
            with self.assertRaises(DownloadCancelled):
                download_parciv(self.cdn.url("/video/720.mp4"), self.out, self.on_bytes, self.stop)
        fake.YoutubeDL.assert_not_called()
        self.assertEqual(os.listdir(self.out), [])  # Nothing half-written is left behind.

    def test_a_refused_answer_leaves_no_file_for_the_next_route(self):
        with self.assertRaises(ParcivError) as caught:
            leech_parciv(self.cdn.url("/refused/06.mp4"), self.out, self.on_bytes, self.stop)
        self.assertIn("403", str(caught.exception))
        self.assertEqual(os.listdir(self.out), [])

    def test_the_direct_copy_can_go_through_a_proxy(self):
        seen: list[str] = []

        class FakeOpener:
            def open(self, request, timeout=None):
                raise urllib.error.URLError("proxy refused")

        with patch("ytdown.parciv.urllib.request.build_opener") as build:
            build.return_value = FakeOpener()
            with self.assertRaises(ParcivError):
                leech_parciv(self.cdn.url("/video/360.mp4"), self.out, self.on_bytes, self.stop,
                             proxy="http://127.0.0.1:9999")
        proxy_handler = build.call_args.args[0]
        self.assertEqual(proxy_handler.proxies, {"http": "http://127.0.0.1:9999",
                                                "https": "http://127.0.0.1:9999"})
        self.assertEqual(seen, [])


class SearchTests(unittest.TestCase):
    """`/search`: the per-family search URLs and the title matching."""

    def test_a_family_search_uses_that_family_base(self):
        api = ParcivApi(PARCIV_API_URL)
        self.assertEqual(search_url(api, "movies", "paw patrol"),
                         "https://api.parciv.workers.dev/moviebox/search?query=paw%20patrol")
        self.assertEqual(search_url(api, "tvshows", "the office"),
                         "https://api.parciv.workers.dev/moviebox/search?query=the%20office")
        self.assertEqual(search_url(api, "anime", "one piece 1130"),
                         "https://api.parciv.workers.dev/anime/search?query=one%20piece%201130")
        self.assertEqual(search_url(api, "donghua", "battle through"),
                         "https://api.parciv.workers.dev/donghua/search?query=battle%20through")

    def test_a_users_own_endpoint_keeps_its_own_base(self):
        api = ParcivApi("https://example.com/api/moviebox")
        self.assertEqual(family_base(api, "kids"), "https://example.com/api/moviebox")
        self.assertEqual(family_base(api, "anime"), "https://example.com/anime")
        self.assertEqual(search_url(api, "kids", "paw"),
                         "https://example.com/api/moviebox/search?query=paw")

    def test_every_word_has_to_appear_in_the_title(self):
        self.assertTrue(matches_words("PAW Patrol: The Dino Movie (2026)", "paw patrol"))
        self.assertTrue(matches_words("PAW Patrol: The Dino Movie (2026)", "dino"))
        self.assertTrue(matches_words("One Piece Episode 1130 [Japanese] [Indonesian Sub]",
                                      "one piece 1130"))
        self.assertTrue(matches_words("Spider-Man_ Hari Baru [480p].mp4", "spider man"))
        self.assertFalse(matches_words("PAW Patrol: The Dino Movie (2026)", "paw patrol 1130"))
        self.assertFalse(matches_words("Matchbox the Movie (2026)", "paw"))

    def test_an_empty_query_matches_nothing(self):
        self.assertFalse(matches_words("PAW Patrol: The Dino Movie (2026)", ""))
        self.assertFalse(matches_words("PAW Patrol: The Dino Movie (2026)", "   "))


if __name__ == "__main__":
    unittest.main()
