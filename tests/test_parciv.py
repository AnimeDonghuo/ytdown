"""apitera readers: the catalogue, the stream/post answers, names and title matching."""
import os
import tempfile
import threading
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from ytdown.apitera import (already_uploaded, listing_lines, new_items, parse_interval, parse_limit,
                            past_titles, interval_text)
from ytdown.parciv import (AUTO_SECTIONS, PARCIV_API_URL, ParcivApi, ParcivError, ParcivItem,
                           download_parciv, is_parciv_media_endpoint, message_titles,
                           normalize_section, parse_catalogue, parse_detail, parse_parciv_source,
                           parse_post, parse_sections, parse_stream, pick_media, quality_label,
                           size_of, title_key, titles_match)

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
    """yt-dlp is mocked; what matters here is the options, the checks and the errors."""

    def setUp(self):
        self.out = tempfile.mkdtemp(prefix="ytdown-parciv-test-")
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
            path = download_parciv("https://cdn.example.com/a.mp4", self.out, lambda *a: None,
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
                download_parciv("https://cdn.example.com/a.mp4", self.out, lambda *a: None,
                                threading.Event(), label="360p", duration=5800)
        self.assertIn("only 4108 bytes", str(caught.exception))

    def test_an_error_page_saved_as_mp4_is_refused(self):
        fake = MagicMock()

        def download(urls):
            self.write(body=b"<html>playlist not found</html>" + b"x" * 4096)

        fake.YoutubeDL.return_value.__enter__.return_value.download.side_effect = download
        with patch("ytdown.parciv.yt_dlp", fake):
            with self.assertRaises(ParcivError) as caught:
                download_parciv("https://cdn.example.com/a.mp4", self.out, lambda *a: None,
                                threading.Event(), label="360p")
        self.assertIn("360p", str(caught.exception))
        self.assertIn("not a video", str(caught.exception))

    def test_a_failed_download_reports_the_reason(self):
        fake = MagicMock()
        fake.YoutubeDL.return_value.__enter__.return_value.download.side_effect = (
            RuntimeError("HTTP Error 403: Forbidden"))
        with patch("ytdown.parciv.yt_dlp", fake):
            with self.assertRaises(ParcivError) as caught:
                download_parciv("https://cdn.example.com/a.mp4", self.out, lambda *a: None,
                                threading.Event(), label="1080p")
        self.assertIn("1080p", str(caught.exception))
        self.assertIn("403", str(caught.exception))

    def test_a_dash_stream_needs_ffmpeg(self):
        with patch("ytdown.parciv.ffmpeg_ready", return_value=False):
            with self.assertRaises(ParcivError) as caught:
                download_parciv("https://sacdn.hakunaymatata.com/dash/index_web.mpd", self.out,
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
                download_parciv("https://cdn.example.com/a.mp4", self.out, lambda *a: None, stop)
        self.assertTrue(stop.is_set())


if __name__ == "__main__":
    unittest.main()
