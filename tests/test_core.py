import unittest
from unittest.mock import patch

from ytdown.core import PlaylistEntry, extract_playlist, normalize_video, parse_source, public_https_url, youtube_title

ID = "3SY1i5CB03Q"


class CoreTests(unittest.TestCase):
    def test_video_links(self):
        for url in (f"https://youtu.be/{ID}?t=5", f"https://www.youtube.com/shorts/{ID}",
                    f"https://music.youtube.com/watch?v={ID}"):
            with self.subTest(url=url):
                self.assertEqual(parse_source(url).url, f"https://www.youtube.com/watch?v={ID}")
        for url in ("https://youtube.com.evil.org/watch?v=" + ID,
                    "http://youtu.be/" + ID, "https://www.youtube.com/watch?v=bad",
                    "https://www.youtube.com/playlist?list=bad"):
            with self.subTest(url=url), self.assertRaises(ValueError):
                parse_source(url)

    def test_playlist_watch_url(self):
        source = parse_source(f"https://youtube.com/watch?v={ID}&list=PL12345678901")
        self.assertEqual(source.kind, "playlist")
        self.assertEqual(source.url, "https://www.youtube.com/playlist?list=PL12345678901")

    def test_provider_formats_are_authoritative(self):
        payload = {"success": True, "availableQuality": [360, 720, 2160], "title": "Example",
                   "duration": 884, "thumbnail": "https://i.ytimg.com/vi/3SY1i5CB03Q/maxresdefault.jpg",
                   "downloads": [
                       {"quality": "360p", "type": "video", "format": "mp4", "url": "https://cdn.example.com/360.mp4"},
                       {"quality": "720p", "type": "video", "format": "webm", "url": "https://cdn.example.com/720.webm"},
                       {"quality": "2160p", "type": "video", "format": "mp4", "url": "http://localhost/secret"},
                   ], "audio_stream_link": "https://cdn.example.com/128.mp3"}
        self.assertEqual(set(normalize_video(payload).streams), {"360p", "mp3"})
        self.assertEqual(normalize_video(payload).title, "Example")  # Fallback if YouTube lookup fails.
        self.assertEqual(normalize_video(payload).duration, 884)
        self.assertTrue(normalize_video(payload).thumbnail.endswith("maxresdefault.jpg"))
        self.assertFalse(public_https_url("https://127.0.0.1/video"))
        self.assertFalse(public_https_url("https://localhost/video"))
        with self.assertRaises(ValueError):
            normalize_video({"status": False, "downloads": payload["downloads"]})

    def test_playlist_flat_entries_filtered_and_bounded(self):
        class FakeYDL:
            def __init__(self, opts):
                self.opts = opts
            def __enter__(self):
                return self
            def __exit__(self, *args):
                pass
            def extract_info(self, url, download=False):
                assert self.opts["extract_flat"] == "in_playlist"
                assert self.opts["playlistend"] == 2
                return {"entries": [{"id": ID, "title": "Actual YouTube title"}, None,
                                    {"id": ID, "title": "duplicate"},
                                    {"id": "abcdefghijk", "title": "[Private video]"}]}
        with patch("ytdown.core.yt_dlp.YoutubeDL", FakeYDL):
            self.assertEqual(extract_playlist("https://youtube.com/playlist?list=PL12345678901", 2),
                             [PlaylistEntry(f"https://www.youtube.com/watch?v={ID}", "Actual YouTube title"),
                              PlaylistEntry("https://www.youtube.com/watch?v=abcdefghijk", "")])
        self.assertEqual(youtube_title("  My  real\n title ", ID), "My real title")
