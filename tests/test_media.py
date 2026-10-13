import io
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from PIL import Image
from ytdown.media import MediaInfo, MediaProbe, jpeg_preview, thumbnail_jpeg, usable_still


class MediaTests(unittest.IsolatedAsyncioTestCase):
    async def test_probe_keeps_bounded_head_and_tail(self):
        probe = MediaProbe()
        probe.add(b"a" * (3 * 1024 * 1024))
        probe.add(b"b" * (5 * 1024 * 1024))
        self.assertEqual(probe.size, 8 * 1024 * 1024)
        self.assertEqual(len(probe.head), 2 * 1024 * 1024)
        self.assertEqual(probe.tail_length, 4 * 1024 * 1024)
        self.assertEqual(b"".join(probe.tail)[-1:], b"b")

    async def test_sparse_probe_preserves_actual_mp4_dimensions(self):
        probe = MediaProbe()
        probe.add(b"beginning" * 100)
        probe.add(b"end" * 100)
        async def examine():
            return b'{"streams":[{"width":1080,"height":1920}],"format":{"duration":"884.6"}}', b""
        proc = SimpleNamespace(communicate=AsyncMock(side_effect=examine), returncode=0, kill=MagicMock())
        with patch("ytdown.media.asyncio.create_subprocess_exec", new=AsyncMock(return_value=proc)) as spawn:
            info = await probe.inspect()
        self.assertEqual(info, MediaInfo(duration=884, width=1080, height=1920))
        self.assertEqual(spawn.call_args.args[0], "ffprobe")

    async def test_probe_uses_display_orientation_for_rotated_video(self):
        probe = MediaProbe()
        probe.add(b"container" * 100)
        output = b'{"streams":[{"width":1920,"height":1080,"side_data_list":[{"rotation":90}]}],"format":{"duration":"61.5"}}'
        proc = SimpleNamespace(communicate=AsyncMock(return_value=(output, b"")),
                               returncode=0, kill=MagicMock())
        with patch("ytdown.media.asyncio.create_subprocess_exec", new=AsyncMock(return_value=proc)):
            self.assertEqual(await probe.inspect(), MediaInfo(61, 1080, 1920))

    async def test_bounded_terabox_frame_preview(self):
        from pathlib import Path
        probe = MediaProbe(".mkv")
        probe.add(b"head" * 100)
        frame = io.BytesIO()
        Image.new("RGB", (1280, 720), "red").save(frame, "JPEG")
        proc = SimpleNamespace(communicate=AsyncMock(return_value=(frame.getvalue(), b"")),
                               returncode=0, kill=MagicMock())
        def spawn(*args, **kwargs):
            self.assertEqual(args[0], "ffmpeg")
            self.assertIn("-threads", args)
            path = Path(args[args.index("-i") + 1])
            self.assertEqual(path.suffix, ".mkv")
            self.assertEqual(path.stat().st_size, probe.size)
            return proc
        with patch("ytdown.media.asyncio.create_subprocess_exec", new=AsyncMock(side_effect=spawn)):
            preview = await probe.preview_jpeg()
        self.assertEqual(preview[1:], (320, 180))
        self.assertLess(len(preview[0]), 200 * 1024)

    async def test_probe_of_near_two_gb_video_uses_sparse_disk(self):
        from collections import deque
        from pathlib import Path
        probe = MediaProbe()
        probe.size = 2000 * 1024 * 1024
        probe.head = bytearray(b"head" * 100)
        probe.tail = deque([b"tail" * 100])
        probe.tail_length = 400
        proc = SimpleNamespace(communicate=AsyncMock(return_value=(b'{"streams":[]}', b"")),
                               returncode=0, kill=MagicMock())
        def spawn(*args, **kwargs):
            path = Path(args[-1])
            self.assertEqual(path.stat().st_size, probe.size)
            self.assertLess(path.stat().st_blocks * 512, 8 * 1024 * 1024)
            return proc
        with patch("ytdown.media.asyncio.create_subprocess_exec", new=AsyncMock(side_effect=spawn)):
            await probe.inspect()

    async def test_thumbnail_is_small_jpeg_and_keeps_ratio(self):
        class Response:
            content_length = None
            status = 200
            headers = {}
            def __init__(self, data):
                self.content = SimpleNamespace(read=AsyncMock(return_value=data))
            async def __aenter__(self):
                return self
            async def __aexit__(self, *args):
                pass
            def raise_for_status(self):
                pass
        data = io.BytesIO()
        Image.new("RGB", (1080, 1920), "red").save(data, "JPEG")
        session = MagicMock()
        session.get.return_value = Response(data.getvalue())
        converted = await thumbnail_jpeg(session, "https://i.ytimg.com/vi/3SY1i5CB03Q/hqdefault.jpg")
        self.assertIsNotNone(converted)
        jpeg, w, h = converted
        self.assertEqual((w, h), (180, 320))
        self.assertLess(len(jpeg), 200 * 1024)
        self.assertEqual(Image.open(io.BytesIO(jpeg)).size, (180, 320))


class ThumbnailSafetyTests(unittest.IsolatedAsyncioTestCase):
    async def test_telethon_attaches_our_jpeg_as_document_thumbnail(self):
        from telethon import TelegramClient
        from telethon.sessions import MemorySession
        from telethon.tl import types
        client = TelegramClient(MemorySession(), 12345, "0" * 32)
        source = io.BytesIO()
        Image.new("RGB", (1280, 720), "red").save(source, format="JPEG")
        preview = jpeg_preview(source.getvalue())
        thumb = io.BytesIO(preview[0])
        thumb.name = "preview.jpg"
        async def upload(file, **kwargs):
            self.assertEqual(file.name, "preview.jpg")
            self.assertLessEqual(len(file.read()), 18 * 1024)
            return types.InputFile(123, 1, file.name, "0" * 32)
        client.upload_file = upload
        _, media, _ = await client._file_to_media(
            types.InputFileBig(321, 2, "video.mp4"), thumb=thumb, mime_type="video/mp4",
            attributes=[types.DocumentAttributeVideo(360, 1280, 720, supports_streaming=True)],
            supports_streaming=True)
        self.assertIsInstance(media, types.InputMediaUploadedDocument)
        self.assertEqual(media.thumb.name, "preview.jpg")
        self.assertEqual((media.attributes[1].w, media.attributes[1].h), (1280, 720))

    async def test_complex_thumbnail_is_small_enough_for_telegram(self):
        import random
        rng = random.Random(42)
        image = Image.frombytes("RGB", (640, 360), rng.randbytes(640 * 360 * 3))
        source = io.BytesIO()
        image.save(source, format="JPEG", quality=92)
        result = jpeg_preview(source.getvalue())
        self.assertIsNotNone(result)
        jpeg, width, height = result
        self.assertLessEqual(len(jpeg), 18 * 1024)
        self.assertLessEqual(max(width, height), 320)
        self.assertEqual(Image.open(io.BytesIO(jpeg)).size, (width, height))

    async def test_mostly_white_placeholder_with_small_logo_is_rejected(self):
        from PIL import ImageDraw
        image = Image.new("RGB", (320, 180), "white")
        ImageDraw.Draw(image).rectangle((145, 70, 175, 95), fill="red")
        source = io.BytesIO()
        image.save(source, format="JPEG", quality=80)
        self.assertIsNone(jpeg_preview(source.getvalue()))
        larger_logo = Image.new("RGB", (320, 180), "white")
        ImageDraw.Draw(larger_logo).rectangle((100, 62, 220, 118), fill="red")
        source = io.BytesIO()
        larger_logo.save(source, format="JPEG", quality=80)
        self.assertIsNone(jpeg_preview(source.getvalue()))

    async def test_blank_placeholder_is_rejected(self):
        image = io.BytesIO()
        Image.new("RGB", (1280, 720), "white").save(image, "JPEG")
        response = MagicMock()
        response.__aenter__ = AsyncMock(return_value=response)
        response.__aexit__ = AsyncMock()
        response.status, response.content_length = 200, None
        response.content.read = AsyncMock(return_value=image.getvalue())
        session = MagicMock()
        session.get.return_value = response
        self.assertIsNone(await thumbnail_jpeg(session, "https://example.com/blank.jpg"))

    async def test_thumbnail_follows_only_public_https_redirects(self):
        response = MagicMock()
        response.__aenter__ = AsyncMock(return_value=response)
        response.__aexit__ = AsyncMock()
        response.status = 302
        response.headers = {"Location": "http://127.0.0.1/image.jpg"}
        session = MagicMock()
        session.get.return_value = response
        self.assertIsNone(await thumbnail_jpeg(session, "https://example.com/start.jpg"))
        self.assertEqual(session.get.call_count, 1)


class ReturnedStillTests(unittest.TestCase):
    def jpeg(self, color, size=(320, 180)):
        from PIL import ImageDraw
        image = Image.new("RGB", size, color)
        if color != "white":
            ImageDraw.Draw(image).rectangle((0, 0, size[0] // 3, size[1]), fill="red")
        out = io.BytesIO()
        image.save(out, format="JPEG", quality=80)
        return out.getvalue()

    def test_usable_still_accepts_real_frames_only(self):
        self.assertTrue(usable_still(self.jpeg("blue")))
        self.assertFalse(usable_still(self.jpeg("white")))
        self.assertFalse(usable_still(b""))
        self.assertFalse(usable_still(b"not an image"))
        self.assertFalse(usable_still(b"x" * (1024 * 1024 + 1)))
        logo = Image.new("RGB", (320, 180), "white")
        from PIL import ImageDraw
        ImageDraw.Draw(logo).rectangle((145, 70, 175, 95), fill="red")
        out = io.BytesIO()
        logo.save(out, format="JPEG", quality=80)
        self.assertFalse(usable_still(out.getvalue()))

