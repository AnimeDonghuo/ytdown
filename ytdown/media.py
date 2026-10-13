"""Bounded media metadata capture and Telegram-compatible JPEG thumbnail."""
from __future__ import annotations

import asyncio
import io
import json
import logging
import tempfile
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urljoin

import aiohttp
from PIL import Image, UnidentifiedImageError

from .core import public_https_url

LOG = logging.getLogger(__name__)
HEAD_BYTES = 2 * 1024 * 1024
TAIL_BYTES = 4 * 1024 * 1024


@dataclass(frozen=True)
class MediaInfo:
    duration: int = 0
    width: int = 0
    height: int = 0


class MediaProbe:
    """Keep only media head/tail so ffprobe can inspect it without storing the video."""
    def __init__(self, suffix: str = ".mp4"):
        self.suffix = suffix if suffix in {".mp4", ".mkv", ".webm", ".avi", ".mov"} else ".mp4"
        self.head = bytearray()
        self.tail = deque()
        self.tail_length = 0
        self.size = 0

    def add(self, data: bytes) -> None:
        self.size += len(data)
        if len(self.head) < HEAD_BYTES:
            self.head.extend(data[:HEAD_BYTES - len(self.head)])
        self.tail.append(data)
        self.tail_length += len(data)
        while self.tail_length > TAIL_BYTES and self.tail:
            excess = self.tail_length - TAIL_BYTES
            first = self.tail.popleft()
            if len(first) > excess:
                self.tail.appendleft(first[excess:])
                self.tail_length -= excess
            else:
                self.tail_length -= len(first)

    def write_sample(self, path: Path) -> None:
        with path.open("wb") as output:
            output.truncate(self.size)  # Sparse: no 2 GB disk allocation.
            output.seek(0)
            output.write(self.head)
            output.seek(max(0, self.size - self.tail_length))
            output.write(b"".join(self.tail))

    async def inspect(self) -> MediaInfo:
        if not self.size:
            return MediaInfo()
        try:
            with tempfile.TemporaryDirectory(prefix="ytdown-probe-") as directory:
                path = Path(directory) / ("sample" + self.suffix)
                self.write_sample(path)
                process = await asyncio.create_subprocess_exec(
                    "ffprobe", "-v", "error", "-threads", "1", "-probesize", "8388608",
                    "-select_streams", "v:0",
                    "-show_entries", "stream=width,height,duration:stream_tags=rotate:stream_side_data=rotation:format=duration",
                    "-of", "json", str(path), stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.DEVNULL)
                try:
                    stdout, _ = await asyncio.wait_for(process.communicate(), timeout=12)
                except asyncio.TimeoutError:
                    process.kill()
                    await process.communicate()
                    return MediaInfo()
            if process.returncode != 0 or len(stdout) > 64 * 1024:
                return MediaInfo()
            info = json.loads(stdout)
            stream = (info.get("streams") or [{}])[0]
            width, height = int(stream.get("width") or 0), int(stream.get("height") or 0)
            rotation = (stream.get("tags") or {}).get("rotate", 0)
            for side_data in stream.get("side_data_list") or []:
                rotation = side_data.get("rotation", rotation)
            if int(float(rotation)) % 180:
                width, height = height, width
            duration = float((info.get("format") or {}).get("duration") or stream.get("duration") or 0)
            if not (0 < width <= 16384 and 0 < height <= 16384):
                width = height = 0
            return MediaInfo(int(duration) if 0 < duration < 7 * 86400 else 0, width, height)
        except (OSError, ValueError, KeyError, TypeError, IndexError) as exc:
            LOG.info("Could not probe media metadata: %s", exc)
            return MediaInfo()

    async def preview_jpeg(self) -> tuple[bytes, int, int] | None:
        """Best-effort Terabox frame when both API thumbnail URLs fail.

        Decode just one frame from our bounded sparse sample, never the full file.
        Limit CPU to one thread and eight seconds for Koyeb free instances.
        """
        if not self.size:
            return None
        try:
            with tempfile.TemporaryDirectory(prefix="ytdown-preview-") as directory:
                path = Path(directory) / ("sample" + self.suffix)
                self.write_sample(path)
                process = await asyncio.create_subprocess_exec(
                    "ffmpeg", "-v", "error", "-threads", "1", "-probesize", "8388608",
                    "-i", str(path), "-an", "-sn", "-frames:v", "1",
                    "-vf", "scale=320:320:force_original_aspect_ratio=decrease",
                    "-vcodec", "mjpeg", "-f", "image2pipe", "pipe:1",
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
                try:
                    image, _ = await asyncio.wait_for(process.communicate(), timeout=8)
                except asyncio.TimeoutError:
                    process.kill()
                    await process.communicate()
                    return None
                except asyncio.CancelledError:
                    process.kill()
                    await process.communicate()
                    raise
            return jpeg_preview(image) if process.returncode == 0 else None
        except OSError as exc:
            LOG.info("Could not generate media preview: %s", exc)
            return None


def mostly_white(image: Image.Image) -> bool:
    """True for blank white placeholders, including ones with a small logo."""
    sample = image.convert("RGB").resize((32, 32), Image.Resampling.BILINEAR)
    pixels = sample.tobytes()
    bright = [min(pixels[i:i + 3]) >= 240 for i in range(0, len(pixels), 3)]
    border = [bright[y * 32 + x] for y in range(32) for x in range(32)
              if x in (0, 31) or y in (0, 31)]
    return sum(bright) >= 0.9 * len(bright) or (
        sum(bright) >= 0.82 * len(bright) and sum(border) >= 0.95 * len(border))


def usable_still(data: bytes) -> bool:
    """Whether Telegram's returned still is a decodable, non-white image."""
    if not data or len(data) > 1024 * 1024:
        return False
    try:
        with Image.open(io.BytesIO(data)) as image:
            if image.width * image.height > 5_000_000:
                return False
            return not mostly_white(image)
    except (OSError, ValueError, UnidentifiedImageError, Image.DecompressionBombError):
        return False


def jpeg_preview(data: bytes) -> tuple[bytes, int, int] | None:
    """Normalize a provider image or extracted frame for Telegram thumbnails."""
    if not data or len(data) > 4 * 1024 * 1024:
        return None
    try:
        image = Image.open(io.BytesIO(data))
        if image.width * image.height > 5_000_000:
            return None
        image.thumbnail((320, 320), Image.Resampling.LANCZOS)
        image = image.convert("RGB")
        # A valid maxresdefault URL can return an almost-white placeholder with
        # a small logo; low variance alone does not catch it. Try HQ instead.
        if mostly_white(image):
            return None
        # Telegram silently ignores many otherwise valid thumbnails above ~20 KB.
        # Compress/downscale until it can reliably attach, preserving aspect ratio.
        for bound in (320, 256, 200, 160, 128):
            candidate = image.copy()
            candidate.thumbnail((bound, bound), Image.Resampling.LANCZOS)
            for quality in (72, 58, 44, 32):
                out = io.BytesIO()
                candidate.save(out, format="JPEG", quality=quality, optimize=True)
                if out.tell() <= 18 * 1024:
                    return out.getvalue(), *candidate.size
        return None
    except (OSError, ValueError, UnidentifiedImageError, Image.DecompressionBombError):
        return None


async def thumbnail_jpeg(session: aiohttp.ClientSession, url: str) -> tuple[bytes, int, int] | None:
    """Download a bounded public thumbnail and resize for Telegram's thumbnail API."""
    if not public_https_url(url):
        return None
    try:
        for _ in range(4):
            if not public_https_url(url):
                return None
            async with session.get(url, allow_redirects=False,
                                   timeout=aiohttp.ClientTimeout(total=15, connect=5)) as response:
                if response.status in (301, 302, 303, 307, 308):
                    location = response.headers.get("Location")
                    if not location:
                        return None
                    url = urljoin(url, location)
                    continue
                response.raise_for_status()
                if response.content_length and response.content_length > 4 * 1024 * 1024:
                    return None
                data = await response.content.read(4 * 1024 * 1024 + 1)
                if len(data) > 4 * 1024 * 1024:
                    return None
                break
        else:
            return None
        return jpeg_preview(data)
    except (aiohttp.ClientError, asyncio.TimeoutError, OSError, ValueError,
            UnidentifiedImageError, Image.DecompressionBombError):
        return None
