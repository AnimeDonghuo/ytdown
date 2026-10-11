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
from PIL import Image, ImageStat, UnidentifiedImageError

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

    async def inspect(self) -> MediaInfo:
        if not self.size:
            return MediaInfo()
        try:
            with tempfile.TemporaryDirectory(prefix="ytdown-probe-") as directory:
                path = Path(directory) / ("sample" + self.suffix)
                with path.open("wb") as output:
                    output.truncate(self.size)  # Sparse: no 2 GB disk allocation.
                    output.seek(0)
                    output.write(self.head)
                    output.seek(max(0, self.size - self.tail_length))
                    output.write(b"".join(self.tail))
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
            LOG.info("Could not probe MP4 metadata: %s", exc)
            return MediaInfo()


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
        image = Image.open(io.BytesIO(data))
        if image.width * image.height > 5_000_000:
            return None
        image.thumbnail((320, 320), Image.Resampling.LANCZOS)
        # YouTube maxresdefault can be a valid HTTP image that is just a blank
        # placeholder; treat it as unavailable and try the next thumbnail URL.
        sample = ImageStat.Stat(image.convert("RGB").resize((32, 32)))
        if min(sample.mean) > 247 and max(sample.var) < 30:
            return None
        width, height = image.size
        image = image.convert("RGB")
        out = io.BytesIO()
        image.save(out, format="JPEG", quality=75, optimize=True)
        if out.tell() > 200 * 1024:
            return None
        return out.getvalue(), width, height
    except (aiohttp.ClientError, asyncio.TimeoutError, OSError, ValueError,
            UnidentifiedImageError, Image.DecompressionBombError):
        return None
