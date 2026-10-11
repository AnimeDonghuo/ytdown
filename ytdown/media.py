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
    """Keep only MP4 head/tail so ffprobe can seek moov without storing the video."""
    def __init__(self):
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
                path = Path(directory) / "sample.mp4"
                with path.open("wb") as output:
                    output.truncate(self.size)  # Sparse: no 2 GB disk allocation.
                    output.seek(0)
                    output.write(self.head)
                    output.seek(max(0, self.size - self.tail_length))
                    output.write(b"".join(self.tail))
                process = await asyncio.create_subprocess_exec(
                    "ffprobe", "-v", "error", "-threads", "1", "-probesize", "8388608",
                    "-select_streams", "v:0",
                    "-show_entries", "stream=width,height,duration:format=duration",
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
        async with session.get(url, allow_redirects=False,
                               timeout=aiohttp.ClientTimeout(total=15, connect=5)) as response:
            response.raise_for_status()
            if response.content_length and response.content_length > 4 * 1024 * 1024:
                return None
            data = await response.content.read(4 * 1024 * 1024 + 1)
            if len(data) > 4 * 1024 * 1024:
                return None
        image = Image.open(io.BytesIO(data))
        if image.width * image.height > 5_000_000:
            return None
        image.thumbnail((320, 320), Image.Resampling.LANCZOS)
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
