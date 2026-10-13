import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp

from ytdown.bot import DownloaderBot, Job
from ytdown.core import PlaylistEntry, Source

PLAYLIST = "https://www.youtube.com/playlist?list=PL12345678901"
VIDEO_URL = "https://www.youtube.com/watch?v=3SY1i5CB03Q"


def api_error(status: int):
    return aiohttp.ClientResponseError(request_info=MagicMock(), history=(), status=status)


class PlaylistSkipReasonTests(unittest.IsolatedAsyncioTestCase):
    async def run_one(self, fetch_side_effect):
        service = DownloaderBot(MagicMock(), MagicMock())
        service.progress = AsyncMock()
        job = Job(1, "peer", 2, Source("playlist", PLAYLIST), ("360p",), None)
        service.jobs[job.token] = job
        with patch("ytdown.bot.extract_playlist_range", return_value=[PlaylistEntry(VIDEO_URL, "Episode 2")]), \
             patch("ytdown.bot.fetch_video", new=AsyncMock(side_effect=fetch_side_effect)):
            await service.run_job(job)
        return service.progress.call_args.args[1]

    async def test_skipped_lookup_reason_appears_in_the_summary(self):
        text = await self.run_one(api_error(403))
        self.assertIn("failed 1", text)
        self.assertIn("item 1: lookup HTTP 403", text)

    async def test_non_http_failure_is_named(self):
        text = await self.run_one(TimeoutError())
        self.assertIn("item 1: lookup TimeoutError", text)


if __name__ == "__main__":
    unittest.main()
