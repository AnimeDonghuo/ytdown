import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

from ytdown.bot import API_URL, DownloaderBot, Job, Selection
from ytdown.core import Source, Video
from ytdown.terabox import TERABOX_API_URL, parse_terabox_source

VIDEO_URL = "https://www.youtube.com/watch?v=3SY1i5CB03Q"
SHARE_URL = "https://terasharefile.com/s/16E1yDzvD_txbiR561VPxBA"


class DashboardTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.client = MagicMock()
        self.client.edit_message = AsyncMock()
        self.client.delete_messages = AsyncMock()
        self.service = DownloaderBot(self.client, MagicMock())
        self.next_id = 100
        async def respond(*args, **kwargs):
            self.next_id += 1
            self.sent.append((self.next_id, args[0], kwargs))
            return SimpleNamespace(id=self.next_id)
        self.sent = []
        self.event = SimpleNamespace(sender_id=7, is_private=True, raw_text="/status",
                                     respond=respond, get_input_chat=AsyncMock(return_value="peer"))

    async def test_user_supplied_terasharefile_url_and_spoof_denied(self):
        self.assertEqual(parse_terabox_source(SHARE_URL), SHARE_URL)
        for spoof in ("https://terasharefile.com.evil.test/s/1",
                      "https://evil-terasharefile.com/s/1"):
            with self.assertRaises(ValueError):
                parse_terabox_source(spoof)

    async def test_one_replaced_live_dashboard_with_running_queued_and_finished(self):
        running = Job(7, "peer", 10, Source("video", VIDEO_URL), ("480p",), None, state="running")
        running.title = "running-video"
        running.item_index = running.item_total = 1
        running.total_bytes, running.uploaded = 1000, 400
        waiting = Job(7, "peer", 11, Source("video", VIDEO_URL), ("360p",), None)
        other_user = Job(8, "other-peer", 12, Source("video", VIDEO_URL), ("480p",), None)
        self.service.jobs = {j.token: j for j in (running, waiting, other_user)}
        self.service.completed[7] = [(asyncio.get_running_loop().time(), "🎬 finished-video\n📋 Finished")]
        await self.service.on_message(self.event)
        self.assertEqual(len(self.sent), 1)
        first = self.sent[0][1]
        for part in ("running-video", "finished-video", running.token, waiting.token,
                     "Queued", "Running"):
            self.assertIn(part, first)
        self.assertNotIn(other_user.token, first)
        self.assertEqual(set(self.client.delete_messages.await_args.args[1]), {10, 11})
        self.event.raw_text = "/status"
        await self.service.on_message(self.event)
        self.assertEqual(len(self.sent), 2)
        self.assertIn(101, self.client.delete_messages.await_args.args[1])
        self.assertEqual(self.service.dashboards[7].message_id, 102)
        await self.service.render(running, force=True)
        self.client.edit_message.assert_awaited()
        self.assertEqual(self.client.edit_message.await_args.args[1], 102)
        self.assertIn(waiting.token, self.client.edit_message.await_args.args[2])
        self.assertGreater(self.service.dashboards[7].next_edit, asyncio.get_running_loop().time())

    async def test_refresh_dashboard_resets_timer_and_old_button_expires(self):
        await self.service.on_message(self.event)
        old = self.service.dashboards[7]
        self.event.raw_text = "/status"
        await self.service.on_message(self.event)
        click = SimpleNamespace(sender_id=7, is_private=True, message_id=101,
                                data=f"d:{old.token}:refresh".encode(), answer=AsyncMock())
        await self.service.callback(click)
        self.assertIn("expired", click.answer.call_args.args[0])
        current = self.service.dashboards[7]
        click.message_id = 102
        click.data = f"d:{current.token}:refresh".encode()
        await self.service.callback(click)
        self.assertIn("Refreshing now", click.answer.call_args.args[0])
        self.assertGreater(current.next_edit, asyncio.get_running_loop().time())
        await self.service.callback(click)
        self.assertIn("Just refreshed", click.answer.call_args.args[0])

    async def test_cancel_without_id_lists_owned_jobs_then_explicit_id_cancels(self):
        running = Job(7, "peer", 10, Source("video", VIDEO_URL), ("480p",), None, state="running")
        running.task = asyncio.create_task(asyncio.sleep(300))
        waiting = Job(7, "peer", 11, Source("video", VIDEO_URL), ("720p",), None)
        waiting.task = asyncio.create_task(asyncio.sleep(300))
        other = Job(8, "other", 12, Source("video", VIDEO_URL), ("360p",), None)
        self.service.jobs = {j.token: j for j in (running, waiting, other)}
        self.event.raw_text = "/cancel"
        await self.service.on_message(self.event)
        self.assertIn(running.token, self.sent[-1][1])
        self.assertIn(waiting.token, self.sent[-1][1])
        self.assertNotIn(other.token, self.sent[-1][1])
        self.assertFalse(running.task.cancelling())
        self.event.raw_text = f"/cancel {waiting.token}"
        await self.service.on_message(self.event)
        self.assertTrue(waiting.task.cancelling())
        self.assertFalse(running.task.cancelling())
        self.event.raw_text = f"/cancel {other.token}"
        await self.service.on_message(self.event)
        self.assertIn("No matching", self.sent[-1][1])
        running.task.cancel()
        await asyncio.gather(running.task, waiting.task, return_exceptions=True)

    async def test_new_queued_job_joins_existing_dashboard_and_panel_is_removed(self):
        await self.service.on_message(self.event)
        self.service.run_job = AsyncMock()
        choice = Selection("abc12345", Source("video", VIDEO_URL), ("480p",), "peer",
                           selected={"480p"})
        self.service.selections[7] = choice
        click = SimpleNamespace(sender_id=7, is_private=True, message_id=55,
                                data=b"s:abc12345:done", answer=AsyncMock())
        await self.service.callback(click)
        self.assertIn(55, self.client.delete_messages.await_args.args[1])
        job = self.service.user_jobs(7)[0]
        self.assertIn(job.token, self.client.edit_message.await_args.args[2])
        await job.task

    async def test_finished_transfer_remains_in_combined_status_with_next_job(self):
        completed = Job(7, "peer", 10, Source("video", VIDEO_URL), ("480p",),
                        Video("Completed title", {"480p": "https://cdn.example.com/480.mp4"}))
        queued = Job(7, "peer", 11, Source("video", VIDEO_URL), ("360p",), None)
        self.service.jobs = {j.token: j for j in (completed, queued)}
        self.service.send_stream = AsyncMock()
        await self.service.on_message(self.event)
        await self.service.run_job(completed)
        self.assertNotIn(completed.token, self.service.jobs)
        self.assertEqual(self.service.user_jobs(7), [queued])
        dashboard_text = self.client.edit_message.await_args.args[2]
        self.assertIn("Completed title", dashboard_text)
        self.assertIn("Finished", dashboard_text)
        self.assertIn("Queued", dashboard_text)
        self.assertIn(queued.token, dashboard_text)

    async def test_numbered_apis_only_removes_own_custom_url(self):
        custom = "https://my-api.example.com/convert?key=abc"
        self.service.set_api(7, custom)
        self.event.raw_text = "/apis"
        await self.service.on_message(self.event)
        text = self.sent[-1][1]
        self.assertIn("1. YouTube default: " + API_URL, text)
        self.assertIn("2. Terabox: " + TERABOX_API_URL, text)
        self.assertIn("3. YouTube custom: " + custom, text)
        self.event.raw_text = "/apis remove 1"
        await self.service.on_message(self.event)
        self.assertEqual(self.service.api_for(7), custom)
        self.event.raw_text = "/apis remove 3"
        await self.service.on_message(self.event)
        self.assertEqual(self.service.api_for(7), API_URL)
        self.assertEqual(self.service.api_for(8), API_URL)
