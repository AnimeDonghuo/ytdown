import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from ytdown.bot import API_URL, DownloaderBot, Job, Selection, cancel_entities
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
        async def send_message(peer, text, **kwargs):
            return await respond(text, **kwargs)
        self.client.send_message = AsyncMock(side_effect=send_message)
        self.event = SimpleNamespace(sender_id=7, is_private=True, raw_text="/status",
                                     respond=respond, get_input_chat=AsyncMock(return_value="peer"))

    async def test_cancel_command_entity_accounts_for_emoji_offsets(self):
        text = "🎬 Video\n🆔 2eef9d0f · /cancel_2eef9d0f"
        entities = cancel_entities(text)
        self.assertEqual(len(entities), 1)
        encoded = text.encode("utf-16-le")
        entity = entities[0]
        self.assertEqual(encoded[entity.offset * 2:(entity.offset + entity.length) * 2]
                         .decode("utf-16-le"), "/cancel_2eef9d0f")

    async def test_change_mode_clears_stale_api_prompt_before_youtube_link(self):
        from ytdown.core import Video
        self.service.awaiting_api.add(7)
        self.event.raw_text = "/change"
        await self.service.on_message(self.event)
        self.assertNotIn(7, self.service.awaiting_api)
        token = self.service.mode_tokens[7]
        click = SimpleNamespace(sender_id=7, is_private=True,
                                data=f"m:{token}:youtube".encode(),
                                answer=AsyncMock(), edit=AsyncMock())
        await self.service.callback(click)
        self.event.raw_text = "https://youtu.be/S7FjeuDF7_k?si=ASgwXk3IybWYujvS"
        self.event.respond = AsyncMock(return_value=SimpleNamespace(edit=AsyncMock()))
        with patch("ytdown.bot.fetch_video", new=AsyncMock(return_value=Video(
                "API", {"480p": "https://cdn.example.com/v.mp4"}))) as fetch, \
             patch("ytdown.bot.get_youtube_title", new=AsyncMock(return_value="YouTube")):
            await self.service.on_message(self.event)
        fetch.assert_awaited_once()
        self.assertIn("S7FjeuDF7_k", fetch.call_args.args[1])
        self.assertIn(7, self.service.selections)
        self.assertNotIn(7, self.service.api_overrides)

    async def test_media_link_not_consumed_by_pending_changeapi(self):
        self.service.awaiting_api.add(7)
        self.event.raw_text = "https://youtu.be/S7FjeuDF7_k?si=ASgwXk3IybWYujvS"
        with patch.object(self.service, "on_link", new=AsyncMock()) as on_link:
            await self.service.on_message(self.event)
        on_link.assert_awaited_once_with(self.event)
        self.assertNotIn(7, self.service.awaiting_api)
        self.service.awaiting_api.add(7)
        self.service.modes[7] = "terabox"
        self.event.raw_text = SHARE_URL
        with patch.object(self.service, "on_link", new=AsyncMock()) as on_link:
            await self.service.on_message(self.event)
        on_link.assert_awaited_once_with(self.event)
        self.assertNotIn(7, self.service.awaiting_api)

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
        self.assertEqual(len(self.sent[0][2]["formatting_entities"]), 2)
        self.assertEqual(len(self.sent[0][2]["buttons"]), 1)  # Refresh only.
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
        self.assertIn(f"/cancel_{waiting.token}", self.sent[-1][1])
        self.assertNotIn(other.token, self.sent[-1][1])
        self.assertNotIn("buttons", self.sent[-1][2])
        self.assertFalse(running.task.cancelling())
        self.event.raw_text = f"/cancel_{waiting.token}@MyBot"
        await self.service.on_message(self.event)
        self.assertTrue(waiting.task.cancelling())
        self.assertFalse(running.task.cancelling())
        self.event.raw_text = f"/cancel_{other.token}"
        await self.service.on_message(self.event)
        self.assertIn("No matching", self.sent[-1][1])
        self.event.raw_text = "/cancel_bad"
        await self.service.on_message(self.event)
        self.assertIn("Invalid job command", self.sent[-1][1])
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
        self.assertEqual(set(self.client.delete_messages.await_args.args[1]), {55, 101})
        job = self.service.user_jobs(7)[0]
        self.assertIn(job.token, self.client.send_message.await_args.args[1])
        self.assertIn("Queued", self.client.send_message.await_args.args[1])
        self.assertEqual(self.service.dashboards[7].message_id, 102)
        await job.task

    async def test_failed_dashboard_send_keeps_job_and_old_panel(self):
        from telethon.errors import FloodWaitError
        self.client.send_message = AsyncMock(side_effect=FloodWaitError(None, capture=4))
        self.service.run_job = AsyncMock()
        choice = Selection("abc12345", Source("video", VIDEO_URL), ("480p",), "peer",
                           selected={"480p"})
        self.service.selections[7] = choice
        click = SimpleNamespace(sender_id=7, is_private=True, message_id=55,
                                data=b"s:abc12345:done", answer=AsyncMock())
        await self.service.callback(click)
        self.assertEqual(len(self.service.user_jobs(7)), 1)
        self.assertEqual(self.service.dashboards.get(7), None)
        self.client.edit_message.assert_awaited_once()
        self.assertEqual(self.client.edit_message.await_args.args[1], 55)
        await self.service.user_jobs(7)[0].task

    async def test_cancel_command_cleans_queued_task_before_it_first_runs(self):
        job = Job(7, "peer", 15, Source("video", VIDEO_URL), ("480p",), None)
        self.service.jobs[job.token] = job
        job.task = asyncio.create_task(self.service.run_job(job))
        job.task.add_done_callback(lambda task: self.service.on_job_done(job, task))
        self.event.raw_text = f"/cancel_{job.token}"
        await self.service.on_message(self.event)
        await asyncio.gather(job.task, return_exceptions=True)
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        self.assertNotIn(job.token, self.service.jobs)
        self.event.raw_text = "/status"
        await self.service.on_message(self.event)
        self.assertIn("Cancelled before transfer started", self.sent[-1][1])

    async def test_done_replaces_running_status_and_keeps_recent_finished(self):
        running = Job(7, "peer", 12, Source("video", VIDEO_URL), ("480p",), None, state="running")
        self.service.jobs[running.token] = running
        self.service.completed[7] = [(asyncio.get_running_loop().time(), "🎬 Yesterday's video · Finished")]
        await self.service.on_message(self.event)  # dashboard #101
        self.service.run_job = AsyncMock()
        choice = Selection("abc12345", Source("video", VIDEO_URL), ("720p",), "peer",
                           video=Video("New title", {"720p": "https://cdn.example.com/720.mp4"}),
                           selected={"720p"})
        self.service.selections[7] = choice
        click = SimpleNamespace(sender_id=7, is_private=True, message_id=55,
                                data=b"s:abc12345:done", answer=AsyncMock())
        await self.service.callback(click)
        new_text = self.client.send_message.await_args.args[1]
        self.assertIn(running.token, new_text)
        self.assertIn("Running", new_text)
        self.assertIn("Yesterday's video", new_text)
        self.assertIn("New title", new_text)
        self.assertIn("Queued", new_text)
        self.assertIn("Queue #1", new_text)
        self.assertEqual(set(self.client.delete_messages.await_args.args[1]), {12, 55, 101})
        self.assertNotIn("Cancel", str(self.client.send_message.await_args.kwargs.get("buttons")))
        queued = next(job for job in self.service.user_jobs(7) if job is not running)
        await queued.task

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
        self.event.raw_text = "/status"
        await self.service.on_message(self.event)
        self.assertIn("Completed title", self.sent[-1][1])
        self.assertIn("Finished", self.sent[-1][1])
        self.assertIn(101, self.client.delete_messages.await_args.args[1])

    async def test_status_retains_five_recent_finished_jobs(self):
        now = asyncio.get_running_loop().time()
        self.service.completed[7] = [(now - i, f"🎬 Finished video {i}") for i in range(5)]
        await self.service.on_message(self.event)
        for i in range(5):
            self.assertIn(f"Finished video {i}", self.sent[-1][1])

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
