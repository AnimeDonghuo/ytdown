import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from ytdown.bot import API_URL, DEFAULT_APIS, DownloaderBot
from ytdown.dailymotion import DailymotionInfo, DMFormat
from ytdown.terabox import TeraboxFile, TeraboxListing

CUSTOM_TERA = "https://tera.example.com/api?key=abc"
CUSTOM_DAILY = "https://daily.example.com/info"
TERA_LINK = "https://terasharefile.com/s/1abcdef"
DAILY_LINK = "https://dai.ly/x7tgad0"


def command(service: DownloaderBot, uid: int, text: str):
    event = SimpleNamespace(sender_id=uid, is_private=True, raw_text=text,
                            respond=AsyncMock(), get_input_chat=AsyncMock(return_value="peer"))
    return event, service.on_message(event)


class ChangeApiTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.service = DownloaderBot(MagicMock(), MagicMock())

    async def send(self, text: str):
        event, call = command(self.service, 7, text)
        await call
        return event.respond.call_args.args[0]

    async def test_each_service_has_its_own_api(self):
        self.assertIn("Custom Terabox API saved", await self.send(f"/changeapi tera {CUSTOM_TERA}"))
        self.assertIn("Custom Dailymotion API saved", await self.send(f"/changeapi daily {CUSTOM_DAILY}"))
        self.assertEqual(self.service.api_for(7, "terabox"), CUSTOM_TERA)
        self.assertEqual(self.service.api_for(7, "dailymotion"), CUSTOM_DAILY)
        self.assertEqual(self.service.api_for(7), API_URL)  # YouTube untouched.
        self.assertEqual(self.service.api_for(8, "terabox"), DEFAULT_APIS["terabox"])

    async def test_keyword_aliases_and_legacy_youtube_form(self):
        await self.send(f"/changeapi dm {CUSTOM_DAILY}")
        await self.send(f"/changeapi youtube {CUSTOM_TERA}")
        self.assertEqual(self.service.api_for(7, "dailymotion"), CUSTOM_DAILY)
        self.assertEqual(self.service.api_for(7), CUSTOM_TERA)
        await self.send(f"/changeapi {CUSTOM_DAILY}")  # No keyword: YouTube, as before.
        self.assertEqual(self.service.api_for(7), CUSTOM_DAILY)

    async def test_bare_changeapi_only_explains(self):
        text = await self.send("/changeapi")
        self.assertIn("/changeapi yt", text)
        self.assertNotIn(7, self.service.awaiting_api)

    async def test_prompt_then_url_applies_to_the_chosen_service(self):
        await self.send("/changeapi tera")
        self.assertIn(7, self.service.awaiting_api)
        event, call = command(self.service, 7, CUSTOM_TERA)
        await call
        self.assertEqual(self.service.api_for(7, "terabox"), CUSTOM_TERA)
        self.assertNotIn(7, self.service.awaiting_api)

    async def test_show_and_reset_one_service_or_all(self):
        await self.send(f"/changeapi tera {CUSTOM_TERA}")
        await self.send(f"/changeapi daily {CUSTOM_DAILY}")
        shown = await self.send("/changeapi show")
        self.assertIn(f"Terabox: {CUSTOM_TERA} (custom)", shown)
        self.assertIn(f"Dailymotion: {CUSTOM_DAILY} (custom)", shown)
        self.assertIn("(built-in)", shown)
        await self.send("/changeapi reset tera")
        self.assertEqual(self.service.api_for(7, "terabox"), DEFAULT_APIS["terabox"])
        self.assertEqual(self.service.api_for(7, "dailymotion"), CUSTOM_DAILY)
        await self.send("/changeapi reset")
        self.assertEqual(self.service.api_for(7, "dailymotion"), DEFAULT_APIS["dailymotion"])
        self.assertNotIn(7, self.service.api_overrides)

    async def test_unsafe_or_wrong_urls_are_refused_per_service(self):
        self.assertIn("API must be a public HTTPS URL",
                      await self.send("/changeapi daily http://daily.example.com/info"))
        self.assertIn("Send an API endpoint",
                      await self.send("/changeapi tera https://www.youtube.com/watch?v=3SY1i5CB03Q"))
        self.assertIn("Unknown service", await self.send("/changeapi reset vimeo"))
        self.assertEqual(self.service.api_overrides, {})

    async def test_terabox_lookup_uses_the_custom_api(self):
        await self.send(f"/changeapi tera {CUSTOM_TERA}")
        self.service.modes[7] = "terabox"
        listing = TeraboxListing(files=(TeraboxFile("f1", "Clip.mp4", "720p", "https://cdn.example.com/c.mp4",
                                                    "", "video/mp4", 1000),))
        notice = SimpleNamespace(edit=AsyncMock())
        event = SimpleNamespace(sender_id=7, raw_text=TERA_LINK, get_input_chat=AsyncMock(return_value="peer"),
                                respond=AsyncMock(return_value=notice))
        with patch("ytdown.bot.fetch_terabox", new=AsyncMock(return_value=listing)) as fetch:
            await self.service.on_link(event)
        self.assertEqual(fetch.call_args.args[2], CUSTOM_TERA)
        self.assertEqual(self.service.selections[7].api_endpoint, CUSTOM_TERA)

    async def test_terabox_folder_range_keeps_the_custom_api(self):
        await self.send(f"/changeapi tera {CUSTOM_TERA}")
        self.service.modes[7] = "terabox"
        files = tuple(TeraboxFile(f"f{i}", f"Clip{i}.mp4", "720p", f"https://cdn.example.com/{i}.mp4", "",
                                  "video/mp4", 1000) for i in range(2))
        notice = SimpleNamespace(edit=AsyncMock())
        event = SimpleNamespace(sender_id=7, raw_text=TERA_LINK, get_input_chat=AsyncMock(return_value="peer"),
                                respond=AsyncMock(return_value=notice))
        with patch("ytdown.bot.fetch_terabox", new=AsyncMock(return_value=TeraboxListing(files=files))):
            await self.service.on_link(event)
        self.assertEqual(self.service.pending_ranges[7].api_endpoint, CUSTOM_TERA)

    async def test_dailymotion_lookup_uses_the_custom_api(self):
        await self.send(f"/changeapi daily {CUSTOM_DAILY}")
        self.service.modes[7] = "dailymotion"
        info = DailymotionInfo("Clip", 10, "", (DMFormat("360p", "https://cdn.example.com/a.m3u8", 640, 360, 0),))
        notice = SimpleNamespace(edit=AsyncMock())
        event = SimpleNamespace(sender_id=7, raw_text=DAILY_LINK, get_input_chat=AsyncMock(return_value="peer"),
                                respond=AsyncMock(return_value=notice))
        with patch("ytdown.bot.fetch_dailymotion", new=AsyncMock(return_value=info)) as fetch:
            await self.service.on_link(event)
        self.assertEqual(fetch.call_args.args[2], CUSTOM_DAILY)
        self.assertEqual(self.service.selections[7].api_endpoint, CUSTOM_DAILY)


if __name__ == "__main__":
    unittest.main()
