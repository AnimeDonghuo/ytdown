"""Tests for the persistent Store (in-memory mode — no MongoDB needed)."""
import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import ytdown.store as store_module
from ytdown.store import Store


class StoreInMemoryTests(unittest.TestCase):
    def setUp(self) -> None:
        # A store with no URL is the in-memory fallback.
        self.store = Store()

    def test_no_url_keeps_state_in_memory(self) -> None:
        self.assertFalse(self.store.connected)
        self.assertEqual(self.store.snapshot(), {"backend": "memory", "uploaded": 0, "users": 0})

    def test_mark_uploaded_records_every_variant(self) -> None:
        self.store.mark_uploaded(7, "PAW Patrol: The Dino Movie (2026)")
        keys = self.store.uploaded_keys(7)
        # Every form titles_match produces is stored, so a re-upload with a slightly
        # different name (year, quality, subtitle tag) is still recognised. "Movie"
        # is on the bot's quality-word strip list, so the stored form is without it.
        self.assertIn("paw patrol the dino", keys)

    def test_uploaded_keys_are_scoped_to_the_user(self) -> None:
        self.store.mark_uploaded(7, "Naruto")
        self.store.mark_uploaded(8, "Naruto")
        self.assertIn("naruto", self.store.uploaded_keys(7))
        self.assertIn("naruto", self.store.uploaded_keys(8))
        self.assertEqual(self.store.uploaded_keys(99), set())

    def test_settings_round_trip(self) -> None:
        self.store.put_settings(7, {"auto_enabled": True, "limit": 5,
                                    "sections": ["movies", "anime"]})
        got = self.store.get_settings(7)
        self.assertEqual(got.get("auto_enabled"), True)
        self.assertEqual(got.get("limit"), 5)
        self.assertEqual(got.get("sections"), ["movies", "anime"])
        # A copy is returned, so mutating it does not change the store.
        got["limit"] = 99
        self.assertEqual(self.store.get_settings(7).get("limit"), 5)

    def test_empty_inputs_are_ignored(self) -> None:
        self.store.mark_uploaded(0, "Naruto")
        self.store.mark_uploaded(7, "")
        self.assertEqual(self.store.uploaded_keys(7), set())
        self.store.put_settings(0, {"auto_enabled": True})
        self.assertEqual(self.store.get_settings(0), {})

    def test_snapshot_user_settings_returns_a_copy(self) -> None:
        self.store.put_settings(7, {"auto_enabled": True, "limit": 3})
        snap = self.store.snapshot_user_settings()
        self.assertEqual(snap.get(7, {}).get("limit"), 3)
        # Mutating the snapshot must not affect the store.
        snap[7]["limit"] = 99
        self.assertEqual(self.store.get_settings(7).get("limit"), 3)


class StoreMongoTests(unittest.IsolatedAsyncioTestCase):
    """With motor installed, the store tries to connect; here we stub the client."""

    async def test_failed_connect_falls_back_to_memory(self) -> None:
        fake_client = MagicMock()
        fake_admin = MagicMock()
        fake_admin.command = AsyncMock(side_effect=RuntimeError("no server"))
        fake_client.admin = fake_admin
        with patch.object(store_module, "AsyncIOMotorClient", return_value=fake_client), \
                patch.object(store_module, "_HAS_MOTOR", True):
            store = Store("mongodb://localhost:1")
            await store.start()
            self.assertFalse(store.connected)
            self.assertEqual(store.snapshot()["backend"], "memory")
            store.mark_uploaded(7, "Naruto")
            self.assertIn("naruto", store.uploaded_keys(7))

    async def test_load_populates_cache(self) -> None:
        async def fake_find_uploaded(_query):
            for row in [{"uid": 7, "key": "naruto", "ts": 1.0}]:
                yield row

        async def fake_find_settings(_query):
            for row in [{"_id": 7, "auto_enabled": True, "limit": 3}]:
                yield row

        fake_collection_uploaded = MagicMock()
        fake_collection_uploaded.find = fake_find_uploaded
        fake_collection_settings = MagicMock()
        fake_collection_settings.find = fake_find_settings

        fake_db = MagicMock()
        fake_db.__getitem__.side_effect = lambda name: (
            fake_collection_uploaded if name == "uploaded" else fake_collection_settings)

        fake_client = MagicMock()
        fake_admin = MagicMock()
        fake_admin.command = AsyncMock(return_value={"ok": 1})
        fake_client.admin = fake_admin
        fake_client.__getitem__ = MagicMock(return_value=fake_db)

        with patch.object(store_module, "AsyncIOMotorClient", return_value=fake_client), \
                patch.object(store_module, "_HAS_MOTOR", True):
            store = Store("mongodb://localhost:1")
            await store.start()
            self.assertTrue(store.connected)
            self.assertIn("naruto", store.uploaded_keys(7))
            self.assertEqual(store.get_settings(7).get("limit"), 3)
            await store.close()


if __name__ == "__main__":
    unittest.main()
