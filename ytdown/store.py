"""Persistent state across bot restarts.

Two collections:

* ``uploaded`` - one row per (user, title) the bot has already uploaded, so the
  automatic watcher does not re-upload the same file after a redeploy.
* ``settings`` - one row per user with their apitera settings (``/auto`` state,
  the per-section list and limits, the API overrides, and the peer to upload to).

The store is optional: when ``MONGODB_URL`` is unset or unreachable the store
falls back to an in-memory dict, so the bot still runs locally without a database.
A failed write or read is logged once and then the in-memory copy is used, so a
flaky database never breaks a download.
"""
from __future__ import annotations

import logging
import os
import threading
from typing import Any

LOG = logging.getLogger(__name__)

# motor is the official async MongoDB driver. The store works without it: the
# in-memory fallback covers local development and any environment where the
# optional dep is not installed.
try:
    from motor.motor_asyncio import AsyncIOMotorClient  # type: ignore
    _HAS_MOTOR = True
except ImportError:  # pragma: no cover - depends on optional install
    AsyncIOMotorClient = None  # type: ignore
    _HAS_MOTOR = False


# Collections used by the store. Kept short so they show up cleanly in mongosh.
UPLOADED_COLLECTION = "uploaded"
SETTINGS_COLLECTION = "settings"
SETTINGS_DB = "ytdown"


def _safe_key(title: str) -> str:
    """The canonical form of ``title`` for storage (the same one the bot matches on).

    Mongo keys cap at 1024 bytes; a long listing title fits easily, but special
    characters would be invalid in BSON keys, so the title is hashed when it
    would not fit. The first variant is used as the canonical key, so a lookup
    by any of ``title_variants(title)`` finds the row.
    """
    from .parciv import title_variants
    variants = sorted(title_variants(title))
    return variants[0] if variants else ""


class Store:
    """A small persistent store for the apitera watcher and per-user settings.

    The store is constructed once at startup. ``start()`` opens the MongoDB
    connection (if any) and loads every user's settings so the bot can answer
    queries and run the watcher without a round trip to the database for the
    first request. ``close()`` flushes pending writes and closes the connection.
    """

    def __init__(self, url: str = "", *, db_name: str = SETTINGS_DB) -> None:
        self._url = url or os.getenv("MONGODB_URL", "").strip()
        self._db_name = db_name
        self._client: Any = None
        self._db: Any = None
        self._lock = threading.Lock()
        # In-memory caches: always populated, kept in sync with the database on
        # every successful read or write so a lookup never needs the network.
        self._uploaded: dict[tuple[int, str], float] = {}
        self._settings: dict[int, dict[str, Any]] = {}
        self._connected = False
        self._warned_disconnect = False

    @property
    def connected(self) -> bool:
        """True when the store is reading from and writing to MongoDB."""
        return self._connected

    async def start(self) -> None:
        """Open the MongoDB connection (when a URL was supplied) and warm the cache."""
        if not self._url or not _HAS_MOTOR:
            if self._url and not _HAS_MOTOR:
                LOG.warning("MONGODB_URL is set but motor is not installed; "
                            "running with an in-memory store only.")
            return
        try:
            self._client = AsyncIOMotorClient(self._url, serverSelectionTimeoutMS=5_000)
            # ping forces the connection to be established; without it the first
            # read after a cold start can hang for ten seconds while the driver
            # discovers the topology.
            await self._client.admin.command("ping")
            self._db = self._client[self._db_name]
            self._connected = True
        except Exception as exc:
            LOG.warning("Could not connect to MongoDB (%s); using in-memory state only.", exc)
            self._client = None
            self._db = None
            self._connected = False
            return
        await self._load_uploaded()
        await self._load_settings()
        LOG.info("MongoDB store ready at %s/%s (%d uploaded titles, %d users)",
                 self._url, self._db_name, len(self._uploaded), len(self._settings))

    async def close(self) -> None:
        """Close the MongoDB connection. Pending in-memory writes are not flushed."""
        if self._client is not None:
            self._client.close()
        self._client = None
        self._db = None
        self._connected = False

    async def _load_uploaded(self) -> None:
        if not self._connected:
            return
        try:
            async for row in self._db[UPLOADED_COLLECTION].find({}):
                uid = int(row.get("uid", 0))
                key = str(row.get("key", ""))
                ts = float(row.get("ts", 0.0))
                if uid and key:
                    self._uploaded[(uid, key)] = ts
        except Exception as exc:
            LOG.warning("Loading uploaded titles from MongoDB failed (%s).", exc)
            self._warned_disconnect = True

    async def _load_settings(self) -> None:
        if not self._connected:
            return
        try:
            async for row in self._db[SETTINGS_COLLECTION].find({}):
                uid = int(row.get("_id", 0) or row.get("uid", 0))
                if not uid:
                    continue
                payload = {key: value for key, value in row.items()
                           if key not in ("_id", "uid")}
                self._settings[uid] = payload
        except Exception as exc:
            LOG.warning("Loading user settings from MongoDB failed (%s).", exc)
            self._warned_disconnect = True

    # ---- uploaded titles ----------------------------------------------------

    def mark_uploaded(self, uid: int, title: str, *, ts: float | None = None) -> None:
        """Record that ``title`` has been uploaded for ``uid``.

        ``title_variants`` produces every form ``already_uploaded`` would match
        against, so all of them are stored. The write is fire-and-forget: a
        Mongo failure is logged but does not block the upload.
        """
        from time import time
        if not uid or not title:
            return
        from .parciv import title_variants
        variants = title_variants(title)
        if not variants:
            return
        when = float(ts if ts is not None else time())
        with self._lock:
            for variant in variants:
                self._uploaded[(uid, variant)] = when
        if self._connected and self._db is not None:
            self._write_uploaded_async(uid, title, when)

    def _write_uploaded_async(self, uid: int, title: str, when: float) -> None:
        # Imported lazily so the bot does not need an event loop just to construct
        # the store. The caller has already mutated the in-memory cache.
        try:
            loop = __import__("asyncio").get_event_loop()
            if loop.is_running():
                loop.create_task(self._write_uploaded(uid, title, when))
        except RuntimeError:
            pass

    async def _write_uploaded(self, uid: int, title: str, when: float) -> None:
        if not self._connected or self._db is None:
            return
        try:
            await self._db[UPLOADED_COLLECTION].update_one(
                {"_id": f"{uid}:{_safe_key(title)}"},
                {"$set": {"uid": uid, "key": _safe_key(title),
                          "title": title[:200], "ts": when}},
                upsert=True)
        except Exception as exc:
            LOG.warning("Writing uploaded title to MongoDB failed (%s).", exc)
            self._warned_disconnect = True

    def uploaded_keys(self, uid: int) -> set[str]:
        """The variant forms of every title ``uid`` has already uploaded."""
        with self._lock:
            return {key for (owner, key) in self._uploaded.keys() if owner == uid}

    # ---- per-user settings -------------------------------------------------

    def get_settings(self, uid: int) -> dict[str, Any]:
        """A copy of the user's settings, or an empty dict when none are stored."""
        with self._lock:
            return dict(self._settings.get(uid, {}))

    def put_settings(self, uid: int, payload: dict[str, Any]) -> None:
        """Store ``payload`` as the user's settings. The write is fire-and-forget."""
        if not uid:
            return
        with self._lock:
            self._settings[uid] = dict(payload)
        if self._connected and self._db is not None:
            self._write_settings_async(uid, payload)

    def _write_settings_async(self, uid: int, payload: dict[str, Any]) -> None:
        try:
            loop = __import__("asyncio").get_event_loop()
            if loop.is_running():
                loop.create_task(self._write_settings(uid, payload))
        except RuntimeError:
            pass

    async def _write_settings(self, uid: int, payload: dict[str, Any]) -> None:
        if not self._connected or self._db is None:
            return
        try:
            await self._db[SETTINGS_COLLECTION].update_one(
                {"_id": uid},
                {"$set": dict(payload, uid=uid)},
                upsert=True)
        except Exception as exc:
            LOG.warning("Writing user settings to MongoDB failed (%s).", exc)
            self._warned_disconnect = True

    # ---- lifecycle helpers -------------------------------------------------

    def snapshot(self) -> dict[str, Any]:
        """A short status string for /status and the health check."""
        with self._lock:
            return {
                "backend": "mongodb" if self._connected else "memory",
                "uploaded": len(self._uploaded),
                "users": len(self._settings),
            }

    def snapshot_user_settings(self) -> dict[int, dict[str, Any]]:
        """A copy of every user's settings, for the bot to populate on startup."""
        with self._lock:
            return {uid: dict(payload) for uid, payload in self._settings.items()}


__all__ = ["Store", "UPLOADED_COLLECTION", "SETTINGS_COLLECTION", "SETTINGS_DB"]
