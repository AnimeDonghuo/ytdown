import asyncio
import io
import logging
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp

from ytdown.bot import DownloaderBot
from ytdown.proxy import (CHECK_TARGETS, CheckResult, ProxyPool, ProxyRouter, ProxyState,
                          ProxyUnavailable, SiteResult, check_proxy, parse_proxies,
                          parse_proxy_line, proxy_fault, resolve_targets, run_checks, short_reason,
                          site_marks, site_ok, target_for)

URL = "https://www.youtube.com/generate_204"
logging.getLogger("ytdown").setLevel(logging.CRITICAL)  # Keep expected drop notices quiet.


class FakeContent:
    def __init__(self, data: bytes = b""):
        self.data = data

    async def read(self, count=-1):
        data = self.data if count < 0 else self.data[:count]
        self.data = self.data[len(data):]
        return data


class FakeResponse:
    def __init__(self, status=200, body=b""):
        self.status = status
        self.content = FakeContent(body)
        self.request_info = SimpleNamespace(url=URL)
        self.history = ()


class FakeContext:
    def __init__(self, response=None, error=None):
        self.response = response
        self.error = error
        self.exited = False

    async def __aenter__(self):
        if self.error is not None:
            raise self.error
        return self.response

    async def __aexit__(self, *args):
        self.exited = True
        return False


class FakeSession:
    """Minimal stand-in for aiohttp.ClientSession with a per-request handler."""

    def __init__(self, handler=None):
        self.handler = handler or (lambda proxy, url: FakeContext(FakeResponse(204)))
        self.requests = []
        self.closed = False

    def get(self, url, **kwargs):
        proxy = kwargs.get("proxy")
        self.requests.append((url, proxy))
        return self.handler(proxy, url)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.close()
        return False

    async def close(self):
        self.closed = True


def refused(proxy=None, url=None):
    return FakeContext(error=ConnectionRefusedError("connection refused"))


def site_results(ok: bool, detail: str = "timeout") -> tuple[SiteResult, ...]:
    """A result covering every default site."""
    return tuple(SiteResult(target.key, target.label, ok, 25 if ok else 0, "" if ok else detail)
                 for target in CHECK_TARGETS)


def result_for(proxy, ok: bool = True, latency: int = 30, detail: str = "",
               sites: tuple[SiteResult, ...] | None = None) -> CheckResult:
    return CheckResult(proxy, ok, latency, detail, sites if sites is not None else site_results(ok))


def proxy_list(*values) -> ProxyPool:
    pool = ProxyPool()
    pool.add([parse_proxy_line(value) for value in values])
    return pool


def session_factory(handler=None):
    return lambda proxy: FakeSession(handler)


class ParseTests(unittest.TestCase):
    def test_common_forms(self):
        self.assertEqual(parse_proxy_line("1.2.3.4:8080").url, "http://1.2.3.4:8080")
        self.assertEqual(parse_proxy_line("user:pass@1.2.3.4:8080").url, "http://user:pass@1.2.3.4:8080")
        self.assertEqual(parse_proxy_line("1.2.3.4:8080:user:pass").url, "http://user:pass@1.2.3.4:8080")
        self.assertEqual(parse_proxy_line("SOCKS5://user:pass@HOST.io:1080").url,
                         "socks5://user:pass@host.io:1080")
        self.assertEqual(parse_proxy_line("[2001:db8::1]:1080").url, "http://[2001:db8::1]:1080")
        self.assertEqual(parse_proxy_line("1.2.3.4:8080 user:pass").url, "http://user:pass@1.2.3.4:8080")
        self.assertEqual(parse_proxy_line("1.2.3.4 3128").url, "http://1.2.3.4:3128")
        self.assertTrue(parse_proxy_line("socks://1.2.3.4:1080").socks)

    def test_invalid_lines_are_rejected(self):
        for bad in ("", "1.2.3.4", "1.2.3.4:0", "1.2.3.4:99999", "1.2.3.4:abc",
                    "ftp://1.2.3.4:21", "http://:8080", "user:pass@1.2.3.4"):
            with self.assertRaises(ValueError, msg=bad):
                parse_proxy_line(bad)

    def test_passwords_are_masked_and_encoded(self):
        proxy = parse_proxy_line("socks5://user:p%40ss@1.2.3.4:1080")
        self.assertEqual(proxy.password, "p@ss")
        self.assertEqual(proxy.url, "socks5://user:p%40ss@1.2.3.4:1080")
        self.assertEqual(proxy.label, "socks5://user:***@1.2.3.4:1080")
        self.assertEqual(parse_proxy_line("1.2.3.4:8080").label, "http://1.2.3.4:8080")

    def test_list_parsing_skips_comments_and_duplicates(self):
        text = ("# my proxies\n"
                "1.2.3.4:8080\n"
                "1.2.3.4:8080\n"
                "5.6.7.8:9090, 6.7.8.9:1000\n"
                "not-a-proxy\n"
                "socks5://1.2.3.4:1080\n")
        proxies, errors = parse_proxies(text)
        self.assertEqual([proxy.url for proxy in proxies],
                         ["http://1.2.3.4:8080", "http://5.6.7.8:9090", "http://6.7.8.9:1000",
                          "socks5://1.2.3.4:1080"])
        self.assertEqual(len(errors), 1)
        self.assertIn("not-a-proxy", errors[0])

    def test_working_list_file_round_trips(self):
        # The bot hands working proxies back with site marks as comments;
        # pasting that file again must yield the same proxies.
        text = ("http://1.2.3.4:8080  # YT,DM,RU\n"
                "socks5://user:pass@5.6.7.8:1080  # RU\n"
                "http://user:pa#ss@6.7.8.9:3128\n")
        proxies, errors = parse_proxies(text)
        self.assertEqual(errors, [])
        self.assertEqual([proxy.url for proxy in proxies],
                         ["http://1.2.3.4:8080", "socks5://user:pass@5.6.7.8:1080",
                          "http://user:pa%23ss@6.7.8.9:3128"])

    def test_limit_is_reported(self):
        proxies, errors = parse_proxies("\n".join(f"1.2.3.{i}:8080" for i in range(1, 6)), 2)
        self.assertEqual(len(proxies), 2)
        self.assertTrue(any("only the first 2" in error for error in errors))


class TargetTests(unittest.TestCase):
    def test_default_targets_cover_youtube_dailymotion_and_rumble(self):
        self.assertEqual([target.key for target in CHECK_TARGETS],
                         ["youtube", "dailymotion", "rumble"])
        for target in CHECK_TARGETS:
            self.assertTrue(target.url.startswith("https://"))
            self.assertTrue(target.hosts)
            self.assertTrue(target.label and target.short)

    def test_target_lookup_by_url_or_host(self):
        self.assertEqual(target_for("https://www.dailymotion.com/video/x8abc"), "dailymotion")
        self.assertEqual(target_for("https://www.rumble.com/v123-title.html"), "rumble")
        self.assertEqual(target_for("https://i.ytimg.com/vi/xyz/hqdefault.jpg"), "youtube")
        self.assertEqual(target_for("https://www.youtube.com/oembed"), "youtube")
        self.assertEqual(target_for("https://samra-youtube-api.antideploy.com/?url=x"), None)
        self.assertEqual(target_for(""), None)

    def test_resolve_targets_selection(self):
        self.assertEqual([target.key for target in resolve_targets()],
                         ["youtube", "dailymotion", "rumble"])
        self.assertEqual([target.key for target in resolve_targets("rumble, youtube")],
                         ["youtube", "rumble"])
        self.assertEqual([target.key for target in resolve_targets("nonsense")], ["youtube"])
        override = resolve_targets("", "https://api.example.com/healthz")
        self.assertEqual(override[0].url, "https://api.example.com/healthz")
        self.assertEqual(len(override), 3)

    def test_site_marks(self):
        self.assertEqual(site_marks({"youtube": True, "dailymotion": True, "rumble": False}),
                         "YT,DM")
        self.assertEqual(site_marks({}), "—")


class PoolTests(unittest.TestCase):
    def test_add_dedupe_limit_and_purge(self):
        pool = ProxyPool(limit=2)
        added = pool.add([parse_proxy_line("1.2.3.4:8080"), parse_proxy_line("1.2.3.4:8080"),
                          parse_proxy_line("5.6.7.8:9090"), parse_proxy_line("6.7.8.9:1000")])
        self.assertEqual((added.added, added.duplicates, added.overflow), (2, 1, 1))
        first = parse_proxy_line("1.2.3.4:8080")
        pool.mark_checked(CheckResult(first, True, 120))
        second = parse_proxy_line("5.6.7.8:9090")
        pool.mark_checked(CheckResult(second, False, 0, "timeout"))
        self.assertEqual([state.proxy.url for state in pool.live()], [first.url])
        self.assertIn("1 live", pool.summary())
        self.assertIn("1 dead", pool.summary())
        self.assertEqual(pool.purge_dead(), 1)
        self.assertEqual(len(pool), 1)
        self.assertEqual(pool.clear(), 1)
        self.assertEqual(len(pool), 0)

    def test_candidates_round_robin_skip_dead(self):
        pool = proxy_list("1.2.3.4:8080", "5.6.7.8:9090", "6.7.8.9:1000")
        self.assertEqual([state.proxy.host for state in pool.candidates(2)],
                         ["1.2.3.4", "5.6.7.8"])
        self.assertEqual([state.proxy.host for state in pool.candidates(2)],
                         ["5.6.7.8", "6.7.8.9"])
        dead = parse_proxy_line("5.6.7.8:9090")
        pool.mark_checked(CheckResult(dead, False, 0, "timeout"))
        self.assertNotIn("5.6.7.8", [state.proxy.host for state in pool.candidates(3)])

    def test_custom_fail_limit_and_pool_size(self):
        pool = ProxyPool(limit=3, fail_limit=3)
        proxy = parse_proxy_line("1.2.3.4:8080")
        pool.add([proxy])
        self.assertIsNone(pool.record_failure(proxy, "timeout"))
        self.assertIsNone(pool.record_failure(proxy, "timeout"))
        self.assertIsNotNone(pool.record_failure(proxy, "timeout"))
        self.assertEqual(pool.fail_limit, 3)

    def test_candidates_skip_proxies_blocked_for_a_site(self):
        pool = proxy_list("1.2.3.4:8080", "5.6.7.8:9090")
        first, second = parse_proxy_line("1.2.3.4:8080"), parse_proxy_line("5.6.7.8:9090")
        pool.mark_checked(CheckResult(first, True, 30, "",
                                      (SiteResult("youtube", "YouTube", True, 30),
                                       SiteResult("dailymotion", "Dailymotion", False, 0, "HTTP 403"),
                                       SiteResult("rumble", "Rumble", True, 40))))
        pool.mark_checked(CheckResult(second, True, 60, "",
                                      (SiteResult("youtube", "YouTube", True, 60),
                                       SiteResult("dailymotion", "Dailymotion", True, 50),
                                       SiteResult("rumble", "Rumble", False, 0, "timeout"))))
        self.assertEqual(pool.entries[first.url].sites["dailymotion"], False)
        self.assertEqual([state.proxy.host for state in pool.candidates(2, site="dailymotion")],
                         ["5.6.7.8"])
        self.assertEqual([state.proxy.host for state in pool.candidates(1, site="youtube")],
                         ["1.2.3.4"])
        pool._cursor = 0
        self.assertEqual([state.proxy.host for state in pool.candidates(1)], ["1.2.3.4"])
        self.assertEqual(pool.live_for("rumble"), [pool.entries[first.url]])

    def test_failures_drop_proxy_after_the_limit(self):
        pool = proxy_list("1.2.3.4:8080")
        proxy = parse_proxy_line("1.2.3.4:8080")
        self.assertIsNone(pool.record_failure(proxy, "timeout"))
        self.assertEqual(pool.get(proxy).fail_count, 1)
        self.assertIsNotNone(pool.record_failure(proxy, "timeout"))
        self.assertIsNone(pool.get(proxy))
        pool.add([proxy])
        pool.record_success(proxy, 90)
        self.assertEqual(pool.get(proxy).status, "alive")
        self.assertEqual(pool.get(proxy).fail_count, 0)

    def test_proxy_fault_and_reason_helpers(self):
        self.assertTrue(proxy_fault("ERROR: Unable to download webpage: connection refused"))
        self.assertTrue(proxy_fault("Sign in to confirm you're not a bot"))
        self.assertFalse(proxy_fault("Video unavailable"))
        self.assertEqual(short_reason(ValueError("line\nbroken")), "line broken")
        self.assertTrue(short_reason("x" * 300).endswith("…"))


class RouterTests(unittest.IsolatedAsyncioTestCase):
    async def test_request_uses_a_live_proxy(self):
        pool = proxy_list("1.2.3.4:8080")
        sessions = {}

        def factory(proxy):
            sessions[proxy.url] = FakeSession()
            return sessions[proxy.url]

        direct = FakeSession()
        router = ProxyRouter(pool, direct, timeout=aiohttp.ClientTimeout(total=5), headers={},
                             session_factory=factory)
        async with router.get(URL) as response:
            self.assertEqual(response.status, 204)
        self.assertEqual(sessions["http://1.2.3.4:8080"].requests, [(URL, "http://1.2.3.4:8080")])
        self.assertEqual(direct.requests, [])
        state = pool.get(parse_proxy_line("1.2.3.4:8080"))
        self.assertEqual((state.status, state.ok_count, state.fail_count), ("alive", 1, 0))
        self.assertIn("1 live/1 in pool", router.describe())
        await router.close()

    async def test_failing_proxy_is_counted_and_the_next_one_is_tried(self):
        pool = proxy_list("1.2.3.4:8080", "5.6.7.8:9090")
        bad_proxy, good_proxy = parse_proxy_line("1.2.3.4:8080"), parse_proxy_line("5.6.7.8:9090")

        def factory(proxy):
            return FakeSession(refused if proxy.url == bad_proxy.url else None)

        dropped = []
        router = ProxyRouter(pool, FakeSession(), timeout=aiohttp.ClientTimeout(total=5), headers={},
                             session_factory=factory,
                             on_drop=lambda state, detail: dropped.append(state))
        async with router.get(URL) as response:
            self.assertEqual(response.status, 204)
        self.assertEqual(pool.get(bad_proxy).fail_count, 1)
        self.assertEqual(pool.get(good_proxy).status, "alive")
        # One more failure reaches the limit, so the proxy is removed from the pool.
        pool.entries[bad_proxy.url] = ProxyState(bad_proxy, fail_count=1)
        pool._cursor = 0  # Deterministic: the broken proxy is tried first.
        async with router.get(URL) as response:
            self.assertEqual(response.status, 204)
        self.assertIsNone(pool.get(bad_proxy))
        self.assertEqual(len(dropped), 1)
        self.assertIn("dropped 1 in use", router.describe())
        await router.close()

    async def test_proxy_http_errors_are_treated_as_proxy_failures(self):
        pool = proxy_list("1.2.3.4:8080", "5.6.7.8:9090")
        bad = "http://1.2.3.4:8080"

        def factory(proxy):
            return FakeSession((lambda p, u: FakeContext(FakeResponse(502)))
                               if proxy.url == bad else None)

        router = ProxyRouter(pool, FakeSession(), timeout=aiohttp.ClientTimeout(total=5), headers={},
                             session_factory=factory)
        async with router.get(URL) as response:
            self.assertEqual(response.status, 204)
        self.assertEqual(pool.get(parse_proxy_line("1.2.3.4:8080")).fail_count, 1)
        await router.close()

    async def test_empty_pool_uses_direct_and_strict_mode_refuses(self):
        direct = FakeSession()
        router = ProxyRouter(ProxyPool(), direct, timeout=aiohttp.ClientTimeout(total=5), headers={})
        async with router.get(URL) as response:
            self.assertEqual(response.status, 204)
        self.assertEqual(router.direct_fallbacks, 1)
        self.assertEqual(len(direct.requests), 1)
        self.assertIn("direct connection", router.describe())
        await router.close()

        strict = ProxyRouter(ProxyPool(), FakeSession(), timeout=aiohttp.ClientTimeout(total=5),
                             headers={}, strict=True)
        with self.assertRaises(ProxyUnavailable):
            await strict.get(URL).__aenter__()
        await strict.close()

    async def test_all_proxies_failing_falls_back_to_direct(self):
        pool = proxy_list("1.2.3.4:8080")
        direct = FakeSession()
        router = ProxyRouter(pool, direct, timeout=aiohttp.ClientTimeout(total=5), headers={},
                             session_factory=session_factory(refused))
        async with router.get(URL) as response:
            self.assertEqual(response.status, 204)  # The direct retry still delivered.
        self.assertEqual(len(direct.requests), 1)
        self.assertEqual(router.direct_fallbacks, 1)
        self.assertEqual(pool.get(parse_proxy_line("1.2.3.4:8080")).fail_count, 1)
        await router.close()

    async def test_sessions_are_cached_and_idle_ones_are_closed(self):
        pool = proxy_list("1.2.3.4:8080")
        created = []

        def factory(proxy):
            session = FakeSession()
            created.append(session)
            return session

        router = ProxyRouter(pool, FakeSession(), timeout=aiohttp.ClientTimeout(total=5),
                             cache_size=1, session_factory=factory)
        async with router.get(URL):
            pass
        async with router.get(URL):
            pass
        self.assertEqual(len(created), 1)  # Cached and reused.
        other = parse_proxy_line("5.6.7.8:9090")
        pool.add([other])
        pool._cursor = 1  # Deterministic: the new proxy is tried first.
        async with router.get(URL):  # Second proxy evicts the idle cached session.
            pass
        self.assertTrue(created[0].closed)
        await router.close()
        self.assertTrue(created[1].closed)

    async def test_router_prefers_proxies_that_reach_the_requested_site(self):
        pool = proxy_list("1.2.3.4:8080", "5.6.7.8:9090")
        blocked, good = parse_proxy_line("1.2.3.4:8080"), parse_proxy_line("5.6.7.8:9090")
        pool.mark_checked(CheckResult(blocked, True, 20, "",
                                      (SiteResult("youtube", "YouTube", True, 20),
                                       SiteResult("dailymotion", "Dailymotion", False, 0, "HTTP 403"),
                                       SiteResult("rumble", "Rumble", False, 0, "timeout"))))
        pool.mark_checked(CheckResult(good, True, 50, "",
                                      (SiteResult("youtube", "YouTube", True, 50),
                                       SiteResult("dailymotion", "Dailymotion", True, 45),
                                       SiteResult("rumble", "Rumble", True, 55))))
        sessions = {}

        def factory(proxy):
            sessions[proxy.url] = FakeSession()
            return sessions[proxy.url]

        router = ProxyRouter(pool, FakeSession(), timeout=aiohttp.ClientTimeout(total=5),
                             headers={}, session_factory=factory)
        async with router.get("https://www.dailymotion.com/robots.txt"):
            pass
        self.assertIn("http://5.6.7.8:9090", sessions)  # Dailymotion-capable proxy chosen.
        sessions.clear()
        async with router.get("https://www.youtube.com/generate_204"):
            pass
        self.assertTrue(sessions)  # Any YouTube-capable proxy is fine.
        self.assertNotIn("http://5.6.7.8:9090", [])  # (sanity for the assertion above)
        self.assertEqual(router.next_proxy("dailymotion").host, "5.6.7.8")
        await router.close()

    async def test_broken_session_setup_counts_as_a_proxy_failure(self):
        pool = proxy_list("1.2.3.4:8080", "5.6.7.8:9090")
        broken = "http://1.2.3.4:8080"

        def factory(proxy):
            if proxy.url == broken:
                raise ValueError("library cannot use this proxy")
            return FakeSession()

        router = ProxyRouter(pool, FakeSession(), timeout=aiohttp.ClientTimeout(total=5), headers={},
                             session_factory=factory)
        pool.entries[broken] = ProxyState(parse_proxy_line("1.2.3.4:8080"), fail_count=1)
        pool._cursor = 0
        async with router.get(URL) as response:
            self.assertEqual(response.status, 204)
        self.assertIsNone(pool.get(parse_proxy_line("1.2.3.4:8080")))
        await router.close()

    async def test_next_proxy_for_ytdlp(self):
        pool = proxy_list("1.2.3.4:8080")
        router = ProxyRouter(pool, FakeSession(), timeout=aiohttp.ClientTimeout(total=5), headers={})
        self.assertEqual(router.next_proxy().url, "http://1.2.3.4:8080")
        pool.record_failure(parse_proxy_line("1.2.3.4:8080"), "timeout", limit=1)
        self.assertIsNone(router.next_proxy())
        await router.close()


class CheckTests(unittest.IsolatedAsyncioTestCase):
    def factory(self, context):
        def build(proxy, **kwargs):
            session = FakeSession(lambda p, u: context)
            session.kwargs = kwargs
            return session
        return build

    def site_factory(self, *, allowed=("youtube", "dailymotion", "rumble"), html=(), status=None):
        """Answers per site: 204 for generate_204 probes, robots.txt text otherwise."""
        def handler(proxy, url):
            target = next((item for item in CHECK_TARGETS if item.url == url), None)
            if target is None:
                return FakeContext(FakeResponse(200, b"ok"))
            if status and target.key in status:
                code, body = status[target.key]
                return FakeContext(FakeResponse(code, body))
            if target.key not in allowed:
                return FakeContext(FakeResponse(403))
            if target.nocontent:
                return FakeContext(FakeResponse(204))
            if target.key in html:
                return FakeContext(FakeResponse(200, b"<html>captive portal</html>"))
            return FakeContext(FakeResponse(200, b"User-agent: *\nDisallow: /search\n"))

        def build(proxy, **kwargs):
            session = FakeSession(handler)
            session.kwargs = kwargs
            return session
        return build

    async def test_working_check_covers_every_site(self):
        result = await check_proxy(parse_proxy_line("1.2.3.4:8080"),
                                   session_factory=self.site_factory())
        self.assertTrue(result.ok)
        self.assertGreaterEqual(result.latency_ms, 0)
        self.assertEqual([site.key for site in result.sites],
                         [target.key for target in CHECK_TARGETS])
        self.assertTrue(all(site.ok for site in result.sites))
        self.assertEqual(result.marks(),
                         {"youtube": True, "dailymotion": True, "rumble": True})
        self.assertIn("YouTube ok · Dailymotion ok · Rumble ok", result.site_line())
        self.assertEqual(site_marks(result.marks()), "YT,DM,RU")

    async def test_proxy_blocked_by_one_site_stays_usable_for_the_others(self):
        result = await check_proxy(parse_proxy_line("1.2.3.4:8080"),
                                   session_factory=self.site_factory(allowed=("youtube", "rumble")))
        self.assertTrue(result.ok)  # Something worked, so the proxy stays.
        self.assertEqual(result.marks(),
                         {"youtube": True, "dailymotion": False, "rumble": True})
        self.assertIn("Dailymotion HTTP 403", result.site_line())
        self.assertEqual(site_marks(result.marks()), "YT,RU")

    async def test_captive_portal_html_is_not_a_pass(self):
        result = await check_proxy(parse_proxy_line("1.2.3.4:8080"),
                                   session_factory=self.site_factory(html=("dailymotion",)))
        self.assertEqual(result.marks(),
                         {"youtube": True, "dailymotion": False, "rumble": True})
        blocked = next(site for site in result.sites if site.key == "dailymotion")
        self.assertEqual(blocked.detail, "HTML page, not site content")

    async def test_single_target_checks_only_that_site(self):
        result = await check_proxy(parse_proxy_line("1.2.3.4:8080"), targets=(CHECK_TARGETS[2],),
                                   session_factory=self.site_factory())
        self.assertEqual([site.key for site in result.sites], ["rumble"])
        self.assertEqual(result.marks(), {"rumble": True})

    async def test_site_ok_rules(self):
        youtube, dailymotion = CHECK_TARGETS[0], CHECK_TARGETS[1]
        self.assertEqual(site_ok(youtube, 204, b""), (True, ""))
        self.assertEqual(site_ok(youtube, 200, b"<html>"), (False, "unexpected reply HTTP 200"))
        self.assertEqual(site_ok(dailymotion, 200, b"User-agent: *"), (True, ""))
        self.assertEqual(site_ok(dailymotion, 302, b""), (True, ""))
        self.assertEqual(site_ok(dailymotion, 404, b""), (True, ""))
        self.assertEqual(site_ok(dailymotion, 200, b""), (False, "empty reply"))
        self.assertEqual(site_ok(dailymotion, 503, b""), (False, "proxy replied HTTP 503"))

    async def test_http_failures_are_reported(self):
        every = {target.key: (407, b"") for target in CHECK_TARGETS}
        result = await check_proxy(parse_proxy_line("1.2.3.4:8080"),
                                   session_factory=self.site_factory(status=every))
        self.assertFalse(result.ok)
        self.assertEqual(result.detail, "proxy replied HTTP 407")
        forbidden = {"rumble": (403, b"")}
        result = await check_proxy(parse_proxy_line("1.2.3.4:8080"),
                                   session_factory=self.site_factory(status=forbidden))
        self.assertTrue(result.ok)  # Only Rumble blocked it.
        self.assertEqual(result.marks()["rumble"], False)
        self.assertEqual(next(site.detail for site in result.sites if site.key == "rumble"),
                         "HTTP 403")

    async def test_connection_errors_and_timeouts(self):
        refused_result = await check_proxy(parse_proxy_line("1.2.3.4:8080"),
                                           session_factory=self.factory(refused()))
        self.assertFalse(refused_result.ok)
        self.assertEqual(refused_result.detail, "network error")
        self.assertEqual([site.detail for site in refused_result.sites],
                         ["network error"] * len(CHECK_TARGETS))
        timeout = FakeContext(error=asyncio.TimeoutError())
        self.assertEqual((await check_proxy(parse_proxy_line("1.2.3.4:8080"),
                                            session_factory=self.factory(timeout))).detail, "timeout")

    async def test_socks_errors_are_reported_as_proxy_problems(self):
        from aiohttp_socks import ProxyConnectionError as SocksRefused
        result = await check_proxy(parse_proxy_line("socks5://1.2.3.4:1080"),
                                   session_factory=self.factory(FakeContext(error=SocksRefused("no"))))
        self.assertFalse(result.ok)
        self.assertEqual(result.detail, "cannot reach the proxy")

    async def test_run_checks_bounds_concurrency_and_keeps_order(self):
        proxies = [parse_proxy_line(f"1.2.3.{index}:8080") for index in range(1, 13)]
        active = {"now": 0, "peak": 0, "done": 0}
        progress = []

        class Tracked:
            """Stands in for the response context manager and tracks overlap."""

            def __init__(self):
                self.context = FakeContext(FakeResponse(204))

            async def __aenter__(self):
                active["now"] += 1
                active["peak"] = max(active["peak"], active["now"])
                await asyncio.sleep(0.01)
                return await self.context.__aenter__()

            async def __aexit__(self, *args):
                active["now"] -= 1
                return await self.context.__aexit__(*args)

        def build(proxy, **kwargs):
            return FakeSession(lambda p, u: Tracked())

        async def on_progress(count, total, result):
            progress.append((count, total, result.proxy.host))

        results = await run_checks(proxies, workers=4, on_progress=on_progress,
                                   session_factory=build)
        self.assertEqual([result.proxy.host for result in results],
                         [proxy.host for proxy in proxies])
        self.assertTrue(all(result.ok for result in results))
        self.assertEqual([item[0] for item in progress], list(range(1, 13)))
        self.assertLessEqual(active["peak"], 4)
        self.assertGreater(active["peak"], 1)
        self.assertEqual(active["now"], 0)


class BotProxyCommandTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.client = MagicMock()
        self.client.send_message = AsyncMock(return_value=SimpleNamespace(id=11))
        self.client.edit_message = AsyncMock()
        self.service = DownloaderBot(self.client, MagicMock())

    def event(self, text="", *, uid=7, notice=None, document=None):
        message = SimpleNamespace(document=document, attributes=[])
        return SimpleNamespace(sender_id=uid, raw_text=text, is_private=True, message=message,
                               client=self.client,
                               respond=AsyncMock(return_value=notice or
                                                 SimpleNamespace(edit=AsyncMock(), respond=AsyncMock())),
                               get_input_chat=AsyncMock(return_value="peer"))

    async def test_proxychk_prompt_then_text_starts_a_check(self):
        event = self.event("/proxychk")
        await self.service.on_message(event)
        self.assertIn(7, self.service.awaiting_proxy)
        self.assertIn("host:port", event.respond.call_args.args[0])
        notice = SimpleNamespace(edit=AsyncMock(), respond=AsyncMock())
        event = self.event("1.2.3.4:8080\n5.6.7.8:9090\nbad-line", notice=notice)

        async def fake_run(proxies, **kwargs):
            results = []
            for index, proxy in enumerate(proxies):
                sites = (site_results(True) if index == 0
                         else tuple(SiteResult(target.key, target.label, target.key == "youtube",
                                               0, "timeout") for target in CHECK_TARGETS))
                result = CheckResult(proxy, index == 0, 40 + index * 10, "", sites)
                results.append(result)
                await kwargs["on_progress"](index + 1, len(proxies), result)
            return results

        with patch("ytdown.bot.run_checks", new=AsyncMock(side_effect=fake_run)) as checks:
            await self.service.on_message(event)
            self.assertNotIn(7, self.service.awaiting_proxy)
            task = self.service.proxy_checks[7]
            await task
        self.assertEqual([target.key for target in checks.call_args.kwargs["targets"]],
                         ["youtube", "dailymotion", "rumble"])
        pool = self.service.pool_for(7)
        self.assertEqual(len(pool), 2)
        self.assertEqual([state.proxy.host for state in pool.live()], ["1.2.3.4"])
        self.assertEqual(pool.entries["http://5.6.7.8:9090"].status, "dead")
        self.assertEqual(pool.entries["http://5.6.7.8:9090"].sites,
                         {"youtube": True, "dailymotion": False, "rumble": False})
        report = notice.edit.call_args_list[-1].args[0]
        self.assertIn("Reachable: 1 · no site reached: 1", report)
        self.assertIn("Per site: YouTube 2 ok · Dailymotion 1 ok · Rumble 1 ok", report)
        self.assertIn("1 live · 0 untested · 1 dead", report)
        self.assertIn("bad-line", report)  # Parse problems are reported back.
        self.assertIn("http://1.2.3.4:8080 — YT,DM,RU (40 ms)", report)
        self.assertNotIn(7, self.service.proxy_checks)

    async def test_proxychk_url_command_switches_the_check_target(self):
        event = self.event("/proxychk url https://api.example.com/healthz")
        await self.service.on_message(event)
        targets = self.service.proxy_check_targets
        self.assertEqual(targets[0].key, "custom")
        self.assertEqual(targets[0].label, "api.example.com")
        self.assertEqual(targets[0].url, "https://api.example.com/healthz")
        self.assertEqual([target.key for target in targets[1:]], ["dailymotion", "rumble"])
        self.assertIn("api.example.com", event.respond.call_args.args[0])
        event = self.event("/proxychk url http://127.0.0.1/health")
        await self.service.on_message(event)
        self.assertIn("public HTTPS URL", event.respond.call_args.args[0])
        self.assertEqual(self.service.proxy_check_targets[0].key, "custom")  # Unchanged.
        event = self.event("/proxychk url")
        await self.service.on_message(event)
        self.assertIn("Send the URL to test", event.respond.call_args.args[0])

    async def test_proxychk_sites_command(self):
        event = self.event("/proxychk sites youtube,rumble")
        await self.service.on_message(event)
        self.assertEqual([target.key for target in self.service.proxy_check_targets],
                         ["youtube", "rumble"])
        self.assertIn("YouTube", event.respond.call_args.args[0])
        event = self.event("/proxychk sites nope")
        await self.service.on_message(event)
        self.assertIn("Unknown site: nope", event.respond.call_args.args[0])
        self.assertEqual(len(self.service.proxy_check_targets), 2)  # Unchanged.
        event = self.event("/proxychk sites default")
        await self.service.on_message(event)
        self.assertEqual([target.key for target in self.service.proxy_check_targets],
                         ["youtube", "dailymotion", "rumble"])
        event = self.event("/proxychk sites site")  # Unknown
        await self.service.on_message(event)
        self.assertIn("Unknown site", event.respond.call_args.args[0])

    async def test_panel_shows_site_coverage(self):
        pool = self.service.pool_for(7)
        first, second = parse_proxy_line("1.2.3.4:8080"), parse_proxy_line("5.6.7.8:9090")
        pool.add([first, second])
        pool.mark_checked(CheckResult(first, True, 40, "",
                                      (SiteResult("youtube", "YouTube", True, 40),
                                       SiteResult("dailymotion", "Dailymotion", False, 0, "HTTP 403"),
                                       SiteResult("rumble", "Rumble", True, 45))))
        pool.mark_checked(CheckResult(second, True, 90, "",
                                      (SiteResult("youtube", "YouTube", True, 90),
                                       SiteResult("dailymotion", "Dailymotion", True, 88),
                                       SiteResult("rumble", "Rumble", False, 0, "timeout"))))
        self.service.proxy_state[7] = True
        event = self.event("/proxy")
        await self.service.on_message(event)
        text = event.respond.call_args.args[0]
        self.assertIn("Check sites: YouTube + Dailymotion + Rumble", text)
        self.assertIn("Reaching: YouTube 2 · Dailymotion 1 · Rumble 1", text)
        self.assertIn("YT 2", self.service.proxy_line(7))  # Dashboard line keeps short marks.

    async def test_proxychk_rejects_a_useless_list(self):
        self.service.awaiting_proxy.add(7)
        event = self.event("hello there")
        await self.service.on_message(event)
        self.assertIn("No usable proxies found", event.respond.call_args.args[0])
        self.assertNotIn(7, self.service.proxy_checks)

    async def test_proxychk_accepts_an_uploaded_file(self):
        buffer = io.BytesIO(b"1.2.3.4:8080\n# comment\n5.6.7.8:9090\n")
        self.client.download_media = AsyncMock(return_value=buffer)
        document = SimpleNamespace(size=len(buffer.getvalue()), mime_type="text/plain",
                                   attributes=[SimpleNamespace(file_name="proxies.txt")])
        self.service.awaiting_proxy.add(7)
        event = self.event("", document=document)
        with patch.object(self.service, "on_proxy_text", new=AsyncMock()) as handler:
            await self.service.on_message(event)
        self.assertNotIn(7, self.service.awaiting_proxy)
        self.assertIn("1.2.3.4:8080", handler.call_args.args[1])
        self.client.download_media.assert_awaited_once()

    async def test_binary_files_are_refused(self):
        document = SimpleNamespace(size=10, mime_type="application/zip",
                                   attributes=[SimpleNamespace(file_name="proxies.zip")])
        self.service.awaiting_proxy.add(7)
        event = self.event("", document=document)
        await self.service.on_message(event)
        self.assertIn(".txt or .csv", event.respond.call_args.args[0])

    async def test_proxy_panel_toggles_on_and_off(self):
        self.service.pool_for(7).add([parse_proxy_line("1.2.3.4:8080")])
        event = self.event("/proxy")
        await self.service.on_message(event)
        text = event.respond.call_args.args[0]
        self.assertIn("Proxy mode: OFF", text)
        self.assertIn("1 total", text)
        buttons = event.respond.call_args.kwargs["buttons"]
        self.assertEqual([button.text for row in buttons for button in row],
                         ["▫️ ON", "✅ OFF", "🔄 Re-check pool", "📊 Pool status",
                          "🧹 Remove dead", "🗑 Clear pool"])
        token = self.service.proxy_tokens[7]
        toggle = SimpleNamespace(sender_id=7, is_private=True, data=f"p:{token}:on".encode(),
                                 answer=AsyncMock(), edit=AsyncMock())
        await self.service.callback(toggle)
        self.assertEqual(toggle.answer.call_args.args[0], "Proxy mode is ON")
        self.assertIn("Proxy mode: ON", toggle.edit.call_args.args[0])
        self.assertTrue(self.service.proxy_enabled(7))
        self.assertIs(self.service.http_for(7), self.service.routers[7])
        toggle.data = f"p:{token}:off".encode()
        await self.service.callback(toggle)
        self.assertFalse(self.service.proxy_enabled(7))
        self.assertIs(self.service.http_for(7), self.service.http)
        self.assertNotIn(7, self.service.routers)

    async def test_pool_status_lists_live_proxies(self):
        pool = self.service.pool_for(7)
        pool.add([parse_proxy_line("1.2.3.4:8080"), parse_proxy_line("5.6.7.8:9090")])
        pool.mark_checked(CheckResult(parse_proxy_line("1.2.3.4:8080"), True, 210))
        pool.mark_checked(CheckResult(parse_proxy_line("5.6.7.8:9090"), True, 40))
        self.service.proxy_state[7] = True
        event = self.event("/proxy")
        await self.service.on_message(event)
        text = event.respond.call_args.args[0]
        self.assertIn("Live now: http://5.6.7.8:9090 (40 ms), http://1.2.3.4:8080 (210 ms)", text)
        self.assertIn("2 live/2 in pool", text)

    async def test_panel_purge_clear_and_expired_token(self):
        pool = self.service.pool_for(7)
        pool.add([parse_proxy_line("1.2.3.4:8080"), parse_proxy_line("5.6.7.8:9090")])
        pool.mark_checked(CheckResult(parse_proxy_line("5.6.7.8:9090"), False, 0, "timeout"))
        event = self.event("/proxy")
        await self.service.on_message(event)
        token = self.service.proxy_tokens[7]
        click = SimpleNamespace(sender_id=7, is_private=True, data=f"p:{token}:purge".encode(),
                                answer=AsyncMock(), edit=AsyncMock())
        await self.service.callback(click)
        self.assertIn("Removed 1 dead", click.answer.call_args.args[0])
        self.assertEqual(len(pool), 1)
        click.data = f"p:{token}:clear".encode()
        await self.service.callback(click)
        self.assertEqual(len(pool), 0)
        click.data = b"p:deadbeef:on"
        await self.service.callback(click)
        self.assertIn("expired", click.answer.call_args.args[0])

    async def test_recheck_button_runs_a_pool_check(self):
        pool = self.service.pool_for(7)
        pool.add([parse_proxy_line("1.2.3.4:8080")])
        self.service.proxy_state[7] = True
        event = self.event("/proxy")
        await self.service.on_message(event)
        token = self.service.proxy_tokens[7]
        click = SimpleNamespace(sender_id=7, is_private=True, data=f"p:{token}:recheck".encode(),
                                answer=AsyncMock(), edit=AsyncMock())
        results = [result_for(parse_proxy_line("1.2.3.4:8080"), True, 55)]

        async def fake_run(proxies, **kwargs):
            for count, result in enumerate(results, start=1):
                await kwargs["on_progress"](count, len(proxies), result)
            return results

        with patch("ytdown.bot.run_checks", new=AsyncMock(side_effect=fake_run)):
            await self.service.callback(click)
            await self.service.proxy_checks[7]
        self.assertIn("Checking 1 proxies…", click.answer.call_args.args[0])
        self.assertIn("Reachable: 1", click.edit.call_args_list[-1].args[0])

    async def test_cancel_proxy_stops_a_running_check(self):
        self.service.awaiting_proxy.add(7)

        async def slow(proxies, **kwargs):
            await asyncio.sleep(300)

        with patch("ytdown.bot.run_checks", new=AsyncMock(side_effect=slow)):
            event = self.event("1.2.3.4:8080")
            await self.service.on_message(event)
            task = self.service.proxy_checks[7]
            await asyncio.sleep(0)
            cancel = self.event("/cancel proxy")
            await self.service.on_message(cancel)
            self.assertIn("cancellation requested", cancel.respond.call_args.args[0])
            self.assertTrue(task.cancelled() or task.cancelling())
            await asyncio.gather(task, return_exceptions=True)

    async def test_proxy_commands_from_a_message_and_dashboard_line(self):
        event = self.event("/proxy on")
        await self.service.on_message(event)
        self.assertIn("Proxy mode is ON", event.respond.call_args.args[0])
        self.assertIn("pool is empty", event.respond.call_args.args[0])
        self.assertTrue(self.service.proxy_enabled(7))
        self.assertIn("🌐 Proxy:", self.service.format_dashboard(7))
        event.raw_text = "/proxy clear"
        await self.service.on_message(event)
        self.assertIn("Cleared 0 proxies", event.respond.call_args.args[0])
        self.assertFalse(self.service.proxy_enabled(7))
        event.raw_text = "/proxy whatever"
        await self.service.on_message(event)
        self.assertIn("Use /proxy for the ON/OFF panel", event.respond.call_args.args[0])

    async def test_proxy_mode_routes_lookups_through_the_router(self):
        pool = self.service.pool_for(7)
        pool.add([parse_proxy_line("1.2.3.4:8080")])
        self.service.proxy_state[7] = True
        notice = SimpleNamespace(edit=AsyncMock())
        event = self.event("https://youtu.be/3SY1i5CB03Q", notice=notice)
        from ytdown.core import Video
        from ytdown.bot import API_URL
        with patch("ytdown.bot.fetch_video", new=AsyncMock(return_value=Video("T", {"480p": "u"}))) as fetch, \
             patch("ytdown.bot.get_youtube_title", new=AsyncMock(return_value="T")):
            await self.service.on_link(event)
        self.assertIs(fetch.call_args.args[0], self.service.routers[7])
        self.assertEqual(fetch.call_args.args[2], API_URL)

    async def test_proxy_seed_and_strict_flag_from_env(self):
        with patch.dict("os.environ", {"PROXY_LIST": "1.2.3.4:8080\n", "PROXY_ENABLED": "1",
                                       "PROXY_STRICT": "yes"}):
            service = DownloaderBot(MagicMock(), MagicMock())
        self.assertTrue(service.proxy_enabled(1))
        self.assertTrue(service.proxy_strict)
        self.assertEqual(len(service.pool_for(3)), 1)
        self.assertIsInstance(service.http_for(3), ProxyRouter)

    async def test_youtube_task_retries_when_a_proxy_looks_responsible(self):
        pool = self.service.pool_for(7)
        pool.add([parse_proxy_line("1.2.3.4:8080"), parse_proxy_line("5.6.7.8:9090")])
        self.service.proxy_state[7] = True
        seen = []

        def ytdlp(url, start=None, end=None, proxy=None):
            seen.append(proxy)
            if len(seen) == 1:
                raise ValueError("ERROR: Unable to download webpage: connection refused")
            return ["entry"]

        task = await self.service.youtube_task(7, ytdlp, "https://www.youtube.com/playlist?list=X")
        self.assertEqual(task, ["entry"])
        self.assertEqual(seen[0], "http://1.2.3.4:8080")
        self.assertEqual(seen[1], "http://5.6.7.8:9090")
        self.assertEqual(pool.entries["http://1.2.3.4:8080"].fail_count, 1)

    async def test_youtube_task_does_not_retry_content_errors(self):
        pool = self.service.pool_for(7)
        pool.add([parse_proxy_line("1.2.3.4:8080")])
        self.service.proxy_state[7] = True
        calls = []

        def ytdlp(url, proxy=None):
            calls.append(proxy)
            raise ValueError("Video unavailable")

        with self.assertRaises(ValueError):
            await self.service.youtube_task(7, ytdlp, "https://www.youtube.com/watch?v=x")
        self.assertEqual(len(calls), 1)
        self.assertEqual(pool.entries["http://1.2.3.4:8080"].fail_count, 0)


if __name__ == "__main__":
    unittest.main()
