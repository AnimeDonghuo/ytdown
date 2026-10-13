"""Per-user proxy pool: parsing, health checks, and failover routing.

A user pastes proxies with ``/proxychk``; they land in that user's pool with a
health status. While proxy mode is on, every provider request (API lookups,
thumbnails, playlist discovery and media streams) is sent through a live proxy
from the pool. A proxy that fails is counted, and after ``PROXY_FAIL_LIMIT``
consecutive failures it is removed from the pool while that request is retried
with the next live proxy. When nothing usable is left, requests either continue
without a proxy (default) or fail loudly in strict mode.
"""
from __future__ import annotations

import asyncio
import ipaddress
import logging
import re
import time
import json
import weakref
from collections import OrderedDict, deque
from dataclasses import dataclass, field, replace
from typing import Awaitable, Callable, Iterable, Mapping
from urllib.parse import quote, unquote, urlencode, urlsplit

import aiohttp

LOG = logging.getLogger(__name__)

DEFAULT_SCHEME = "http"
# "socks" is the common shorthand for SOCKS5.
PROXY_SCHEMES = ("http", "https", "socks", "socks4", "socks4a", "socks5", "socks5h")
MAX_PROXIES = 5000
CHECK_URL = "https://www.youtube.com/generate_204"
PROXY_FAIL_LIMIT = 2  # Consecutive real-use failures before a proxy is dropped.


@dataclass(frozen=True)
class CheckTarget:
    """One site a proxy is checked against.

    ``nocontent`` marks a probe that answers 200/204 with an empty body
    (``generate_204``); the others read a small text file, so an HTML answer
    means something between the proxy and the site intercepted the request.
    ``json`` marks a probe of the service's own API: only an answer that parses
    as JSON proves the proxy can actually use that API, so HTML error pages,
    login walls and forged replies fail no matter which HTTP status they use.
    """

    key: str
    label: str
    short: str
    url: str
    hosts: tuple[str, ...] = ()
    nocontent: bool = False
    lax: bool = False  # User-supplied URL: accept an empty or plain-text answer.
    json: bool = False  # Probe of a JSON API the bot really calls.


# Default checks: Dailymotion and Rumble only. YouTube stays available through
# /proxychk sites youtube (and PROXY_CHECK_SITES) for pools used on YouTube.
DAILYMOTION_TARGET = CheckTarget("dailymotion", "Dailymotion", "DM",
                                 "https://www.dailymotion.com/robots.txt",
                                 hosts=("dailymotion.com", "dmcdn.net", "dm-event.net"))
RUMBLE_TARGET = CheckTarget("rumble", "Rumble", "RU", "https://rumble.com/robots.txt",
                            hosts=("rumble.com",))
# Terabox is selectable but not checked by default; its own probe becomes the
# Terabox API when the bot knows which endpoint this user's links go through.
TERABOX_TARGET = CheckTarget("terabox", "Terabox", "TB", "https://www.terabox.com/robots.txt",
                             hosts=("terabox.com", "1024terabox.com", "teraboxapp.com",
                                    "teraboxlink.com"))
YOUTUBE_TARGET = CheckTarget("youtube", "YouTube", "YT", CHECK_URL, nocontent=True,
                             hosts=("youtube.com", "youtu.be", "ytimg.com", "googlevideo.com",
                                    "youtube-nocookie.com", "ggpht.com"))
CHECK_TARGETS = (DAILYMOTION_TARGET, RUMBLE_TARGET)
# Every site a check can be pointed at, not just the selected ones.
KNOWN_TARGETS = (DAILYMOTION_TARGET, RUMBLE_TARGET, YOUTUBE_TARGET, TERABOX_TARGET)
# A stable public link per service, so the API probe makes the provider do real
# work instead of answering a bare request with a generic error page.
API_PROBES = {
    "youtube": "https://www.youtube.com/watch?v=3SY1i5CB03Q",
    "terabox": "https://www.terabox.com/s/12345",
    "dailymotion": "https://www.dailymotion.com/video/x7tgad0",
    "rumble": "https://rumble.com/v4t3sz8-playing-some-fallout-76-sleep-token-day.html",
}
SERVICE_KEYS = ("youtube", "terabox", "dailymotion", "rumble")
PROXY_ATTEMPTS = 3  # Proxies tried for one request before the direct fallback.
RETRY_STATUSES = frozenset({407, 502, 503, 504})  # Proxy-level HTTP errors, not the site's.
MIN_LATENCY_MS = 20  # Replies faster than this mean no real round trip: auto-fail.
TOO_FAST = "too fast"  # Prefix of the failure reason, matched by the bot's report.
SESSION_CACHE = 8  # Routed sessions kept open (idle ones are closed first).
HOSTNAME = re.compile(r"^(?=.{1,253}$)[A-Za-z0-9_](?:[A-Za-z0-9._-]*[A-Za-z0-9_])?$")

try:  # SOCKS support is optional; HTTP/HTTPS proxies need nothing extra.
    from aiohttp_socks import (ProxyConnectionError as SocksConnectionError, ProxyConnector,
                               ProxyError as SocksProxyError, ProxyTimeoutError as SocksTimeoutError)
except ImportError:  # pragma: no cover - only without the optional dependency.
    ProxyConnector = None  # type: ignore[assignment]

    class SocksProxyError(Exception):  # type: ignore[no-redef]
        """Placeholder so SOCKS failures stay catchable without aiohttp-socks."""

    class SocksConnectionError(SocksProxyError):  # type: ignore[no-redef]
        pass

    class SocksTimeoutError(SocksProxyError):  # type: ignore[no-redef]
        pass


# Connection-level failures that count against a proxy. aiohttp-socks raises its
# own exception classes, which are not OSError subclasses.
SOCKS_ERRORS = (SocksProxyError, SocksConnectionError, SocksTimeoutError)
CONNECTION_ERRORS = (aiohttp.ClientError, asyncio.TimeoutError, OSError) + SOCKS_ERRORS


class ProxyUnavailable(ValueError):
    """Raised when strict proxy mode has no usable proxy for a request."""


@dataclass(frozen=True)
class Proxy:
    """One validated proxy endpoint."""

    scheme: str
    host: str
    port: int
    username: str = ""
    password: str = ""

    @property
    def socks(self) -> bool:
        return self.scheme.startswith("socks")

    @property
    def address(self) -> str:
        """host:port, with brackets for IPv6 literals."""
        return f"[{self.host}]:{self.port}" if ":" in self.host else f"{self.host}:{self.port}"

    @property
    def url(self) -> str:
        """Canonical URL, safe to hand to aiohttp or yt-dlp."""
        credentials = ""
        if self.username or self.password:
            credentials = f"{quote(self.username, safe='')}:{quote(self.password, safe='')}@"
        return f"{self.scheme}://{credentials}{self.address}"

    @property
    def label(self) -> str:
        """Printable form used in Telegram messages: never exposes the password."""
        credentials = ""
        if self.username:
            credentials = f"{self.username}:***@" if self.password else f"{self.username}@"
        return f"{self.scheme}://{credentials}{self.address}"


def _valid_host(value: str) -> str:
    host = value.strip().strip("[]").lower()
    if not host:
        raise ValueError("missing host")
    if len(host) > 253 or not (HOSTNAME.fullmatch(host) or _ip_literal(host)):
        raise ValueError("invalid host")
    return host


def _ip_literal(host: str) -> bool:
    try:
        ipaddress.ip_address(host)
        return True
    except ValueError:
        return False


def _valid_port(value: str) -> int:
    port = value.strip()
    if not port.isdigit() or not 0 < int(port) <= 65535:
        raise ValueError("invalid port")
    return int(port)


def _split_host_port(value: str) -> tuple[str, int]:
    value = value.strip()
    if value.startswith("["):  # IPv6 literal such as [2001:db8::1]:8080
        host, separator, port = value.rpartition("]:")
        if not separator:
            raise ValueError("invalid IPv6 address")
        return _valid_host(host[1:]), _valid_port(port)
    host, separator, port = value.rpartition(":")
    if not separator:
        raise ValueError("missing port")
    return _valid_host(host), _valid_port(port)


def parse_proxy_line(line: str) -> Proxy:
    """Accept the common proxy-list forms: host:port[:user:pass], user:pass@host:port,
    scheme://[user:pass@]host:port (http, https, socks4, socks5 and friends)."""
    raw = re.sub(r"\s+#.*$", "", line).strip().strip(",;").strip()  # drop "# YT,DM" comments
    if not raw:
        raise ValueError("empty line")
    parts = raw.split()
    if len(parts) == 2 and "://" not in raw:  # "host:port user:pass" and "host port".
        endpoint, extra = parts
        if _looks_like_proxy(endpoint) and CREDENTIALS.fullmatch(extra):
            raw = _merge_credentials(endpoint, extra)
        elif _host_like(endpoint) and extra.isdigit():
            raw = f"{endpoint}:{extra}"
    username = password = ""
    if "://" in raw:
        scheme, _, raw = raw.partition("://")
        scheme = scheme.strip().lower()
        if scheme not in PROXY_SCHEMES:
            raise ValueError(f"unsupported scheme {scheme!r}")
        scheme = "socks5" if scheme == "socks" else scheme
        if "@" in raw:
            credentials, _, raw = raw.rpartition("@")
            username, _, password = credentials.partition(":")
            username, password = unquote(username), unquote(password)
        host, port = _split_host_port(raw)
        return Proxy(scheme, host, port, username, password)
    if "@" in raw:
        credentials, _, raw = raw.rpartition("@")
        username, _, password = credentials.partition(":")
        host, port = _split_host_port(raw)
        return Proxy(DEFAULT_SCHEME, host, port, username, password)
    if raw.startswith("["):  # Bracketed IPv6 literal without a scheme.
        host, port = _split_host_port(raw)
        return Proxy(DEFAULT_SCHEME, host, port)
    fields = raw.split(":")
    if len(fields) == 2:
        host, port = _split_host_port(raw)
        return Proxy(DEFAULT_SCHEME, host, port)
    if len(fields) == 4:  # ip:port:user:pass
        host, port = _split_host_port(f"{fields[0]}:{fields[1]}")
        return Proxy(DEFAULT_SCHEME, host, port, fields[2], fields[3])
    raise ValueError("use host:port, host:port:user:pass or user:pass@host:port")


CREDENTIALS = re.compile(r"[^:@\s]+:[^:@\s]*")


def _looks_like_proxy(token: str) -> bool:
    if "://" in token:
        return True
    host, separator, port = token.rpartition(":")
    return bool(separator and host and port.isdigit())


def _host_like(token: str) -> bool:
    return ":" not in token and bool(HOSTNAME.fullmatch(token) or _ip_literal(token))


def _merge_credentials(endpoint: str, credentials: str) -> str:
    scheme, separator, rest = endpoint.partition("://")
    return f"{scheme}://{credentials}@{rest}" if separator else f"{credentials}@{endpoint}"


def _line_candidates(line: str) -> list[str]:
    """Split one line that may hold several proxies, credentials or separators."""
    pieces = [piece for piece in re.split(r"[,;]", line) if piece.strip()]
    parts = line.split()
    if len(parts) > 1 and all(_looks_like_proxy(part) for part in parts):
        return parts  # "1.2.3.4:8080 5.6.7.8:9090" is two proxies.
    return pieces or [line]


def parse_proxies(text: str, limit: int = MAX_PROXIES) -> tuple[list[Proxy], list[str]]:
    """Parse a pasted list or uploaded file. Returns the proxies plus error notes."""
    proxies: list[Proxy] = []
    errors: list[str] = []
    seen: set[str] = set()
    for raw_line in (text or "").splitlines() or [text or ""]:
        line = re.sub(r"\s+#.*$", "", raw_line).strip()  # "# YT,DM" comments stay out of splitting
        if not line or line.startswith(("#", "//")):
            continue  # Whole-line comments.
        for candidate in _line_candidates(line):
            try:
                proxy = parse_proxy_line(candidate)
            except ValueError as exc:
                errors.append(f"{candidate.strip()[:60]}: {exc}")
                continue
            if proxy.url in seen:
                continue
            if len(proxies) >= limit:
                errors.append(f"only the first {limit} proxies were used")
                return proxies, errors
            seen.add(proxy.url)
            proxies.append(proxy)
    return proxies, errors


@dataclass(frozen=True)
class ProxyState:
    """Health of one proxy inside a pool."""

    proxy: Proxy
    status: str = "untested"  # untested | alive | dead
    ok_count: int = 0
    fail_count: int = 0  # Consecutive real-use failures.
    latency_ms: int = 0
    detail: str = ""
    checked_at: float = 0.0  # time.monotonic of the last check or use.
    sites: dict[str, bool] = field(default_factory=dict)  # target key -> reachable,

    def reaches(self, site: str | None) -> bool:
        """Unusable only when this site was checked and did not answer."""
        return site is None or self.sites.get(site) is not False


@dataclass(frozen=True)
class PoolAdd:
    added: int = 0
    duplicates: int = 0
    overflow: int = 0

    @property
    def total(self) -> int:
        return self.added + self.duplicates + self.overflow


@dataclass(frozen=True)
class SiteResult:
    """Outcome of one site probe for one proxy."""

    key: str
    label: str
    ok: bool
    latency_ms: int = 0
    detail: str = ""


@dataclass(frozen=True)
class CheckResult:
    proxy: Proxy
    ok: bool  # True when at least one checked site answered through the proxy.
    latency_ms: int = 0
    detail: str = ""
    sites: tuple[SiteResult, ...] = ()

    def marks(self) -> dict[str, bool]:
        return {site.key: site.ok for site in self.sites}

    @property
    def verdict(self) -> str:
        """``ok``, ``api`` (the API answered but not usable) or ``proxy`` (no answer).

        A check must not throw away a working proxy because the service's API is
        having a bad day, so an answer that carries a real HTTP status is kept
        apart from a proxy that never got one.
        """
        if self.ok:
            return "ok"
        details = [site.detail for site in self.sites if site.detail]
        if details and all(API_SIDE.fullmatch(detail) for detail in details):
            return "api"
        return "proxy"

    def site_line(self) -> str:
        return " · ".join(f"{site.label} {'ok' if site.ok else site.detail or 'failed'}"
                          for site in self.sites)


@dataclass(frozen=True)
class CheckStats:
    """Totals of the last completed proxy check."""

    when: float
    checked: int
    working: int  # Proxies that reached at least one site.
    sites: dict[str, int] = field(default_factory=dict)  # target key -> working proxies


# "HTTP 500", "HTTP 403, not a JSON API answer": the API answered, badly.
API_SIDE = re.compile(r"HTTP \d{3}(?:, .*)?")

# Wording yt-dlp uses when the host/proxy, not the video, is the problem.
NETWORK_HINTS = ("unable to download webpage", "timed out", "timeout", "connection", "connect",
                 "network", "ssl", "proxy", "certificate", "temporary failure", "refused",
                 "reset by peer", "unreachable", "name resolution", "eof occurred", "http error 403",
                 "403 forbidden", "not a bot", "unusual traffic", "blocked")


def proxy_fault(message: object) -> bool:
    """True when an error message points at the proxy/network rather than the content."""
    lowered = str(message or "").lower()
    return any(hint in lowered for hint in NETWORK_HINTS)


def short_reason(exc: object, limit: int = 90) -> str:
    """One readable line describing why a proxy attempt failed."""
    text = " ".join(str(exc or "").split()) or type(exc).__name__
    return text[:limit - 1] + "…" if len(text) > limit else text


class ProxyPool:
    """Ordered, bounded pool of one user's proxies with health state."""

    def __init__(self, limit: int = MAX_PROXIES, fail_limit: int = PROXY_FAIL_LIMIT):
        self.limit = max(1, limit)
        self.fail_limit = max(1, fail_limit)  # Consecutive failures before removal.
        self.entries: dict[str, ProxyState] = {}
        self.last_check: CheckStats | None = None
        self._cursor = 0

    def __len__(self) -> int:
        return len(self.entries)

    def get(self, proxy: Proxy) -> ProxyState | None:
        return self.entries.get(proxy.url)

    def add(self, proxies: Iterable[Proxy],
            marks: Mapping[str, Mapping[str, bool]] | None = None) -> PoolAdd:
        """Add proxies, keeping the health of any that are already in the pool.

        ``marks`` (see ``parse_proxy_marks``) restores what an exported list
        recorded for a proxy: it comes back **live** with its per-site marks, so
        a list saved before a restart works again without another check.
        """
        added = duplicates = overflow = 0
        for proxy in proxies:
            if proxy.url in self.entries:
                duplicates += 1
                continue
            if len(self.entries) >= self.limit:
                overflow += 1
                continue
            restored = dict((marks or {}).get(proxy.url) or {})
            self.entries[proxy.url] = ProxyState(proxy, "alive" if restored else "untested",
                                                 sites=restored)
            added += 1
        return PoolAdd(added, duplicates, overflow)

    def remove(self, proxy: Proxy) -> ProxyState | None:
        return self.entries.pop(proxy.url, None)

    def clear(self) -> int:
        count = len(self.entries)
        self.entries.clear()
        self._cursor = 0
        return count

    def purge_dead(self) -> int:
        dead = [key for key, state in self.entries.items() if state.status == "dead"]
        for key in dead:
            self.entries.pop(key, None)
        return len(dead)

    def live(self) -> list[ProxyState]:
        return [state for state in self.entries.values() if state.status == "alive"]

    def live_for(self, site: str) -> list[ProxyState]:
        """Proxies known to reach one site (untested ones do not count)."""
        return [state for state in self.live() if state.sites.get(site) is True]

    def untested(self) -> list[ProxyState]:
        return [state for state in self.entries.values() if state.status == "untested"]

    def candidates(self, count: int = PROXY_ATTEMPTS, site: str | None = None) -> list[ProxyState]:
        """Round-robin over usable proxies so one request does not hammer one proxy.

        ``site`` skips proxies that were checked against that site and failed, so
        a proxy blocked by one service is not wasted on requests for it.
        """
        usable = [state for state in self.entries.values()
                  if state.status != "dead" and state.reaches(site)]
        if not usable:
            return []
        start = self._cursor % len(usable)
        self._cursor = (start + 1) % len(usable)
        return [usable[(start + offset) % len(usable)] for offset in range(min(count, len(usable)))]

    def record_success(self, proxy: Proxy, latency_ms: int = 0) -> None:
        state = self.entries.get(proxy.url)
        if state is None:
            return
        self.entries[proxy.url] = ProxyState(proxy, "alive", state.ok_count + 1, 0,
                                             latency_ms or state.latency_ms, "",
                                             time.monotonic(), state.sites)

    def record_failure(self, proxy: Proxy, detail: str,
                       limit: int | None = None) -> ProxyState | None:
        """Count a real-use failure; returns the state when the proxy was dropped."""
        state = self.entries.get(proxy.url)
        if state is None:
            return None
        if state.fail_count + 1 >= (limit or self.fail_limit):
            return self.remove(proxy)
        self.entries[proxy.url] = ProxyState(proxy, state.status, state.ok_count,
                                             state.fail_count + 1, state.latency_ms,
                                             detail, time.monotonic(), state.sites)
        return None

    def mark_checked(self, result: CheckResult) -> None:
        """Apply a health check: per-site results, alive/dead status and latency.

        A proxy that got no usable answer because the *service* answered badly
        (``verdict == "api"``, e.g. HTTP 500 from the provider's own API) keeps
        the status it had: only a proxy-level failure marks one dead, so a
        broken API cannot empty the pool. Its per-site marks are still recorded,
        so routing for that site prefers proxies that did work.
        """
        state = self.entries.get(result.proxy.url)
        if state is None:
            return
        sites = dict(state.sites)
        sites.update(result.marks())
        if result.ok:
            status = "alive"
        elif result.verdict == "api":
            status = state.status
        else:
            status = "dead"
        self.entries[result.proxy.url] = ProxyState(
            result.proxy, status, state.ok_count + (1 if result.ok else 0), 0,
            result.latency_ms, result.detail, time.monotonic(), sites)

    def summary(self) -> str:
        alive = len(self.live())
        dead = sum(1 for state in self.entries.values() if state.status == "dead")
        untested = len(self.entries) - alive - dead
        line = f"Pool: {len(self.entries)} total · {alive} live · {untested} untested · {dead} dead"
        stats = self.last_check
        if stats:
            line += (f"\nLast check {human_ago(time.monotonic() - stats.when)}: "
                     f"{stats.checked} tested · {stats.working} reachable")
        return line


def resolve_targets(keys: str = "", url: str = "", apis: Mapping[str, str] | None = None
                    ) -> tuple[CheckTarget, ...]:
    """Pick which sites to check; an empty ``keys`` keeps the Dailymotion + Rumble default.

    ``apis`` maps a service to the API endpoint the bot uses for it (built-in or
    the user's own): each named service is then probed through *that* API, so a
    proxy only passes when it can really use the API rather than fetch a page.
    """
    parts = [piece.lower() for piece in re.split(r"[,;\s]+", keys or "") if piece]
    chosen: dict[str, CheckTarget] = {}
    if parts:
        # Keep the order the user typed, ignoring names we do not know.
        for part in parts:
            target = next((item for item in KNOWN_TARGETS if item.key == part), None)
            if target is not None and target.key not in chosen:
                chosen[target.key] = target
    if not chosen:
        chosen = {target.key: target for target in CHECK_TARGETS}
    targets = tuple(api_target(target, (apis or {}).get(target.key, "")) for target in chosen.values())
    return with_url(targets, url)


def api_probe_url(endpoint: str, key: str) -> str:
    """The API endpoint with a real lookup attached, e.g. ...?url=<video link>."""
    probe = API_PROBES.get(key)
    if not probe:
        return endpoint
    return f"{endpoint}{'&' if '?' in endpoint else '?'}{urlencode({'url': probe})}"


def api_target(target: CheckTarget, endpoint: str) -> CheckTarget:
    """Turn a site probe into an API probe for this service, or keep it as it is."""
    if not public_api_url(endpoint):
        return target
    return replace(target, label=f"{target.label} API", url=api_probe_url(endpoint, target.key),
                   json=True, nocontent=False, lax=False)


def public_api_url(url: object) -> bool:
    """True for a public HTTPS endpoint we may hand to a check."""
    if not isinstance(url, str) or not url:
        return False
    parts = urlsplit(url)
    return parts.scheme == "https" and bool(parts.hostname) and "." in (parts.hostname or "")


def with_url(targets, url: str) -> tuple[CheckTarget, ...]:
    """Add or replace a check URL: the same site's probe is replaced, others are kept."""
    items = list(targets)
    if not url:
        return tuple(items)
    key = target_for(url)
    index = next((position for position, target in enumerate(items) if target.key == key), None) \
        if key else None
    if index is not None:  # e.g. a Dailymotion URL replaces the built-in Dailymotion probe.
        items[index] = replace(items[index], url=url, nocontent=False)
        return tuple(items)
    custom = CheckTarget("custom", urlsplit(url).hostname or "custom", "API", url, lax=True)
    items = [target for target in items if target.key != "custom"] + [custom]
    return tuple(items)


def target_for(url_or_host: str) -> str | None:
    """Site key for a URL or host, so a request can prefer proxies that reach it."""
    host = (url_or_host or "").strip().lower()
    if "//" in host:
        host = urlsplit(host).hostname or ""
    host = host.rstrip(".")
    if not host:
        return None
    for target in KNOWN_TARGETS:
        if any(host == domain or host.endswith("." + domain) for domain in target.hosts):
            return target.key
    return None


def target_label(key: str) -> str:
    return next((target.label for target in KNOWN_TARGETS if target.key == key), key.title())


def target_short(key: str) -> str:
    return next((target.short for target in KNOWN_TARGETS if target.key == key), key[:2].upper())


def site_marks(sites: dict[str, bool]) -> str:
    """Compact list of the sites a proxy reaches, e.g. "YT,DM"."""
    return ",".join(target.short for target in KNOWN_TARGETS
                    if sites.get(target.key) is True) or "—"


def marked_sites(comment: str) -> dict[str, bool]:
    """Read a ``# DM,RU`` comment back into per-site marks (the inverse of ``site_marks``)."""
    names = {piece.lower() for piece in re.split(r"[^A-Za-z0-9]+", comment or "") if piece}
    return {target.key: True for target in KNOWN_TARGETS
            if target.key in names or target.short.lower() in names}


def parse_proxy_marks(text: str) -> dict[str, dict[str, bool]]:
    """Per-proxy site marks from an exported list, e.g. ``1.2.3.4:8080  # DM,RU``.

    ``/proxychk export`` writes each proxy's marks as a trailing comment; this
    reads them back, so a list saved before a restart comes back with its
    per-service results instead of losing them.
    """
    table: dict[str, dict[str, bool]] = {}
    for raw_line in (text or "").splitlines():
        head, hashes, comment = raw_line.partition("#")
        if not hashes or not head.strip():
            continue  # No comment, or a whole-line comment such as an export header.
        marks = marked_sites(comment)
        if not marks:
            continue
        line = re.sub(r"\s+#.*$", "", raw_line).strip()
        for candidate in _line_candidates(line):
            try:
                proxy = parse_proxy_line(candidate)
            except ValueError:
                continue
            table[proxy.url] = marks
    return table


def export_text(states: Iterable[ProxyState], header: str = "") -> str:
    """The paste-back list: one proxy per line, its site marks in a comment."""
    lines = [header] if header else []
    for state in states:
        marks = site_marks(state.sites)
        lines.append(f"{state.proxy.url}  # {marks}" if marks != "—" else state.proxy.url)
    return "\n".join(lines) + "\n"


def human_ago(seconds: float) -> str:
    seconds = max(0, int(seconds))
    if seconds < 60:
        return f"{seconds}s ago"
    if seconds < 3600:
        return f"{seconds // 60}m ago"
    return f"{seconds // 3600}h ago"


def describe_error(exc: BaseException) -> str:
    """Short, user-safe reason for a failed proxy attempt."""
    if isinstance(exc, (asyncio.TimeoutError, aiohttp.ServerTimeoutError, SocksTimeoutError)):
        return "timeout"
    if isinstance(exc, SocksConnectionError):
        return "cannot reach the proxy"
    if isinstance(exc, aiohttp.ClientHttpProxyError):
        return f"proxy refused the tunnel (HTTP {exc.status})"
    if isinstance(exc, (aiohttp.ClientProxyConnectionError, aiohttp.ClientConnectorError)):
        return "cannot reach the proxy"
    if isinstance(exc, aiohttp.ClientError):
        return "connection failed"
    if isinstance(exc, SOCKS_ERRORS):
        return "proxy error"
    if isinstance(exc, OSError):
        return "network error"
    return type(exc).__name__


def make_session(proxy: Proxy, *, timeout, headers=None, limit: int = 8,
                 force_close: bool = False) -> aiohttp.ClientSession:
    """Session whose traffic leaves through exactly one proxy.

    HTTP/HTTPS proxies need only a normal connector plus a per-request ``proxy``
    argument (see :func:`request_kwargs`); SOCKS proxies need their own connector.
    """
    client_timeout = (aiohttp.ClientTimeout(total=timeout, connect=timeout)
                      if isinstance(timeout, (int, float)) else timeout)
    if proxy.socks:
        if ProxyConnector is None:
            raise ProxyUnavailable("SOCKS proxies need the aiohttp-socks package installed.")
        from aiohttp_socks import ProxyType  # Optional dependency, imported with the connector.
        connector = ProxyConnector(
            host=proxy.host, port=proxy.port,
            proxy_type=ProxyType.SOCKS4 if proxy.scheme.startswith("socks4") else ProxyType.SOCKS5,
            username=proxy.username or None, password=proxy.password or None,
            # socks5h/socks4a resolve names at the proxy; plain socks5 defaults to that too.
            rdns=proxy.scheme in ("socks4a", "socks5", "socks5h"), limit=limit,
            force_close=force_close)
    else:
        connector = aiohttp.TCPConnector(limit=limit, ttl_dns_cache=300, force_close=force_close)
    return aiohttp.ClientSession(timeout=client_timeout, headers=headers, connector=connector)


def request_kwargs(proxy: Proxy) -> dict:
    """Extra ``session.get`` arguments for this proxy (SOCKS uses its connector)."""
    return {} if proxy.socks else {"proxy": proxy.url}


def looks_like_json(body: bytes) -> bool:
    """True when the body is a JSON object/array, the shape these APIs answer with."""
    stripped = body.lstrip(b" \t\r\n")
    if not stripped[:1] in (b"{", b"["):
        return False
    try:
        json.loads(stripped.decode("utf-8", "replace"))
    except (ValueError, UnicodeDecodeError):
        return False
    return True


def site_ok(target: CheckTarget, status: int, body: bytes) -> tuple[bool, str]:
    """Decide whether an HTTP answer proves this proxy reached this site.

    A short, plain-text answer counts as site content; HTML is how captive
    portals and login walls reply, so it never passes. An API probe is stricter:
    only a JSON answer counts, whatever the status code says.
    """
    if status in RETRY_STATUSES:
        return False, f"proxy replied HTTP {status}"
    if target.json:
        if looks_like_json(body):
            return True, ""  # The API itself answered, even if it reported an error.
        if not body:
            return False, f"empty reply (HTTP {status})"
        return False, f"HTTP {status}, not a JSON API answer"
    if status in (301, 302, 303, 307, 308):
        return True, ""  # The site answered with a redirect.
    if status == 404:
        return True, ""  # The site answered; only the probe path is missing.
    if status in (200, 204):
        if not body:
            # generate_204 and user health endpoints answer empty.
            return (True, "") if target.nocontent or target.lax else (False, "empty reply")
        if body.lstrip(b" \t\r\n")[:1] == b"<":
            return False, "HTML page, not site content"
        if target.nocontent:  # A body means something else answered generate_204.
            return False, f"unexpected reply HTTP {status}"
        return True, ""
    return False, f"HTTP {status}"


async def probe_site(session: aiohttp.ClientSession, proxy: Proxy, target: CheckTarget,
                     timeout: float, min_latency_ms: int = 0) -> SiteResult:
    """One request through an existing session; network problems become a result.

    ``min_latency_ms`` rejects replies that arrive implausibly fast: a proxy
    cannot reach a remote site in a few milliseconds, so such an answer came
    from the proxy itself (cached, forged or a local intercept).
    """
    started = time.monotonic()
    try:
        # identity keeps the body readable, so an HTML answer is recognisable.
        async with session.get(target.url, allow_redirects=False, auto_decompress=False,
                               headers={"Accept-Encoding": "identity"},
                               timeout=aiohttp.ClientTimeout(total=timeout, connect=timeout),
                               **request_kwargs(proxy)) as response:
            body = await response.content.read(4096)
            status = response.status
    except CONNECTION_ERRORS as exc:
        return SiteResult(target.key, target.label, False, 0, describe_error(exc))
    except ValueError as exc:  # aiohttp rejects malformed proxy URLs/headers.
        return SiteResult(target.key, target.label, False, 0, short_reason(exc, 60))
    latency = int((time.monotonic() - started) * 1000)
    ok, detail = site_ok(target, status, body)
    if ok and min_latency_ms and latency < min_latency_ms:
        return SiteResult(target.key, target.label, False, latency,
                          f"{TOO_FAST} ({latency} ms < {min_latency_ms} ms)")
    return SiteResult(target.key, target.label, ok, latency, detail)


async def check_proxy(proxy: Proxy, *, targets: tuple[CheckTarget, ...] = CHECK_TARGETS,
                      timeout: float = 8.0, headers=None, min_latency_ms: int = 0,
                      session_factory=make_session) -> CheckResult:
    """Check one proxy against every configured site and summarise the answer."""
    chosen = tuple(targets) or CHECK_TARGETS
    reason = ""
    session = None
    try:
        session = session_factory(proxy, timeout=timeout, headers=headers, limit=4, force_close=True)
    except ProxyUnavailable as exc:
        reason = short_reason(exc, 60)
    except (aiohttp.ClientError, OSError) as exc:
        reason = describe_error(exc)
    except Exception as exc:  # A broken proxy URL or missing SOCKS support.
        reason = short_reason(exc, 60)
    results: list[SiteResult] = []
    if reason:
        results = [SiteResult(target.key, target.label, False, 0, reason) for target in chosen]
    else:
        try:
            for target in chosen:
                results.append(await probe_site(session, proxy, target, timeout, min_latency_ms))
        except CONNECTION_ERRORS as exc:
            results.extend(SiteResult(target.key, target.label, False, 0, describe_error(exc))
                           for target in chosen[len(results):])
        finally:
            # A cancelled check must still close its session, or the process
            # later reports an unclosed client session.
            await close_session(session)
    answered = [site for site in results if site.ok]
    detail = next((site.detail for site in results if not site.ok and site.detail), "")
    return CheckResult(proxy, bool(answered),
                       min((site.latency_ms for site in answered), default=0),
                       detail or ("" if answered else "no site answered"),
                       tuple(results))


_CLOSERS: set[asyncio.Task] = set()  # Keeps background closes alive until they finish.


async def close_session(session: aiohttp.ClientSession) -> None:
    """Close a session even when the calling task is being cancelled.

    A cancelled task cannot await anything, so a plain ``await session.close()``
    in a ``finally`` can be skipped entirely — which is how aiohttp sessions end
    up being garbage-collected open ("Unclosed client session"). The close runs
    in its own task behind a shield, so cancelling the caller cannot stop it.
    """
    try:
        await asyncio.shield(_close_quietly(session))
    except asyncio.CancelledError:  # The close itself keeps running.
        raise


async def _close_quietly(session: aiohttp.ClientSession) -> None:
    try:
        await session.close()
    except Exception:  # pragma: no cover - closing must never raise.
        LOG.info("Could not close a proxy session", exc_info=True)


def close_soon(session: aiohttp.ClientSession) -> None:
    """Schedule a close from synchronous code (leases released while cancelled)."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:  # pragma: no cover - no loop, nothing to close on.
        return
    task = loop.create_task(_close_quietly(session))
    _CLOSERS.add(task)
    task.add_done_callback(_CLOSERS.discard)


@dataclass
class _CacheEntry:
    session: aiohttp.ClientSession
    uses: int = 0


@dataclass
class _Lease:
    session: aiohttp.ClientSession
    key: str = ""
    temporary: bool = False


class RoutedRequest:
    """Async context manager mirroring ``ClientSession.get`` with proxy failover."""

    def __init__(self, router: "ProxyRouter", url: str, kwargs: dict):
        self.router = router
        self.url = url
        self.kwargs = kwargs
        self._cm = None
        self._lease: _Lease | None = None

    async def __aenter__(self):
        router = self.router
        last_error: BaseException | None = None
        # Skip proxies already known to be blocked for this specific site,
        # including the user's own API hosts (see ProxyRouter.site_for).
        site = router.site_for(self.url)
        for state in router.pool.candidates(site=site):
            lease = None
            started = time.monotonic()
            try:
                lease = await router.acquire(state.proxy)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # Not even usable as a client: count it and move on.
                last_error = exc
                router.note_failure(state, short_reason(exc))
                continue
            try:
                cm = lease.session.get(self.url, **request_kwargs(state.proxy), **self.kwargs)
                response = await cm.__aenter__()
            except CONNECTION_ERRORS as exc:
                router.release(lease)
                detail = describe_error(exc)
                last_error = exc
                router.note_failure(state, detail)
                continue
            except BaseException:
                router.release(lease)
                raise
            if response.status in RETRY_STATUSES:
                # A proxy-level error page, not the site's own answer. Closing the
                # response first keeps the connection from being reused.
                detail = f"proxy replied HTTP {response.status}"
                last_error = aiohttp.ClientResponseError(response.request_info, response.history,
                                                         status=response.status, message=detail)
                try:
                    await cm.__aexit__(None, None, None)
                except BaseException:
                    pass
                router.release(lease)
                router.note_failure(state, detail)
                continue
            router.pool.record_success(state.proxy, int((time.monotonic() - started) * 1000))
            self._cm, self._lease = cm, lease
            return response
        fallback = await router.direct_request(self.url, self.kwargs, last_error)
        self._cm = fallback
        return await fallback.__aenter__()

    async def __aexit__(self, exc_type, exc, traceback) -> None:
        lease, self._lease = self._lease, None
        cm, self._cm = self._cm, None
        try:
            if cm is not None:
                await cm.__aexit__(exc_type, exc, traceback)
        except BaseException:
            if lease is not None:  # Never leave a temporary session open.
                self.router.release(lease)
            raise
        if lease is not None:
            self.router.release(lease)  # Synchronous: a cancelled task cannot skip it.


class ProxyRouter:
    """Route a user's requests through the pool, retrying failing proxies.

    Each request tries up to ``PROXY_ATTEMPTS`` live proxies in turn. A proxy
    that fails is counted, and after ``PROXY_FAIL_LIMIT`` consecutive failures
    it is removed from the pool. When no candidate is left the request is
    retried without a proxy unless ``strict`` is set, in which case
    :class:`ProxyUnavailable` is raised instead.
    """

    def __init__(self, pool: ProxyPool, direct: aiohttp.ClientSession, *, timeout, headers=None,
                 strict: bool = False, cache_size: int = SESSION_CACHE,
                 session_factory: Callable[[Proxy], aiohttp.ClientSession] | None = None,
                 on_drop: Callable[[ProxyState, str], None] | None = None):
        self.pool = pool
        self.direct = direct
        self.timeout = timeout
        self.headers = headers
        self.strict = strict
        self.cache_size = max(1, cache_size)
        self.on_drop = on_drop
        self.session_factory = session_factory or (
            lambda proxy: make_session(proxy, timeout=timeout, headers=headers))
        self.dropped: deque[str] = deque(maxlen=5)
        self.direct_fallbacks = 0
        # API host -> service key, so a request to the user's own API endpoint
        # prefers proxies the check proved for that service.
        self.api_hosts: dict[str, str] = {}
        self._sessions: "OrderedDict[str, _CacheEntry]" = OrderedDict()
        self._open: "weakref.WeakSet[aiohttp.ClientSession]" = weakref.WeakSet()
        self._lock = asyncio.Lock()

    def site_for(self, url: str) -> str | None:
        """Which service a URL belongs to, for known sites and configured APIs."""
        site = target_for(url)
        if site is not None:
            return site
        host = (urlsplit(url).hostname or "").lower().rstrip(".")
        if not host:
            return None
        for known, key in self.api_hosts.items():
            if host == known or host.endswith("." + known):
                return key
        return None

    def open_sessions(self) -> int:
        """Routed sessions that are still open (for status and tests)."""
        return sum(1 for session in self._open if not session.closed)

    # -- request routing -------------------------------------------------
    def get(self, url: str, **kwargs) -> RoutedRequest:
        """Same signature as ``ClientSession.get`` but with pool failover."""
        return RoutedRequest(self, url, kwargs)

    def next_proxy(self, site: str | None = None) -> Proxy | None:
        """One live proxy for callers that cannot use aiohttp (yt-dlp)."""
        candidates = self.pool.candidates(1, site=site)
        return candidates[0].proxy if candidates else None

    def note_failure(self, state: ProxyState, detail: str) -> bool:
        """Count a failure for a pool proxy; True when it was dropped."""
        dropped = self.pool.record_failure(state.proxy, detail)
        if dropped is None:
            return False
        self._remember_drop(dropped, detail)
        return True

    def note_failure_url(self, url: str, detail: str) -> bool:
        """Same as :meth:`note_failure` for a canonical proxy URL."""
        state = self.pool.entries.get(url)
        return self.note_failure(state, detail) if state is not None else False

    def _remember_drop(self, state: ProxyState, detail: str) -> None:
        self.dropped.append(f"{state.proxy.label} ({detail})")
        LOG.warning("Removed proxy %s from the pool: %s", state.proxy.label, detail)
        if self.on_drop is not None:
            try:
                self.on_drop(state, detail)
            except Exception:  # A notification problem must never break a transfer.
                LOG.warning("Proxy drop notification failed", exc_info=True)

    async def direct_request(self, url: str, kwargs: dict, last_error: BaseException | None):
        """Last resort: no usable proxy, so either go direct or refuse."""
        if self.strict:
            reason = f" (last failure: {describe_error(last_error)})" if last_error else ""
            raise ProxyUnavailable(
                "No usable proxy left in your pool, and strict proxy mode is on. "
                f"Add proxies with /proxychk.{reason}")
        self.direct_fallbacks += 1
        if last_error is not None and LOG.isEnabledFor(logging.INFO):
            LOG.info("Proxies failed for %s (%s); retrying without a proxy",
                     urlsplit(url).hostname, describe_error(last_error))
        return self.direct.get(url, **kwargs)

    # -- session cache ---------------------------------------------------
    async def acquire(self, proxy: Proxy) -> _Lease:
        evicted = None
        async with self._lock:
            entry = self._sessions.get(proxy.url)
            if entry is not None:
                entry.uses += 1
                self._sessions.move_to_end(proxy.url)
                return _Lease(entry.session, proxy.url)
            if len(self._sessions) >= self.cache_size:
                idle = next((key for key, item in self._sessions.items() if not item.uses), None)
                if idle is None:  # Every cached proxy is busy: use a one-shot session.
                    session = self.session_factory(proxy)
                    self._open.add(session)
                    return _Lease(session, temporary=True)
                evicted = self._sessions.pop(idle)
            session = self.session_factory(proxy)
            self._open.add(session)
            self._sessions[proxy.url] = _CacheEntry(session, 1)
            lease = _Lease(session, proxy.url)
        if evicted is not None:
            await close_session(evicted.session)
        return lease

    def release(self, lease: _Lease) -> None:
        """Return a lease. Synchronous on purpose: it must also run while the
        calling task is being cancelled, when awaiting is not possible."""
        if lease.temporary:
            close_soon(lease.session)
            return
        entry = self._sessions.get(lease.key)
        if entry is not None:
            entry.uses = max(0, entry.uses - 1)
            self._sessions.move_to_end(lease.key)

    async def close(self) -> None:
        """Close every routed session, cached or otherwise.

        Tracking every session the router created means a lease that was dropped
        by a cancelled request is still closed here instead of leaking.
        """
        async with self._lock:
            sessions = [entry.session for entry in self._sessions.values()]
            self._sessions.clear()
            sessions.extend(session for session in self._open if session not in sessions)
        for session in sessions:
            await close_session(session)

    # -- reporting -------------------------------------------------------
    def describe(self) -> str:
        alive = len(self.pool.live())
        total = len(self.pool)
        text = f"{alive} live/{total} in pool"
        stats = self.pool.last_check
        counts = (stats.sites if stats else {target.key: len(self.pool.live_for(target.key))
                                             for target in CHECK_TARGETS} if self.pool.entries else {})
        for key, count in counts.items():
            if count:
                text += f" · {target_short(key)} {count}"
        if not alive and not self.strict:
            text += " · using a direct connection"
        if self.dropped:
            text += f" · dropped {len(self.dropped)} in use"
        if self.direct_fallbacks:
            text += f" · {self.direct_fallbacks} direct fallback(s)"
        return text


def check_verdict(results: Iterable[CheckResult]) -> str:
    """``api`` when nothing failed for a proxy-level reason, else ``mixed``/``proxy``."""
    verdicts = {result.verdict for result in results}
    if verdicts == {"api"}:
        return "api"
    if verdicts == {"proxy"}:
        return "proxy"
    return "mixed"


async def run_checks(proxies: list[Proxy], *, targets: tuple[CheckTarget, ...] = CHECK_TARGETS,
                     timeout: float = 8.0, workers: int = 24, headers=None,
                     min_latency_ms: int = 0, session_factory=make_session,
                     on_progress: Callable[[int, int, CheckResult], Awaitable[None]] | None = None
                     ) -> list[CheckResult]:
    """Check many proxies with bounded concurrency; results keep input order.

    Each worker probes every configured site for its proxy, so the connection
    count stays bounded by ``workers`` however many sites are checked.
    """
    semaphore = asyncio.Semaphore(max(1, workers))
    results: list[CheckResult] = [None] * len(proxies)  # type: ignore[list-item]
    done = 0

    async def run(index: int, proxy: Proxy) -> None:
        nonlocal done
        async with semaphore:
            result = await check_proxy(proxy, targets=targets, timeout=timeout, headers=headers,
                                       min_latency_ms=min_latency_ms,
                                       session_factory=session_factory)
        results[index] = result
        done += 1
        if on_progress is not None:
            await on_progress(done, len(proxies), result)

    tasks = [asyncio.create_task(run(index, proxy)) for index, proxy in enumerate(proxies)]
    try:
        await asyncio.gather(*tasks)
    except BaseException:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise
    return results
