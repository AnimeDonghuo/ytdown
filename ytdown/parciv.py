"""apitera (api.parciv.workers.dev): the Moviebox, Anime and Donghua API.

Three families of endpoints, one set of readers:

* **Moviebox catalogue** - ``/moviebox/movies``, ``/moviebox/tv``, ``/moviebox/tvshows``,
  ``/moviebox/animation``, ``/moviebox/kids``, ``/moviebox/recommend`` - every page is a
  list of subjects with ``subjectId``, ``name``, ``poster``, ``year`` and so on.
* **Moviebox detail and stream** - ``/moviebox/detail?subjectId=`` and
  ``/moviebox/stream?id=`` - the stream answer lists one entry per episode or cut with
  ``streams`` (DASH plus a CloudFront signature cookie), ``resources`` (plain MP4 per
  resolution) and ``subtitles`` (one signed ``.srt`` per language).
* **Anime and Donghua** - ``/anime/latest`` … ``/anime/post`` and the same under
  ``/donghua/`` - the API builder's menu; their payloads have their own shapes, so the
  readers here look for titles, ids and media lists wherever they turn up rather than
  trusting one exact layout.

The uploader works from these readers: one title, every quality it offers, the audio
language tags it reported and the subtitle language, in the same ``media_name`` shape the
rest of the bot uses, e.g. ``PAW Patrol_ The Dino Movie [Hindi] [Eng Sub] [360p].mp4``.

Nothing is downloaded until a job runs: the catalogue is only read, and the media URLs are
fetched at upload time (the CDN links are signed and expire). One source is tried three ways,
in this order: a plain HTTP copy (these CDNs list progressive MP4s, and a copy cannot fail in
a post-processor or need ffmpeg), yt-dlp (manifests and the odd source a copy cannot take),
and the API's own leech call ``…/<family>/stream?url=<source>`` — the route that still works
when a CDN refuses the bot, or when only the worker can reach that host.
"""
from __future__ import annotations

import ipaddress
import json
import logging
import os
import re
import shutil
import subprocess
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, replace
from typing import Iterable
from urllib.parse import parse_qs, quote, urlparse, urlsplit

import yt_dlp

from .core import public_https_url
from .dailymotion import DownloadCancelled, media_problem, short_reason

LOG = logging.getLogger(__name__)

PARCIV_API_URL = "https://api.parciv.workers.dev/moviebox"  # The API builder's own default.
PARCIV_HOSTS = ("api.parciv.workers.dev",)
PARCIV_CDNS = ("hakunaymatata.com", "aoneroom.com", "parciv.workers.dev")
MAX_PARCIV_BYTES = 8_000_000  # One catalogue page, stream answer or post.
DEFAULT_AUTO_LIMIT = 3  # Items per automatic run and per section.
MAX_AUTO_LIMIT = 10
DM_SCAN_LIMIT = 300  # DM messages read for the "already uploaded" check.

# The catalogue sections, exactly as api.parciv.workers.dev names them.
PARCIV_SECTIONS = {
    "movies": "movies",
    "tv": "tv",
    "tvshows": "tvshows",
    "animation": "animation",
    "kids": "kids",
    "recommend": "recommend",
}
# Sections that live under /anime or /donghua instead of /moviebox.
PARCIV_STREAM_SECTIONS = {"anime": "anime", "donghua": "donghua"}
SECTION_ALIASES = {
    "movie": "movies", "movies": "movies", "moviebox": "movies", "film": "movies",
    "tv": "tv", "series": "tv", "serial": "tv", "show": "tv", "shows": "tv",
    "tvshows": "tvshows", "tv-shows": "tvshows", "tv show": "tvshows", "tvshow": "tvshows",
    "animation": "animation", "animated": "animation", "cartoon": "animation", "cartoons": "animation",
    "kids": "kids", "kid": "kids", "recommend": "recommend", "recommended": "recommend",
    "recommendations": "recommend", "anime": "anime", "donghua": "donghua", "donghuas": "donghua",
}
# What the automatic watcher checks unless the user names other sections.
AUTO_SECTIONS = ("movies", "anime", "donghua")
SECTION_LABELS = {"movies": "Moviebox movies", "tv": "Moviebox TV", "tvshows": "Moviebox TV shows",
                  "animation": "Moviebox animation", "kids": "Moviebox kids",
                  "recommend": "Moviebox recommendations", "anime": "Anime", "donghua": "Donghua"}


class ParcivError(ValueError):
    """A user-facing apitera problem (bad link, empty catalogue, refused API)."""


# --------------------------------------------------------------------------- API URLs


@dataclass(frozen=True)
class ParcivApi:
    """The URLs of one apitera endpoint: the built-in one or the user's own."""

    endpoint: str

    @property
    def root(self) -> str:
        parts = urlsplit(self.endpoint)
        return f"{parts.scheme}://{parts.netloc}"

    @property
    def moviebox(self) -> str:
        """The Moviebox base, whether the endpoint is the root or already /moviebox."""
        base = self.endpoint.rstrip("/")
        if urlsplit(base).path.rstrip("/").endswith("/moviebox"):
            return base
        return f"{self.root}/moviebox"

    def section(self, section: str, page: int = 1) -> str:
        if section in PARCIV_SECTIONS:
            return f"{self.moviebox}/{PARCIV_SECTIONS[section]}?page={max(1, int(page))}"
        if section in PARCIV_STREAM_SECTIONS:
            return f"{self.root}/{PARCIV_STREAM_SECTIONS[section]}/latest"
        raise ParcivError(f"Unknown apitera section {section!r}.")

    def detail(self, subject_id: str) -> str:
        return f"{self.moviebox}/detail?subjectId={quote(str(subject_id))}"

    def stream(self, subject_id: str) -> str:
        return f"{self.moviebox}/stream?id={quote(str(subject_id))}"

    def post_candidates(self, section: str, item: "ParcivItem") -> tuple[tuple[str, str], ...]:
        """Every plausible media call for one Anime/Donghua entry, best guess first.

        The API builder's menu is not identical between builds: the same episode has
        been served as ``/post?slug=``, ``/post?id=`` and ``/post?url=``, and asking
        with the wrong one answers 200 with something that is not media at all. The
        bot therefore asks each shape until one answers with media — the same thing a
        person does by hand — instead of trusting a single guess.
        """
        base = f"{self.root}/{section}"
        seen: set[str] = set()
        candidates: list[tuple[str, str]] = []

        def add(label: str, url: str) -> None:
            if url and url not in seen:
                seen.add(url)
                candidates.append((label, url))

        if item.post_url:  # A link the user sent, or one the listing handed over.
            add("its own link", item.post_url)
        for key in ("slug", "post_slug", "postSlug"):
            value = item.extra.get(key, "")
            if value:
                add("slug", f"{base}/post?slug={quote(str(value))}")
                break
        for key in ("postId", "post_id", "episodeId", "id"):
            value = item.extra.get(key, "")
            if value:
                add("id", f"{base}/post?id={quote(str(value))}")
                break
        if item.subject_id:
            add("subject id", f"{base}/post?id={quote(item.subject_id)}")
        own = item.extra.get("url", "")
        if own:
            add("api url", f"{base}/post?url={quote(own, safe='')}")
        for key in ("slug", "post_slug", "postSlug"):
            value = item.extra.get(key, "")
            if value:
                add("videos slug", f"{base}/videos?slug={quote(str(value))}")
                break
        for key in ("postId", "post_id", "episodeId", "id"):
            value = item.extra.get(key, "")
            if value:
                add("videos id", f"{base}/videos?id={quote(str(value))}")
                break
        if item.subject_id:
            add("videos subject id", f"{base}/videos?subjectId={quote(item.subject_id)}")
        if item.subject_id:
            add("subject id param", f"{base}/post?subjectId={quote(item.subject_id)}")
        if item.subject_id:
            # The whole catalogue: every entry with its stream link, matched by title.
            add("videos", f"{base}/videos")
        if item.subject_id:
            add("detail", f"{base}/detail?id={quote(item.subject_id)}")
        return tuple(candidates[:11])

    def post(self, section: str, item: "ParcivItem") -> str:
        """The episode-detail call for one Anime/Donghua entry (the first shape)."""
        candidates = self.post_candidates(section, item)
        if not candidates:
            raise ParcivError("This apitera entry carries no id to look up.")
        return candidates[0][1]


def normalize_section(value: str) -> str:
    """Accept `anime`, `moviebox/animation`, `tv-posts`… as the section they name."""
    text = " ".join(str(value or "").split()).strip().lower().replace("_", " ")
    for prefix in ("moviebox/", "moviebox ", "anime/", "donghua/"):
        if text.startswith(prefix):
            text = text[len(prefix):]
            break
    for suffix in ("/latest", "/videos", "/trending", "/post", " latest", " videos"):
        if text.endswith(suffix):
            text = text[: len(text) - len(suffix)]
    return SECTION_ALIASES.get(text, "")


def parse_sections(raw: str) -> tuple[str, ...]:
    """The sections a `/auto sections …` line names, in order and without duplicates."""
    names: list[str] = []
    for part in re.split(r"[,;\s]+", str(raw or "")):
        if not part:
            continue
        section = normalize_section(part)
        if not section:
            raise ParcivError(f"Unknown apitera section {part!r}. Use "
                              + ", ".join(sorted(set(PARCIV_SECTIONS) | set(PARCIV_STREAM_SECTIONS)))
                              + ".")
        if section not in names:
            names.append(section)
    return tuple(names)


def family_base(api: "ParcivApi", section: str) -> str:
    """The family URL a section lives under: the Moviebox base, Anime or Donghua."""
    if section in PARCIV_STREAM_SECTIONS:
        return f"{api.root}/{PARCIV_STREAM_SECTIONS[section]}"
    return api.moviebox


def search_url_for(base: str, words: str) -> str:
    """A family's own search call, `…/<family>/search?query=…`.

    Whether the family really has a search call is the API's business: a refusal
    is answered by scanning its catalogue pages instead.
    """
    query = quote(" ".join(str(words or "").split()), safe="")
    return f"{str(base).rstrip('/')}/search?query={query}"


def search_url(api: "ParcivApi", section: str, words: str) -> str:
    """That section's family search call (see `search_url_for`)."""
    return search_url_for(family_base(api, section), words)


def leech_url(api: "ParcivApi", section: str, url: str) -> str:
    """The API's own copy of one source: `…/<family>/stream?url=<encoded>`.

    The worker fetches that URL itself, which is the route that still works when a
    CDN refuses the bot directly (or when only the worker can reach that host).
    """
    return f"{family_base(api, section)}/stream?url={quote(str(url), safe='')}"


def parse_parciv_source(raw: str) -> str:
    """Return the apitera link a message points at, or raise ValueError."""
    text = str(raw or "").strip()
    if len(text) > 2048 or not public_https_url(text):
        raise ValueError("apitera mode accepts only https://api.parciv.workers.dev links.")
    parsed = urlsplit(text)
    host = (parsed.hostname or "").lower().rstrip(".")
    if not any(host == suffix or host.endswith("." + suffix) for suffix in PARCIV_HOSTS):
        raise ValueError("apitera mode accepts only api.parciv.workers.dev links "
                         "(send /latest to browse instead).")
    if len(parsed.path.strip("/")) < 2:
        raise ValueError("That apitera URL names no endpoint. Send /latest to browse the catalogue.")
    return text


def is_parciv_media_endpoint(url: str) -> bool:
    """True for an apitera URL that is one item's media rather than an API base."""
    parts = urlsplit(str(url or ""))
    host = (parts.hostname or "").lower().rstrip(".")
    if not any(host == suffix or host.endswith("." + suffix) for suffix in PARCIV_HOSTS):
        return False
    path = parts.path.rstrip("/").lower()
    query = parts.query.lower()
    return ("/stream" in path or "/detail" in path or "/post" in path
            or "subjectid=" in query or "?id=" in query or "&id=" in query)


# ----------------------------------------------------------------------- title keys


TITLE_TAGS = re.compile(r"\[[^\]]{0,40}\]|\([^)]{0,40}\)")
MEDIA_SUFFIX = re.compile(r"\.(?:mp4|mkv|webm|mov|m4v|avi|ts|mp3|m4a|aac|srt|ass|vtt|zip|rar|7z)$",
                          re.IGNORECASE)
QUALITY_WORD = re.compile(r"\b(?:\d{3,4}p|fhd|uhd|hd|sd|hevc|h\.?265|x265|h\.?264|x264|aac|mp4|mkv|"
                          r"dash|hls|stream|video|movie|480p)\b", re.IGNORECASE)
LANGUAGE_WORD = re.compile(r"\b(?:hindi|english|indonesian|malay|spanish|french|portuguese|arabic|"
                           r"japanese|korean|chinese|tamil|telugu|bengali|urdu|thai|vietnamese|turkish|"
                           r"russian|german|italian|dual|multi|audio|subs?|subtitles?|eng|hin|ind)\b",
                           re.IGNORECASE)
YEAR_WORD = re.compile(r"\b(?:19|20)\d{2}\b")
EPISODE_WORD = re.compile(r"\b(?:s\d{1,2}|e\d{1,3}|season|episode|eps|ep)\b", re.IGNORECASE)


def title_key(text: object) -> str:
    """A comparison key for a title: no tags, no extension, no quality or language words."""
    cleaned = TITLE_TAGS.sub(" ", str(text or ""))
    cleaned = MEDIA_SUFFIX.sub(" ", cleaned.strip())
    cleaned = QUALITY_WORD.sub(" ", cleaned)
    cleaned = LANGUAGE_WORD.sub(" ", cleaned)
    cleaned = re.sub(r"[\W_]+", " ", cleaned, flags=re.UNICODE)  # "_" separates words too.
    return " ".join(cleaned.lower().split())


def title_variants(text: object) -> set[str]:
    """The forms a past upload can be recognised by, year and episode words aside."""
    key = title_key(text)
    if not key:
        return set()
    forms = {key, YEAR_WORD.sub(" ", key).strip(), EPISODE_WORD.sub(" ", key).strip()}
    return {form for form in forms if form}


def titles_match(left: object, right: object) -> bool:
    """True when two titles name the same thing (tags, year and episode words ignored)."""
    for first in title_variants(left):
        for second in title_variants(right):
            if first == second:
                return True
            shorter, longer = sorted((first, second), key=len)
            if longer.startswith(shorter) and longer[len(shorter):len(shorter) + 1] == " ":
                tail = longer[len(shorter):].strip()
                # "the dino movie" vs "the dino movie 2026", or a series against one
                # of its episodes ("one piece" / "one piece episode 1130").
                if len(shorter) >= 14 or (len(shorter) >= 9 and
                                          re.match(r"(?:episode|season|ep|s\d)\b|\d", tail)):
                    return True
    return False


URL_TITLE_SKIP = frozenset({
    "2024", "2025", "2026", "2027", "2028", "Donghua", "Anime", "Animation", "Cartoon",
    "Cartoons", "Kids", "Movie", "Movies", "TV", "Shows", "Series", "Sub", "Dub",
    "downloads", "files", "cdn", "videos", "video", "media", "stream", "streams",
    "movie", "tv", "show", "donghua", "anime", "moviebox", "source", "sources",
    "apitera", "parciv", "episodes", "episode", "latest", "trending", "new",
    "english", "hindi", "japanese", "chinese", "korean", "indonesian", "subbed", "dubbed",
    "1080p", "720p", "480p", "360p", "240p", "manifest", "playlist", "master", "index",
    "output", "hls", "dash", "segment", "segments", "chunk", "chunks", "api",
})


def url_title_token(url: object) -> str:
    """The most title-like folder name in a media URL's path, else "".

    `…/Donghua/KuangWang2/06-480p.mp4` → `KuangWang2`. Used to catch the data-mismatch
    case where the API hands back a file that lives in someone else's folder.
    """
    parts = [part for part in urlsplit(str(url or "")).path.split("/") if part]
    if not parts:
        return ""
    # The token is the folder just before the file, unless that folder is generic.
    folder = ""
    for part in parts[:-1]:
        if part in URL_TITLE_SKIP:
            continue
        if re.match(r"^\d+$", part):
            continue
        folder = part
    if folder:
        return folder
    # Fall back to the file's stem (after stripping a leading episode/quality number).
    stem = os.path.splitext(parts[-1])[0]
    if stem.lower() in URL_TITLE_SKIP:
        return ""
    cleaned = re.sub(r"^[-\d_\s]+", "", stem).strip("-_ ")
    if not cleaned or len(cleaned) < 4 or cleaned.lower() in URL_TITLE_SKIP:
        return ""
    return cleaned


def url_token_matches_title(token: str, title: str) -> bool:
    """True when a token from a URL (e.g. `KuangWang2`) plausibly names `title` (e.g. `Kuang Wang 2`).

    Abbreviations (≤3 chars, or a common acronym like `op`, `bw`) are accepted because
    short tokens are not informative enough to refuse on. Otherwise the two are split
    into word bags (case + digit boundaries for the token) and at least one token word
    must start the title.
    """
    if not token or not title:
        return True
    # "KuangWang2" → "kuang wang 2"
    left = re.sub(r"(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])|(?<=[a-zA-Z])(?=\d)|(?<=\d)(?=[a-zA-Z])",
                  " ", token)
    left = re.sub(r"[_\-]+", " ", left).lower()
    left_words = [word for word in re.split(r"\s+", left) if word]
    # The right side is the listing's title (already words).
    right_words = [word for word in re.split(r"\s+", re.sub(r"[_\-]+", " ", title.lower())) if word]
    if not left_words or not right_words:
        return True
    # Abbreviations and numeric words: too short or non-linguistic to be sure, accept.
    meaningful = [word for word in left_words if len(word) > 3 and not word.isdigit()]
    if not meaningful:
        return True
    # At least one full token word must appear in the title (or the first 4 chars of one).
    for word in meaningful:
        for candidate in right_words:
            if word == candidate or word.startswith(candidate[:4]):
                return True
    return False


def media_file_url(value: object) -> bool:
    """True when a URL names a media file (so a leech link is a file, not a page)."""
    if not public_media_url(value):
        return False
    return urlsplit(str(value)).path.lower().endswith(LEECH_SUFFIXES + (".mpd", ".m3u8"))


def leech_link_file(value: object) -> str:
    """The media file inside the API's own leech link, else "".

    ``…/donghua/stream?url=http%3A%2F%2Fsantol.whatbox.ca%3A29318%2F…%2F06-360p.mp4``
    is not a page: fetching it hands back the video itself, exactly like a leech
    command does. The ``/videos`` catalogue hands out links of this shape, so the
    bot reads them as the file they carry.
    """
    if not isinstance(value, str) or len(value) > 2048:
        return ""
    if not any(host in value for host in PARCIV_HOSTS):
        return ""
    try:
        query = parse_qs(urlsplit(value).query)
    except ValueError:
        return ""
    for inner in query.get("url", []):
        if media_file_url(inner):
            return inner
    return ""


def is_downloadable_url(value: object) -> bool:
    """True for the URL of a file this bot can fetch: a media file or a leech link."""
    return media_file_url(value) or bool(leech_link_file(value))


def file_title(url: object) -> str:
    """A readable title from a file URL: `…/KuangWang2/06-360p.mp4` → `KuangWang2 06`."""
    inner = leech_link_file(url)
    if inner:
        return file_title(inner)  # A leech link names the route, not the file.
    parts = [part for part in urlsplit(str(url)).path.split("/") if part]
    stem = ""
    if parts:
        stem = os.path.splitext(parts[-1])[0]
        stem = re.sub(r"[_\s-]*\d{3,4}[pP]$", "", stem).strip("-_ ") or stem
    folder = parts[-2] if len(parts) > 1 else ""
    text = " ".join(dict.fromkeys(part for part in (folder, stem) if part))
    return (text or stem or "apitera file")[:120]


def find_media_url(value: object, depth: int = 0) -> str:
    """The first media-file URL anywhere in this answer, whatever the key is called.

    These families' listings are not consistent between builds: the file has turned
    up as ``url``, ``link``, ``play``, inside ``servers`` or inside ``episodes``. So
    the whole entry is scanned instead of trusting one key name — which is exactly
    how a leech bot gets to the file.
    """
    if depth > 5:  # Deep enough for `episodes[] → servers[] → url`.
        return ""
    if isinstance(value, str):
        return value if is_downloadable_url(value) else ""
    if isinstance(value, dict):
        for entry in value.values():
            found = find_media_url(entry, depth + 1)
            if found:
                return found
    elif isinstance(value, list):
        for entry in value[:12]:
            found = find_media_url(entry, depth + 1)
            if found:
                return found
    return ""


def leech_link_media(url: str, item: "ParcivItem") -> ParcivMedia:
    """The media of a direct file link: one source, no lookup call needed."""
    target = leech_link_file(url) or str(url)  # A leech link's own file names the quality.
    label = quality_label(item.name, target) or "video"
    return ParcivMedia(title=item.name or file_title(url), poster=item.poster,
                       summary=item.summary, episode=item.episode, langs=item.langs,
                       sources=(ParcivSource(label=label, url=url),),
                       subject_id=item.subject_id)


def merged_media(media: ParcivMedia, sources: Iterable[ParcivSource]) -> ParcivMedia:
    """`media` with every source it does not already list added (by URL).

    The API's own answer for a title and the links its listing handed over are both
    kept: each one is a file, and a quality that is only in one of them would
    otherwise be lost.
    """
    known = {source.url for source in media.sources}
    extra = tuple(source for source in sources if source.url and source.url not in known)
    return replace(media, sources=media.sources + extra) if extra else media


def link_media(urls: Iterable[str], item: "ParcivItem") -> ParcivMedia:
    """The media of one or more direct file links: one source per link, no lookup call."""
    links = tuple(dict.fromkeys(str(url) for url in urls if url))
    if not links:
        raise ParcivError("That link names no downloadable file.")
    rest = tuple(ParcivSource(label=quality_label(leech_link_file(url) or url), url=url)
                 for url in links[1:])
    return merged_media(leech_link_media(links[0], item), rest)


def matches_words(title: object, words: str) -> bool:
    """True when every word of a search query appears in this title.

    Tags, quality and language words are ignored, so "paw patrol" finds
    `PAW Patrol: The Dino Movie (2026)` and "one piece 1130" finds
    `One Piece Episode 1130 [Japanese] [Indonesian Sub]`.
    """
    if not str(words or "").strip():
        return False
    text = str(title or "").lower()
    key = title_key(title)
    for part in str(words or "").split():
        needle = part.strip().lower()
        if not needle:
            continue
        stripped = title_key(part)
        if needle in text or (stripped and stripped in key):
            continue
        return False
    return True


def message_titles(message: object) -> tuple[str, ...]:
    """Every name one DM message carries: the file name it holds and its caption."""
    names: list[str] = []
    document = getattr(message, "document", None)
    if document is not None:
        media = getattr(message, "file", None)
        name = str(getattr(media, "name", "") or "") if media is not None else ""
        if not name:
            for attribute in getattr(document, "attributes", None) or ():
                name = str(getattr(attribute, "file_name", "") or "")
                if name:
                    break
        if name:
            names.append(name)
    caption = str(getattr(message, "message", "") or "")
    if caption:
        names.append(caption)
    return tuple(names)


# ------------------------------------------------------------------------- languages


LANGUAGES = {
    "en": "English", "eng": "English", "hi": "Hindi", "hin": "Hindi", "in": "Indonesian",
    "id": "Indonesian", "ind": "Indonesian", "in_id": "Indonesian", "ms": "Malay",
    "msa": "Malay", "es": "Spanish", "spa": "Spanish", "fr": "French", "fra": "French",
    "pt": "Portuguese", "por": "Portuguese", "ar": "Arabic", "ara": "Arabic", "ja": "Japanese",
    "jpn": "Japanese", "ko": "Korean", "kor": "Korean", "zh": "Chinese", "zho": "Chinese",
    "ta": "Tamil", "te": "Telugu", "bn": "Bengali", "ur": "Urdu", "th": "Thai", "vi": "Vietnamese",
    "tr": "Turkish", "ru": "Russian", "de": "German", "it": "Italian", "nl": "Dutch",
    "pl": "Polish", "fil": "Filipino", "tl": "Filipino",
}
AUDIO_WORDS = {"dual audio": "Dual Audio", "multi audio": "Multi Audio", "original audio": "",
               "dubbed": "Dub", "dub": "Dub", "vo": "", "sub": ""}


def language_name(value: object) -> str:
    """A readable name for a language code or word ("" when it is not a language)."""
    text = " ".join(str(value or "").split()).strip(" .:-_/").lower()
    if not text:
        return ""
    if text in AUDIO_WORDS:
        return AUDIO_WORDS[text]
    if text in LANGUAGES:
        return LANGUAGES[text]
    primary = re.split(r"[-_ ]", text)[0]
    if primary in LANGUAGES:
        return LANGUAGES[primary]
    if text in ("sub", "subs", "subtitle", "subtitles", "cc"):
        return ""
    if re.fullmatch(r"[a-z ]{3,24}", text):
        return " ".join(word[:1].upper() + word[1:] for word in text.split())
    return ""


SUBTITLE_WORD = re.compile(r"\b(?:sub|subs|subtitle|subtitles|srt|cc)\b", re.IGNORECASE)


def language_names(values: object, kind: str = "audio") -> tuple[str, ...]:
    """The language names inside one API value (a string, a list or a dict), in order.

    ``kind`` is ``"audio"`` (a value that says "Sub" is a subtitle, not a track) or
    ``"subtitles"`` (only those say it). ``"all"`` keeps everything.
    """
    found: list[str] = []
    items: Iterable[object] = values if isinstance(values, (list, tuple, set)) else (values,)
    for value in items:
        if isinstance(value, dict):
            candidates = [value.get(key) for key in ("lang", "language", "code", "name", "label")]
        else:
            candidates = [value]
        for candidate in candidates:
            raw = str(candidate or "")
            if kind != "all" and bool(SUBTITLE_WORD.search(raw)) != (kind == "subtitles"):
                continue
            if kind == "subtitles":
                raw = SUBTITLE_WORD.sub(" ", raw)  # "English Sub" is the language English.
            name = language_name(raw)
            if name and name not in found:
                found.append(name)
    return tuple(found)


def subtitle_tag(names: tuple[str, ...]) -> str:
    """One "[English Sub]" style tag: the first subtitle the API listed."""
    return f"[{names[0]} Sub]" if names else ""


# ------------------------------------------------------------------------- readers


def _text(value: object, limit: int = 300) -> str:
    return " ".join(str(value or "").split())[:limit]


def _int(value: object) -> int:
    match = re.search(r"\d+", str(value or ""))
    return int(match.group()) if match else 0


def first(raw: dict, *keys: str) -> object:
    """The first non-empty value among several possible key spellings."""
    for key in keys:
        value = raw.get(key)
        if value not in (None, "", [], {}):
            return value
    return None


def unwrap(payload: object) -> object:
    """The interesting part of an answer: the payload itself or what it wraps."""
    if isinstance(payload, dict):
        for key in ("data", "result", "response", "payload"):
            value = payload.get(key)
            if isinstance(value, (dict, list)) and value:
                return value
    return payload


def dict_list(value: object, depth: int = 0) -> list[dict]:
    """Every dict in the list at this key, one or two wrappers down."""
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    if isinstance(value, dict) and depth < 3:
        for key in ("data", "result", "list", "items", "posts", "episodes", "medias"):
            nested = value.get(key)
            if isinstance(nested, (list, dict)):
                found = dict_list(nested, depth + 1)
                if found:
                    return found
    return []


LIST_KEYS = ("items", "data", "result", "list", "movies", "tv", "tvshows", "anime", "animeList",
             "anime_list", "donghua", "donghuaList", "cartoons", "animation", "kids", "recommend",
             "recommendations", "latest", "videos", "episodes", "posts", "medias", "results",
             "search", "trending", "slider", "popular")


def find_dicts(payload: object, keys: tuple[str, ...] = LIST_KEYS) -> list[dict]:
    """The first list of dicts this answer holds under one of the known keys."""
    data = unwrap(payload)
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    if not isinstance(data, dict):
        return []
    for key in keys:
        found = dict_list(data.get(key))
        if found:
            return found
    for value in data.values():  # Last resort: the one list in the answer.
        found = dict_list(value)
        if found:
            return found
    return []


def url_value(raw: dict, *keys: str) -> str:
    """The first public HTTPS URL among these keys (covers, subtitles, API links)."""
    for key in keys:
        value = _text(raw.get(key), 2048)
        if public_https_url(value):
            return value
    return ""


def public_media_url(value: object) -> bool:
    """True for a URL the bot may copy media from.

    apitera's CDNs hand out plain ``http://`` sources on their own ports (the
    ``santol.whatbox.ca:29318`` kind), so media URLs are held to a weaker rule than
    the API endpoints: any public host, http or https, but never a local or private
    address and never with credentials in it.
    """
    if not isinstance(value, str) or len(value) > 2048:
        return False
    try:
        parsed = urlparse(value)
        host = parsed.hostname
    except ValueError:
        return False
    if parsed.scheme not in ("http", "https") or not host or parsed.username or parsed.password:
        return False
    low = host.lower()
    if low == "localhost" or low.endswith((".localhost", ".local")):
        return False
    try:
        return ipaddress.ip_address(host).is_global
    except ValueError:
        return "." in host and all(host.split("."))


def media_url(raw: dict, *keys: str) -> str:
    """The first media URL among these keys (see `public_media_url`)."""
    for key in keys:
        value = _text(raw.get(key), 2048)
        if public_media_url(value):
            return value
    return ""


def quality_label(*values: object) -> str:
    """One "1080p" style label from whatever the API called the resolution.

    A URL is read by its own file name: `…/2026/Donghua/KuangWang2/06-360p.mp4` is
    "360p", not "2026p" from the folder or the port next to it.
    """
    named: list[tuple[str, bool]] = []
    for value in values:
        text = _text(value, 300)
        was_url = "://" in text
        if was_url:
            text = urlsplit(text).path.rsplit("/", 1)[-1]
        named.append((text, was_url))
    for text, _ in named:
        match = re.search(r"(\d{3,4})\s*[pP]\b", text) or re.search(r"\b(\d{3,4})\b", text)
        if match and 120 <= int(match.group(1)) <= 4320:
            return f"{int(match.group(1))}p"
    for text, was_url in named:  # No resolution: keep the API's own word (DASH, HLS, …).
        text = _text(text, 24)
        if text and not was_url:
            return text.upper() if text.lower() in ("dash", "hls") else text
    return "Video"


def size_of(raw: dict) -> int:
    value = first(raw, "size", "bytes", "fileSize", "file_size", "sizeBytes", "size_bytes")
    try:
        number = int(float(value))
    except (TypeError, ValueError):
        return 0
    return number if 0 < number < 10 * 1024**3 else 0


def cookie_headers(raw: dict) -> tuple[tuple[str, str], ...]:
    """The CloudFront cookie the DASH/MP4 links need, as an HTTP header list."""
    cookie = _text(first(raw, "signCookie", "sign_cookie", "cookie", "signCookies"), 4000)
    headers: list[tuple[str, str]] = []
    if cookie:
        headers.append(("Cookie", cookie))
    policy = _text(first(raw, "cookiePolicy", "policy", "signPolicy"), 200)
    if policy and not cookie:
        headers.append(("Cookie", policy))
    extra = raw.get("headers")
    if isinstance(extra, dict):
        headers += [(str(key), str(value)) for key, value in extra.items() if value]
    return tuple(headers)


def episode_label(raw: dict) -> str:
    """A "S1 E2" style label when the API numbered the episode."""
    season = _int(first(raw, "se", "season", "season_number", "seasonNum"))
    episode = _int(first(raw, "ep", "episode", "episode_number", "epNum", "episodeNumber"))
    if season > 1 and episode:
        return f"S{season} E{episode}"
    if episode:
        return f"Episode {episode}"
    return ""


@dataclass(frozen=True)
class ParcivSource:
    """One downloadable file the API listed for a subject."""

    label: str  # "1080p", "360p", "DASH"…
    url: str
    size: int = 0
    duration: int = 0
    codec: str = ""
    headers: tuple[tuple[str, str], ...] = ()
    dash: bool = False
    langs: tuple[str, ...] = ()

    @property
    def height(self) -> int:
        match = re.search(r"(\d{3,4})\s*p", self.label)
        return int(match.group(1)) if match else 0

    @property
    def width(self) -> int:
        height = self.height
        return int(height * 16 / 9) if height else 0

    @property
    def header_dict(self) -> dict[str, str]:
        return dict(self.headers)


@dataclass(frozen=True)
class ParcivItem:
    """One catalogue entry: a film, a series or an episode."""

    subject_id: str
    name: str
    section: str = "movies"
    year: str = ""
    poster: str = ""
    episode: str = ""
    summary: str = ""
    langs: tuple[str, ...] = ()
    post_url: str = ""
    extra: dict = None  # type: ignore[assignment]  # Raw ids for the post lookup.

    def __post_init__(self) -> None:
        if self.extra is None:
            object.__setattr__(self, "extra", {})

    @property
    def title(self) -> str:
        """The name the API gave, with its year, exactly as it should be uploaded."""
        text = self.name or f"subject {self.subject_id}"
        if self.year and not re.search(rf"\b{re.escape(self.year)}\b", text):
            text = f"{text} ({self.year})"
        return text

    @property
    def kind(self) -> str:
        return SECTION_LABELS.get(self.section, "apitera")


@dataclass(frozen=True)
class ParcivMedia:
    """Everything one upload needs: the title, the cover and every quality."""

    title: str
    poster: str = ""
    summary: str = ""
    duration: int = 0
    episode: str = ""
    langs: tuple[str, ...] = ()
    subtitles: tuple[str, ...] = ()
    sources: tuple[ParcivSource, ...] = ()
    subject_id: str = ""

    def name_tags(self, source: ParcivSource | None = None) -> str:
        """The tags the user asked for: audio language(s), then the subtitle language."""
        langs = list(self.langs)
        if source is not None:
            langs += [name for name in source.langs if name not in langs]
        parts = [f"[{name}]" for name in langs[:2]]
        sub = subtitle_tag(self.subtitles)
        if sub:
            parts.append(sub)
        return " ".join(parts)

    def display_title(self, source: ParcivSource | None = None) -> str:
        """The upload title: the catalogue name plus its language tags."""
        tags = self.name_tags(source)
        return f"{self.title} {tags}".strip()

    def labelled_sources(self) -> tuple[ParcivSource, ...]:
        """Sources with a unique label each, so the buttons and names stay unambiguous."""
        seen: dict[str, int] = {}
        labelled: list[ParcivSource] = []
        for source in self.sources:
            count = seen.get(source.label, 0) + 1
            seen[source.label] = count
            label = source.label if count == 1 else f"{source.label} ({count})"
            labelled.append(source if label == source.label else
                            ParcivSource(label, source.url, source.size, source.duration,
                                         source.codec, source.headers, source.dash, source.langs))
        return tuple(labelled)

    def source(self, label: str) -> ParcivSource | None:
        return next((item for item in self.labelled_sources() if item.label == label), None)


def source_from_dict(raw: dict, dash: bool = False,
                     strict: bool = False) -> ParcivSource | None:
    """Read one media entry of a ``streams``/``resources``/``videos`` list.

    In ``strict`` mode the URL must be a media file or a leech link, so scanning an
    unknown answer cannot mistake a page or a catalogue link for the video.
    """
    url = media_url(raw, "url", "link", "src", "file", "stream_url", "streamUrl", "download_url")
    if not url:
        return None
    if strict and not is_downloadable_url(url):
        return None
    named = leech_link_file(url) or url
    label = quality_label(first(raw, "resolution", "label", "quality", "height", "name"),
                          first(raw, "format", "type", "linkType"), named)
    if dash and label.endswith("p"):
        label = f"{label} DASH"  # The same height can exist as both an MP4 and a DASH stream.
    duration = _int(first(raw, "duration", "seconds", "length"))
    return ParcivSource(label=label, url=url, size=size_of(raw), duration=duration,
                        codec=_text(first(raw, "codec", "vcodec"), 20),
                        headers=cookie_headers(raw),
                        dash=dash or url.split("?")[0].endswith((".mpd", ".mpd/")) or
                        _text(first(raw, "format", "type")).lower() == "dash",
                        langs=media_langs(raw))


SOURCE_KEYS = ("resources", "streams", "downloads", "videos", "servers", "sources", "qualities",
               "links", "medias", "playlist")


def media_sources(payload: dict) -> tuple[ParcivSource, ...]:
    """Every source this answer lists, plain MP4s first and DASH entries after them."""
    direct: list[ParcivSource] = []
    dashwise: list[ParcivSource] = []
    for key in SOURCE_KEYS:
        for raw in dict_list(payload.get(key)):
            dash = key == "streams" or _text(first(raw, "format", "type")).lower() == "dash"
            source = source_from_dict(raw, dash=dash)
            if source is None:
                continue
            (dashwise if source.dash else direct).append(source)
    merged = direct + dashwise
    unique: list[ParcivSource] = []
    for source in merged:
        if source.url not in {item.url for item in unique}:
            unique.append(source)
    return tuple(unique)


def media_sources_anywhere(payload: object, depth: int = 0) -> tuple[ParcivSource, ...]:
    """Every file this answer holds, whatever key or nesting the API used.

    The Anime/Donghua answers are not consistent between builds: one lists its files
    under ``videos``, another as a flat list of qualities, another as one file per
    episode. When `media_sources` (the known keys) finds nothing, the whole answer is
    walked instead — and a file link the API's own leech route carries counts as a
    file, because fetching it hands back the video.
    """
    if depth > 5:
        return ()
    found: list[ParcivSource] = []
    if isinstance(payload, dict):
        entry = source_from_dict(payload, strict=True)
        if entry is not None:
            found.append(entry)
        else:
            for value in payload.values():
                found.extend(media_sources_anywhere(value, depth + 1))
    elif isinstance(payload, list):
        for value in payload[:40]:
            found.extend(media_sources_anywhere(value, depth + 1))
    elif is_downloadable_url(payload):
        found.append(ParcivSource(label=quality_label(leech_link_file(payload) or payload),
                                  url=str(payload)))
    unique: list[ParcivSource] = []
    for source in found:
        if source.url not in {item.url for item in unique}:
            unique.append(source)
    return tuple(unique)


def catalogue_entries(payload: object) -> list[dict]:
    """The entries of an answer that is a whole catalogue, not one episode.

    The `/videos` family answers every title at once. Reading such an answer as one
    episode would download the other titles' files, so it is only a catalogue when
    its entries carry their own titles.
    """
    entries = find_dicts(payload)
    if len(entries) >= 2 and sum(1 for entry in entries if media_title(entry)) >= 2:
        return entries
    return []


def catalogue_entry(payload: object, item: "ParcivItem | None") -> dict | None:
    """This item's own entry when an answer is a whole catalogue (a plain /videos list)."""
    if item is None:
        return None
    handles = {item.subject_id, str(item.extra.get("slug", "")), str(item.extra.get("id", "")),
               str(item.extra.get("postId", ""))}
    handles.discard("")
    for entry in find_dicts(payload):
        if handles & {str(entry.get(key, "")) for key in
                      ("id", "slug", "subjectId", "postId", "episodeId", "animeId", "donghuaId")}:
            return entry
        name = media_title(entry)
        if name and item.name and titles_match(name, item.name):
            return entry
    return None


def media_subtitles(payload: dict) -> tuple[str, ...]:
    """The subtitle languages the answer lists, from its subtitle list or its fields."""
    found: list[str] = []
    for key in ("subtitles", "subtitle", "subs", "subsList"):
        for name in language_names(payload.get(key), "all"):
            if name not in found:
                found.append(name)
    for key in LANGUAGE_KEYS:
        for name in language_names(payload.get(key), "subtitles"):
            if name not in found:
                found.append(name)
    return tuple(found)


LANGUAGE_KEYS = ("langs", "languages", "language", "audio", "audio_lang", "audioLangs", "dub")


def media_langs(payload: dict) -> tuple[str, ...]:
    """The audio languages the answer lists ("Indonesian Sub" is not one of them)."""
    found: list[str] = []
    for key in LANGUAGE_KEYS:
        for name in language_names(payload.get(key), "audio"):
            if name not in found:
                found.append(name)
    return tuple(found)


def media_title(payload: dict, fallback: str = "") -> str:
    """The API's own title, with its "_360P" style suffix taken off."""
    raw = _text(first(payload, "title", "name", "subjectName", "anime_title", "postTitle"), 200)
    if not raw:
        return fallback
    cleaned = re.sub(r"[_ ]\d{3,4}[pP]$", "", raw).strip(" _-")
    return cleaned or fallback


def parse_stream(payload: object, subject_id: str = "") -> tuple[ParcivMedia, ...]:
    """The media entries a ``/moviebox/stream`` answer holds (one per episode or cut)."""
    data = unwrap(payload)
    entries: list[dict] = []
    if isinstance(data, list):
        entries = [item for item in data if isinstance(item, dict)]
    elif isinstance(data, dict):
        nested = [raw for key in ("episodes", "items", "medias", "streams", "results")
                  for raw in dict_list(data.get(key))]
        with_media = [raw for raw in nested if media_sources(raw)]
        if with_media:
            entries = with_media
        else:
            entries = [data]
    media: list[ParcivMedia] = []
    for raw in entries:
        sources = media_sources(raw)
        if not sources:
            continue
        item_id = _text(first(raw, "subjectId", "subject_id", "id"), 40) or subject_id
        duration = _int(first(raw, "duration", "seconds")) or max(
            (source.duration for source in sources), default=0)
        media.append(ParcivMedia(title=media_title(raw), poster=url_value(raw, "poster", "image"),
                                 summary="", duration=duration,
                                 episode=episode_label(raw), langs=media_langs(raw),
                                 subtitles=media_subtitles(raw), sources=sources,
                                 subject_id=item_id))
    if not media:
        raise ParcivError("The apitera stream answer listed no downloadable files.")
    return tuple(media)


def parse_post(payload: object, item: "ParcivItem | None" = None,
               _depth: int = 0) -> ParcivMedia:
    """One Anime/Donghua episode: its title, cover, media list and subtitles.

    When the answer is really a whole catalogue (`/videos` answers every title at
    once), only this item's own entry is read — never every title's files.
    """
    if item is not None and _depth < 2:
        entries = catalogue_entries(payload)
        if entries:
            entry = catalogue_entry(payload, item)
            if entry is None:
                raise ParcivError(f"the answer is a catalogue of {len(entries)} titles and "
                                  f"lists no entry for {item.title}")
            return parse_post(entry, item, _depth + 1)
    data = unwrap(payload)
    raw = data
    if isinstance(data, list):
        # A list is a catalogue (a `/videos` family answer): take this item's own entry.
        entries = [entry for entry in data if isinstance(entry, dict)]
        entry = catalogue_entry(payload, item)
        if entry is None:
            # The answer is a catalogue and this title is not in it. Falling through
            # to `entries[0]` would pair the listing's name with someone else's files.
            names = [media_title(candidate) for candidate in entries if media_title(candidate)]
            hint = f" (the API returned: {', '.join(names[:3])})" if names else ""
            raise ParcivError(f"the answer is a list of {len(entries)} entries and lists "
                              f"no entry for {item.title}{hint}")
        raw = entry
    if not isinstance(raw, dict):
        raise ParcivError("The apitera answer for this episode was not usable.")
    sources = media_sources(raw) or media_sources_anywhere(raw)
    if not sources:
        raise ParcivError("The apitera answer for this episode listed no downloadable files.")
    duration = _int(first(raw, "duration", "seconds", "total_duration")) or max(
        (source.duration for source in sources), default=0)
    title = media_title(raw, item.title if item else "")
    # When a lookup shape answers with someone else's episode, refuse so the ladder
    # keeps looking — the listing's name and the file the API hands over must agree.
    if item is not None and title and item.title and not titles_match(title, item.name):
        raise ParcivError(f"the answer is for {title!r}, not {item.title!r}")
    # The same data-mismatch case can also live in the file path: the API says it is
    # "Kuang Wang 2" but the URL points at "…/Naruto/01-720p.mp4". The folder a CDN
    # URL lives in is the title that file really is; refuse when the two disagree.
    if item is not None and item.title:
        for source in sources:
            other = url_title_token(source.url)
            if other and not url_token_matches_title(other, item.title):
                raise ParcivError(f"the answer's file names {other!r}, not {item.title!r}")
    return ParcivMedia(
        title=title,
        poster=url_value(raw, "poster", "image", "thumbnail", "cover") or (item.poster if item else ""),
        summary=_text(first(raw, "desc", "description", "synopsis", "overview", "summary"), 900)
        or (item.summary if item else ""),
        duration=duration,
        episode=episode_label(raw) or (item.episode if item else ""),
        langs=media_langs(raw),
        subtitles=media_subtitles(raw),
        sources=sources,
        subject_id=(item.subject_id if item else "") or _text(first(raw, "subjectId", "id"), 40),
    )


def parse_catalogue(payload: object, section: str, api: "ParcivApi | None" = None) -> tuple[ParcivItem, ...]:
    """The entries of one catalogue page (movies, tv, tvshows, animation, kids, anime…)."""
    items: list[ParcivItem] = []
    for raw in find_dicts(payload):
        item = item_from_dict(raw, section, api)
        if item is not None:
            items.append(item)
    return tuple(items)


def item_from_dict(raw: dict, section: str, api: "ParcivApi | None" = None) -> ParcivItem | None:
    """One catalogue entry, read tolerantly from whichever keys this answer uses."""
    name = media_title(raw)  # Cleans the API's "_360P" style suffixes as well.
    if not name:
        return None
    slug = _text(first(raw, "slug", "post_slug", "postSlug", "permalink"), 120)
    # The file itself, when this answer hands it over (any key name, a few levels deep).
    own_media = (media_url(raw, "media", "mediaUrl", "play_url", "playUrl", "video_url",
                           "videoUrl")
                 or find_media_url(raw))
    if own_media and not is_downloadable_url(own_media):
        own_media = ""  # An API page link is a lookup call, not the file.
    post_url = url_value(raw, "url", "link", "endpoint", "href")
    if post_url and not post_url.startswith(api.root if api else "https://"):
        post_url = ""
    if post_url and is_downloadable_url(post_url):
        # The link the entry carries is the file itself, not a page to look up.
        own_media = own_media or post_url
        post_url = ""
    if not post_url:
        path = _text(first(raw, "path", "endpoint_path", "permalink_path"), 200)
        if path.startswith("/") and api is not None:
            post_url = f"{api.root}{path}"
        if not post_url and slug and api is not None and section in PARCIV_STREAM_SECTIONS:
            post_url = f"{api.root}/{section}/post?slug={quote(slug)}"
    subject_id = _text(first(raw, "subjectId", "subject_id", "id", "postId", "animeId", "donghuaId",
                             "slugId"), 40)
    if not subject_id:
        # Anime/Donghua lists often carry only a slug, which their /post call needs.
        subject_id = slug
    if not subject_id and not (own_media or post_url):
        return None  # Nothing to look up and no file: this entry cannot be used.
    # An entry whose only handle is a link (a /videos row: title + file) keeps an empty
    # subject id on purpose — a URL is not an id, and asking with it would be a wasted
    # call. Such an entry is downloaded from its own link instead.
    year = _text(first(raw, "year", "release_year", "releaseYear", "released"), 8)
    # Every file this entry carries: a listing that gives several qualities is kept whole
    # instead of downloading whichever link happened to be first.
    files = tuple(source for source in (media_sources(raw) or media_sources_anywhere(raw))
                  if is_downloadable_url(source.url))
    if not files and own_media:
        files = (ParcivSource(label=quality_label(leech_link_file(own_media) or own_media),
                              url=own_media),)
    extra = {key: _text(raw.get(key), 60) for key in
             ("postId", "post_id", "episodeId", "id", "subjectId") if raw.get(key) not in (None, "")}
    if slug:
        extra["slug"] = slug  # The /post call of these families often wants the slug.
    if post_url:
        extra["url"] = post_url  # Some builds answer /post?url=<their own link>.
    if own_media:
        extra["media_url"] = own_media  # The listing handed the file itself over.
    if files:
        extra["media_sources"] = files  # …and these are all its files, one source each.
    extra["raw"] = raw  # Kept whole: many listings carry the media list inline.
    return ParcivItem(
        subject_id=subject_id,
        name=name,
        section=section,
        year=year if re.fullmatch(r"(?:19|20)\d{2}", year or "") else "",
        poster=url_value(raw, "poster", "image", "thumbnail", "cover", "posterUrl",
                         "coverUrl", "cover_url", "imageUrl", "thumbnailUrl", "img"),
        episode=episode_label(raw),
        summary=_text(first(raw, "desc", "description", "synopsis", "overview", "summary"), 900),
        langs=media_langs(raw),
        post_url=post_url,
        extra=extra,
    )


def parse_detail(payload: object, item: ParcivItem) -> ParcivItem:
    """What the detail call adds to a catalogue entry: its own name, cover and summary.

    A detail answer that turns out to be a list of episodes changes nothing: the
    catalogue already gave the title, and the stream call gives the episode.
    """
    data = unwrap(payload)
    raw = data if isinstance(data, dict) else {}
    name = media_title(raw)
    poster = url_value(raw, "poster", "image", "thumbnail", "cover", "posterUrl")
    year = _text(first(raw, "year", "release_year", "releaseYear", "released"), 8)
    return replace(
        item,
        name=name or item.name,
        poster=poster or item.poster,
        year=year if re.fullmatch(r"(?:19|20)\d{2}", year or "") else item.year,
        summary=_text(first(raw, "desc", "description", "synopsis", "overview", "summary"), 900)
        or item.summary,
        episode=episode_label(raw) or item.episode,
        langs=media_langs(raw) or item.langs,
        subject_id=_text(first(raw, "subjectId", "subject_id", "id"), 40) or item.subject_id,
    )


def pick_media(media: tuple[ParcivMedia, ...], item: ParcivItem) -> ParcivMedia:
    """Which entry of a stream answer belongs to this catalogue item."""
    wanted = _int(re.sub(r"\D+", " ", item.episode).strip().split(" ")[-1]) if item.episode else 0
    if wanted:
        for entry in media:
            if _int(re.sub(r"\D+", " ", entry.episode).strip().split(" ")[-1]) == wanted:
                return entry
    return media[0]


# ----------------------------------------------------------------- the downloader


def ffmpeg_ready() -> bool:
    from shutil import which
    return all(which(tool) for tool in ("ffmpeg", "ffprobe"))


LEECH_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36")
LEECH_CHUNK = 256 * 1024
ARIA2_POLL = 0.25  # Seconds between the aria2c progress readings.
ARIA2_CONNECTIONS = 4  # Parallel connections per source; these CDNs allow a few.
LEECH_SUFFIXES = (".mp4", ".mkv", ".webm", ".ts", ".m4v", ".mov", ".avi", ".flv", ".mp3", ".m4a")
# Content types that mean "this is a page or a manifest, not the video".
REFUSED_TYPES = ("text/", "application/json", "application/xml", "+xml", "mpegurl")


def unwrap_reason(text: str, label: str) -> str:
    """Drop one nested "<label> could not be downloaded (…)" wrapper from a reason."""
    prefix = f"{label} "
    if not text.startswith(prefix):
        return text
    inner = text[len(prefix):]
    body = inner[:-1] if inner.endswith(".") else inner
    head, _, tail = body.partition("(")
    if not tail.endswith(")"):
        return text
    if head.strip() == "could not be downloaded":
        return tail[:-1].strip()
    if head.strip() == "did not come back as a video":
        return f"not a video ({tail[:-1].strip()})"
    return text


def brief(exc: object, limit: int = 70) -> str:
    """One short line for an error, so several routes fit in one message."""
    text = " ".join(str(exc).split()) or type(exc).__name__
    return text if len(text) <= limit else text[: limit - 1].rstrip() + "…"


def aria2_path() -> str:
    """The aria2c binary this host has, if any (`ARIA2_PATH` overrides the lookup)."""
    return os.getenv("ARIA2_PATH") or shutil.which("aria2c") or ""


def aria2_parciv(url: str, out_dir: str, on_bytes, stop: threading.Event,
                 proxy: str | None = None, headers: Iterable[tuple[str, str]] = (),
                 label: str = "", duration: int = 0, timeout: int = 60) -> str:
    """Copy one progressive source with aria2c: several connections, resumes, no ffmpeg.

    aria2c is the most reliable plain downloader for these CDNs, and it is the
    route to reach for when yt-dlp gets in the way (post-processing, extractor
    refusals). The file size is polled while it runs so the job's progress bar
    keeps moving, `stop` terminates the process, and the result is checked like
    every other route. Raises `ParcivError` when this host has no aria2c, so the
    caller can fall back to the built-in copy.
    """
    aria2 = aria2_path()
    if not aria2:
        raise ParcivError("aria2c is not installed on this host")
    target = os.path.join(out_dir, "stream.mp4")
    args = [aria2, "--dir", out_dir, "--out", os.path.basename(target),
            "--allow-overwrite=true", "--auto-file-renaming=false", "--continue=true",
            "--file-allocation=none", "--console-log-level=warn", "--summary-interval=0",
            f"--max-connection-per-server={ARIA2_CONNECTIONS}", f"--split={ARIA2_CONNECTIONS}",
            "--min-split-size=1M", "--max-tries=3", "--retry-wait=2",
            f"--connect-timeout={min(60, timeout)}", f"--timeout={timeout}"]
    if proxy:
        args.append(f"--all-proxy={proxy}")
    for name, value in headers:
        args.append(f"--header={name}: {value}")
    args.append(url)
    process = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    try:
        while process.poll() is None:
            if stop.is_set():
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                raise DownloadCancelled("Cancelled")
            if os.path.exists(target):
                on_bytes(os.path.getsize(target), 0)
            time.sleep(ARIA2_POLL)
        output = (process.stdout.read() if process.stdout else "") or ""
        if process.returncode:
            reason = next((line.strip() for line in reversed(output.splitlines())
                           if line.strip()), "")
            raise ParcivError(f"aria2c exited {process.returncode}"
                              + (f": {brief(reason)}" if reason else ""))
        problem = media_problem(target, duration)
        if problem:
            raise ParcivError(f"did not come back as a video ({problem})")
        return target
    except BaseException:
        if process.poll() is None:
            process.kill()
        for leftover in (target, target + ".aria2"):
            try:
                os.remove(leftover)
            except OSError:
                pass
        raise


JSON_LINK_KEYS = ("url", "link", "stream", "streamUrl", "stream_url", "download",
                  "downloadUrl", "download_url", "file", "src", "location", "video")
LEECH_HOPS = 2  # How many link answers one copy follows before it gives up.


def json_media_link(payload: object, depth: int = 0) -> str:
    """The first media URL an API's JSON answer names (leech routes often answer so)."""
    if depth > 4:
        return ""
    if isinstance(payload, dict):
        for key in JSON_LINK_KEYS:
            value = payload.get(key)
            if isinstance(value, str) and public_media_url(value):
                return value
        for value in payload.values():
            found = json_media_link(value, depth + 1)
            if found:
                return found
    elif isinstance(payload, list):
        for entry in payload[:10]:
            found = json_media_link(entry, depth + 1)
            if found:
                return found
    return ""


def leech_parciv(url: str, out_dir: str, on_bytes, stop: threading.Event,
                 proxy: str | None = None, headers: Iterable[tuple[str, str]] = (),
                 label: str = "", duration: int = 0, timeout: int = 60,
                 hop: int = 0) -> str:
    """Copy one progressive source with a single HTTP GET: no yt-dlp, no ffmpeg.

    The apitera CDNs list plain MP4s, and a plain copy cannot trip over a
    post-processor. An answer that is a link rather than the bytes (leech routes
    often answer ``{"url": …}``) is followed, exactly as a leech bot does. Answers
    that are neither (an HTML page, a playlist manifest) are refused here, so the
    caller can try the next route, and a partial file is removed so the next route
    starts clean.
    """
    suffix = os.path.splitext(urlsplit(url).path)[1].lower()
    if suffix not in LEECH_SUFFIXES:
        suffix = ".mp4"
    path = os.path.join(out_dir, f"stream{suffix}")
    handlers = []
    if proxy:
        handlers.append(urllib.request.ProxyHandler({"http": proxy, "https": proxy}))
    opener = urllib.request.build_opener(*handlers)
    request = urllib.request.Request(url, headers={"User-Agent": LEECH_UA, **dict(headers)})
    try:
        try:
            response = opener.open(request, timeout=timeout)
        except urllib.error.HTTPError as exc:
            raise ParcivError(f"HTTP {exc.code} {exc.reason}".strip()) from exc
        except (urllib.error.URLError, OSError, ValueError) as exc:
            raise ParcivError(brief(exc)) from exc
        with response:
            kind = str(response.headers.get("Content-Type") or "").split(";")[0].strip().lower()
            if "json" in kind:
                body = response.read(1024 * 1024)
                try:
                    payload = json.loads(body)
                except ValueError:
                    raise ParcivError("the answer is not usable JSON") from None
                link = json_media_link(payload)
                if link and hop < LEECH_HOPS:
                    LOG.info("The answer is a link; following it (%s)", brief(url))
                    return leech_parciv(link, out_dir, on_bytes, stop, proxy=proxy,
                                        headers=headers, label=label, duration=duration,
                                        timeout=timeout, hop=hop + 1)
                raise ParcivError("the answer is a JSON message, not a video")
            if kind and any(bad in kind for bad in REFUSED_TYPES):
                raise ParcivError(f"the answer is {kind}, not a video")
            try:
                total = int(response.headers.get("Content-Length") or 0)
            except ValueError:
                total = 0
            done = 0
            with open(path, "wb") as handle:
                while True:
                    if stop.is_set():
                        raise DownloadCancelled("Cancelled")
                    chunk = response.read(LEECH_CHUNK)
                    if not chunk:
                        break
                    handle.write(chunk)
                    done += len(chunk)
                    on_bytes(done, total)
        if total and done < total:
            raise ParcivError(f"the connection stopped after {done} of {total} bytes")
        problem = media_problem(path, duration)
        if problem:
            raise ParcivError(f"did not come back as a video ({problem})")
        return path
    except BaseException:
        try:
            os.remove(path)
        except OSError:
            pass
        raise


def download_with_ytdlp(url: str, out_dir: str, on_bytes, stop: threading.Event,
                        proxy: str | None = None, headers: Iterable[tuple[str, str]] = (),
                        label: str = "", duration: int = 0, title: str = "") -> str:
    """Blocking yt-dlp download of one apitera stream; returns the media path.

    ``on_bytes(done, total)`` is called from the worker thread and setting ``stop``
    aborts at the next progress update. The signed CloudFront cookie that DASH and some
    MP4 links need is passed as a request header. The file is checked before it is
    handed back, so a refused download can never be uploaded as a video.
    """
    if str(url).split("?")[0].endswith(".mpd") and not ffmpeg_ready():
        raise ParcivError("A DASH stream needs ffmpeg and ffprobe on this host "
                          "(the Koyeb Docker image includes them).")

    def hook(status: dict) -> None:
        if stop.is_set():
            raise DownloadCancelled("Cancelled")
        if status.get("status") == "downloading":
            on_bytes(int(status.get("downloaded_bytes") or 0),
                     int(status.get("total_bytes") or status.get("total_bytes_estimate") or 0))

    options = {
        "outtmpl": os.path.join(out_dir, "stream.%(ext)s"),
        "merge_output_format": "mp4",
        "quiet": True,
        "noprogress": True,
        "no_warnings": True,
        "noplaylist": True,
        "retries": 3,
        "fragment_retries": 3,
        "socket_timeout": 30,
        "concurrent_fragment_downloads": 1,
        "progress_hooks": [hook],
    }
    if proxy:
        options["proxy"] = proxy
    header_dict = dict(headers)
    if header_dict:
        options["http_headers"] = header_dict
    try:
        with yt_dlp.YoutubeDL(options) as ydl:
            ydl.download([url])
    except DownloadCancelled:
        raise
    except Exception as exc:
        raise ParcivError(f"{label or 'This quality'} could not be downloaded ({short_reason(exc)}).")

    paths = [os.path.join(out_dir, name) for name in os.listdir(out_dir)
             if name.startswith("stream.") and not name.endswith((".part", ".ytdl"))]
    path = max(paths, key=os.path.getsize) if paths else ""
    problem = media_problem(path, duration)
    if problem:
        raise ParcivError(f"{label or 'This quality'} did not come back as a video ({problem}).")
    return path


def download_parciv(url: str, out_dir: str, on_bytes, stop: threading.Event,
                    proxy: str | None = None, headers: Iterable[tuple[str, str]] = (),
                    label: str = "", duration: int = 0, title: str = "",
                    api: "ParcivApi | None" = None, section: str = "",
                    dash: bool = False) -> str:
    """Download one apitera source, trying every route that reaches it.

    1. **A direct copy** of the source URL: **aria2c** when the host has it (parallel
       connections and resumes), otherwise the built-in single GET. These CDNs list
       progressive MP4s, so this needs neither yt-dlp nor ffmpeg and cannot fail in a
       post-processor (some hosts have no ffmpeg at all).
    2. **yt-dlp**, for manifests (DASH/HLS) and the odd source a copy cannot take.
    3. **The API's own leech** (``…/<family>/stream?url=``): the worker fetches the
       source itself — the route to use when yt-dlp fails and the CDN refuses the bot.

    Every route's reason is kept, so the user reads why the file could not be fetched
    instead of one bare library name.
    """
    label = label or "This quality"
    plain = str(url).split("?")[0].lower()
    manifest = dash or plain.endswith((".mpd", ".m3u8", ".mpd/"))
    reasons: list[str] = []
    if not manifest:
        attempts = [("aria2c", aria2_parciv)] if aria2_path() else []
        attempts.append(("direct copy", leech_parciv))
        for name, fetcher in attempts:
            try:
                return fetcher(url, out_dir, on_bytes, stop, proxy=proxy, headers=headers,
                               label=label, duration=duration)
            except DownloadCancelled:
                raise
            except ParcivError as exc:
                reasons.append(f"{name}: {brief(exc)}")
    try:
        return download_with_ytdlp(url, out_dir, on_bytes, stop, proxy=proxy, headers=headers,
                                   label=label, duration=duration, title=title)
    except DownloadCancelled:
        raise
    except ParcivError as exc:
        reasons.append(f"yt-dlp: {brief(unwrap_reason(str(exc), label))}")
    if api is not None and section and not manifest:
        try:
            return leech_parciv(leech_url(api, section, url), out_dir, on_bytes, stop,
                                proxy=proxy, headers=headers, label=label, duration=duration)
        except DownloadCancelled:
            raise
        except ParcivError as exc:
            reasons.append(f"via the API: {brief(exc)}")
    raise ParcivError(f"{label} could not be downloaded ({'; '.join(reasons)}).")
