"""apitera automation: the `/auto` watcher, the `/latest` panel and the DM check.

What one upload does (manual or automatic):

1. Read the catalogue - page 1 of the Moviebox sections (movies, tv, tvshows,
   animation, kids, recommend) and/or the Anime and Donghua "latest" endpoints.
2. Skip every title the DM already holds. Matching ignores the tags, quality,
   year and language words, so the catalogue's "PAW Patrol: The Dino Movie"
   matches the file already in the chat named
   "PAW Patrol_ The Dino Movie [Hindi] [Eng Sub] [360p].mp4".
3. For every remaining item, read its detail (the canonical name) and its stream
   answer, then upload **every quality** it offers with the name the catalogue
   gave, its audio language tags and its subtitle language.

`/auto on` starts the watcher (every ``PARCIV_AUTO_INTERVAL`` seconds, default 15
minutes, up to ``limit`` items per section), `/auto off` stops it, `/auto now`
checks once, and `/latest` shows the same check's result with buttons so a manual
run can be started for the new titles only or for everything listed.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field

from .parciv import (AUTO_SECTIONS, DEFAULT_AUTO_LIMIT, MAX_AUTO_LIMIT, PARCIV_API_URL,
                     PARCIV_SECTIONS, PARCIV_STREAM_SECTIONS, SECTION_LABELS, ParcivError,
                     ParcivItem, message_titles, parse_sections, titles_match, title_variants)

AUTO_INTERVAL_DEFAULT = 900  # Seconds between automatic checks.
AUTO_INTERVAL_MIN = 60
AUTO_INTERVAL_MAX = 24 * 3600
PAGE_DEFAULT = 1
LIST_ITEMS = 12  # How many catalogue entries one /latest reply shows.


def parse_interval(value: str) -> int:
    """Read a `/auto every <n>` value (seconds, or ``15m`` / ``2h``)."""
    text = str(value or "").strip().lower()
    match = re.fullmatch(r"(\d{1,6})\s*(s|m|h)?", text)
    if not match:
        raise ValueError("Give the interval in seconds, or as 15m / 2h.")
    number = int(match.group(1))
    unit = match.group(2) or "s"
    seconds = number * {"s": 1, "m": 60, "h": 3600}[unit]
    return max(AUTO_INTERVAL_MIN, min(AUTO_INTERVAL_MAX, seconds))


def parse_limit(value: str) -> int:
    """Read a `/auto limit <n>` value (1 - MAX_AUTO_LIMIT items per section)."""
    text = str(value or "").strip()
    if not re.fullmatch(r"\d{1,3}", text):
        raise ValueError(f"Give a number of items per section (1-{MAX_AUTO_LIMIT}).")
    return max(1, min(MAX_AUTO_LIMIT, int(text)))


def interval_text(seconds: int) -> str:
    if seconds % 3600 == 0:
        return f"{seconds // 3600}h"
    if seconds % 60 == 0:
        return f"{seconds // 60}m"
    return f"{seconds}s"


@dataclass(frozen=True)
class CataloguePage:
    """One catalogue page: its section, the page number and the entries it listed."""

    section: str
    page: int
    items: tuple[ParcivItem, ...] = ()
    url: str = ""

    @property
    def label(self) -> str:
        return SECTION_LABELS.get(self.section, self.section.title())


@dataclass(frozen=True)
class ParcivBatch:
    """What one apitera job uploads: the items, the qualities and the DM check.

    ``qualities`` empty means every quality each item offers, which is what the
    automatic watcher does. ``check_dm`` is off for a link the user sent by hand
    (he asked for that title), and on for `/latest` and the watcher.
    """

    items: tuple[ParcivItem, ...] = ()
    qualities: tuple[str, ...] = ()
    check_dm: bool = True
    page: int = PAGE_DEFAULT
    source_url: str = PARCIV_API_URL
    sections: tuple[str, ...] = field(default_factory=tuple)

    @property
    def title(self) -> str:
        if len(self.items) == 1:
            return self.items[0].title
        return f"{len(self.items)} apitera title(s)"

    @property
    def where(self) -> str:
        names = sorted({SECTION_LABELS.get(item.section, item.section) for item in self.items})
        return ", ".join(names) or "apitera"

    def plan_line(self) -> str:
        """One line for /status: what this batch will do."""
        quality = "every quality" if not self.qualities else ", ".join(self.qualities)
        checked = "skipping what is already in your DM" if self.check_dm else "as asked"
        return f"{len(self.items)} item(s) from {self.where} · {quality} · {checked}"


def past_titles(messages) -> set[str]:
    """Every title form the DM already holds, from the messages read for the check."""
    keys: set[str] = set()
    for message in messages:
        for name in message_titles(message):
            keys |= title_variants(name)
    return keys


def already_uploaded(title: str, keys: set[str]) -> bool:
    """True when this title is one of the files already in the DM."""
    return any(titles_match(title, key) for key in keys)


def listing_lines(page: CataloguePage, keys: set[str], limit: int = LIST_ITEMS) -> list[str]:
    """The numbered catalogue list, marking what the DM already holds."""
    lines: list[str] = []
    for position, item in enumerate(page.items[:limit], 1):
        mark = "✅" if already_uploaded(item.title, keys) else "⬇️"
        tags = f" · {', '.join(item.langs)}" if item.langs else ""
        episode = f" · {item.episode}" if item.episode else ""
        lines.append(f"{position}. {mark} {item.title}{episode}{tags}")
    if len(page.items) > limit:
        lines.append(f"… and {len(page.items) - limit} more on this page.")
    return lines


def new_items(items: tuple[ParcivItem, ...], keys: set[str]) -> tuple[ParcivItem, ...]:
    """The listed items the DM does not hold yet."""
    return tuple(item for item in items if not already_uploaded(item.title, keys))


def sections_text(sections: tuple[str, ...]) -> str:
    return ", ".join(SECTION_LABELS.get(section, section) for section in sections)


def normalise_sections(sections) -> tuple[str, ...]:
    """Fall back to the default sections when none were given."""
    chosen = tuple(section for section in (sections or ()) if section)
    return chosen or AUTO_SECTIONS


def limit_text(limit: int) -> str:
    return f"{limit} item(s) per section"


SEARCH_SECTIONS = tuple(PARCIV_SECTIONS) + tuple(
    section for section in PARCIV_STREAM_SECTIONS if section not in PARCIV_SECTIONS)


def parse_search(text: str) -> tuple[tuple[str, ...], str]:
    """Read `/search [sections] <words>` (also `… sections=movies,anime`).

    The first word is taken as a section list only when it names sections, so
    `/search movies paw patrol` searches Moviebox movies while `/search paw
    patrol` searches every family. The words are returned untouched.
    """
    line = " ".join(str(text or "").split())
    line = re.sub(r"^/[a-z_]+(?:@[A-Za-z0-9_]+)?", "", line, count=1).strip()
    named: tuple[str, ...] = ()
    match = re.search(r"(?:^|\s)sections?=(\S.*)$", line)
    if match:
        wanted = match.group(1).strip()
        line = line[: match.start()].strip()
        try:
            named = parse_sections(wanted)
        except ParcivError:  # Not a section list after all: search for those words.
            line = f"{line} {wanted}".strip()
    if not named and line:
        first, _, rest = line.partition(" ")
        try:
            found = parse_sections(first)
        except ParcivError:
            found = ()
        if found and rest.strip():
            named, line = found, rest.strip()
    return (named or SEARCH_SECTIONS), line


__all__ = [
    "AUTO_INTERVAL_DEFAULT", "AUTO_INTERVAL_MAX", "AUTO_INTERVAL_MIN", "LIST_ITEMS", "PAGE_DEFAULT",
    "CataloguePage", "ParcivBatch", "already_uploaded", "interval_text", "limit_text",
    "listing_lines", "new_items", "normalise_sections", "parse_interval", "parse_limit",
    "parse_search", "past_titles", "sections_text", "DEFAULT_AUTO_LIMIT", "SEARCH_SECTIONS",
]
