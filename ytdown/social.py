"""Instagram and Facebook links through the DESI API (samra-insta-fb-api).

The API answers with a title, a thumbnail and a list of media files, each one
labelled with its quality and type. Instagram posts carry an image and/or a
video; Facebook links usually carry one video plus a picture. Every entry is
downloaded straight from the CDN URL the API returned, through the user's own
session (so proxy mode applies).

This API is slower than the others - it scrapes the platforms - so its calls
get a longer timeout and one retry on a connection error.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from urllib.parse import urlparse

from .core import public_https_url

SOCIAL_API_URL = "https://samra-insta-fb-api.krishnalucky193.workers.dev/"
SOCIAL_HOSTS = {
    "instagram.com": "Instagram",
    "instagr.am": "Instagram",
    "facebook.com": "Facebook",
    "fb.com": "Facebook",
    "fb.watch": "Facebook",
    "m.facebook.com": "Facebook",
    "web.facebook.com": "Facebook",
}
MAX_SOCIAL_OPTIONS = 8  # Buttons per single link.
# Titles a scraper sends when it has no caption; the bot shows its own name instead.
PLACEHOLDER_TITLES = frozenset({
    "reel", "post", "video", "media", "photo", "instagram", "facebook", "untitled",
    "instagram reel", "instagram post", "instagram video", "instagram media", "instagram photo",
    "facebook reel", "facebook post", "facebook video", "facebook media", "facebook photo",
    "unsupported url", "instagram", "no title",
})
MAX_SOCIAL_LINKS = 5  # Links one message may carry.
MAX_SOCIAL_CHOICES = 12  # Buttons in total, for a batch of links.
LINK_RE = re.compile(r"https?://[^\s<>\"']+")
VIDEO_FORMATS = ("mp4", "webm", "mkv", "mov", "m4v", "ts", "3gp")
IMAGE_FORMATS = ("jpg", "jpeg", "png", "webp", "heic", "gif")


class SocialError(ValueError):
    """A user-facing Instagram/Facebook problem (bad link, no media, API down)."""


@dataclass(frozen=True)
class SocialDownload:
    """One downloadable file the API listed."""

    label: str  # "HD Video Stream"
    kind: str  # "video" | "image" | "file"
    format: str  # "mp4", "jpg"…
    url: str

    @property
    def is_video(self) -> bool:
        return self.kind == "video"

    @property
    def is_image(self) -> bool:
        return self.kind == "image"

    @property
    def suffix(self) -> str:
        return "." + (self.format or ("mp4" if self.is_video else "jpg")).lower().lstrip(".")


@dataclass(frozen=True)
class SocialMedia:
    platform: str
    title: str
    thumbnail: str
    downloads: tuple[SocialDownload, ...] = ()
    duration: str = ""

    def options(self) -> tuple[SocialDownload, ...]:
        """The choices shown as buttons, videos first and capped."""
        ranked = sorted(self.downloads, key=lambda item: (not item.is_video, item.label))
        return tuple(ranked[:MAX_SOCIAL_OPTIONS])

    def option(self, label: str) -> SocialDownload | None:
        return next((item for item in self.options() if item.label == label), None)


def find_social_links(text: str, limit: int = MAX_SOCIAL_LINKS) -> tuple[str, ...]:
    """Every Instagram/Facebook link in one message, in order and without duplicates."""
    found: list[str] = []
    for match in LINK_RE.findall(text or ""):
        candidate = match.rstrip(".,;:!?)]}\u201d\"'")
        try:
            link = parse_social_source(candidate)
        except ValueError:
            continue
        if link not in found:
            found.append(link)
        if len(found) >= max(1, limit):
            break
    return tuple(found)


def social_source(raw: str) -> str:
    """The link a message carries: the first one when several are pasted."""
    links = find_social_links(raw)
    if links:
        return links[0]
    return parse_social_source(raw)  # Raises with the precise reason this text is not usable.


def parse_social_source(raw: str) -> str:
    """Return the canonical Instagram/Facebook link, or raise ValueError."""
    text = raw.strip()
    if len(text) > 2048 or not public_https_url(text):
        raise ValueError("Send a full https:// Instagram or Facebook link.")
    parsed = urlparse(text)
    host = (parsed.hostname or "").lower().rstrip(".")
    if not any(host == suffix or host.endswith("." + suffix) for suffix in SOCIAL_HOSTS):
        raise ValueError("Instagram/Facebook mode accepts only Instagram or Facebook links.")
    if host == "fb.watch" or host.endswith(".fb.watch"):
        return text  # fb.watch short links carry the video id in the path.
    # The keyword has to be a whole path segment: "/p/…" is a post, but
    # "/profile.php" merely starts with a "p" and is not downloadable.
    if not re.search(r"/(?:p|reel|reels|tv|share|watch|videos?|stories?|photo|permalink)(?:/|$)"
                     r"|/video\.php|/groups/[^/]+/posts", parsed.path):
        raise ValueError("This link does not point at an Instagram post/reel or a Facebook video.")
    return text


@dataclass(frozen=True)
class SocialChoice:
    """One button: a download of one link, labelled the way the button shows it."""

    label: str
    media: SocialMedia
    item: SocialDownload


@dataclass(frozen=True)
class SocialBatch:
    """Several links pasted at once, shown as one list of choices.

    A slot stays None when that link could not be read, so every remaining
    option keeps the number of the link it actually came from.
    """

    media: tuple[SocialMedia | None, ...] = ()

    def present(self) -> tuple[tuple[int, SocialMedia], ...]:
        """The links that were read, with the position that was pasted."""
        return tuple((position, media) for position, media in enumerate(self.media, 1) if media)

    @property
    def platform(self) -> str:
        names = {media.platform for _, media in self.present()}
        return next(iter(names)) if len(names) == 1 else " / ".join(sorted(names))

    @property
    def title(self) -> str:
        """What the prompt calls this paste: one link's title, or "2 Instagram links"."""
        if len(self.media) <= 1:
            present = self.present()
            return present[0][1].title if present else "Instagram/Facebook"
        names = {media.platform for _, media in self.present()}
        label = next(iter(names)) if len(names) == 1 else "Instagram/Facebook"
        return f"{len(self.media)} {label} links"

    @property
    def thumbnail(self) -> str:
        return next((media.thumbnail for _, media in self.present() if media.thumbnail), "")

    def options(self) -> tuple[SocialChoice, ...]:
        """Every option of every link, numbered, capped so the keyboard stays usable."""
        numbered = len(self.media) > 1  # One link alone needs no "1 · " prefix.
        choices = [SocialChoice(f"{position} · {item.label}" if numbered else item.label, media, item)
                   for position, media in self.present() for item in media.options()]
        return tuple(choices[:MAX_SOCIAL_CHOICES])

    @property
    def dropped(self) -> int:
        """How many options did not fit on the keyboard, for the prompt to say so."""
        total = sum(len(media.options()) for _, media in self.present())
        return max(0, total - MAX_SOCIAL_CHOICES)

    def option(self, label: str) -> SocialChoice | None:
        return next((choice for choice in self.options() if choice.label == label), None)


def social_choices(media: "SocialMedia | SocialBatch") -> tuple[SocialChoice, ...]:
    """The buttons for one link's media or for a batch of links, same shape."""
    if isinstance(media, SocialBatch):
        return media.options()
    return tuple(SocialChoice(item.label, media, item) for item in media.options())


def platform_for(url: str) -> str:
    host = (urlparse(url).hostname or "").lower().rstrip(".")
    return next((name for suffix, name in SOCIAL_HOSTS.items()
                 if host == suffix or host.endswith("." + suffix)), "Instagram")


def _text(value: object, limit: int = 200) -> str:
    return " ".join(str(value or "").split())[:limit]


def _downloads(payload: dict) -> list:
    """The media list, from whichever key this API version used."""
    for key in ("downloads", "medias", "media", "links", "urls", "data", "result", "items",
                "video_url", "image_url", "url", "video"):
        value = payload.get(key)
        if isinstance(value, list):
            return value
        if isinstance(value, dict):
            nested = _downloads(value)
            if nested:
                return nested
            # {"HD": "https://…", "SD": "https://…"} is common too.
            return [{"quality": name, "url": link} for name, link in value.items()
                    if isinstance(link, str)]
        if isinstance(value, str) and public_https_url(value):
            return [{"url": value}]  # A bare link: the label is derived from its format.
    return []


def api_error(payload: object) -> str:
    if isinstance(payload, dict):
        for key in ("message", "error", "reason", "detail"):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return _text(value, 160)
    return ""


def normalize_social(payload: object, url: str = "") -> SocialMedia:
    """Turn an API answer into the media the bot shows and downloads."""
    if not isinstance(payload, dict):
        raise SocialError("The Instagram/Facebook API returned an unexpected response.")
    status = payload.get("status", payload.get("success"))
    if status is False:
        raise SocialError(api_error(payload) or "The API could not read this Instagram/Facebook link.")
    platform = _text(payload.get("platform"), 40) or platform_for(url)
    raw = _downloads(payload)
    downloads: list[SocialDownload] = []
    seen: set[str] = set()
    for item in raw:
        if not isinstance(item, dict):
            continue
        link = next((_text(item.get(key), 2048) for key in
                     ("url", "download_url", "link", "src", "hd", "sd")
                     if public_https_url(_text(item.get(key), 2048))), "")
        if not link or link in seen:
            continue
        seen.add(link)
        kind = _text(item.get("type"), 20).lower()
        fmt = _text(item.get("format") or item.get("ext") or item.get("extension"), 10).lower().lstrip(".")
        if not fmt:
            fmt = (urlparse(link).path.rsplit(".", 1)[-1] if "." in urlparse(link).path else "").lower()
        if kind not in ("video", "image", "audio", "file"):
            kind = ("video" if fmt in VIDEO_FORMATS else "image" if fmt in IMAGE_FORMATS else "file")
        if kind == "audio" and fmt in VIDEO_FORMATS:
            kind = "video"
        label = _text(item.get("quality") or item.get("label"), 40)
        if not label:
            label = f"{fmt.upper() or kind.title()} {kind}"
        if any(existing.label == label for existing in downloads):
            label = f"{label} ({fmt or kind})" if fmt else f"{label} 2"
        downloads.append(SocialDownload(label, kind, fmt or ("mp4" if kind == "video" else "jpg"), link))
    if not downloads:
        raise SocialError("The API listed no downloadable media for this link.")
    thumbnail = next((_text(payload.get(key), 2048) for key in ("thumbnail", "thumb", "cover", "image")
                      if public_https_url(_text(payload.get(key), 2048))), "")
    title = _text(payload.get("title") or payload.get("caption") or payload.get("description"), 200)
    # "Post", "Reel" and "Facebook video" are placeholders, but a real caption may
    # well be "Kitchen reel", so only the whole title has to match.
    if not title or title.lower().strip(" .:-") in PLACEHOLDER_TITLES or len(title.strip()) <= 3:
        title = f"{platform} {'reel' if platform == 'Instagram' else 'video'}"
    duration = _text(payload.get("duration"), 40)
    return SocialMedia(platform, title, thumbnail, tuple(downloads), duration)
