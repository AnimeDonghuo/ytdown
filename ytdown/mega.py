"""MEGA links and the SAMRA MEGA Cloud API's file/folder listing.

A MEGA ``/file/`` or ``/folder/`` link is resolved through the API, which
returns the folder tree and, for every file, a download URL on the API itself
(plus the raw ``mega.nz`` link). Folders come back flat: each file names its
own parent, so the bot can rebuild the path and list the whole tree.
"""
from __future__ import annotations

import mimetypes
import re
from dataclasses import dataclass, replace
from pathlib import PurePosixPath
from urllib.parse import urlparse

from .core import public_https_url

MEGA_API_URL = "https://samra-mega-api.onrender.com/api/info"
MEGA_HOSTS = ("mega.nz", "mega.io", "mega.co.nz")
MAX_MEGA_FILES = 2000
MAX_MEGA_LINKS = 10  # How many links one message may carry before the bot says that is too many.
LINK_RE = re.compile(r"https?://[^\s<>\"']+")
VIDEO_SUFFIXES = (".mp4", ".mkv", ".webm", ".avi", ".mov", ".m4v", ".ts", ".flv", ".wmv")


class MegaError(ValueError):
    """A user-facing MEGA problem (bad link, no files, refused listing)."""


@dataclass(frozen=True)
class MegaFile:
    """One file the API listed, with the URLs it offered for it."""

    file_id: str
    filename: str
    parent_id: str
    parent_name: str
    size: int
    mime: str
    download_url: str
    stream_url: str
    direct_link: str
    path: str = ""  # "Module 01 - Introduction/01-Introduction.mp4.mp4"
    thumbnail_urls: tuple[str, ...] = ()  # The API sends none; kept for send_stream.

    @property
    def title(self) -> str:
        stem = PurePosixPath(self.filename).stem
        # "01- Intro.mp4.mp4" and "02- Layout.mp4.ts" are normal on MEGA: the
        # stem keeps a media suffix, which does not belong in a title.
        inner = PurePosixPath(stem)
        if inner.suffix.lower() in VIDEO_SUFFIXES + (".pdf", ".zip", ".rar", ".7z", ".srt", ".vtt"):
            stem = inner.stem or stem
        return stem[:200] or self.filename[:200]

    @property
    def suffix(self) -> str:
        return PurePosixPath(self.filename).suffix.lower()

    @property
    def video(self) -> bool:
        # ".mp4.ts" and other double extensions are common here, so the MIME the
        # API reported and the extension both count.
        return self.mime.startswith("video/") or self.suffix in VIDEO_SUFFIXES

    @property
    def url(self) -> str:
        """The URL the bot streams from: the API's streaming route for videos."""
        return (self.stream_url or self.download_url or self.direct_link) if self.video else (
            self.download_url or self.stream_url or self.direct_link)

    def alternates(self) -> tuple[str, ...]:
        """Other URLs for the same file, tried when the first one is refused."""
        seen = [self.url]
        for candidate in (self.download_url, self.stream_url, self.direct_link):
            if candidate and candidate not in seen:
                seen.append(candidate)
        return tuple(seen)


@dataclass(frozen=True)
class MegaFolder:
    folder_id: str
    parent_id: str
    name: str
    path: str = ""


@dataclass(frozen=True)
class MegaListing:
    root_id: str
    root_name: str
    files: tuple[MegaFile, ...] = ()
    folders: tuple[MegaFolder, ...] = ()
    reported_files: int = 0
    truncated: bool = False  # The API had more files than it sent.
    sources: tuple[str, ...] = ()  # The links this listing merged (more than one = a batch).

    def file_at(self, index: int) -> MegaFile | None:
        return self.files[index] if 0 <= index < len(self.files) else None

    @property
    def multi(self) -> bool:
        """True when several links were merged into this listing."""
        return len(self.sources) > 1

    def names(self, limit: int = 20, paths: bool | None = None) -> list[str]:
        """A short numbered list of the files, for the range prompt.

        With several links merged in, each entry is shown with its path so
        files of the same name from different links stay apart.
        """
        show_path = self.multi if paths is None else paths
        lines = [f"{position}. {file.path if show_path and file.path else file.filename}"
                 for position, file in enumerate(self.files[:limit], 1)]
        if len(self.files) > limit:
            lines.append(f"… and {len(self.files) - limit} more")
        return lines


def find_mega_links(text: str, limit: int = MAX_MEGA_LINKS) -> tuple[str, ...]:
    """Every MEGA link in one message, in order and without duplicates.

    People paste several links at once, so the bot takes them all: a link that
    is not a MEGA file/folder link is ignored here rather than refusing the
    whole message.
    """
    found: list[str] = []
    for match in LINK_RE.findall(text or ""):
        candidate = match.rstrip(".,;:!?)]}\u201d\"'")
        try:
            link = parse_mega_source(candidate)
        except ValueError:
            continue
        if link not in found:
            found.append(link)
        if len(found) >= max(1, limit):
            break
    return tuple(found)


def mega_source(raw: str) -> str:
    """The MEGA link a message carries: the first one when several are pasted."""
    links = find_mega_links(raw)
    if links:
        return links[0]
    return parse_mega_source(raw)  # Raises with the precise reason this text is not usable.


def merge_listings(listings: "list[MegaListing] | tuple[MegaListing, ...]",
                   labels: "list[str] | tuple[str, ...]" = ()) -> MegaListing:
    """Combine several links' listings into one, so a range covers all of them.

    Each file keeps its own download URLs (they are per file), and its path is
    prefixed with the link's number so the numbered list stays readable when two
    links hold files of the same name.
    """
    usable = [item for item in listings if item and item.files]
    if not usable:
        raise MegaError("None of those MEGA links listed any files.")
    # One label per merged link, for the paths and the prompt's source count.
    given = tuple(labels)
    sources = tuple(given[position] if position < len(given)
                    else (item.sources[0] if item.sources else f"link {position + 1}")
                    for position, item in enumerate(usable))
    files: list[MegaFile] = []
    folders: list[MegaFolder] = []
    seen_folders: set[str] = set()
    for position, item in enumerate(usable, 1):
        label = f"{position}" if len(usable) > 1 else ""
        for file in item.files:
            path = file.path or file.filename
            files.append(replace(file, path=f"[{label}] {path}") if label else file)
        for folder in item.folders:
            if folder.folder_id and folder.folder_id in seen_folders:
                continue
            seen_folders.add(folder.folder_id)
            folders.append(folder)
    return MegaListing(root_id=usable[0].root_id, root_name=usable[0].root_name,
                       files=tuple(files), folders=tuple(folders),
                       reported_files=sum(item.reported_files for item in usable),
                       truncated=any(item.truncated for item in usable),
                       sources=tuple(source for source in sources if source))


def parse_mega_source(raw: str) -> str:
    """Return the canonical MEGA link for a file or folder, or raise ValueError."""
    text = raw.strip()
    if len(text) > 2048 or not public_https_url(text):
        raise ValueError("Send a full https:// MEGA link.")
    parsed = urlparse(text)
    host = (parsed.hostname or "").lower().rstrip(".")
    if not any(host == suffix or host.endswith("." + suffix) for suffix in MEGA_HOSTS):
        raise ValueError("MEGA mode accepts only mega.nz links (file or folder).")
    path = parsed.path.strip("/")
    if not re.fullmatch(r"(file|folder)/[A-Za-z0-9_-]{6,}", path):
        raise ValueError("This MEGA link does not name a file or a folder.")
    if not parsed.fragment:
        # mega.nz links carry the decryption key after '#': without it the API cannot list anything.
        raise ValueError("This MEGA link is missing its # key — copy the whole link again.")
    return text


def mega_files(payload: object) -> tuple[list, list]:
    """The raw folders/files arrays of an API answer (tolerates wrapped shapes)."""
    if not isinstance(payload, dict):
        raise MegaError("MEGA API returned an unexpected response.")
    data = payload
    for key in ("data", "result"):
        if isinstance(payload.get(key), dict):
            data = payload[key]
            break
    folders = data.get("folders")
    files = data.get("files")
    return (folders if isinstance(folders, list) else [],
            files if isinstance(files, list) else [])


def api_ok(payload: object) -> bool:
    """True when the API reported success (``success`` or ``status``)."""
    if not isinstance(payload, dict):
        return False
    for key in ("success", "status"):
        value = payload.get(key)
        if isinstance(value, bool):
            return value
    return "files" in payload or "folders" in payload


def api_error(payload: object) -> str:
    """The API's own reason, when it gave one."""
    if isinstance(payload, dict):
        for key in ("error", "message", "detail", "msg"):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return " ".join(value.split())[:160]
    return ""


def _text(value: object, limit: int = 200) -> str:
    return " ".join(str(value or "").split())[:limit]


def _size(value: object) -> int:
    try:
        number = int(float(value)) if isinstance(value, (int, float, str)) else 0
    except (TypeError, ValueError, OverflowError):
        return 0
    return number if 0 < number < 10 * 1024**4 else 0


def normalize_mega(payload: object) -> MegaListing:
    """Turn an API answer into the folder tree the bot shows and downloads."""
    if not api_ok(payload):
        raise MegaError(api_error(payload) or "MEGA API could not resolve this link.")
    raw_folders, raw_files = mega_files(payload)
    if not raw_files:
        raise MegaError("This MEGA link has no files the API could list.")
    if len(raw_files) > MAX_MEGA_FILES:
        raw_files = raw_files[:MAX_MEGA_FILES]
        truncated = True
    else:
        truncated = False
    # Folder names, so a file can be labelled with the folder it came from.
    names: dict[str, str] = {}
    parents: dict[str, str] = {}
    for item in raw_folders:
        if not isinstance(item, dict):
            continue
        folder_id = _text(item.get("folder_id") or item.get("id"), 64)
        if folder_id:
            names[folder_id] = _text(item.get("name"), 120)
            parents[folder_id] = _text(item.get("parent_id"), 64)
    root_id = ""
    if isinstance(payload, dict):
        root_id = _text(payload.get("root_folder_id"), 64) or _text(payload.get("root_id"), 64)

    def trail(folder_id: str) -> str:
        """Folder path from the root down, cycle-safe."""
        parts: list[str] = []
        seen: set[str] = set()
        current = folder_id
        while current and current not in seen and len(parts) < 12:
            seen.add(current)
            if current != root_id and names.get(current):
                parts.append(names[current])
            current = parents.get(current, "")
        return "/".join(reversed(parts))

    folders: list[MegaFolder] = []
    for item in raw_folders:
        if not isinstance(item, dict):
            continue
        folder_id = _text(item.get("folder_id") or item.get("id"), 64)
        if not folder_id:
            continue
        folders.append(MegaFolder(folder_id, _text(item.get("parent_id"), 64),
                                  names.get(folder_id, ""), trail(folder_id)))
    files: list[MegaFile] = []
    for item in raw_files:
        if not isinstance(item, dict):
            continue
        links = [_text(item.get(key), 2048) for key in
                 ("stream_proxy", "download_url", "direct_mega_link")]
        if not any(public_https_url(link) for link in links):
            continue
        parent_id = _text(item.get("parent_id"), 64)
        name = _text(item.get("name") or item.get("filename"), 180) or "MEGA file"
        name = name.replace("\\", "/").rsplit("/", 1)[-1].strip()[:180] or "MEGA file"
        folder = names.get(parent_id) or _text(item.get("parent_name"), 120)
        trail_text = trail(parent_id)
        extension = PurePosixPath(name).suffix.lower()
        mime = _text(item.get("mime") or item.get("mime_type"), 100)
        if not re.fullmatch(r"[a-zA-Z0-9.+-]+/[a-zA-Z0-9.+-]+", mime or ""):
            mime = mimetypes.guess_type(name)[0] or ""
        if not mime or (extension in VIDEO_SUFFIXES and not mime.startswith("video/")):
            # .ts is TypeScript/Qt to mimetypes.guess_type, but here it is MPEG-TS
            # video, and Telegram needs to be told so.
            if extension in VIDEO_SUFFIXES:
                mime = "video/mp2t" if extension == ".ts" else "video/mp4"
            else:
                mime = "application/octet-stream"
        files.append(MegaFile(
            file_id=_text(item.get("file_id") or item.get("id") or name, 64),
            filename=name, parent_id=parent_id, parent_name=(trail_text.split("/")[-1] if trail_text
                                                             else folder),
            size=_size(item.get("size_bytes") or item.get("size")),
            mime=mime,
            download_url=links[1] if public_https_url(links[1]) else "",
            stream_url=links[0] if public_https_url(links[0]) else "",
            direct_link=links[2] if public_https_url(links[2]) else "",
            path=f"{trail_text}/{name}" if trail_text else name,
        ))
    if not files:
        raise MegaError("MEGA API returned no downloadable files for this link.")
    root_name = ""
    if isinstance(payload, dict):
        root_name = _text(payload.get("root_folder_name") or payload.get("name"), 160)
    reported = 0
    if isinstance(payload, dict):
        try:
            reported = int(payload.get("total_files") or 0)
        except (TypeError, ValueError):
            reported = 0
    return MegaListing(root_id=root_id or (folders[0].parent_id if folders else ""),
                       root_name=root_name or (folders[0].name if folders else "") or "MEGA",
                       files=tuple(files), folders=tuple(folders),
                       reported_files=max(reported, len(files)), truncated=truncated)


def video_choice(file: MegaFile) -> str:
    """The single selection label for one file: MEGA files come as they are."""
    if file.video:
        return "Video"
    if file.mime.startswith("audio/"):
        return "Audio"
    return "File"
