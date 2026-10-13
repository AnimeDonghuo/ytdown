# YouTube, Terabox, Dailymotion and Rumble downloader Telegram bot

The bot defaults to **YouTube mode**. Use `/change` to select YouTube-only, Terabox-only, Dailymotion-only or Rumble-only links; mode settings are per user and reset on restart. Send a YouTube video or public playlist URL in YouTube mode. For a video, it asks the [Samra YouTube API](https://samra-youtube-api.antideploy.com/) for **actually downloadable** MP4 qualities and MP3 audio, then displays toggle buttons. Select as many as you like and tap **Done**. The provider can take 1–2 minutes (the bot waits up to 4 minutes per lookup). For a playlist, the bot first reports the video count (up to 5,000 public entries). Reply with an **inclusive range** such as `1-29` or `200-300` (at most 150 entries per job), then select 360p, 480p, 720p, 1080p, 1440p (2K), and/or 2160p (4K). After Done it discovers only that range of public video IDs with `yt-dlp` and queries the API per video. Qualities absent from an individual API response are skipped, **not replaced with another quality**. Files are transferred and sent one at a time.

Titles come from YouTube first (oEmbed with a `yt-dlp` backup for single videos, and YouTube's playlist entries for playlists). If neither YouTube method returns a title, the bot uses **that video's API title**; only if that is missing does it use the video ID. The filename and Telegram caption end with the chosen quality, for example `Actual Video Title [480p].mp4` and `Actual Video Title [480p]`; audio is labeled `[MP3]`. A subtitle marker such as `[EngSub]` or `[MultiSub]` (also accepted in parentheses, any case) is removed from the title and placed **after** the quality, for example `Episode 15 [360p] [MultiSub]`. For YouTube playlist items, the caption and live-status title show only the title, quality and subtitle tag (no playlist number), while the filename starts with a zero-padded playlist index, for example `02-39 One Way or Another Episode 15 [360p] [MultiSub].mp4` (`002-150` when the total is 150). Terabox names and captions are unchanged. Videos prefer the **API-provided thumbnail URL**; if it returns a mostly white placeholder or fails, the bot tries YouTube's reliable HQ image before max-resolution. If none of those is usable (playlist items, for example, get no API thumbnail), the bot takes one frame from the bounded video sample already captured while streaming, so the preview comes from the video itself. Thumbnails are resized/compressed under 18 KiB (Telegram may silently ignore larger images) and uploaded as a `.jpg` with video dimensions. Videos are sent as streamable MP4 so they **stay playable inline**; a document is only used as a last resort if Telegram refuses the file as video. After a video is delivered, the bot makes one bounded check of the still-image thumbnail Telegram returned (read in 64 KiB requests and capped at 256 KiB, then decoded locally). If that still is missing, unreadable, or white, the bot **replies to the video** with the prepared JPEG as a separate cover photo. A cover problem never causes the video to be uploaded again; it is only logged. This cover check applies to YouTube videos; Terabox thumbnails are unchanged. If Telegram rejects the prepared thumbnail on the send itself, the already-uploaded file is re-sent once without it, because that send did not succeed. Container logs record whether a usable thumbnail was prepared and whether a cover photo was sent. `ffprobe` reads the bounded beginning/end of the actual MP4 to supply Telegram its **duration and real frame dimensions**, without downloading a second full copy; the API duration (including `HH:MM:SS` strings) and thumbnail shape are fallbacks when the stream cannot be probed. Rotated videos have their display dimensions swapped when ffprobe reports a rotation. Ratios cannot be corrected if black/white borders are actually encoded into the source frames. The `ffmpeg`/`ffprobe` package makes the Docker image larger, so a cold Koyeb image download may take longer. Without ffmpeg (for example on BotKeep, which installs only Python packages), the bot logs a startup warning, does not probe the video, and skips the frame-from-sample thumbnail; it still uses the API and YouTube thumbnails.

### Dailymotion mode

Switch with `/change` → **Dailymotion**, then send a Dailymotion video link: `dai.ly/...`, `dailymotion.com/video/...`, `dailymotion.com/embed/video/...`, or a `geo.dailymotion.com/player...?video=...` link. Playlists and user pages are refused. The bot asks the Cloudflare Worker `samra-dailymotion.krishnalucky193.workers.dev` for the title, duration, poster and HLS master playlist, then lists **only the heights that the playlist actually offers** (for example `360p`, `720p`). Pick one or more and tap Done. Each quality is one HLS variant: yt-dlp downloads its segments into a **temporary folder**, ffmpeg remuxes them to MP4, and the file is uploaded from disk as a streamable video, so it stays playable inline. The Dailymotion poster is the cover when it is usable. Dailymotion mode does not change YouTube or Terabox behaviour.

Requirements and limits:

- **Hosting:** Dailymotion mode is supported on **Koyeb** (the Docker image includes ffmpeg and ffprobe). It needs ffmpeg and ffprobe on the host. On BotKeep it is unavailable unless those tools exist there; Dailymotion jobs then stop with a clear message, while YouTube and Terabox keep working.
- Each quality needs free disk space of about 1.2× its size estimate, computed from the playlist's peak bandwidth and duration. Temporary files are deleted after each quality. Qualities above the 2,000 MiB Telegram limit are skipped.
- The bot reads the stream playlist from Dailymotion's CDN **from its own IP address**. If the CDN refuses that address, the bot reads the Worker's proxy copy of the same playlist instead, and logs which copy it used. If both fail, the message names the HTTP status (for example `HTTP 403`). A quality that fails during download is reported in the job summary (for example `720p: HTTP Error 403`) and the other qualities still run.
- Private, paid or region-restricted videos fail with the API's message. Nothing is bypassed.
- Cancelling a job stops an active Dailymotion download at its next progress update.


Switch with `/change` → **Terabox**, then send an HTTPS Terabox share URL (including `terasharefile.com/s/...`). The bot calls the [Samrat Terabox API](https://samratbdownload.krishnalucky193.workers.dev/?url=) and shows the formats/qualities **actually returned** for a single file. Select one or several and tap Done. For an exposed folder listing, the bot reports how many files have usable links, asks for an inclusive range, then presents the qualities in that range. Files and selected quality variants are transferred **one by one**, with their real filename extension, MIME, and size (when provided). For video thumbnails the bot tries the file's API preview URL, then the parent listing's preview, applying the same redirect checks, blank-image rejection, and Telegram-compatible JPEG resize used for YouTube. If both fail, it makes one bounded, single-threaded attempt to extract a frame from the video sample already captured during streaming (no second full download); some streams cannot be decoded from this sample, so a thumbnail cannot be guaranteed. If the API reports a folder total but does not expose links for all files, the bot can only process the returned links; it cannot traverse an unlisted folder. Repeated entries with the same file ID and different qualities are treated as one file with multiple choices. The bot refreshes expiring links before transfer when the API supplies `expires_at`. Terabox mode does not accept YouTube links, and vice versa. Each service has its own API: set one with `/changeapi tera <URL>` (see Custom APIs below). Signed links may expire during long jobs, and neither service bypasses private/blocked content.

### Rumble mode

Switch with `/change` → **Rumble**, then send one Rumble video link: `rumble.com/v…-title.html` or `rumble.com/embed/v…`. Channels, search pages and playlists are refused. The bot asks the [Samra Rumble API](https://samra-rumble-api.krishnalucky193.workers.dev/) for the title, duration, thumbnail and the MP4 renditions it lists, then labels each quality by its **real pixel height** (for example `360p`, `720p`, `1080p`). When Rumble lists two renditions at the same height, the bot keeps the one whose stated height matches exactly, so a padded 1081-line entry does not replace the 1080 one. Pick one or more qualities and tap Done. Each quality is a direct MP4 streamed to Telegram as a streamable video, so it stays playable inline. Titles, captions and filenames follow the same title, quality and subtitle-tag format as YouTube. The API's thumbnail is the only cover source; the reply cover-photo fallback still applies. Rumble needs **no ffmpeg**, so it works on BotKeep as well as Koyeb. Rumble does not provide audio, so MP3 is not offered. Rumble links are refused in the other modes, and Rumble mode uses its own API (`/changeapi rumble <URL>`).

## Custom APIs

Each service calls its own link API with `url=` set to the link you sent:

- **YouTube:** the Samra YouTube API (default `https://samra-youtube-api.antideploy.com/`). It must return the YouTube format list described above.
- **Terabox:** the Samrat Terabox API (default `https://samratbdownload.krishnalucky193.workers.dev/`). It must return the Terabox file listing with download links.
- **Dailymotion:** the Cloudflare Worker (default `https://samra-dailymotion.krishnalucky193.workers.dev/api/info`). It must return `success`, `title`, `duration`, `poster` and an HTTPS `master_m3u8` playlist.
- **Rumble:** the Samra Rumble API (default `https://samra-rumble-api.krishnalucky193.workers.dev/`). It must return `success`, `title`, `thumbnail`, `duration` and a `streams` list whose entries have `type` `video`, `format` `mp4`, a `real_height` or `height`, and an HTTPS `url`.

To use your own API for one service, for example Dailymotion:

1. Send `/changeapi daily https://your-api.example.com/info` (or `/changeapi daily` and then the URL).
2. Send a Dailymotion link. The bot now asks your API.
3. Send `/changeapi daily reset` to return to the built-in API.

A custom API must be public HTTPS, must not point at a YouTube or Rumble link, and must return the same JSON shape as the built-in one. If it does not, the bot reports that the link could not be loaded. Your custom URL is kept in memory only and is shown back to you by `/apis` and `/changeapi show`, so do not put secrets you do not want in your chat history into the URL. The Koyeb deployment needs no extra settings for this.

## Koyeb deployment (Docker)

1. Create a Telegram bot with BotFather; keep its bot token secret. Get an **API ID** and **API hash** for your Telegram application from [my.telegram.org](https://my.telegram.org/).
2. Create a Koyeb Web Service from this repository using its `Dockerfile`. Select the free instance and **one replica only** (multiple instances of the same bot can duplicate work).
3. Configure Koyeb **secret environment variables** `BOT_TOKEN`, `API_ID`, and `API_HASH`. These are required. Do not commit them to the repository. **No string session or user account login is needed**: Telethon logs in as the bot using an in-memory MTProto session and your bot token. A restart creates a fresh login session.
4. Use service port **8000** (`PORT` can override it) and the HTTP health check **`/healthz`**. Telegram updates and uploads use MTProto; the web port is only for Koyeb health checks. No webhook is needed.
5. Optionally set `MAX_FILE_MB` (default 2000, clamped to 1–2000), `MAX_RANGE_VIDEOS` (default 150, clamped to 1–150), and `UPLOAD_IN_FLIGHT` (default 2, clamped to 1–3 upload parts). Send `/start` to your bot.

### BotKeep deployment (plain Python, no Docker)

BotKeep runs the bot directly from `requirements.txt`; it does not use the `Dockerfile`, and it cannot install system packages such as ffmpeg with apt.

1. Create a Python workload from this repository (GitHub or ZIP deploy). Use the repository root, which contains `requirements.txt`. Use Python **3.11 or newer** if BotKeep offers a choice; the Dockerfile uses 3.11.
2. Install command: `pip install -r requirements.txt`
3. Start command: `python -m ytdown`
4. Add the secret environment variables `BOT_TOKEN`, `API_ID`, and `API_HASH` in the BotKeep panel. Do not commit them or a `.env` file. As on Koyeb, no string session or user login is needed.
5. Run one instance only, as on Koyeb, so the bot does not duplicate work.

Notes for BotKeep:

- **No public port is required.** Telegram traffic uses MTProto. The `/healthz` endpoint starts only if the platform gives the process a port (`PORT`, default 8000). If the port cannot be bound, the bot logs a warning and keeps running. Do not rely on `/healthz` to keep the workload alive.
- **ffmpeg may be missing.** At startup the bot logs a warning naming any missing `ffmpeg`/`ffprobe`. Without them there are no probe-based durations or frame thumbnails: durations come from the API and thumbnails come from the API or YouTube images, with the same safety checks as before. Uploads still work. If the platform does not provide ffmpeg, expect this degraded mode.
- **Native packages.** `cryptg` is an optional native accelerator. If its install fails on BotKeep, remove the line from `requirements.txt`; the bot falls back to pure-Python encryption and still works, only more slowly.
- **Rumble works on BotKeep.** It needs only the Rumble API and direct MP4 downloads, with no ffmpeg.
- **Dailymotion is Koyeb-only for now.** Dailymotion needs ffmpeg and ffprobe, which the Koyeb Docker image provides and BotKeep may not. Without them, Dailymotion jobs fail with a clear message while YouTube and Terabox keep working. Dailymotion temporary files use the host's disk, so check the free storage before downloading long videos.
- **Ephemeral state.** Like Koyeb, `/changeapi` settings, the queue, and status history are in memory and reset on a restart or redeploy.
- Keep the BotKeep console open or check it when a job seems stuck. Logs show lookup retries, refused links that were refreshed, and any cover-photo fallback.

## Large uploads and free-tier limits

The bot uses **MTProto rather than the standard Bot API upload endpoint**, so its configured per-file limit is **2,000 MiB** (about 2 GB), instead of 50 MB. With 512 KiB parts this is exactly 4,000 parts, the Telegram client upload-part boundary; larger sizes such as 2,047 MiB would need too many parts and are rejected before transfer. If a media URL supplies `Content-Length`, the bot streams bounded 512 KiB chunks directly from the provider into Telegram's MTProto upload; it does **not** save the whole video on Koyeb or load it into RAM. It checks the declared and actual size. The transfer must remain accessible throughout the upload. If a Terabox URL omits `Content-Length` but the API supplied `size_bytes`, that size is checked against the actual stream and used for bounded streaming. Otherwise, a missing length temporarily spools files up to **64 MiB** to discover their size, and skips larger unknown-size files. Redirects must remain on public HTTPS hosts.

Uploads near the 2,000 MiB limit can take a long time and consume significant bandwidth; Koyeb's free-tier network, runtime, and resource limits may interrupt them. Success depends on the provider link staying valid and the Telegram servers accepting the media. A single global transfer slot and four waiting jobs bound concurrent resource use. You can send another link in your active mode and choose its formats while an earlier job is running; each completed selection joins the FIFO queue. One pending format-selection panel per user is supported. The global queue holds at most five jobs total (one active plus four waiting). At most two link lookups run concurrently; playlists are counted using flat metadata and individual video API lookups remain sequential. Temporary files are deleted after use; jobs and selections are in memory and are **not restored** after a restart. Playlist API lookups run sequentially and can take 1–2 minutes *per video*, so a 100-video selection can take a long time. The final summary lists why items were skipped, for example `item 1: lookup HTTP 403` or `item 3 720p: HTTP 403`. Between playlist items the bot pauses about 2 seconds before each lookup. A lookup that fails with a rate limit (HTTP 429), a server error, or a non-JSON provider page is retried up to three times with short waits (4 s, then 12 s); other client errors are not retried. When the provider refuses a download link (HTTP 403, 404, or 410) before any bytes have been sent, the bot requests one fresh link from the API and retries that quality once. A link refused after transfer has started is not refreshed and is reported as failed. The bot does not transcode or split files.

### Live status and speed

Each time you tap **Done** after choosing qualities, the bot posts a **new combined live status** showing the newly queued job and any running/queued jobs. The status lists **running and queued jobs only**: finished, failed, and user-cancelled jobs never appear in it, regardless of how long ago they ended. It deletes the old status/selection panels where Telegram permits it. `/status` (or `/queue`) likewise replaces the old message with a fresh combined status. Each job displays a tappable `/cancel_JOBID` command rather than a Cancel button. YouTube status titles include the subtitle tag after the quality, for example `🎬 One Way or Another Episode 15 [360p] [MultiSub]`. The status edits automatically approximately every **12 seconds** and has **Refresh all**, which edits immediately and resets the automatic timer; repeat taps within two seconds are ignored to avoid flood waits. The dashboard includes progress bars, source and Telegram bytes, upload speed, ETA, and queue position. If posting a new dashboard fails, the job's selection message still shows its own live progress. Dashboard state is bounded to 100 users; an idle dashboard may be evicted when this limit is reached, and `/status` reopens it. During an API lookup or playlist discovery, size and ETA are not yet known. Resource readings use the **container cgroup CPU quota and RAM usage** (not the Python process alone) when available; Koyeb's dashboard may show a different sampling window. If cgroup data is unavailable, the status says so rather than claiming it matches Koyeb.

The optional native **`cryptg`** accelerator reduces MTProto encryption CPU usage. Large and small known-size uploads pipeline **two 512 KiB parts** by default instead of waiting for each part's round trip. This uses about 1 MiB of in-flight part data per job and keeps the single global transfer limit. On a particularly CPU-constrained free instance, set `UPLOAD_IN_FLIGHT=1`; for faster networks, 2 is the recommended starting value (3 is the maximum). Throughput is still limited by the provider, Telegram, and Koyeb; this cannot guarantee a particular speed.

Only download media you have permission to use. Private, region-blocked, or age-restricted videos are not bypassed.

## Run locally

Python 3.11+ is recommended:

```sh
python -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
export BOT_TOKEN='your-bot-token'
export API_ID='your-numeric-api-id'
export API_HASH='your-api-hash'
python -m ytdown
```

Offline tests require neither credentials nor network access:

```sh
python -m unittest discover -s tests -v
```

## Commands

- `/start` or `/help` — usage instructions
- `/change` — YouTube-only / Terabox-only / Dailymotion-only / Rumble-only mode buttons; mode changes affect future links, not queued transfers, and clear any stale `/changeapi` URL prompt
- `/status` or `/queue` — replace the old status with one live dashboard showing running and queued jobs only; selecting a quality and tapping Done also posts a fresh dashboard automatically
- `/cancel` — list your running/queued job IDs and tappable `/cancel_JOBID` commands without stopping anything; `/cancel_JOBID` stops exactly that job, even if it is running. `/cancel JOB_ID` remains supported. `/cancel lookup` stops a pending link lookup. If no jobs are active, `/cancel` clears a pending selection or lookup.
- `/apis` — lists the API used for YouTube, Terabox, Dailymotion and Rumble for your account (built-in or your custom URL). `/apis remove yt|tera|daily|rumble` restores one service's built-in API. Built-in APIs cannot be deleted.
- `/changeapi` — shows the usage. `/changeapi yt|tera|daily|rumble <URL>` sets your own public HTTPS API for that service (`yt` = YouTube, `tera` = Terabox, `daily` = Dailymotion, `rumble` = Rumble; `youtube`, `terabox`, `dailymotion`, `rum` also work). `/changeapi yt` on its own asks for the URL. `/changeapi show` lists all four; `/changeapi <service> reset` restores one built-in API; `/changeapi reset` restores all of them. A bare `/changeapi <URL>` still sets the YouTube API. Changes affect **future** selections; already selected or queued jobs keep the API they started with. A media link sent while the prompt is open is treated as a media link, not as the API URL. Settings are in memory and reset after a restart.
- `/limits` — current upload, playlist-range, and queue limits

In YouTube mode, a watch URL containing `list=` is treated as a playlist; remove that parameter to download only the video. `youtu.be` links are treated as single videos. Playlists must be publicly discoverable through `yt-dlp`; keep `yt-dlp` updated if YouTube changes its site.
