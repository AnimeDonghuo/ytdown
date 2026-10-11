# YouTube downloader Telegram bot

Send the bot a YouTube video or public playlist URL. For a video, it asks the [Samra YouTube API](https://samra-youtube-api.antideploy.com/) for **actually downloadable** MP4 qualities and MP3 audio, then displays toggle buttons. Select as many as you like and tap **Done**. The provider can take 1–2 minutes (the bot waits up to 4 minutes per lookup). For a playlist, the bot displays 360p, 480p, 720p, 1080p, 1440p (2K), and 2160p (4K) first; after Done it discovers public video IDs with `yt-dlp` and queries the API per video. Qualities absent from an individual API response are skipped, **not replaced with another quality**. Files are transferred and sent one at a time.

Titles come from YouTube first (oEmbed with a `yt-dlp` backup for single videos, and YouTube's playlist entries for playlists). If neither YouTube method returns a title, the bot uses **that video's API title**; only if that is missing does it use the video ID. The filename and Telegram caption end with the chosen quality, for example `Actual Video Title [480p].mp4` and `Actual Video Title [480p]`; audio is labeled `[MP3]`. Videos get a resized YouTube thumbnail. `ffprobe` reads the bounded beginning/end of the actual MP4 to supply Telegram its **duration and real frame dimensions**, without downloading a second full copy; the API duration and thumbnail shape are fallbacks when the stream cannot be probed. Ratios cannot be corrected if black/white borders are actually encoded into the source frames. The `ffmpeg`/`ffprobe` package makes the Docker image larger, so a cold Koyeb image download may take longer.

## Koyeb deployment (Docker)

1. Create a Telegram bot with BotFather; keep its bot token secret. Get an **API ID** and **API hash** for your Telegram application from [my.telegram.org](https://my.telegram.org/).
2. Create a Koyeb Web Service from this repository using its `Dockerfile`. Select the free instance and **one replica only** (multiple instances of the same bot can duplicate work).
3. Configure Koyeb **secret environment variables** `BOT_TOKEN`, `API_ID`, and `API_HASH`. These are required. Do not commit them to the repository. **No string session or user account login is needed**: Telethon logs in as the bot using an in-memory MTProto session and your bot token. A restart creates a fresh login session.
4. Use service port **8000** (`PORT` can override it) and the HTTP health check **`/healthz`**. Telegram updates and uploads use MTProto; the web port is only for Koyeb health checks. No webhook is needed.
5. Optionally set `MAX_FILE_MB` (default 2000, clamped to 1–2000), `MAX_PLAYLIST_VIDEOS` (default 25, clamped to 1–50), and `UPLOAD_IN_FLIGHT` (default 2, clamped to 1–3 upload parts). Send `/start` to your bot.

### Large uploads and free-tier limits

The bot uses **MTProto rather than the standard Bot API upload endpoint**, so its configured per-file limit is **2,000 MiB** (about 2 GB), instead of 50 MB. With 512 KiB parts this is exactly 4,000 parts, the Telegram client upload-part boundary; larger sizes such as 2,047 MiB would need too many parts and are rejected before transfer. If a media URL supplies `Content-Length`, the bot streams bounded 512 KiB chunks directly from the provider into Telegram's MTProto upload; it does **not** save the whole video on Koyeb or load it into RAM. It checks the declared and actual size. The transfer must remain accessible throughout the upload. If a provider does **not** supply `Content-Length`, it temporarily spools files up to **64 MiB** to discover their size, and skips larger unknown-size files. Redirects must remain on public HTTPS hosts.

Uploads near the 2,000 MiB limit can take a long time and consume significant bandwidth; Koyeb's free-tier network, runtime, and resource limits may interrupt them. Success depends on the provider link staying valid and the Telegram servers accepting the media. A single global transfer slot and four waiting jobs bound concurrent resource use. You can send another YouTube link and choose its formats while an earlier job is running; each completed selection joins the FIFO queue. One pending format-selection panel per user is supported. The global queue holds at most five jobs total (one active plus four waiting). Temporary files are deleted after use; jobs and selections are in memory and are **not restored** after a restart. Playlist API lookups run sequentially and can take 1–2 minutes *per video*, so a 25-video playlist can take a long time. The bot does not transcode or split files.

### Live status and speed

The job message refreshes approximately every **12 seconds** with a progress bar, source and Telegram bytes, upload speed, ETA, queue position, and **Refresh** and **Cancel** buttons. Refresh triggers an **immediate edit** and resets the next automatic edit to 12 seconds later; repeat taps within two seconds are ignored to avoid flood waits. `/status` shows one job's full status or a compact list of your queued jobs; `/queue` does the same. During an API lookup or playlist discovery, size and ETA are not yet known. Resource readings use the **container cgroup CPU quota and RAM usage** (not the Python process alone) when available; Koyeb's dashboard may show a different sampling window. If cgroup data is unavailable, the status says so rather than claiming it matches Koyeb.

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
- `/status` or `/queue` — status of your jobs and queue positions
- `/cancel` — cancel your active (or oldest queued) job; `/cancel JOB_ID` targets one job shown by `/queue`. Each job also has its own Cancel button.
- `/changeapi` — prompt for your personal public HTTPS API endpoint; you can also use `/changeapi https://example.com/?url=` directly. The bot supplies the normalized YouTube link as `url=`. `/changeapi show` indicates whether an override is set; `/changeapi reset` (or `remove`) returns to the built-in API. Changes affect **future** selections; already selected/queued jobs retain their API. Settings are in memory and reset after a Koyeb restart. Do not put secret tokens in the API URL if you do not want them held in memory.
- `/limits` — current upload and playlist limits

A watch URL containing `list=` is treated as a playlist; remove that parameter to download only the video. `youtu.be` links are treated as single videos. Playlists must be publicly discoverable through `yt-dlp`; keep `yt-dlp` updated if YouTube changes its site.
