# YouTube downloader Telegram bot

Send the bot a YouTube video or public playlist URL. For a video, it asks the [Samra YouTube API](https://samra-youtube-api.antideploy.com/) for **actually downloadable** MP4 qualities and MP3 audio, then displays toggle buttons. Select as many as you like and tap **Done**. The provider can take 1–2 minutes (the bot waits up to 4 minutes per lookup). For a playlist, the bot displays 360p, 480p, 720p, 1080p, 1440p (2K), and 2160p (4K) first; after Done it discovers public video IDs with `yt-dlp` and queries the API per video. Qualities absent from an individual API response are skipped, **not replaced with another quality**. Files are transferred and sent one at a time.

Titles come from YouTube first (oEmbed with a `yt-dlp` backup for single videos, and YouTube's playlist entries for playlists). If neither YouTube method returns a title, the bot uses **that video's API title**; only if that is missing does it use the video ID. The filename and Telegram caption end with the chosen quality, for example `Actual Video Title [480p].mp4` and `Actual Video Title [480p]`; audio is labeled `[MP3]`.

## Koyeb deployment (Docker)

1. Create a Telegram bot with BotFather; keep its bot token secret. Get an **API ID** and **API hash** for your Telegram application from [my.telegram.org](https://my.telegram.org/).
2. Create a Koyeb Web Service from this repository using its `Dockerfile`. Select the free instance and **one replica only** (multiple instances of the same bot can duplicate work).
3. Configure Koyeb **secret environment variables** `BOT_TOKEN`, `API_ID`, and `API_HASH`. These are required. Do not commit them to the repository. **No string session or user account login is needed**: Telethon logs in as the bot using an in-memory MTProto session and your bot token. A restart creates a fresh login session.
4. Use service port **8000** (`PORT` can override it) and the HTTP health check **`/healthz`**. Telegram updates and uploads use MTProto; the web port is only for Koyeb health checks. No webhook is needed.
5. Optionally set `MAX_FILE_MB` (default 2047, clamped to 1–2047) and `MAX_PLAYLIST_VIDEOS` (default 25, clamped to 1–50). Send `/start` to your bot.

### Large uploads and free-tier limits

The bot uses **MTProto rather than the standard Bot API upload endpoint**, so its configurable per-file limit is near Telegram's **2 GiB** limit instead of 50 MB. If a media URL supplies `Content-Length`, the bot streams bounded 512 KiB chunks directly from the provider into Telegram's MTProto upload; it does **not** save the whole video on Koyeb or load it into RAM. It checks the declared and actual size. The transfer must remain accessible throughout the upload. If a provider does **not** supply `Content-Length`, it temporarily spools files up to **64 MiB** to discover their size, and skips larger unknown-size files. Redirects must remain on public HTTPS hosts.

Uploads near 2 GiB can take a long time and consume significant bandwidth; Koyeb's free-tier network, runtime, and resource limits may interrupt them. Success depends on the provider link staying valid and the Telegram servers accepting the media. A single global transfer slot and four waiting jobs bound concurrent resource use. Temporary files are deleted after use; jobs and selections are in memory and are **not restored** after a restart. Playlist API lookups run sequentially and can take a long time. The bot does not transcode or split files.

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
- `/status` — lookup, queue, or transfer status
- `/cancel` — cancel the current lookup/job or clear selection
- `/limits` — current upload and playlist limits

A watch URL containing `list=` is treated as a playlist; remove that parameter to download only the video. `youtu.be` links are treated as single videos. Playlists must be publicly discoverable through `yt-dlp`; keep `yt-dlp` updated if YouTube changes its site.
