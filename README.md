# YouTube downloader Telegram bot

Send the bot a YouTube video or public playlist URL. For a video, it asks the [Samra YouTube API](https://samra-youtube-api.antideploy.com/) for **actually downloadable** MP4 qualities and MP3 audio, then displays toggle buttons. Select as many as you like and tap **Done**. The provider can take 1–2 minutes (the bot waits up to 4 minutes for each lookup). For a playlist, the bot displays 360p, 480p, 720p, 1080p, 1440p (2K), and 2160p (4K) first; after Done it discovers the public playlist's video IDs with `yt-dlp` and queries the API for each video. Qualities absent from an individual API response are skipped, **not replaced with a different quality**. Each selected format is downloaded to a temporary file, uploaded, and removed before the next one begins. Titles come from YouTube first (oEmbed with a `yt-dlp` backup for single videos, and YouTube's playlist entries for playlists). If neither YouTube metadata method returns a title for a video, the bot uses **that video's API title**; only if that is also missing does it use the video ID. The uploaded filename and Telegram caption end with the selected quality, for example `Actual Video Title [480p].mp4` and `Actual Video Title [480p]`; audio is labeled `[MP3]`.

Only use this for media you have permission to download. This is a private-chat bot; it does not bypass unavailable/private/age-restricted videos or guarantee the external API or YouTube will be reachable from your hosting region.

## Deploy to Koyeb (Docker)

1. Create a Telegram bot with BotFather and keep its token secret.
2. Create a Koyeb **Web Service** from this GitHub repository, using the included `Dockerfile`. Choose the free instance and **one replica only** (multiple polling instances sharing a token conflict).
3. Set the secret environment variable `BOT_TOKEN` to your bot token. Set the service port to **8000** (`PORT` can override it) and HTTP health check to **`/healthz`**. The server binds to `0.0.0.0` while Telegram updates are received by long polling; you do not need a webhook or public Telegram endpoint.
4. Optionally set `MAX_FILE_MB` (default 47, clamped to 1–47) and `MAX_PLAYLIST_VIDEOS` (default 25, clamped to 1–50). The first N playlist entries are processed; raise N cautiously on a free instance. Send `/start` to the bot.

Telegram's standard Bot API limits bot uploads to 50 MB. Files exceeding the configured limit are skipped; the bot **does not transcode or split** them. A global single-transfer slot and at most four waiting jobs keep disk, bandwidth, and memory use bounded. Each user can have one lookup/job at a time. Files live only in temporary storage. Pending jobs and selections are in memory, so a Koyeb restart/sleep interrupts them; send the link again afterwards. A large playlist may take a long time because the API is queried sequentially for each video.

## Run locally

Python 3.11+ is recommended:

```sh
python -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
export BOT_TOKEN='your-token-from-BotFather'
python -m ytdown
```

To run offline unit tests (no Telegram token or API calls needed):

```sh
python -m unittest discover -s tests -v
```

## Commands

- `/start` or `/help` — usage instructions
- `/status` — lookup / queue / transfer status
- `/cancel` — cancel the current lookup/job or clear selection
- `/limits` — current upload and playlist limits

A watch URL containing `list=` is treated as a playlist; remove that parameter to download only the video. `youtu.be` links are treated as single videos. Playlist videos must be publicly discoverable through `yt-dlp`; keep `yt-dlp` updated if YouTube changes its site.
