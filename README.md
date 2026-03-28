# 📡 Stream Monitor

A self-hosted, no-API-key livestream monitor for **YouTube** and **Twitch**. Runs in Docker, displays live and upcoming streams in a real-time web UI, and can record streams directly to disk.

![Stream Monitor UI](https://i.imgur.com/Ltn1FIL.png)

---

## Features

- **Live detection** — YouTube and Twitch, with concurrent viewer counts
- **Upcoming stream detection** — countdowns, scheduled times, waiting counts
- **Multiple upcoming streams per channel** — shows all scheduled streams, not just the next one
- **Members-only stream support** — detects live and upcoming members-only streams (requires valid cookies)
- **Stream recording** — download live streams via yt-dlp with live chat capture
- **No API keys required** — scrapes YouTube RSS feeds and page HTML directly
- **Cookie authentication** — upload your browser cookies for members-only content
- **Drag-to-reorder** — arrange channels however you like
- **Platform filter** — view YouTube-only or Twitch-only
- **Notifications** — in-browser alerts when a channel goes live or schedules a stream
- **Free chat filter** — configurable list of titles to ignore (free chat rooms, schedule posts, etc.)

---

## Requirements

- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- A browser (Chrome recommended for cookie export)

---

## Quick Start

### 1. Clone the repository

```bash
git clone https://github.com/AndouJJ/Livestream-monitor.git
cd Livestream-monitor
```

### 2. Create required files

Create an empty `channels.json`:
```bash
echo "[]" > channels.json
```

Create a `settings.json` **file** (not a folder — this is important):
```bash
echo "{}" > settings.json
```

Create an empty `cookies.txt`:
```bash
touch cookies.txt
```

Create a `.env` file to set where downloads are saved:
```env
DOWNLOAD_HOST_PATH=./downloads
```

On Windows with a specific drive:
```env
DOWNLOAD_HOST_PATH=D:\recordings
```

### 3. Start the containers

```bash
docker compose up -d --build
```

### 4. Open the UI

Navigate to [http://localhost:5000](http://localhost:5000)

---

## Project Structure

```
stream-monitor/
├── app.py              # Flask backend — all scraping, detection, and API logic
├── index.html          # Frontend UI (served as a static file)
├── Dockerfile          # Container definition
├── docker-compose.yml  # Multi-container setup (monitor + bgutil provider)
├── .env                # Download path configuration
├── channels.json       # Persisted channel list and last-known status
├── settings.json       # Persisted settings (interval, auto-check, etc.)
├── cookies.txt         # YouTube/Twitch browser cookies (Netscape format)
├── downloads/          # Recorded streams saved here
└── logs/
    └── monitor.log     # Application log
```

---

## Configuration

### Adding Channels

In the sidebar, enter a channel handle, URL, or ID:

| Platform | Accepted formats |
|----------|-----------------|
| YouTube  | `@channelname`, full channel URL, or `UC...` channel ID |
| Twitch   | Username or `twitch.tv/username` URL |

### Settings

| Setting | Default | Description |
|---------|---------|-------------|
| Auto-check | Off | Automatically re-check channels on a timer |
| Interval | 90s | How often to check (minimum 30s) |

### Free Chat Filter

Streams whose titles contain any of these strings are ignored and not shown as live or upcoming. Edit `FREE_CHAT_PATTERNS` in `app.py` to add or remove patterns. Current defaults include:

- `free chat`, `freechat`, `フリーチャット`, `フリチャ`
- `🪐🪐🪐`, `🌙FreeしのみんChat🐾`
- `stream schedule`, `スケジュール`
- `壁紙配布中`, `チャットルーム`, `発売中`

### Download Location

Set `DOWNLOAD_HOST_PATH` in your `.env` file:

```env
# Linux / Mac
DOWNLOAD_HOST_PATH=/home/user/streams

# Windows
DOWNLOAD_HOST_PATH=D:\recordings
```

---

## Cookie Authentication

Cookies are required for:
- **Members-only streams** — detecting and recording
- **Reduced bot-detection** — YouTube serves full page content to authenticated sessions

### How to export cookies

1. Install the **[Get cookies.txt LOCALLY](https://chrome.google.com/webstore/detail/get-cookiestxt-locally/cclelndahbckbenkjhflpdbgdldlbecc)** Chrome extension
2. Log into YouTube in your browser (make sure you have membership for any members-only channels)
3. Go to `youtube.com`
4. Click the extension → Export → `youtube.com`
5. Make sure **"Include HttpOnly cookies"** is checked
6. Upload the exported file via **Settings → YouTube Cookies** in the UI

### Verifying cookies are working

Hit `http://localhost:5000/api/diag` — the `cookie_auth` field will show:
- `"logged in as: YourName"` — ✅ working
- `"NOT authenticated"` — ❌ cookies expired or missing key cookies

Cookies typically expire after 1–3 months and will need to be re-exported.

---

## Stream Recording

Click the **Record** button on any live stream card. Downloads are managed in the **Downloads** drawer (top-right button).

### YouTube recording

Uses `yt-dlp` with the `bgutil-ytdlp-pot-provider` plugin for PO token handling. Records:
- Video + audio (best quality MP4)
- Live chat (`.json` subtitle file)
- Thumbnail, description, and metadata

### Twitch recording

Uses `yt-dlp`. Records:
- Video + audio (best quality)
- Rechat subtitles
- Thumbnail, description, and metadata

### Download folder structure

```
downloads/
└── <download-id>/
    ├── Stream Title.mp4
    ├── Stream Title.jpg       # thumbnail
    ├── Stream Title.description
    ├── Stream Title.info.json
    └── chat.live_chat.json    # YouTube live chat
```

---

## API Reference

All endpoints are JSON. Base URL: `http://localhost:5000`

### Channels

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/channels` | List all channels with current status |
| `POST` | `/api/channels` | Add a channel `{"query": "@handle", "platform": "youtube"}` |
| `DELETE` | `/api/channels/<id>?platform=youtube` | Remove a channel |
| `PATCH` | `/api/channels/<id>/rename` | Rename `{"name": "New Name", "platform": "youtube"}` |
| `POST` | `/api/channels/reorder` | Reorder `{"order": ["youtube:UC...", "twitch:user"]}` |
| `POST` | `/api/channels/sort` | Sort by key: `name_asc`, `name_desc`, `live_first`, `platform`, `checked_newest` |

### Checking

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/check` | Trigger an immediate check of all channels |
| `POST` | `/api/check/upcoming` | Force upcoming stream scan for all channels |
| `GET` | `/api/status` | Current auto-check state, interval, last check time |
| `PATCH` | `/api/settings` | Update settings `{"auto_check": true, "interval": 90}` |

### Downloads

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/downloads` | List all active and completed downloads |
| `POST` | `/api/downloads` | Start a download `{"url": "...", "name": "...", "platform": "youtube"}` |
| `DELETE` | `/api/downloads/<id>` | Cancel or clear a download |
| `GET` | `/api/downloads/<id>/log` | Tail of the download process log |
| `GET` | `/api/downloads/<id>/rawlog` | Full raw log file |
| `GET` | `/api/downloads/<id>/files` | List output files |
| `GET` | `/api/downloads/<id>/file/<filename>` | Download a specific output file |

### Diagnostics

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/diag` | Check bgutil server, yt-dlp, and cookie auth status |
| `GET` | `/api/logs?lines=N` | Last N lines of the application log |
| `GET` | `/api/debug/channel/<id>` | Full signal dump for a channel (live detection debugging) |
| `GET` | `/api/cookies` | Cookie file status |
| `GET` | `/api/cookies/debug` | Which auth cookies are present |
| `POST` | `/api/cookies` | Upload a new cookies.txt |
| `DELETE` | `/api/cookies` | Remove cookies |

### Channel Status Fields

Each channel in `/api/channels` includes a `last_status` object:

```json
{
  "is_live": true,
  "is_upcoming": false,
  "is_waiting": false,
  "video_id": "dQw4w9WgXcW",
  "title": "Stream title",
  "url": "https://youtube.com/watch?v=...",
  "started_at": "2026-03-28T03:31:54+00:00",
  "scheduled_at": null,
  "viewer_count": 12345,
  "error": null
}
```

And an `upcoming_statuses` array for additional scheduled streams on the same channel.

---

## Live Detection Methods

The monitor uses up to 6 methods to detect live and upcoming streams, in order:

1. **JSON-LD BroadcastEvent** — fastest; YouTube embeds structured data for live streams
2. **ytInitialPlayerResponse** — parses the full player JSON for `isLive` and `isUpcoming` flags
3. **upcomingEventData** — scans the page for scheduled stream timestamps
4. **Raw isUpcoming scan** — fallback raw search for upcoming signals
5. **RSS feed fallback** — checks the last 5 RSS entries' watch pages for live/upcoming
6. **Membership tab** — for channels with a free-chat placeholder blocking the `/live` page

For multiple upcoming streams, `_fetch_all_upcoming` additionally scans:
- RSS feed entries
- The channel's `/live` page (members-only upcoming)
- The channel's `/streams` tab

---

## Limitations

### Detection

- **Members-only streams** are only detectable if:
  - Your cookies are valid and the account has an active membership
  - YouTube surfaces the stream on the `/live` page or membership tab (not guaranteed)
  - First-time detection requires the stream to appear on `/live` — subsequent checks use the stored video ID

- **Multiple upcoming streams** — only found when the channel's `/streams` tab or RSS feed contains them. Streams announced very recently may not appear until YouTube indexes them.

- **Free-chat placeholders** on the `/live` page block detection of actual live streams. The monitor attempts to work around this via the membership tab and stored video ID fallback, but first-time detection may fail.

- **Twitch** has no upcoming stream detection (no public API or structured data available via scraping).

### Recording

- **Members-only recording** requires valid cookies with an active membership.
- **YouTube PO tokens** are handled automatically by the bgutil provider — if the provider is down, YouTube recordings may fail.
- **Live-from-start** recording (`--live-from-start` flag) may miss the first few seconds of a stream that is already in progress when recording starts.

### Rate Limiting

YouTube will rate-limit the scraper if too many requests are made in a short time. The monitor mitigates this by:
- Throttling the upcoming stream scan to once every 10 check cycles (~15 minutes per channel)
- Staggering the scan across channels so only ~4 channels scan per cycle
- Adding 0.3–0.5s delays between watch page requests

If you experience rate limiting, increase the check interval in Settings.

### Cookies

- Cookies expire every 1–3 months and must be re-exported from your browser.
- The `shortViewCount` viewer/waiting count requires cookies — anonymous requests may return different page structures.

---

## Troubleshooting

### `settings.json Is a Directory` error

Docker created `settings.json` as a directory instead of a file. Fix:

```bash
docker compose down
# Windows:
Remove-Item -Recurse -Force ".\settings.json"
# Linux/Mac:
rm -rf settings.json
echo "{}" > settings.json
docker compose up -d
```

### Channels showing as offline when they're live

1. Check `http://localhost:5000/api/debug/channel/<channel-id>` for signal details
2. Check `http://localhost:5000/api/diag` to verify cookies are authenticated
3. Check `http://localhost:5000/api/logs?lines=100` for error messages

### Members-only streams not detected

1. Verify cookies are valid: `http://localhost:5000/api/diag` should show `"logged in as: ..."`
2. Verify all auth cookies are present: `http://localhost:5000/api/cookies/debug` — `auth_ok` should be `true`
3. Re-export cookies from your browser if they're expired

### Downloads failing

1. Check `http://localhost:5000/api/diag` — bgutil should show `"bgutil_ping": 200`
2. For members-only: ensure cookies are valid and the account has membership
3. Check the raw log: `http://localhost:5000/api/downloads/<id>/rawlog`

### Force an upcoming stream scan

```powershell
Invoke-WebRequest -Method POST -Uri http://localhost:5000/api/check/upcoming
```

---

## Architecture

```
┌─────────────────────────────────────────────┐
│  Docker Compose                             │
│                                             │
│  ┌─────────────────┐  ┌──────────────────┐  │
│  │   yt-monitor    │  │ bgutil-provider  │  │
│  │  (Flask :5000)  │◄─│    (:4416)       │  │
│  │                 │  │  PO Token gen    │  │
│  │  - Scraper      │  └──────────────────┘  │
│  │  - API          │                         │
│  │  - yt-dlp       │                         │
│  └─────────────────┘                         │
│          │                                   │
│    ┌─────▼──────┐                            │
│    │  Volumes   │                            │
│    │ channels.json                           │
│    │ cookies.txt                             │
│    │ downloads/ │                            │
│    │ logs/      │                            │
│    └────────────┘                            │
└─────────────────────────────────────────────┘
```

The backend is a single Flask process with a background thread for auto-checking. Channel checks run concurrently via `ThreadPoolExecutor` (10 workers). All state is persisted to `channels.json`.

---

## Tech Stack

- **Backend**: Python 3.12, Flask, requests
- **Frontend**: Vanilla JS, HTML/CSS (no framework)
- **Recording**: yt-dlp + bgutil-ytdlp-pot-provider
- **Container**: Docker + Docker Compose

---

## License

MIT
