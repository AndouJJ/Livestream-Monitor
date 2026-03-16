#!/usr/bin/env python3
"""
Livestream Monitor — YouTube + Twitch
Method: YouTube RSS + JSON-LD scrape / Twitch page scrape (no API keys)
Install: pip install flask requests
Run:     python app.py
Open:    http://localhost:5000
"""

import json
import logging
import os
import re
import threading
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

import requests
from flask import Flask, jsonify, request, send_from_directory

# ─── Logging setup ────────────────────────────────────────────────────────────
LOG_FILE = Path("logs/monitor.log")
LOG_FILE.parent.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(),          # keep printing to Docker logs too
    ],
)
log = logging.getLogger("monitor")

app = Flask(__name__, static_folder="static")

DATA_FILE        = "channels.json"
DEFAULT_INTERVAL = 90
REQ_TIMEOUT      = 15

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

# ─── Shared state ─────────────────────────────────────────────────────────────

SETTINGS_FILE = Path("settings.json")

def _load_settings() -> dict:
    if SETTINGS_FILE.exists():
        try:
            return json.loads(SETTINGS_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}

def _save_settings():
    SETTINGS_FILE.write_text(json.dumps({
        "auto_check":    state["auto_check"],
        "interval":      state["interval"],
        "download_dir":  state["download_dir"],
    }, indent=2), encoding="utf-8")

_saved = _load_settings()

state = {
    "channels":          [],
    "auto_check":        _saved.get("auto_check", True),
    "interval":          _saved.get("interval", DEFAULT_INTERVAL),
    "download_dir":      _saved.get("download_dir", os.environ.get("DOWNLOAD_BASE_DIR", "downloads")),
    "last_global_check": None,
    "_last_auto_ts":     0,
}
_lock = threading.Lock()

# ── Notification event queue (live-goes-live + download-done) ─────────────
import collections
_notif_queue: collections.deque = collections.deque(maxlen=200)
_notif_lock = threading.Lock()

def _push_notif(kind: str, title: str, body: str, url: str = ""):
    with _notif_lock:
        _notif_queue.append({
            "id":    f"{kind}_{int(time.time()*1000)}",
            "kind":  kind,   # "live" | "download_done" | "download_error"
            "title": title,
            "body":  body,
            "url":   url,
            "ts":    datetime.now(timezone.utc).isoformat(),
        })

# ─── Persistence ──────────────────────────────────────────────────────────────

def load_channels() -> list:
    if Path(DATA_FILE).exists():
        with open(DATA_FILE, encoding="utf-8") as f:
            return json.load(f)
    return []


def save_channels():
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(state["channels"], f, indent=2, ensure_ascii=False)


# ─── Free chat filter ─────────────────────────────────────────────────────────

FREE_CHAT_PATTERNS = [
    "free chat", "freechat",
    "フリーチャット", "ふりーちゃっと", "フリチャ",
    "ぽたく集会所", "フリフリチャット", "メン限壁紙配布中",
    "🌙FreeしのみんChat🐾",
]

def _is_free_chat(title: str) -> bool:
    if not title:
        return False
    t = title.lower()
    return any(p.lower() in t for p in FREE_CHAT_PATTERNS)


# ─── YouTube ──────────────────────────────────────────────────────────────────

RSS_NS = "http://www.w3.org/2005/Atom"

def _fetch_rss(channel_id: str):
    try:
        url = f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"
        r = requests.get(url, headers=HEADERS, timeout=REQ_TIMEOUT)
        r.raise_for_status()
        return ET.fromstring(r.text)
    except Exception:
        return None


def _fetch_youtube_avatar(channel_id: str, html: str = None) -> str:
    """Scrape YouTube channel page for the avatar URL."""
    try:
        if html is None:
            r = requests.get(f"https://www.youtube.com/channel/{channel_id}",
                             headers=HEADERS, timeout=REQ_TIMEOUT)
            html = r.text
        m = (re.search(r'"avatar"\s*:\s*\{"thumbnails"\s*:\s*\[.*?"url"\s*:\s*"([^"]+)"', html, re.DOTALL)
          or re.search(r'<meta[^>]+property="og:image"[^>]+content="([^"]+)"', html)
          or re.search(r'"url"\s*:\s*"(https://yt3\.ggpht\.com/[^"]+)"', html))
        if m:
            url = m.group(1).split("=")[0] + "=s240-c-k-c0x00ffffff-no-rj"
            return url
    except Exception:
        pass
    return ""


def _fetch_twitch_avatar(login: str) -> str:
    """Scrape Twitch channel page for the avatar URL."""
    try:
        r    = requests.get(f"https://www.twitch.tv/{login}", headers=HEADERS, timeout=REQ_TIMEOUT)
        html = r.text
        m = re.search(r'<meta[^>]+property="og:image"[^>]+content="([^"]+)"', html)
        if m:
            return m.group(1)
        m = re.search(r'"profile_image_url"\s*:\s*"([^"]+)"', html)
        if m:
            return m.group(1).replace("\\u003D", "=")
    except Exception:
        pass
    return ""


def resolve_youtube(query: str) -> tuple:
    """Return (channel_id, display_name, avatar_url) for a YouTube channel."""
    query = query.strip()

    if re.match(r"^UC[\w-]{22}$", query):
        tree = _fetch_rss(query)
        if tree is not None:
            el     = tree.find(f"{{{RSS_NS}}}title")
            name   = el.text if el is not None else query
            avatar = _fetch_youtube_avatar(query)
            return query, name, avatar
        raise ValueError(f"YouTube channel ID not found: {query}")

    if query.startswith("http"):
        url = query.rstrip("/")
    elif query.startswith("@"):
        url = f"https://www.youtube.com/{query}"
    else:
        url = f"https://www.youtube.com/@{query}"

    try:
        r = requests.get(url, headers=HEADERS, timeout=REQ_TIMEOUT)
        r.raise_for_status()
        html = r.text
    except Exception as e:
        raise ValueError(f"Could not fetch YouTube channel page: {e}")

    m = (re.search(r'"externalId"\s*:\s*"(UC[\w-]{22})"', html)
      or re.search(r'channel_id=(UC[\w-]{22})', html)
      or re.search(r'/channel/(UC[\w-]{22})', html))

    if not m:
        raise ValueError(
            f"Could not extract channel ID from: {url}\n"
            "Try pasting the full youtube.com/channel/UC… URL."
        )

    ch_id  = m.group(1)
    tree   = _fetch_rss(ch_id)
    if tree is None:
        raise ValueError(f"RSS feed unavailable for channel ID: {ch_id}")
    el     = tree.find(f"{{{RSS_NS}}}title")
    name   = el.text if el is not None else ch_id
    avatar = _fetch_youtube_avatar(ch_id, html)
    return ch_id, name, avatar


def _base_status(is_live=False, is_upcoming=False, **kw) -> dict:
    """Return a fully-populated status dict with safe defaults."""
    return {
        "is_live":      is_live,
        "is_upcoming":  is_upcoming,
        "video_id":     kw.get("video_id"),
        "title":        kw.get("title"),
        "url":          kw.get("url"),
        "started_at":   kw.get("started_at"),
        "scheduled_at": kw.get("scheduled_at"),
        "error":        kw.get("error"),
    }


def _yt_parse_initial_player(html: str) -> dict:
    """
    Extract ytInitialPlayerResponse robustly.
    YouTube embeds it as a large JSON object; the closing pattern varies,
    so we use a JSON decoder that stops at the first complete object.
    """
    m = re.search(r"ytInitialPlayerResponse\s*=\s*(\{)", html)
    if not m:
        return {}
    try:
        dec = json.JSONDecoder()
        obj, _ = dec.raw_decode(html, m.start(1))
        return obj
    except Exception:
        return {}


def _yt_parse_initial_data(html: str) -> dict:
    """Extract ytInitialData robustly."""
    m = re.search(r"(?:var\s+ytInitialData|ytInitialData)\s*=\s*(\{)", html)
    if not m:
        return {}
    try:
        dec = json.JSONDecoder()
        obj, _ = dec.raw_decode(html, m.start(1))
        return obj
    except Exception:
        return {}


def check_youtube_live(channel_id: str) -> dict:
    """Scrape YouTube /live page for live and upcoming stream status."""
    live_url = f"https://www.youtube.com/channel/{channel_id}/live"
    try:
        r    = requests.get(live_url, headers=HEADERS, timeout=REQ_TIMEOUT)
        html = r.text

        upcoming_candidate = None

        # ── Method 1: JSON-LD BroadcastEvent ─────────────────────────────
        for block in re.findall(
            r'<script[^>]+type="application/ld\+json"[^>]*>(.*?)</script>',
            html, re.DOTALL
        ):
            try:
                data  = json.loads(block)
                items = data if isinstance(data, list) else [data]
                for item in items:
                    if item.get("@type") != "BroadcastEvent":
                        continue
                    title      = item.get("name") or ""
                    start_date = item.get("startDate") or ""
                    vid_m      = re.search(r"(?:v=|youtu\.be/)([\w-]{11})", item.get("url", ""))
                    video_id   = vid_m.group(1) if vid_m else None
                    if _is_free_chat(title):
                        continue
                    if item.get("isLiveBroadcast"):
                        return _base_status(
                            is_live=True, video_id=video_id, title=title,
                            url=f"https://youtube.com/watch?v={video_id}" if video_id else None,
                            started_at=start_date or None,
                        )
                    # isLiveBroadcast=false + future startDate = upcoming
                    if start_date and upcoming_candidate is None:
                        try:
                            sched = datetime.fromisoformat(start_date.replace("Z", "+00:00"))
                            if sched > datetime.now(timezone.utc):
                                upcoming_candidate = _base_status(
                                    is_upcoming=True, video_id=video_id, title=title,
                                    url=f"https://youtube.com/watch?v={video_id}" if video_id else None,
                                    scheduled_at=start_date,
                                )
                        except Exception:
                            pass
            except (json.JSONDecodeError, AttributeError):
                continue

        # ── Method 2: ytInitialPlayerResponse (robust JSON parse) ────────
        ipr = _yt_parse_initial_player(html)
        if ipr:
            vs  = ipr.get("videoDetails", {})
            mf  = ipr.get("microformat", {}).get("playerMicroformatRenderer", {})
            lbd = mf.get("liveBroadcastDetails", {})
            title    = vs.get("title") or ""
            video_id = vs.get("videoId")
            url_v    = f"https://youtube.com/watch?v={video_id}" if video_id else None

            if not _is_free_chat(title):
                if vs.get("isLive"):
                    return _base_status(
                        is_live=True, video_id=video_id, title=title, url=url_v,
                        started_at=lbd.get("startTimestamp"),
                    )
                # isUpcoming is a boolean YouTube sets explicitly on scheduled streams
                if vs.get("isUpcoming") and upcoming_candidate is None:
                    sched = lbd.get("startTimestamp")
                    if sched:
                        try:
                            t = datetime.fromisoformat(sched.replace("Z", "+00:00"))
                            if t > datetime.now(timezone.utc):
                                upcoming_candidate = _base_status(
                                    is_upcoming=True, video_id=video_id,
                                    title=title, url=url_v, scheduled_at=sched,
                                )
                        except Exception:
                            pass

        # ── Method 3: ytInitialData upcomingEventData ─────────────────────
        # upcomingEventData contains startTime as a unix timestamp string.
        # We search for it by finding "upcomingEventData" then scanning
        # the next 500 chars for "startTime" — avoids [^}]* nesting bugs.
        if upcoming_candidate is None and "upcomingEventData" in html:
            idx = html.find('"upcomingEventData"')
            while idx != -1:
                chunk = html[idx:idx + 500]
                ts_m = re.search(r'"startTime"\s*:\s*"(\d{9,11})"', chunk)
                if ts_m:
                    start_ts = int(ts_m.group(1))
                    if start_ts > int(datetime.now(timezone.utc).timestamp()):
                        sched_iso = datetime.fromtimestamp(start_ts, tz=timezone.utc).isoformat()
                        # Get videoId — first one found in page (the scheduled video)
                        vid_m2 = re.search(r'"videoId"\s*:\s*"([\w-]{11})"', html)
                        vid2   = vid_m2.group(1) if vid_m2 else None
                        # Title from ytInitialPlayerResponse (already parsed) or og:title
                        title2 = ""
                        if ipr:
                            title2 = (ipr.get("videoDetails", {}).get("title") or "")
                        if not title2:
                            og_m = re.search(
                                r'<meta[^>]+property="og:title"[^>]+content="([^"]+)"', html)
                            if og_m:
                                import html as _hm2
                                title2 = _hm2.unescape(og_m.group(1))
                        if not _is_free_chat(title2):
                            upcoming_candidate = _base_status(
                                is_upcoming=True, video_id=vid2, title=title2,
                                url=f"https://youtube.com/watch?v={vid2}" if vid2 else None,
                                scheduled_at=sched_iso,
                            )
                        break
                idx = html.find('"upcomingEventData"', idx + 1)

        # ── Method 4: raw "isUpcoming":true scan ─────────────────────────
        # Last resort: YouTube sometimes embeds isUpcoming as a plain bool
        # in various JSON blobs. If we see it, scrape startTimestamp nearby.
        if upcoming_candidate is None and (
            '"isUpcoming":true' in html or '"isUpcoming": true' in html
        ):
            # Find startTimestamp within 2000 chars of isUpcoming
            for pat in ('"isUpcoming":true', '"isUpcoming": true'):
                idx = html.find(pat)
                if idx == -1:
                    continue
                chunk = html[max(0, idx - 500):idx + 1500]
                ts_m = re.search(r'"startTimestamp"\s*:\s*"([^"]+)"', chunk)
                vid_m3 = re.search(r'"videoId"\s*:\s*"([\w-]{11})"', chunk)
                if ts_m:
                    sched_raw = ts_m.group(1)
                    try:
                        t = datetime.fromisoformat(sched_raw.replace("Z", "+00:00"))
                        if t > datetime.now(timezone.utc):
                            vid3   = vid_m3.group(1) if vid_m3 else None
                            title3 = (ipr.get("videoDetails", {}).get("title") or "") if ipr else ""
                            if not title3:
                                og_m = re.search(
                                    r'<meta[^>]+property="og:title"[^>]+content="([^"]+)"', html)
                                if og_m:
                                    import html as _hm3
                                    title3 = _hm3.unescape(og_m.group(1))
                            if not _is_free_chat(title3):
                                upcoming_candidate = _base_status(
                                    is_upcoming=True, video_id=vid3, title=title3,
                                    url=f"https://youtube.com/watch?v={vid3}" if vid3 else None,
                                    scheduled_at=sched_raw,
                                )
                                break
                    except Exception:
                        pass

        if upcoming_candidate:
            log.info(
                f"[live-check] YouTube {channel_id}: UPCOMING "
                f"{upcoming_candidate.get('title','')!r} @ {upcoming_candidate.get('scheduled_at','')}"
            )
            return upcoming_candidate

        # ── Method 5: RSS feed fallback ───────────────────────────────────
        # When /live doesn't redirect to the upcoming stream, probe the
        # last few RSS entries directly for isUpcoming signals.
        rss_tree = _fetch_rss(channel_id)
        if rss_tree:
            yt_ns   = "http://www.youtube.com/xml/schemas/2015"
            entries = rss_tree.findall(f"{{{RSS_NS}}}entry")[:5]
            for entry in entries:
                vid_el = entry.find(f"{{{yt_ns}}}videoId")
                if vid_el is None:
                    continue
                vid_id = vid_el.text
                try:
                    vr    = requests.get(f"https://www.youtube.com/watch?v={vid_id}",
                                         headers=HEADERS, timeout=REQ_TIMEOUT)
                    vhtml = vr.text
                    # Quick guard — skip if no upcoming signals at all
                    if ('"isUpcoming":true' not in vhtml and
                            '"isUpcoming": true' not in vhtml and
                            '"upcomingEventData"' not in vhtml):
                        continue
                    vipr = _yt_parse_initial_player(vhtml)
                    if not vipr:
                        continue
                    vvs  = vipr.get("videoDetails", {})
                    vmf  = vipr.get("microformat", {}).get("playerMicroformatRenderer", {})
                    vlbd = vmf.get("liveBroadcastDetails", {})
                    vtitle = vvs.get("title") or ""
                    if _is_free_chat(vtitle):
                        continue
                    if vvs.get("isLive"):
                        return _base_status(
                            is_live=True, video_id=vid_id, title=vtitle,
                            url=f"https://youtube.com/watch?v={vid_id}",
                            started_at=vlbd.get("startTimestamp"),
                        )
                    if vvs.get("isUpcoming"):
                        sched = vlbd.get("startTimestamp")
                        if sched:
                            try:
                                t = datetime.fromisoformat(sched.replace("Z", "+00:00"))
                                if t > datetime.now(timezone.utc):
                                    log.info(
                                        f"[live-check] YouTube {channel_id}: UPCOMING (RSS) "
                                        f"{vtitle!r} @ {sched}"
                                    )
                                    return _base_status(
                                        is_upcoming=True, video_id=vid_id,
                                        title=vtitle,
                                        url=f"https://youtube.com/watch?v={vid_id}",
                                        scheduled_at=sched,
                                    )
                            except Exception:
                                pass
                    # Also check upcomingEventData in the watch page
                    if '"upcomingEventData"' in vhtml:
                        idx = vhtml.find('"upcomingEventData"')
                        chunk = vhtml[idx:idx + 500]
                        ts_m = re.search(r'"startTime"\s*:\s*"(\d{9,11})"', chunk)
                        if ts_m:
                            start_ts = int(ts_m.group(1))
                            if start_ts > int(datetime.now(timezone.utc).timestamp()):
                                sched_iso = datetime.fromtimestamp(start_ts, tz=timezone.utc).isoformat()
                                if not _is_free_chat(vtitle):
                                    log.info(
                                        f"[live-check] YouTube {channel_id}: UPCOMING (RSS/upcomingEventData) "
                                        f"{vtitle!r} @ {sched_iso}"
                                    )
                                    return _base_status(
                                        is_upcoming=True, video_id=vid_id,
                                        title=vtitle,
                                        url=f"https://youtube.com/watch?v={vid_id}",
                                        scheduled_at=sched_iso,
                                    )
                except Exception:
                    continue

        return _base_status()

    except requests.Timeout:
        return _base_status(error="timeout")
    except Exception as e:
        log.warning(f"[live-check] YouTube error for {channel_id}: {e}")
        return _base_status(error=str(e)[:80])


# ─── Twitch ───────────────────────────────────────────────────────────────────

def resolve_twitch(query: str) -> tuple:
    """
    Return (login_name, display_name) for a Twitch channel.
    Accepts: twitch.tv/username or plain username.
    """
    import html as html_mod

    query = query.strip().rstrip("/")

    # Extract username from URL
    m = re.search(r"twitch\.tv/([A-Za-z0-9_]+)", query)
    login = m.group(1) if m else query.lstrip("@")

    if not re.match(r"^[A-Za-z0-9_]{1,25}$", login):
        raise ValueError(f"Invalid Twitch username: {login!r}")

    # Use login as display name (scraping returns garbled CJK)
    # Fetch avatar separately via the avatar helper
    avatar = _fetch_twitch_avatar(login)
    return login.lower(), login, avatar


def check_twitch_live(login: str) -> dict:
    """
    Scrape Twitch channel page for live status.
    Only trusts explicit isLiveBroadcast signals — avoids false positives
    from offline pages that say 'Watch <name> live' in their description.
    """
    url = f"https://www.twitch.tv/{login}"
    try:
        r    = requests.get(url, headers=HEADERS, timeout=REQ_TIMEOUT)
        html = r.content.decode("utf-8", errors="replace")  # bypass charset auto-detect

        # Method 1: JSON-LD BroadcastEvent / VideoObject with isLiveBroadcast
        for block in re.findall(
            r'<script[^>]+type="application/ld\+json"[^>]*>(.*?)</script>',
            html, re.DOTALL
        ):
            try:
                data  = json.loads(block)
                items = data if isinstance(data, list) else [data]
                for item in items:
                    if item.get("@type") in ("VideoObject", "BroadcastEvent"):
                        pub = item.get("publication", {})
                        if pub.get("isLiveBroadcast") or item.get("isLiveBroadcast"):
                            title = item.get("name") or item.get("description", "")
                            return _base_status(
                                is_live=True,
                                title=title,
                                url=f"https://twitch.tv/{login}",
                            )
            except (json.JSONDecodeError, AttributeError):
                continue

        # Method 2: Raw JSON blob signals.
        # "isLiveBroadcast":true  — schema.org signal (present when live)
        # "isLive":true           — Twitch-specific stream state
        # Both are only present when the channel is actively streaming.
        is_live_signal = (
            '"isLiveBroadcast":true' in html
            or '"isLiveBroadcast": true' in html
            or '"isLive":true' in html
            or '"isLive": true' in html
        )
        if is_live_signal:
            import html as _hm
            m     = re.search(r'<meta[^>]+property="og:title"[^>]+content="([^"]+)"', html)
            title = _hm.unescape(m.group(1)) if m else ""
            ma = re.search(r'"startedAt"\s*:\s*"([^"]+)"', html)
            started_at = ma.group(1) if ma else None
            return _base_status(
                is_live=True,
                title=title,
                url=f"https://twitch.tv/{login}",
                started_at=started_at,
            )

        return _base_status()

    except requests.Timeout:
        return _base_status(error="timeout")
    except Exception as e:
        log.warning(f"[live-check] Twitch error for {login}: {e}")
        return _base_status(error=str(e)[:80])


# ─── Unified resolve / check ──────────────────────────────────────────────────

def resolve_channel(query: str, platform: str) -> tuple:
    """Return (channel_id, display_name, avatar_url) for the given platform."""
    if platform == "twitch":
        return resolve_twitch(query)
    return resolve_youtube(query)


def check_live(ch: dict) -> dict:
    if ch.get("platform") == "twitch":
        return check_twitch_live(ch["channel_id"])
    return check_youtube_live(ch["channel_id"])


def check_all_channels():
    with _lock:
        channels = list(state["channels"])
    for ch in channels:
        prev          = ch.get("last_status", {})
        was_live      = prev.get("is_live", False)
        was_upcoming  = prev.get("is_upcoming", False)
        prev_started  = prev.get("started_at")
        status        = check_live(ch)

        if status.get("is_live"):
            if not was_live:
                # offline/upcoming → live: stamp start time if scraper didn't get it
                if not status.get("started_at"):
                    status["started_at"] = datetime.now(timezone.utc).isoformat()
            elif prev_started and not status.get("started_at"):
                status["started_at"] = prev_started

        ch["last_status"]  = status
        ch["last_checked"] = datetime.now(timezone.utc).isoformat()

        # Notification: any transition to live (offline→live or upcoming→live)
        if not was_live and status.get("is_live"):
            name  = ch.get("name", ch.get("channel_id", ""))
            title = status.get("title") or ""
            url   = status.get("url") or ""
            _push_notif("live", f"🔴 {name} is live",
                        title or "Started streaming", url)
        # Notification: new upcoming stream detected
        elif not was_upcoming and status.get("is_upcoming"):
            name     = ch.get("name", ch.get("channel_id", ""))
            title    = status.get("title") or ""
            sched    = status.get("scheduled_at") or ""
            _push_notif("upcoming", f"📅 {name} scheduled a stream",
                        title, status.get("url") or "")
    with _lock:
        state["last_global_check"] = datetime.now(timezone.utc).isoformat()
    save_channels()


# ─── Auto-checker thread ──────────────────────────────────────────────────────

def auto_check_loop():
    while True:
        time.sleep(1)
        if state["auto_check"] and state["channels"]:
            if time.time() - state["_last_auto_ts"] >= state["interval"]:
                state["_last_auto_ts"] = time.time()
                check_all_channels()


threading.Thread(target=auto_check_loop, daemon=True).start()


# ─── Flask routes ─────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory("static", "index.html")


@app.route("/api/avatar")
def avatar_proxy():
    """Proxy remote avatar images to avoid browser CORS blocks."""
    from flask import Response
    from urllib.parse import urlparse
    url = request.args.get("url", "").strip()
    if not url:
        return "", 400
    allowed = ("yt3.googleusercontent.com", "yt3.ggpht.com",
               "static-cdn.jtvnw.net", "pbs.twimg.com")
    host = urlparse(url).netloc
    if not any(host.endswith(d) for d in allowed):
        return "", 403
    try:
        r = requests.get(url, headers=HEADERS, timeout=10, stream=True)
        return Response(
            r.content,
            content_type=r.headers.get("Content-Type", "image/jpeg"),
            headers={"Cache-Control": "public, max-age=86400"},
        )
    except Exception:
        return "", 502


@app.route("/api/status")
def api_status():
    return jsonify({
        "auto_check":        state["auto_check"],
        "interval":          state["interval"],
        "download_dir":      state["download_dir"],
        "download_base":     str(DOWNLOAD_DIR),
        "last_global_check": state.get("last_global_check"),
        "channel_count":     len(state["channels"]),
    })


@app.route("/api/channels", methods=["GET"])
def api_get_channels():
    return jsonify(state["channels"])


@app.route("/api/channels", methods=["POST"])
def api_add_channel():
    data     = request.json or {}
    query    = (data.get("query") or "").strip()
    platform = (data.get("platform") or "youtube").strip().lower()
    if not query:
        return jsonify({"error": "query is required"}), 400
    if platform not in ("youtube", "twitch"):
        return jsonify({"error": "platform must be 'youtube' or 'twitch'"}), 400

    try:
        ch_id, ch_name, avatar = resolve_channel(query, platform)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    with _lock:
        if any(c["channel_id"] == ch_id and c.get("platform", "youtube") == platform
               for c in state["channels"]):
            return jsonify({"error": f"Already added: {ch_name}"}), 409

        label = (data.get("label") or ch_name).strip() or ch_name
        ch = {
            "channel_id":   ch_id,
            "name":         label,
            "platform":     platform,
            "avatar":       avatar,
            "last_checked": None,
            "last_status":  {},
        }
        state["channels"].append(ch)
        save_channels()
    return jsonify(ch), 201


@app.route("/api/channels/<path:channel_id>", methods=["DELETE"])
def api_remove_channel(channel_id):
    platform = request.args.get("platform", "youtube")
    with _lock:
        before = len(state["channels"])
        state["channels"] = [
            c for c in state["channels"]
            if not (c["channel_id"] == channel_id and c.get("platform", "youtube") == platform)
        ]
        if len(state["channels"]) == before:
            return jsonify({"error": "Not found"}), 404
        save_channels()
    return jsonify({"ok": True})


@app.route("/api/channels/<path:channel_id>/rename", methods=["PATCH"])
def api_rename(channel_id):
    data     = request.json or {}
    name     = (data.get("name") or "").strip()
    platform = data.get("platform", "youtube")
    if not name:
        return jsonify({"error": "name is required"}), 400
    with _lock:
        for ch in state["channels"]:
            if ch["channel_id"] == channel_id and ch.get("platform", "youtube") == platform:
                ch["name"] = name
                save_channels()
                return jsonify(ch)
    return jsonify({"error": "Not found"}), 404


@app.route("/api/channels/reorder", methods=["POST"])
def api_reorder():
    data        = request.json or {}
    ordered_ids = data.get("order", [])  # list of "platform:channel_id"
    with _lock:
        id_map = {f"{c.get('platform','youtube')}:{c['channel_id']}": c
                  for c in state["channels"]}
        reordered = [id_map[k] for k in ordered_ids if k in id_map]
        seen = set(ordered_ids)
        for c in state["channels"]:
            k = f"{c.get('platform','youtube')}:{c['channel_id']}"
            if k not in seen:
                reordered.append(c)
        state["channels"] = reordered
        save_channels()
    return jsonify({"ok": True})


@app.route("/api/channels/sort", methods=["POST"])
def api_sort():
    data = request.json or {}
    key  = data.get("key", "name_asc")
    with _lock:
        if key == "name_asc":
            state["channels"].sort(key=lambda c: c["name"].lower())
        elif key == "name_desc":
            state["channels"].sort(key=lambda c: c["name"].lower(), reverse=True)
        elif key == "live_first":
            state["channels"].sort(
                key=lambda c: not c.get("last_status", {}).get("is_live", False))
        elif key == "platform":
            state["channels"].sort(key=lambda c: c.get("platform", "youtube"))
        elif key == "checked_newest":
            state["channels"].sort(
                key=lambda c: c.get("last_checked") or "", reverse=True)
        save_channels()
    return jsonify(state["channels"])


@app.route("/api/check", methods=["POST"])
def api_check_now():
    threading.Thread(target=check_all_channels, daemon=True).start()
    return jsonify({"ok": True})


@app.route("/api/settings", methods=["PATCH"])
def api_settings():
    data = request.json or {}
    if "auto_check" in data:
        state["auto_check"] = bool(data["auto_check"])
    if "interval" in data:
        iv = int(data["interval"])
        if iv < 30:
            return jsonify({"error": "Minimum interval is 30 seconds"}), 400
        state["interval"] = iv
    if "download_dir" in data:
        d = str(data["download_dir"]).strip().strip("/\\")
        if not d:
            # Empty = reset to base
            d = ""
        # Resolve against the base mount — never allow escaping it
        base   = DOWNLOAD_DIR
        target = (base / d) if d else base
        try:
            # Prevent path traversal
            target.resolve().relative_to(base.resolve())
        except ValueError:
            return jsonify({"error": "Subfolder must be inside the downloads mount"}), 400
        try:
            target.mkdir(parents=True, exist_ok=True)
            test_f = target / ".write_test"
            test_f.touch(); test_f.unlink()
        except Exception as e:
            return jsonify({"error": f"Cannot write to '{target}': {e}"}), 400
        # Store as relative subpath so it stays portable across restarts
        state["download_dir"] = str(target)
        log.info(f"[settings] download_dir -> {target}")
    _save_settings()
    return jsonify({
        "auto_check":   state["auto_check"],
        "interval":     state["interval"],
        "download_dir": state["download_dir"],
    })


@app.route("/api/notifications", methods=["GET"])
def api_notifications():
    """Return and clear pending notifications."""
    since = request.args.get("since", "")
    with _notif_lock:
        if since:
            notifs = [n for n in _notif_queue if n["ts"] > since]
        else:
            notifs = list(_notif_queue)
        _notif_queue.clear()
    return jsonify(notifs)



# ─── Download manager ─────────────────────────────────────────────────────────
#
# YouTube streams  → ytarchive (video + metadata + thumbnail) +
#                    yt-dlp parallel process (live chat .json)
# Twitch streams   → yt-dlp only (with metadata + chat)
# ──────────────────────────────────────────────────────────────────────────────

import subprocess, uuid, shutil, signal
from pathlib import Path as FPath

# Base download directory — set via DOWNLOAD_BASE_DIR env var or defaults to "downloads"
DOWNLOAD_DIR = FPath(os.environ.get("DOWNLOAD_BASE_DIR", "downloads"))
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

_UNSAFE_FS = re.compile(r'[<>:"/\\|?*\x00-\x1f]')

def _make_dl_dir(channel_name: str, stream_title: str, video_id: str) -> FPath:
    """Create: <download_dir>/<channel_name> <stream_title> <video_id>/"""
    def _c(v, n): return _UNSAFE_FS.sub("_", v).strip(" ._")[:n]
    parts = [p for p in (_c(channel_name,40), _c(stream_title,60), _c(video_id,30)) if p]
    base = FPath(state["download_dir"])
    base.mkdir(parents=True, exist_ok=True)
    folder = " ".join(parts) if parts else "download"
    dl_dir = base / folder
    dl_dir.mkdir(parents=True, exist_ok=True)
    return dl_dir

# ── bgutil POT provider ───────────────────────────────────────────────────────
# URL of the bgutil-ytdlp-pot-provider HTTP server (set via env var in docker-compose)
BGUTIL_URL   = os.environ.get("BGUTIL_PROVIDER_URL", "http://localhost:4416")
COOKIES_FILE = Path(os.environ.get("COOKIES_FILE", "cookies.txt"))

def _get_pot(content_binding: str = "jfKfPfyJRdk") -> str:
    """
    Fetch a PO token from the bgutil provider server.
    Endpoint: POST /get_pot
    Response: { "contentBinding", "poToken", "expiresAt" }
    Returns the poToken string, or "" on failure.
    """
    try:
        ping = requests.get(f"{BGUTIL_URL}/ping", timeout=5)
        if not ping.ok:
            log.error(f"[bgutil] Ping failed: HTTP {ping.status_code}")
            return ""
    except Exception as e:
        log.error(f"[bgutil] Server unreachable at {BGUTIL_URL}: {e}")
        return ""

    try:
        r = requests.post(
            f"{BGUTIL_URL}/get_pot",
            json={"content_binding": content_binding},
            timeout=30,
        )
        if r.ok:
            po_token = r.json().get("poToken") or ""
            if po_token:
                log.info(f"[bgutil] PO token ok ({po_token[:12]}...)")
                return po_token
            log.warning(f"[bgutil] No poToken in response: {r.text[:200]}")
        else:
            log.error(f"[bgutil] /get_pot HTTP {r.status_code}: {r.text[:300]}")
    except Exception as e:
        log.error(f"[bgutil] /get_pot failed: {e}")
    return ""

# ── Download state ─────────────────────────────────────────────────────────────
# downloads[id] = {id, name, url, platform, status, progress, files, log, error, pids}
downloads: dict = {}
_dl_lock = threading.Lock()


def _kill_pids(pids: list):
    for pid in pids:
        try:
            os.kill(pid, signal.SIGTERM)
        except Exception:
            pass


def _read_output(proc, dl: dict, parse_progress: bool = True, log_file=None):
    """
    Drain stdout+stderr into dl['log'] (capped for UI) and optionally a
    per-download log file (full, uncapped, for debugging).
    """
    for line in proc.stdout:
        line = line.rstrip()

        # Write full line to per-download log file if provided
        if log_file:
            try:
                log_file.write(line + "\n")
                log_file.flush()
            except Exception:
                pass

        # Keep last 500 lines at full width in memory for the UI log panel
        dl["log"].append(line)
        if len(dl["log"]) > 500:
            dl["log"].pop(0)

        if not parse_progress:
            continue
        # Strip ANSI escape codes for reliable parsing
        clean = re.sub(r"\[[0-9;]*[A-Za-z]", "", line).strip()

        # yt-dlp fragment progress: "[download] frag 45/300"
        m = re.search(r"\[download\]\s+frag\s+(\d+)/(\d+)", clean)
        if m:
            cur, tot = int(m.group(1)), int(m.group(2))
            dl["fragments"] = cur; dl["total_fragments"] = tot
            if tot > 0: dl["progress"] = min(99.0, cur/tot*100.0)

        # yt-dlp size: "[download]  1.23MiB at ..." (streaming chat/subs)
        m = re.search(r"\[download\]\s+([\d.]+)(MiB|KiB|GiB)\s+at", clean)
        if m:
            v, u = float(m.group(1)), m.group(2)
            mb = v if u=="MiB" else (v/1024 if u=="KiB" else v*1024)
            dl["downloaded"] = f"{mb:.1f} MB" if mb >= 1 else f"{v:.0f} KB"

        # yt-dlp percentage: "[download]  42.3% of ..."
        m = re.search(r"\[download\]\s+([\d.]+)%", clean)
        if m: dl["progress"] = float(m.group(1))

        # Twitch HLS: TWITCH-TOTAL-SECS + ffmpeg time=
        m = re.search(r"TWITCH-TOTAL-SECS:([\d.]+)", line)
        if m: dl["_tw_total_secs"] = float(m.group(1))
        m = re.search(r"time=(\d+):(\d+):([\d.]+)", clean)
        if m:
            elapsed = int(m.group(1))*3600+int(m.group(2))*60+float(m.group(3))
            tot = dl.get("_tw_total_secs", 0)
            if tot > 0: dl["progress"] = min(99.0, elapsed/tot*100.0)

        # ffmpeg size=
        m = re.search(r"size=\s*(\d+)[Kk][Bi][Bi]?", clean)
        if m:
            kb = int(m.group(1))
            dl["downloaded"] = f"{kb/1024:.1f} MB" if kb>=1024 else f"{kb} KB"

        if "Destination:" in clean:
            f = clean.split("Destination:")[-1].strip()
            if f and f not in dl["files"]: dl["files"].append(f)
        if "has already been downloaded" in clean: dl["progress"] = 100.0


def _run_youtube(dl_id: str, url: str):
    """
    YouTube: use yt-dlp for everything — video, audio, metadata, thumbnail,
    and live chat. The bgutil plugin handles PO tokens automatically.
    yt-dlp with --live-from-start downloads the stream from the beginning.
    """
    dl = downloads[dl_id]
    dl["status"] = "downloading"
    log.info(f"[download:{dl_id}] Starting YouTube download: {dl.get('name','?')} — {url}")

    _m = re.search(r"(?:v=|youtu\.be/)([\w-]{11})", url)
    dl_dir = _make_dl_dir(dl.get("name",""), dl.get("stream_title",""), _m.group(1) if _m else dl_id)
    dl["dl_dir"] = str(dl_dir)

    has_cookies = COOKIES_FILE.exists() and COOKIES_FILE.stat().st_size > 0
    if has_cookies:
        dl["log"].append(f"[cookies] Using {COOKIES_FILE}")
    else:
        dl["log"].append("[cookies] No cookies.txt — members streams may fail")

    # ── yt-dlp video command ───────────────────────────────────────────────
    # --live-from-start: record from beginning of stream
    # --concurrent-fragments 8: parallel fragment download for speed
    # bgutil plugin auto-injects PO token via the provider server
    vid_tmpl = str(dl_dir / "%(title)s.%(ext)s")
    vid_cmd = [
        "yt-dlp",
        "--newline", "--progress",
        "--live-from-start",
        "--concurrent-fragments", "8",
        "--add-metadata",
        "--write-thumbnail",
        "--write-description",
        "--write-info-json",
        # Use Deno for EJS n-challenge solving (enabled by default but explicit is safer)
        "--js-runtimes", "deno",
        "--remote-components", "ejs:github",
        # bgutil plugin base_url — separate flag from youtube: extractor args
        "--extractor-args", f"youtubepot-bgutilhttp:base_url={BGUTIL_URL}",
        # tv+mweb: no n-challenge needed, works for members content; web as fallback
        "--extractor-args", "youtube:player_client=tv,mweb,web",
        "-f", "bestvideo[ext=mp4]+bestaudio[ext=m4a]/bestvideo+bestaudio/best",
    ]
    if has_cookies:
        vid_cmd += ["--cookies", str(COOKIES_FILE)]
    vid_cmd += ["-o", vid_tmpl, url]

    # ── yt-dlp chat command ────────────────────────────────────────────────
    chat_tmpl = str(dl_dir / "chat.%(ext)s")
    chat_cmd = [
        "yt-dlp",
        "--no-warnings", "--newline",
        "--skip-download",
        "--no-check-formats",
        "--ignore-no-formats-error",
        "--write-subs",
        "--sub-langs", "live_chat",
        "--live-from-start",
        "--js-runtimes", "deno",
        "--remote-components", "ejs:github",
        "--extractor-args", f"youtubepot-bgutilhttp:base_url={BGUTIL_URL}",
        "--extractor-args", "youtube:player_client=tv,mweb,web",
    ]
    if has_cookies:
        chat_cmd += ["--cookies", str(COOKIES_FILE)]
    chat_cmd += ["-o", chat_tmpl, url]

    vid_proc  = None
    chat_proc = None

    yta_log_path  = dl_dir / "ytarchive.log"   # keep same name for rawlog compat
    dl["log_file"] = str(yta_log_path)
    chat_log_path = dl_dir / "chat.log"

    vid_log_fh  = None
    chat_log_fh = None

    try:
        vid_log_fh  = open(yta_log_path,  "w", encoding="utf-8", buffering=1)
        chat_log_fh = open(chat_log_path, "w", encoding="utf-8", buffering=1)

        vid_log_fh.write(f"=== yt-dlp video command ===\n{' '.join(vid_cmd)}\n\n=== output ===\n")
        chat_log_fh.write(f"=== yt-dlp chat command ===\n{' '.join(chat_cmd)}\n\n=== output ===\n")
        vid_log_fh.flush()
        chat_log_fh.flush()

        vid_proc  = subprocess.Popen(vid_cmd,  stdout=subprocess.PIPE,
                                     stderr=subprocess.STDOUT, text=True, bufsize=1)
        chat_proc = subprocess.Popen(chat_cmd, stdout=subprocess.PIPE,
                                     stderr=subprocess.STDOUT, text=True, bufsize=1)

        dl["pids"] = [vid_proc.pid, chat_proc.pid]

        vid_reader  = threading.Thread(
            target=_read_output, args=(vid_proc,  dl, True,  vid_log_fh),  daemon=True)
        chat_reader = threading.Thread(
            target=_read_output, args=(chat_proc, dl, False, chat_log_fh), daemon=True)
        vid_reader.start()
        chat_reader.start()

        vid_proc.wait()
        vid_reader.join()

        if dl["status"] == "cancelled":
            try: chat_proc.terminate()
            except Exception: pass
            return

        if vid_proc.returncode != 0:
            err_msg = (f"yt-dlp exited with code {vid_proc.returncode} "
                       f"for {dl_id} ({dl.get('name','?')}). Last log: {dl['log'][-3:]}")
            log.error(err_msg)
            dl["status"] = "error"
            last_lines = [l for l in dl["log"] if "ERROR" in l or "WARNING" in l]
            last_msg = last_lines[-1] if last_lines else (dl["log"][-1] if dl["log"] else "unknown")
            dl["error"] = f"yt-dlp exited {vid_proc.returncode}: {last_msg[:200]}"
            try: chat_proc.terminate()
            except Exception: pass
            chat_reader.join(timeout=5)
            return

        chat_proc.wait()
        chat_reader.join()

        for f in sorted(dl_dir.rglob("*")):
            if f.is_file() and f.name not in ("ytarchive.log", "chat.log"):
                p = str(f)
                if p not in dl["files"]:
                    dl["files"].append(p)

        log.info(f"[download:{dl_id}] Completed: {dl.get('name','?')}")
        dl["status"]   = "done"
        dl["progress"] = 100.0
        _push_notif("download_done", "⬇ Recording complete",
                    dl.get("name", "stream"), "")

    except FileNotFoundError as e:
        log.error(f"[download:{dl_id}] Binary not found: {e}")
        dl["status"] = "error"
        dl["error"]  = f"yt-dlp not found — check Dockerfile"
    except Exception as e:
        log.error(f"[download:{dl_id}] Error: {e}", exc_info=True)
        dl["status"] = "error"
        dl["error"]  = str(e)
    finally:
        for fh in (vid_log_fh, chat_log_fh):
            try:
                if fh: fh.close()
            except Exception: pass
        for proc in (vid_proc, chat_proc):
            if proc and proc.poll() is None:
                try: proc.terminate()
                except Exception: pass


def _run_twitch(dl_id: str, url: str):
    """
    Twitch: yt-dlp for video + metadata, then a second pass for rechat subtitles.
    rechat is only available after the VOD is processed, so we attempt it after
    the main download completes (non-fatal if it fails).
    """
    dl = downloads[dl_id]
    dl["status"] = "downloading"
    log.info(f"[download:{dl_id}] Starting Twitch download: {dl.get('name','?')} — {url}")

    _login = url.rstrip("/").split("/")[-1]
    dl_dir = _make_dl_dir(dl.get("name",""), dl.get("stream_title",""), _login)
    dl["dl_dir"] = str(dl_dir)

    out_tmpl = str(dl_dir / "%(uploader)s_%(title)s.%(ext)s")

    # Set up log file (same pattern as YouTube so the UI rawlog button works)
    tw_log_path = dl_dir / "ytarchive.log"
    dl["log_file"] = str(tw_log_path)

    has_cookies = COOKIES_FILE.exists() and COOKIES_FILE.stat().st_size > 0
    if has_cookies:
        dl["log"].append(f"[cookies] Using {COOKIES_FILE}")
    else:
        dl["log"].append("[cookies] No cookies.txt")

    # ── Main video command (no --write-subs; rechat 404s on live streams) ──
    vid_cmd = [
        "yt-dlp",
        "--no-warnings", "--newline", "--progress",
        "--live-from-start",
        "--add-metadata",
        "--write-thumbnail",
        "--write-description",
        "--write-info-json",
        "--concurrent-fragments", "8",
        "-f", "best",
    ]
    if has_cookies:
        vid_cmd += ["--cookies", str(COOKIES_FILE)]
    vid_cmd += ["-o", out_tmpl, url]

    tw_log_fh = None
    proc = None
    try:
        tw_log_fh = open(tw_log_path, "w", encoding="utf-8", buffering=1)
        tw_log_fh.write(f"=== yt-dlp twitch command ===\n{' '.join(vid_cmd)}\n\n=== output ===\n")
        tw_log_fh.flush()

        proc = subprocess.Popen(vid_cmd, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, text=True, bufsize=1)
        dl["pids"] = [proc.pid]
        _read_output(proc, dl, parse_progress=True, log_file=tw_log_fh)
        proc.wait()

        if dl["status"] == "cancelled":
            return

        if proc.returncode != 0:
            log.error(f"[download:{dl_id}] yt-dlp exited {proc.returncode} for {dl.get('name','?')}. Last log: {dl['log'][-5:]}")
            dl["status"] = "error"
            last_lines = [l for l in dl["log"] if "ERROR" in l or "WARNING" in l]
            last_msg = last_lines[-1] if last_lines else (dl["log"][-1] if dl["log"] else "unknown")
            dl["error"] = f"yt-dlp exited {proc.returncode}: {last_msg[:200]}"
            return

        # ── Optional rechat pass (VOD only — non-fatal) ───────────────────
        # After a live stream ends, Twitch converts it to a VOD. The rechat
        # subtitle is only served then. We attempt it silently; failure is OK.
        chat_cmd = [
            "yt-dlp",
            "--no-warnings", "--newline",
            "--skip-download",
            "--no-check-formats",
            "--ignore-no-formats-error",
            "--write-subs",
            "--sub-langs", "rechat",
        ]
        if has_cookies:
            chat_cmd += ["--cookies", str(COOKIES_FILE)]
        chat_cmd += ["-o", out_tmpl, url]

        try:
            tw_log_fh.write("\n=== rechat pass ===\n" + " ".join(chat_cmd) + "\n\n")
            tw_log_fh.flush()
            chat_proc = subprocess.Popen(chat_cmd, stdout=subprocess.PIPE,
                                         stderr=subprocess.STDOUT, text=True, bufsize=1)
            dl["pids"].append(chat_proc.pid)
            _read_output(chat_proc, dl, parse_progress=False, log_file=tw_log_fh)
            chat_proc.wait()
            if chat_proc.returncode == 0:
                dl["log"].append("[chat] rechat subtitles saved ✓")
            else:
                dl["log"].append("[chat] rechat not available (stream may still be live or no VOD yet)")
        except Exception as ce:
            dl["log"].append(f"[chat] rechat pass skipped: {ce}")

        # Collect all output files
        for f in sorted(dl_dir.rglob("*")):
            if f.is_file() and f.name not in ("ytarchive.log",):
                p = str(f)
                if p not in dl["files"]:
                    dl["files"].append(p)

        log.info(f"[download:{dl_id}] Twitch download completed: {dl.get('name','?')}")
        dl["status"]   = "done"
        dl["progress"] = 100.0
        _push_notif("download_done", "⬇ Recording complete",
                    dl.get("name", "stream"), "")

    except FileNotFoundError:
        log.error(f"[download:{dl_id}] yt-dlp not found")
        dl["status"] = "error"
        dl["error"]  = "yt-dlp not found — check your Dockerfile"
    except Exception as e:
        log.error(f"[download:{dl_id}] Unexpected error: {e}", exc_info=True)
        dl["status"] = "error"
        dl["error"]  = str(e)
    finally:
        if tw_log_fh:
            try: tw_log_fh.close()
            except Exception: pass
        if proc and proc.poll() is None:
            try: proc.terminate()
            except Exception: pass


def _probe_vod_meta(url: str, extra_args: list) -> dict:
    """Run yt-dlp --dump-json to get real title + channel before downloading."""
    cmd = ["yt-dlp", "--no-warnings", "--dump-json", "--no-download"] + extra_args + [url]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if r.returncode == 0 and r.stdout.strip():
            data = json.loads(r.stdout.strip().splitlines()[0])
            return {
                "title":   data.get("title", ""),
                "channel": data.get("channel") or data.get("uploader") or "",
            }
    except Exception:
        pass
    return {"title": "", "channel": ""}


def _run_vod(dl_id: str, url: str):
    """
    Download a finished VOD (YouTube or Twitch).

    Design:
    - Probes metadata first (--dump-json) to get real channel+title for folder name.
    - YouTube: player_client=tv,web so members-only content works with cookies.
    - Video and subtitles are SEPARATE passes:
        Pass 1: video only (never fails due to subtitles).
        Pass 2: chat/rechat, completely non-fatal — a 404 or any error is logged
                but does NOT mark the download as failed.
    """
    dl = downloads[dl_id]
    dl["status"] = "downloading"
    platform = "twitch" if "twitch.tv" in url else "youtube"
    dl["platform"] = platform
    log.info(f"[download:{dl_id}] VOD ({platform}): {url}")

    has_cookies = COOKIES_FILE.exists() and COOKIES_FILE.stat().st_size > 0

    if platform == "youtube":
        # Client selection rationale:
        #   tv    — accesses members-only content with cookies; doesn't need n-challenge
        #   mweb  — mobile web client, also bypasses n-challenge, good public fallback
        #   web   — kept last as it needs n-challenge for full format access
        # --remote-components ejs:github lets deno download the EJS n-challenge solver;
        # without this, web formats are throttled to images-only on some videos.
        base_args = [
            "--js-runtimes", "deno",
            "--remote-components", "ejs:github",
            "--extractor-args", f"youtubepot-bgutilhttp:base_url={BGUTIL_URL}",
            "--extractor-args", "youtube:player_client=tv,mweb,web",
        ]
        if has_cookies:
            base_args += ["--cookies", str(COOKIES_FILE)]
        vid_args = [
            "yt-dlp", "--newline", "--progress",
            "--add-metadata", "--write-thumbnail",
            "--write-description", "--write-info-json",
            # Broad format fallback: try merged mp4/m4a, then any merged, then best single
            "-f", "bestvideo[ext=mp4]+bestaudio[ext=m4a]/bestvideo+bestaudio/best",
        ] + base_args
        sub_args = [
            "yt-dlp", "--newline", "--no-warnings",
            "--skip-download", "--ignore-no-formats-error",
            "--write-subs", "--sub-langs", "live_chat",
        ] + base_args

    else:  # twitch
        base_args = list(["--cookies", str(COOKIES_FILE)] if has_cookies else [])
        vid_args = [
            "yt-dlp", "--newline", "--progress",
            "--add-metadata", "--write-thumbnail",
            "--write-description", "--write-info-json",
            # No --write-subs here — rechat in a separate non-fatal pass
            "-f", "best",
        ] + base_args
        # rechat: separate pass; 404s on many VODs, always non-fatal
        sub_args = [
            "yt-dlp", "--newline", "--no-warnings",
            "--skip-download", "--no-check-formats", "--ignore-no-formats-error",
            "--write-subs", "--sub-langs", "rechat",
        ] + base_args

    # Probe for real folder name
    dl["log"].append("[probe] Fetching video metadata…")
    meta    = _probe_vod_meta(url, base_args)
    channel = meta["channel"] or dl.get("name", "")
    title   = meta["title"]   or dl.get("stream_title", "")
    dl["log"].append(f"[probe] channel={channel!r} title={title!r}")

    _m = re.search(r"(?:v=|youtu\.be/)([\w-]{11})", url) or re.search(r"twitch\.tv/videos/(\d+)", url)
    vid_id = _m.group(1) if _m else dl_id

    dl_dir = _make_dl_dir(channel, title, vid_id)
    dl["dl_dir"]       = str(dl_dir)
    dl["name"]         = channel or dl.get("name", "VOD")
    dl["stream_title"] = title
    dl["log"].append(f"[folder] {dl_dir}")
    dl["log"].append(f"[cookies] {'Using ' + str(COOKIES_FILE) if has_cookies else 'No cookies.txt'}")

    out_tmpl = str(dl_dir / "%(title)s.%(ext)s")
    log_path = dl_dir / "ytarchive.log"
    dl["log_file"] = str(log_path)

    log_fh = None
    proc   = None
    try:
        log_fh = open(log_path, "w", encoding="utf-8", buffering=1)

        # ── Pass 1: video ─────────────────────────────────────────────────
        cmd = vid_args + ["-o", out_tmpl, url]
        log_fh.write(f"=== VOD video ({platform}) ===\n{' '.join(cmd)}\n\n=== output ===\n")
        log_fh.flush()
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, text=True, bufsize=1)
        dl["pids"] = [proc.pid]
        _read_output(proc, dl, parse_progress=True, log_file=log_fh)
        proc.wait()

        if dl["status"] == "cancelled":
            return

        if proc.returncode != 0:
            err_lines = [l for l in dl["log"] if "ERROR" in l]
            msg = (err_lines[-1] if err_lines else (dl["log"][-1] if dl["log"] else "unknown"))[:200]
            log.error(f"[download:{dl_id}] VOD video failed ({proc.returncode}): {msg}")
            dl["status"] = "error"; dl["error"] = msg
            return

        # ── Pass 2: subtitles/chat (fully non-fatal) ──────────────────────
        try:
            sub_cmd = sub_args + ["-o", out_tmpl, url]
            log_fh.write(f"\n=== subtitles pass ===\n{' '.join(sub_cmd)}\n\n")
            log_fh.flush()
            sp = subprocess.Popen(sub_cmd, stdout=subprocess.PIPE,
                                  stderr=subprocess.STDOUT, text=True, bufsize=1)
            dl["pids"].append(sp.pid)
            _read_output(sp, dl, parse_progress=False, log_file=log_fh)
            sp.wait()
            dl["log"].append("[subs] saved ✓" if sp.returncode == 0
                             else "[subs] not available (this is normal for many VODs)")
        except Exception as se:
            dl["log"].append(f"[subs] skipped: {se}")

        # Collect files
        for f in sorted(dl_dir.rglob("*")):
            if f.is_file() and f.suffix != ".log":
                p = str(f)
                if p not in dl["files"]: dl["files"].append(p)

        log.info(f"[download:{dl_id}] VOD done: {channel} — {title}")
        dl["status"] = "done"; dl["progress"] = 100.0
        _push_notif("download_done", f"⬇ Download complete",
                    f"{channel} — {title}" if title else channel, "")

    except FileNotFoundError:
        dl["status"] = "error"; dl["error"] = "yt-dlp not found — check Dockerfile"
    except Exception as e:
        log.error(f"[download:{dl_id}] VOD error: {e}", exc_info=True)
        dl["status"] = "error"; dl["error"] = str(e)
    finally:
        if log_fh:
            try: log_fh.close()
            except Exception: pass
        if proc and proc.poll() is None:
            try: proc.terminate()
            except Exception: pass



# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/api/downloads", methods=["GET"])
def api_list_downloads():
    with _dl_lock:
        return jsonify(list(downloads.values()))


@app.route("/api/downloads", methods=["POST"])
def api_start_download():
    data         = request.json or {}
    url          = (data.get("url") or "").strip()
    name         = (data.get("name") or "stream").strip()
    platform     = (data.get("platform") or "youtube").strip()
    stream_title = (data.get("stream_title") or "").strip()
    is_vod       = bool(data.get("is_vod", False))
    if not url:
        return jsonify({"error": "url is required"}), 400
    if "twitch.tv" in url and platform == "youtube":
        platform = "twitch"

    if not shutil.which("yt-dlp"):
        return jsonify({"error": "yt-dlp not found in container. Check Dockerfile."}), 500

    dl_id = str(uuid.uuid4())[:8]
    dl = {
        "id":              dl_id,
        "name":            name,
        "stream_title":    stream_title,
        "url":             url,
        "platform":        platform,
        "is_vod":          is_vod,
        "status":          "starting",
        "progress":        0.0,
        "fragments":       0,
        "total_fragments": 0,
        "downloaded":      "",
        "files":           [],
        "log":             [],
        "error":           "",
        "pids":            [],
    }
    with _dl_lock:
        downloads[dl_id] = dl

    if is_vod:
        threading.Thread(target=_run_vod,    args=(dl_id, url), daemon=True).start()
    elif platform == "youtube":
        threading.Thread(target=_run_youtube, args=(dl_id, url), daemon=True).start()
    else:
        threading.Thread(target=_run_twitch,  args=(dl_id, url), daemon=True).start()

    return jsonify(dl), 201


@app.route("/api/downloads/<dl_id>", methods=["DELETE"])
def api_cancel_download(dl_id):
    with _dl_lock:
        dl = downloads.get(dl_id)
        if not dl:
            return jsonify({"error": "Not found"}), 404
        if dl["status"] in ("downloading", "starting"):
            # Still running — kill processes but keep entry as cancelled
            _kill_pids(dl.get("pids", []))
            dl["status"] = "cancelled"
        else:
            # Finished/errored/cancelled — fully remove from dict
            del downloads[dl_id]
    return jsonify({"ok": True})


@app.route("/api/downloads/<dl_id>/log")
def api_download_log(dl_id):
    with _dl_lock:
        dl = downloads.get(dl_id)
    if not dl:
        return jsonify({"error": "Not found"}), 404
    return jsonify({"log": dl.get("log", [])})


@app.route("/api/downloads/<dl_id>/files")
def api_download_files(dl_id):
    """List all output files for a completed download."""
    with _dl_lock:
        dl = downloads.get(dl_id)
    if not dl:
        return jsonify({"error": "Not found"}), 404
    dl_dir = FPath(dl.get("dl_dir") or str(FPath(state["download_dir"]) / dl_id))
    files = []
    if dl_dir.exists():
        for f in sorted(dl_dir.rglob("*")):
            if f.is_file() and f.suffix != ".log":
                files.append({"name": f.name, "size": f.stat().st_size, "path": str(f)})
    return jsonify(files)


@app.route("/api/downloads/<dl_id>/file/<path:filename>")
def api_serve_file(dl_id, filename):
    """Serve a specific output file for browser download."""
    from flask import send_file
    with _dl_lock:
        _dl_obj = downloads.get(dl_id)
    dl_dir = FPath(_dl_obj.get("dl_dir") if _dl_obj else str(DOWNLOAD_DIR / dl_id))
    fpath = dl_dir / filename if not Path(filename).is_absolute() else Path(filename)
    if not fpath.exists():
        matches = list(dl_dir.rglob(Path(filename).name)) if dl_dir.exists() else []
        if not matches:
            return jsonify({"error": "File not found"}), 404
        fpath = matches[0]
    # Security: resolved path must be inside the download's own dl_dir
    # (prevents ".." traversal attacks; dl_dir itself is set by our own code)
    try:
        fpath.resolve().relative_to(dl_dir.resolve())
    except ValueError:
        return jsonify({"error": "Forbidden"}), 403
    return send_file(str(fpath), as_attachment=True, download_name=fpath.name)



@app.route("/api/debug/channel/<path:channel_id>")
def api_debug_channel(channel_id):
    """Debug endpoint: shows raw signals found on the /live page."""
    with _lock:
        ch = next((c for c in state["channels"] if c["channel_id"] == channel_id), None)
    name = ch["name"] if ch else channel_id

    # Fetch the /live page and expose every signal we look for
    live_url = f"https://www.youtube.com/channel/{channel_id}/live"
    try:
        r    = requests.get(live_url, headers=HEADERS, timeout=REQ_TIMEOUT)
        html = r.text
        final_url = r.url
    except Exception as e:
        return jsonify({"error": str(e)})

    signals = {
        "final_url":           final_url,
        "html_length":         len(html),
        "has_isLiveBroadcast_true":  '"isLiveBroadcast":true' in html or '"isLiveBroadcast": true' in html,
        "has_isLive_true":     '"isLive":true' in html or '"isLive": true' in html,
        "has_isUpcoming_true": '"isUpcoming":true' in html or '"isUpcoming": true' in html,
        "has_upcomingEventData": '"upcomingEventData"' in html,
        "has_startTimestamp":  '"startTimestamp"' in html,
        "has_scheduledStartTime": '"scheduledStartTime"' in html,
        "json_ld_types":       [],
        "ipr_videoDetails":    {},
        "ipr_liveBroadcastDetails": {},
        "rss_recent_video_ids": [],
        "rss_upcoming_check":  [],
    }

    # JSON-LD types
    for block in re.findall(r'<script[^>]+type="application/ld\+json"[^>]*>(.*?)</script>', html, re.DOTALL):
        try:
            data = json.loads(block)
            items = data if isinstance(data, list) else [data]
            for item in items:
                signals["json_ld_types"].append({
                    "@type": item.get("@type"),
                    "isLiveBroadcast": item.get("isLiveBroadcast"),
                    "startDate": item.get("startDate"),
                    "name": item.get("name", "")[:80],
                })
        except Exception:
            pass

    # ytInitialPlayerResponse
    ipr = _yt_parse_initial_player(html)
    if ipr:
        vs  = ipr.get("videoDetails", {})
        mf  = ipr.get("microformat", {}).get("playerMicroformatRenderer", {})
        lbd = mf.get("liveBroadcastDetails", {})
        signals["ipr_videoDetails"] = {
            "videoId":   vs.get("videoId"),
            "title":     vs.get("title", "")[:80],
            "isLive":    vs.get("isLive"),
            "isUpcoming": vs.get("isUpcoming"),
            "isLiveContent": vs.get("isLiveContent"),
        }
        signals["ipr_liveBroadcastDetails"] = lbd

    # startTimestamp context if present
    if '"startTimestamp"' in html:
        idx = html.find('"startTimestamp"')
        signals["startTimestamp_context"] = html[idx:idx+120]

    # upcomingEventData chunk if present
    if '"upcomingEventData"' in html:
        idx = html.find('"upcomingEventData"')
        signals["upcomingEventData_chunk"] = html[idx:idx+300]

    # RSS feed — check last 5 video IDs for upcoming
    rss_tree = _fetch_rss(channel_id)
    if rss_tree:
        entries = rss_tree.findall(f"{{{RSS_NS}}}entry")[:5]
        for entry in entries:
            yt_ns = "http://www.youtube.com/xml/schemas/2015"
            vid_el = entry.find(f"{{{yt_ns}}}videoId")
            title_el = entry.find(f"{{{RSS_NS}}}title")
            vid_id = vid_el.text if vid_el is not None else None
            title  = title_el.text if title_el is not None else ""
            signals["rss_recent_video_ids"].append({"videoId": vid_id, "title": title[:60]})

        # Check each RSS video for upcoming signal
        for entry_info in signals["rss_recent_video_ids"]:
            vid_id = entry_info.get("videoId")
            if not vid_id:
                continue
            try:
                vr = requests.get(f"https://www.youtube.com/watch?v={vid_id}",
                                  headers=HEADERS, timeout=10)
                vhtml = vr.text
                is_up = '"isUpcoming":true' in vhtml or '"isUpcoming": true' in vhtml
                has_sched = '"startTimestamp"' in vhtml
                vipr = _yt_parse_initial_player(vhtml)
                vvs  = vipr.get("videoDetails", {}) if vipr else {}
                vmf  = vipr.get("microformat", {}).get("playerMicroformatRenderer", {}) if vipr else {}
                vlbd = vmf.get("liveBroadcastDetails", {})
                signals["rss_upcoming_check"].append({
                    "videoId":    vid_id,
                    "title":      entry_info.get("title", "")[:60],
                    "isUpcoming": vvs.get("isUpcoming"),
                    "isLive":     vvs.get("isLive"),
                    "startTimestamp": vlbd.get("startTimestamp"),
                    "raw_isUpcoming_true": is_up,
                    "has_startTimestamp": has_sched,
                })
            except Exception as e:
                signals["rss_upcoming_check"].append({"videoId": vid_id, "error": str(e)})

    fresh = check_live(ch) if ch else check_youtube_live(channel_id)
    return jsonify({
        "channel":     name,
        "stored":      ch.get("last_status") if ch else None,
        "fresh_check": fresh,
        "signals":     signals,
    })


@app.route("/api/diag")
def api_diag():
    """Diagnostic endpoint — checks bgutil server and ytarchive availability."""
    import shutil
    result = {}

    # 1. Check bgutil server — ping then get_pot
    try:
        ping = requests.get(f"{BGUTIL_URL}/ping", timeout=5)
        result["bgutil_ping"] = ping.status_code
        pot_r = requests.post(f"{BGUTIL_URL}/get_pot",
                              json={"content_binding": "jfKfPfyJRdk"}, timeout=15)
        result["bgutil_status"] = pot_r.status_code
        result["bgutil_response"] = pot_r.json() if pot_r.ok else pot_r.text[:300]
    except Exception as e:
        result["bgutil_status"] = "error"
        result["bgutil_response"] = str(e)

    # 2. Check yt-dlp
    result["ytdlp_path"] = shutil.which("yt-dlp") or "NOT FOUND"
    if shutil.which("yt-dlp"):
        try:
            v = subprocess.run(["yt-dlp", "--version"],
                               capture_output=True, text=True, timeout=5)
            result["ytdlp_version"] = v.stdout.strip()
        except Exception as e:
            result["ytdlp_version"] = str(e)

    # 4. Show BGUTIL_URL
    result["bgutil_url"] = BGUTIL_URL

    return jsonify(result)


@app.route("/api/logs")
def api_get_logs():
    """Return the last N lines of the app log file."""
    try:
        n     = int(request.args.get("lines", 200))
        lines = LOG_FILE.read_text(encoding="utf-8").splitlines()
        return jsonify({"lines": lines[-n:], "total": len(lines)})
    except FileNotFoundError:
        return jsonify({"lines": [], "total": 0})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/downloads/<dl_id>/rawlog")
def api_download_rawlog(dl_id):
    """Return the full raw log file for a specific download."""
    with _dl_lock:
        dl = downloads.get(dl_id)
    if not dl:
        return jsonify({"error": "Not found"}), 404
    _base    = FPath(state["download_dir"])
    _dl_dir  = FPath(dl.get("dl_dir") or str(_base / dl_id))
    log_path = _dl_dir / "ytarchive.log"
    if not log_path.exists():
        log_path = _base / f"{dl_id}_ytarchive.log"
    if not log_path.exists():
        log_path = _base / f"{dl_id}.log"
    if not log_path.exists():
        return jsonify({"error": "Log file not yet created"}), 404
    from flask import Response
    return Response(
        log_path.read_text(encoding="utf-8", errors="replace"),
        mimetype="text/plain; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{dl_id}.log"'}
    )


@app.route("/api/cookies", methods=["GET"])
def api_cookies_status():
    """Check if a cookies.txt file is present."""
    exists = COOKIES_FILE.exists() and COOKIES_FILE.stat().st_size > 0
    return jsonify({
        "exists": exists,
        "path": str(COOKIES_FILE),
        "size": COOKIES_FILE.stat().st_size if exists else 0,
    })

@app.route("/api/cookies", methods=["POST"])
def api_cookies_upload():
    """Upload a cookies.txt (Netscape format) file."""
    data = request.get_data()
    if not data:
        return jsonify({"error": "No data received"}), 400
    # Basic sanity check — Netscape cookies start with a comment
    text = data.decode("utf-8", errors="replace")
    if "HTTP Cookie File" not in text and "Netscape HTTP" not in text and "# " not in text[:100]:
        return jsonify({"error": "File does not look like a Netscape cookies.txt"}), 400
    COOKIES_FILE.write_bytes(data)
    log.info(f"[cookies] Uploaded cookies.txt ({len(data)} bytes)")
    return jsonify({"ok": True, "size": len(data)})

@app.route("/api/cookies", methods=["DELETE"])
def api_cookies_delete():
    """Remove the cookies.txt file."""
    if COOKIES_FILE.exists():
        COOKIES_FILE.unlink()
        log.info("[cookies] Deleted cookies.txt")
    return jsonify({"ok": True})

# ─── Boot ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    state["channels"] = load_channels()
    # Backfill started_at for channels already live on boot
    _now_iso = datetime.now(timezone.utc).isoformat()
    for _ch in state["channels"]:
        _st = _ch.get("last_status", {})
        if _st.get("is_live") and not _st.get("started_at"):
            _st["started_at"] = _ch.get("last_checked") or _now_iso
    print("=" * 52)
    print("  Livestream Monitor (YouTube + Twitch)")
    print("  No API keys required")
    print("  Open: http://localhost:5000")
    print("=" * 52)
    app.run(host="0.0.0.0", debug=False, port=5000)
