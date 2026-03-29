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

# Cookies file path — defined early so scrapers can use it
# (same value as COOKIES_FILE in the download manager, set via env var)
_COOKIES_PATH = Path(os.environ.get("COOKIES_FILE", "cookies.txt"))

def _make_session() -> requests.Session:
    """
    Build a requests.Session with cookies loaded from cookies.txt (Netscape format).
    Used for watch-page scraping so YouTube serves full content instead of consent walls.
    Falls back to a plain session if the file is missing or unparseable.
    """
    session = requests.Session()
    session.headers.update(HEADERS)
    if not (_COOKIES_PATH.exists() and _COOKIES_PATH.stat().st_size > 0):
        return session
    try:
        from http.cookiejar import MozillaCookieJar
        jar = MozillaCookieJar()
        jar.load(str(_COOKIES_PATH), ignore_discard=True, ignore_expires=True)
        session.cookies = jar  # type: ignore[assignment]
    except Exception as e:
        log.warning(f"[scrape] Could not load cookies for session: {e}")
    return session

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
    upcoming = [c["name"] for c in state["channels"] if c.get("last_status", {}).get("is_upcoming") or c.get("last_status", {}).get("is_waiting")]
    if upcoming:
        log.info(f"[save] upcoming/waiting channels: {upcoming}")
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(state["channels"], f, indent=2, ensure_ascii=False)


# ─── Free chat filter ─────────────────────────────────────────────────────────

FREE_CHAT_PATTERNS = [
    "free chat", "freechat",
    "フリーチャット", "ふりーちゃっと", "フリチャ",
    "ぽたく集会所", "フリフリチャット",
    "🌙FreeしのみんChat🐾",
    "🪐🪐🪐",
    "stream schedule",
    "壁紙配布中",
    "チャットルーム",
    "発売中",
    "スケジュール",
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


def _base_status(is_live=False, is_upcoming=False, is_waiting=False, **kw) -> dict:
    """Return a fully-populated status dict with safe defaults."""
    return {
        "is_live":      is_live,
        "is_upcoming":  is_upcoming,
        "is_waiting":   is_waiting,   # past scheduled_at, not yet live
        "video_id":     kw.get("video_id"),
        "title":        kw.get("title"),
        "url":          kw.get("url"),
        "started_at":   kw.get("started_at"),
        "scheduled_at": kw.get("scheduled_at"),
        "error":        kw.get("error"),
        "viewer_count": kw.get("viewer_count"),
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


def _get_viewer_count(video_id: str, session=None) -> int | None:
    """
    Fetch the current concurrent viewer count for a live stream,
    or waiting count for an upcoming stream.
    Returns an integer or None if unavailable.
    """
    if not video_id:
        return None
    try:
        if session is None:
            session = _make_session()
        r    = session.get(f"https://www.youtube.com/watch?v={video_id}",
                           timeout=REQ_TIMEOUT)
        html = r.text
        if len(html) < 50000:
            return None  # bot-detection wall

        # 1. concurrentViewers — most reliable for live streams (integer string)
        m = re.search(r'"concurrentViewers"\s*:\s*"(\d+)"', html)
        if m:
            return int(m.group(1))

        # 2. viewCount runs format inside videoViewCountRenderer
        # e.g. [{"text":"12,345"},{"text":" watching"}]
        m = re.search(
            r'"videoViewCountRenderer".*?"text"\s*:\s*"([\d,]+)".*?"text"\s*:\s*" (watching|waiting)"',
            html, re.DOTALL
        )
        if m:
            return int(m.group(1).replace(",", ""))

        # 3. viewCount simpleText — "12,345 watching now" or "32 waiting"
        m = re.search(r'"viewCount"\s*:\s*\{"simpleText"\s*:\s*"([\d,]+)\s+(watching|waiting)', html)
        if m:
            return int(m.group(1).replace(",", ""))

        # 4. Scan all occurrences of watching/waiting near a number
        # Look specifically within 200 chars of videoViewCountRenderer
        idx = html.find('"videoViewCountRenderer"')
        if idx != -1:
            chunk = html[idx:idx+500]
            log.info(f"[viewer_count] {video_id} renderer chunk: {chunk[:200]!r}")
            m = re.search(r'"([\d,]+)"\s*\}\s*,\s*\{\s*"text"\s*:\s*" (watching|waiting)"', chunk)
            if m:
                return int(m.group(1).replace(",", ""))
            # Also try simpleText within the renderer
            m = re.search(r'"simpleText"\s*:\s*"([\d,]+)\s+(watching|waiting)', chunk)
            if m:
                return int(m.group(1).replace(",", ""))
        else:
            log.info(f"[viewer_count] {video_id}: no videoViewCountRenderer found")

    except Exception:
        pass
    return None


def _to_iso(val: str) -> str:
    """
    Normalise a schedule time value to an ISO 8601 string.
    YouTube uses two formats:
      - Unix timestamp string: "1742810400"
      - ISO string: "2026-03-24T11:00:00+00:00"
    Returns the ISO string, or the original value if conversion fails.
    """
    if not val:
        return val
    try:
        if val.isdigit() or (val.lstrip("-").isdigit()):
            # Unix timestamp
            return datetime.fromtimestamp(int(val), tz=timezone.utc).isoformat()
        # Already ISO — normalise Z suffix
        return datetime.fromisoformat(val.replace("Z", "+00:00")).isoformat()
    except Exception:
        return val
    """Scrape YouTube /live page for live and upcoming stream status."""
    live_url = f"https://www.youtube.com/channel/{channel_id}/live"
    try:
        session = _make_session()
        r    = session.get(live_url, timeout=REQ_TIMEOUT)
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
                    # isLiveBroadcast=false + startDate = upcoming (future) or waiting (recently past)
                    # Skip if endDate is present — stream already ended
                    if start_date and upcoming_candidate is None and not item.get("endDate"):
                        try:
                            sched = datetime.fromisoformat(start_date.replace("Z", "+00:00"))
                            now   = datetime.now(timezone.utc)
                            age   = (now - sched).total_seconds()
                            if sched > now:
                                log.info(f"[live-check] {channel_id} M1:JSON-LD upcoming title={title!r:.40} startDate={start_date} endDate=None")
                                upcoming_candidate = _base_status(
                                    is_upcoming=True, video_id=video_id, title=title,
                                    url=f"https://youtube.com/watch?v={video_id}" if video_id else None,
                                    scheduled_at=start_date,
                                )
                            elif 0 <= age <= 48 * 3600:
                                log.info(f"[live-check] {channel_id} M1:JSON-LD waiting title={title!r:.40} startDate={start_date} age={age:.0f}s endDate=None")
                                upcoming_candidate = _base_status(
                                    is_waiting=True, video_id=video_id, title=title,
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

            if vs.get("isLive") and not _is_free_chat(title):
                return _base_status(
                    is_live=True, video_id=video_id, title=title, url=url_v,
                    started_at=_find_live_start_time(html, ipr, video_id) or None,
                )
            # isUpcoming is a boolean YouTube sets explicitly on scheduled streams
            # Only trust if endTimestamp absent (not a completed stream)
            # Note: free-chat titled videos are skipped but we still fall through
            # to RSS so members-only streams behind a wallpaper post are found.
            if not _is_free_chat(title) and vs.get("isUpcoming") and not lbd.get("endTimestamp") and upcoming_candidate is None:
                    # startTimestamp may be in liveBroadcastDetails, or
                    # scheduledStartTime may be in videoDetails or microformat
                    sched = _to_iso(lbd.get("startTimestamp")
                             or vs.get("scheduledStartTime")
                             or mf.get("liveBroadcastDetails", {}).get("startTimestamp"))
                    # Also scan raw HTML for scheduledStartTime as last resort
                    if not sched and '"scheduledStartTime"' in html:
                        m_sched = re.search(r'"scheduledStartTime"\s*:\s*"([^"]+)"', html)
                        if m_sched:
                            sched = _to_iso(m_sched.group(1))
                    # Even with no schedule time, isUpcoming=True is authoritative
                    # — mark as upcoming and let the UI show no countdown
                    if sched:
                        try:
                            t   = datetime.fromisoformat(sched.replace("Z", "+00:00"))
                            now = datetime.now(timezone.utc)
                            age = (now - t).total_seconds()
                            if t > now:
                                upcoming_candidate = _base_status(
                                    is_upcoming=True, video_id=video_id,
                                    title=title, url=url_v, scheduled_at=sched,
                                )
                            elif 0 <= age <= 48 * 3600:
                                upcoming_candidate = _base_status(
                                    is_waiting=True, video_id=video_id,
                                    title=title, url=url_v, scheduled_at=sched,
                                )
                            else:
                                # Sched too old — still upcoming per YouTube flag
                                upcoming_candidate = _base_status(
                                    is_upcoming=True, video_id=video_id,
                                    title=title, url=url_v, scheduled_at=sched,
                                )
                        except Exception:
                            upcoming_candidate = _base_status(
                                is_upcoming=True, video_id=video_id,
                                title=title, url=url_v, scheduled_at=sched,
                            )
                    else:
                        log.info(f"[live-check] {channel_id} M2: isUpcoming=True, no sched — marking upcoming anyway")
                        upcoming_candidate = _base_status(
                            is_upcoming=True, video_id=video_id,
                            title=title, url=url_v, scheduled_at=None,
                        )

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
                        t   = datetime.fromisoformat(sched_raw.replace("Z", "+00:00"))
                        now = datetime.now(timezone.utc)
                        age = (now - t).total_seconds()
                        if t > now or 0 <= age <= 48 * 3600:
                            vid3   = vid_m3.group(1) if vid_m3 else None
                            title3 = (ipr.get("videoDetails", {}).get("title") or "") if ipr else ""
                            if not title3:
                                og_m = re.search(
                                    r'<meta[^>]+property="og:title"[^>]+content="([^"]+)"', html)
                                if og_m:
                                    import html as _hm3
                                    title3 = _hm3.unescape(og_m.group(1))
                            if not _is_free_chat(title3):
                                _is_past = t <= now
                                upcoming_candidate = _base_status(
                                    is_upcoming=not _is_past, is_waiting=_is_past,
                                    video_id=vid3, title=title3,
                                    url=f"https://youtube.com/watch?v={vid3}" if vid3 else None,
                                    scheduled_at=sched_raw,
                                )
                                break
                    except Exception:
                        pass

        if upcoming_candidate:
            log.info(
                f"[live-check] YouTube {channel_id}: UPCOMING/WAITING "
                f"{upcoming_candidate.get('title','')!r} @ {upcoming_candidate.get('scheduled_at','')} "
                f"is_upcoming={upcoming_candidate.get('is_upcoming')} is_waiting={upcoming_candidate.get('is_waiting')}"
            )
            return upcoming_candidate

        # ── Method 5: RSS feed fallback ───────────────────────────────────
        # When /live doesn't redirect to the upcoming stream, probe the
        # last few RSS entries directly for isUpcoming signals.
        rss_tree = _fetch_rss(channel_id)
        if rss_tree:
            yt_ns   = "http://www.youtube.com/xml/schemas/2015"
            entries = rss_tree.findall(f"{{{RSS_NS}}}entry")[:3]
            log.info(f"[live-check] {channel_id} RSS fallback: checking {len(entries)} entries")
            rss_session = _make_session()
            for entry in entries:
                vid_el = entry.find(f"{{{yt_ns}}}videoId")
                if vid_el is None:
                    continue
                vid_id = vid_el.text
                try:
                    vr    = rss_session.get(f"https://www.youtube.com/watch?v={vid_id}",
                                            timeout=REQ_TIMEOUT)
                    vhtml = vr.text
                    vipr = _yt_parse_initial_player(vhtml)
                    if not vipr:
                        log.info(f"[live-check] {channel_id} RSS skip {vid_id}: vipr parse failed (len={len(vhtml)})")
                        continue
                    vvs  = vipr.get("videoDetails", {})
                    vmf  = vipr.get("microformat", {}).get("playerMicroformatRenderer", {})
                    vlbd = vmf.get("liveBroadcastDetails", {})
                    vtitle = vvs.get("title") or ""
                    if _is_free_chat(vtitle):
                        continue

                    # Currently live
                    if vvs.get("isLive"):
                        return _base_status(
                            is_live=True, video_id=vid_id, title=vtitle,
                            url=f"https://youtube.com/watch?v={vid_id}",
                            started_at=_find_live_start_time(vhtml, vipr, vid_id) or None,
                        )

                    # isUpcoming=true → scheduled, time may be future or recently past
                    # Only trust if no endTimestamp (stream hasn't ended)
                    if vvs.get("isUpcoming") and not vlbd.get("endTimestamp"):
                        sched = _to_iso(vlbd.get("startTimestamp")
                                 or vvs.get("scheduledStartTime"))
                        if not sched and '"scheduledStartTime"' in vhtml:
                            m_sched = re.search(r'"scheduledStartTime"\s*:\s*"([^"]+)"', vhtml)
                            if m_sched:
                                sched = _to_iso(m_sched.group(1))
                        if sched:
                            try:
                                t   = datetime.fromisoformat(sched.replace("Z", "+00:00"))
                                now = datetime.now(timezone.utc)
                                age = (now - t).total_seconds()
                                _is_past = t <= now and age <= 48 * 3600
                                if t > now or _is_past:
                                    log.info(
                                        f"[live-check] YouTube {channel_id}: "
                                        f"{'WAITING' if _is_past else 'UPCOMING'} (RSS) "
                                        f"{vtitle!r} @ {sched}"
                                    )
                                    return _base_status(
                                        is_upcoming=not _is_past, is_waiting=_is_past,
                                        video_id=vid_id, title=vtitle,
                                        url=f"https://youtube.com/watch?v={vid_id}",
                                        scheduled_at=sched,
                                    )
                            except Exception:
                                pass
                        # isUpcoming=True but no sched time — trust it anyway
                        log.info(f"[live-check] YouTube {channel_id}: UPCOMING (RSS/no-sched) {vtitle!r}")
                        return _base_status(
                            is_upcoming=True, video_id=vid_id, title=vtitle,
                            url=f"https://youtube.com/watch?v={vid_id}",
                            scheduled_at=None,
                        )
                    elif vvs.get("isUpcoming"):
                        log.info(f"[live-check] {channel_id} RSS {vid_id}: isUpcoming but endTimestamp={vlbd.get('endTimestamp')!r}")
                    else:
                        log.info(f"[live-check] {channel_id} RSS {vid_id}: isLive={vvs.get('isLive')} isUpcoming={vvs.get('isUpcoming')} isLiveContent={vvs.get('isLiveContent')} title={vtitle!r:.40}")

                    # upcomingEventData as final fallback
                    if '"upcomingEventData"' in vhtml:
                        idx = vhtml.find('"upcomingEventData"')
                        chunk = vhtml[idx:idx + 500]
                        ts_m = re.search(r'"startTime"\s*:\s*"(\d{9,11})"', chunk)
                        if ts_m:
                            start_ts = int(ts_m.group(1))
                            now_ts   = int(datetime.now(timezone.utc).timestamp())
                            age_s    = now_ts - start_ts
                            if start_ts > now_ts or 0 <= age_s <= 48 * 3600:
                                sched_iso = datetime.fromtimestamp(start_ts, tz=timezone.utc).isoformat()
                                _is_past  = start_ts <= now_ts
                                if not _is_free_chat(vtitle):
                                    log.info(
                                        f"[live-check] YouTube {channel_id}: "
                                        f"{'WAITING' if _is_past else 'UPCOMING'} (RSS/upcomingEventData) "
                                        f"{vtitle!r} @ {sched_iso}"
                                    )
                                    return _base_status(
                                        is_upcoming=not _is_past, is_waiting=_is_past,
                                        video_id=vid_id, title=vtitle,
                                        url=f"https://youtube.com/watch?v={vid_id}",
                                        scheduled_at=sched_iso,
                                    )
                except Exception:
                    continue

        log.info(f"[live-check] {channel_id} returning offline (no signals found)")
        log.info(f"[live-check] {channel_id} returning offline (no signals found)")
        return _base_status()

    except requests.Timeout:
        return _base_status(error="timeout")
    except Exception as e:
        log.warning(f"[live-check] YouTube error for {channel_id}: {e}")
        return _base_status(error=str(e)[:80])

def _find_live_start_time(html: str, ipr: dict, video_id: str = None) -> str:
    """
    Find a live stream's real start time.
    Checks the /live page first, then fetches the watch page if video_id given.
    Returns ISO string or empty string if not found.
    """
    if ipr:
        mf  = ipr.get("microformat", {}).get("playerMicroformatRenderer", {})
        lbd = mf.get("liveBroadcastDetails", {})

        # 1. liveBroadcastDetails.startTimestamp
        t = lbd.get("startTimestamp")
        if t:
            return _to_iso(t)

        # 2. publishDate in microformat
        t = mf.get("publishDate") or mf.get("uploadDate")
        if t:
            return _to_iso(t)

    # 3. Raw scan of /live page HTML
    if '"startTimestamp"' in html:
        m = re.search(r'"startTimestamp"\s*:\s*"([^"]+)"', html)
        if m:
            return _to_iso(m.group(1))

    if '"startDate"' in html:
        m = re.search(r'"startDate"\s*:\s*"(\d{4}-[^"]+)"', html)
        if m:
            return _to_iso(m.group(1))

    # 4. Fetch the watch page — startTimestamp is reliably there
    if video_id:
        try:
            session = _make_session()
            wr   = session.get(f"https://www.youtube.com/watch?v={video_id}",
                               timeout=REQ_TIMEOUT)
            wipr = _yt_parse_initial_player(wr.text)
            if wipr:
                wlbd = wipr.get("microformat", {}).get(
                    "playerMicroformatRenderer", {}).get("liveBroadcastDetails", {})
                t = wlbd.get("startTimestamp")
                if t:
                    return _to_iso(t)
            # Raw scan of watch page
            if '"startTimestamp"' in wr.text:
                m = re.search(r'"startTimestamp"\s*:\s*"([^"]+)"', wr.text)
                if m:
                    return _to_iso(m.group(1))
        except Exception:
            pass

    return ""



def check_youtube_live(channel_id: str) -> dict:
    """Scrape YouTube /live page for live and upcoming stream status."""
    live_url = f"https://www.youtube.com/channel/{channel_id}/live"
    try:
        session = _make_session()
        r    = session.get(live_url, timeout=REQ_TIMEOUT)
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
                    # isLiveBroadcast=false + startDate = upcoming (future) or waiting (recently past)
                    # Skip if endDate present — stream already ended
                    if start_date and upcoming_candidate is None and not item.get("endDate"):
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
                        started_at=_find_live_start_time(html, ipr, video_id) or None,
                    )
                # isUpcoming is a boolean YouTube sets explicitly on scheduled streams
                # Only trust if endTimestamp absent (not a completed stream)
                if vs.get("isUpcoming") and not lbd.get("endTimestamp") and upcoming_candidate is None:
                    sched = _to_iso(lbd.get("startTimestamp")
                             or vs.get("scheduledStartTime")
                             or mf.get("liveBroadcastDetails", {}).get("startTimestamp"))
                    if not sched and '"scheduledStartTime"' in html:
                        m_sched = re.search(r'"scheduledStartTime"\s*:\s*"([^"]+)"', html)
                        if m_sched:
                            sched = _to_iso(m_sched.group(1))
                    if sched:
                        try:
                            t   = datetime.fromisoformat(sched.replace("Z", "+00:00"))
                            now = datetime.now(timezone.utc)
                            age = (now - t).total_seconds()
                            if t > now:
                                upcoming_candidate = _base_status(
                                    is_upcoming=True, video_id=video_id,
                                    title=title, url=url_v, scheduled_at=sched,
                                )
                            elif 0 <= age <= 48 * 3600:
                                upcoming_candidate = _base_status(
                                    is_waiting=True, video_id=video_id,
                                    title=title, url=url_v, scheduled_at=sched,
                                )
                            else:
                                upcoming_candidate = _base_status(
                                    is_upcoming=True, video_id=video_id,
                                    title=title, url=url_v, scheduled_at=sched,
                                )
                        except Exception:
                            upcoming_candidate = _base_status(
                                is_upcoming=True, video_id=video_id,
                                title=title, url=url_v, scheduled_at=sched,
                            )
                    else:
                        log.info(f"[live-check] {channel_id} M2: isUpcoming=True, no sched — marking upcoming anyway")
                        upcoming_candidate = _base_status(
                            is_upcoming=True, video_id=video_id,
                            title=title, url=url_v, scheduled_at=None,
                        )

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
                    now_ts   = int(datetime.now(timezone.utc).timestamp())
                    age_secs = now_ts - start_ts
                    if start_ts > now_ts or 0 <= age_secs <= 48 * 3600:
                        sched_iso = datetime.fromtimestamp(start_ts, tz=timezone.utc).isoformat()
                        vid_m2 = re.search(r'"videoId"\s*:\s*"([\w-]{11})"', html)
                        vid2   = vid_m2.group(1) if vid_m2 else None
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
                            _is_past = now_ts >= start_ts
                            upcoming_candidate = _base_status(
                                is_upcoming=not _is_past, is_waiting=_is_past,
                                video_id=vid2, title=title2 or None,
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

        # ── Method 6: membership tab scrape ───────────────────────────────
        # For channels with a free-chat placeholder on /live, the actual
        # members-only stream is only visible on the membership tab.
        # Only attempt if /live page showed a free-chat (ipr had a video but
        # it was filtered) — detected by checking if ipr parsed ok but no
        # candidate was set.
        if ipr and ipr.get("videoDetails", {}).get("videoId"):
            # /live page had a video but it was skipped (free-chat or ended)
            # Try the membership tab
            try:
                mem_url = f"https://www.youtube.com/channel/{channel_id}/membership"
                mem_r   = session.get(mem_url, timeout=REQ_TIMEOUT)
                mem_html = mem_r.text
                if len(mem_html) > 50000:
                    # Look for videoId of a live or upcoming stream in the page
                    # Membership tab embeds video renderers with isLive signals
                    mem_ipr = _yt_parse_initial_player(mem_html)
                    if mem_ipr:
                        mvs = mem_ipr.get("videoDetails", {})
                        if mvs.get("videoId") and not _is_free_chat(mvs.get("title") or ""):
                            mvid   = mvs.get("videoId")
                            mtitle = mvs.get("title") or ""
                            if mvs.get("isLive"):
                                log.info(f"[live-check] YouTube {channel_id}: LIVE (membership tab) {mtitle!r}")
                                return _base_status(
                                    is_live=True, video_id=mvid, title=mtitle,
                                    url=f"https://youtube.com/watch?v={mvid}",
                                    started_at=_find_live_start_time(mem_html, mem_ipr, mvid) or None,
                                )
                            if mvs.get("isUpcoming") and not _is_free_chat(mtitle):
                                mmf  = mem_ipr.get("microformat", {}).get("playerMicroformatRenderer", {})
                                mlbd = mmf.get("liveBroadcastDetails", {})
                                sched = _to_iso(mlbd.get("startTimestamp") or mvs.get("scheduledStartTime"))
                                if not sched and '"scheduledStartTime"' in mem_html:
                                    ms = re.search(r'"scheduledStartTime"\s*:\s*"([^"]+)"', mem_html)
                                    if ms:
                                        sched = _to_iso(ms.group(1))
                                log.info(f"[live-check] YouTube {channel_id}: UPCOMING (membership tab) {mtitle!r}")
                                if upcoming_candidate is None:
                                    upcoming_candidate = _base_status(
                                        is_upcoming=True, video_id=mvid, title=mtitle,
                                        url=f"https://youtube.com/watch?v={mvid}",
                                        scheduled_at=sched,
                                    )
                    # Also scan for video IDs in the membership tab page
                    # and probe each one for live/upcoming status
                    mem_vids = list(dict.fromkeys(
                        re.findall(r'"videoId"\s*:\s*"([\w-]{11})"', mem_html)
                    ))[:5]
                    mem_session = _make_session()
                    for mvid in mem_vids:
                        if mem_ipr and mvid == mem_ipr.get("videoDetails", {}).get("videoId"):
                            continue  # already checked
                        try:
                            vr    = mem_session.get(f"https://www.youtube.com/watch?v={mvid}",
                                                    timeout=REQ_TIMEOUT)
                            vhtml = vr.text
                            if len(vhtml) < 50000:
                                continue
                            vipr  = _yt_parse_initial_player(vhtml)
                            vvs   = vipr.get("videoDetails", {}) if vipr else {}
                            vtitle = vvs.get("title") or ""
                            if _is_free_chat(vtitle):
                                continue
                            if vvs.get("isLive"):
                                log.info(f"[live-check] YouTube {channel_id}: LIVE (membership/probe) {vtitle!r}")
                                return _base_status(
                                    is_live=True, video_id=mvid, title=vtitle,
                                    url=f"https://youtube.com/watch?v={mvid}",
                                    started_at=_find_live_start_time(vhtml, vipr, mvid) or None,
                                )
                            if vvs.get("isUpcoming") and upcoming_candidate is None:
                                vmf  = vipr.get("microformat", {}).get("playerMicroformatRenderer", {}) if vipr else {}
                                vlbd = vmf.get("liveBroadcastDetails", {})
                                sched = _to_iso(vlbd.get("startTimestamp") or vvs.get("scheduledStartTime"))
                                if not sched and '"scheduledStartTime"' in vhtml:
                                    ms = re.search(r'"scheduledStartTime"\s*:\s*"([^"]+)"', vhtml)
                                    if ms:
                                        sched = _to_iso(ms.group(1))
                                log.info(f"[live-check] YouTube {channel_id}: UPCOMING (membership/probe) {vtitle!r}")
                                upcoming_candidate = _base_status(
                                    is_upcoming=True, video_id=mvid, title=vtitle,
                                    url=f"https://youtube.com/watch?v={mvid}",
                                    scheduled_at=sched,
                                )
                        except Exception:
                            continue
            except Exception as e:
                log.debug(f"[live-check] {channel_id} membership tab error: {e}")

        # Return any upcoming candidate found by Methods 1-6 before RSS fallback
        if upcoming_candidate:
            log.info(
                f"[live-check] YouTube {channel_id}: UPCOMING "
                f"{upcoming_candidate.get('title','')!r} @ {upcoming_candidate.get('scheduled_at','')}"
            )
            return upcoming_candidate

        # ── Method 5: RSS feed fallback ───────────────────────────────────
        # When /live page is blocked by a free-chat placeholder or has no
        # signals, probe the last few RSS entries for isLive or isUpcoming.
        rss_tree = _fetch_rss(channel_id)
        if rss_tree:
            yt_ns   = "http://www.youtube.com/xml/schemas/2015"
            entries = rss_tree.findall(f"{{{RSS_NS}}}entry")[:5]
            log.info(f"[live-check] {channel_id} RSS fallback: checking {len(entries)} entries")
            rss_session = _make_session()
            for entry in entries:
                vid_el = entry.find(f"{{{yt_ns}}}videoId")
                if vid_el is None:
                    continue
                vid_id = vid_el.text
                try:
                    vr    = rss_session.get(f"https://www.youtube.com/watch?v={vid_id}",
                                            timeout=REQ_TIMEOUT)
                    vhtml = vr.text
                    # Skip tiny pages (bot-detection wall)
                    if len(vhtml) < 50000:
                        continue
                    # Quick guard — skip if no live/upcoming signals at all
                    has_live     = '"isLive":true' in vhtml or '"isLive": true' in vhtml
                    has_upcoming = ('"isUpcoming":true' in vhtml
                                    or '"isUpcoming": true' in vhtml
                                    or '"upcomingEventData"' in vhtml)
                    if not has_live and not has_upcoming:
                        continue
                    vipr = _yt_parse_initial_player(vhtml)
                    if not vipr:
                        log.info(f"[live-check] {channel_id} RSS skip {vid_id}: vipr parse failed (len={len(vhtml)})")
                        continue
                    vvs  = vipr.get("videoDetails", {})
                    vmf  = vipr.get("microformat", {}).get("playerMicroformatRenderer", {})
                    vlbd = vmf.get("liveBroadcastDetails", {})
                    vtitle = vvs.get("title") or ""
                    if _is_free_chat(vtitle):
                        continue
                    if vvs.get("isLive"):
                        log.info(f"[live-check] YouTube {channel_id}: LIVE (RSS) {vtitle!r}")
                        return _base_status(
                            is_live=True, video_id=vid_id, title=vtitle,
                            url=f"https://youtube.com/watch?v={vid_id}",
                            started_at=_find_live_start_time(vhtml, vipr, vid_id) or None,
                        )
                    if vvs.get("isUpcoming") and not vlbd.get("endTimestamp"):
                        sched = _to_iso(vlbd.get("startTimestamp")
                                        or vvs.get("scheduledStartTime"))
                        if not sched and '"scheduledStartTime"' in vhtml:
                            m_sched = re.search(r'"scheduledStartTime"\s*:\s*"([^"]+)"', vhtml)
                            if m_sched:
                                sched = _to_iso(m_sched.group(1))
                        if not sched and '"upcomingEventData"' in vhtml:
                            idx   = vhtml.find('"upcomingEventData"')
                            chunk = vhtml[idx:idx + 500]
                            ts_m  = re.search(r'"startTime"\s*:\s*"(\d{9,11})"', chunk)
                            if ts_m:
                                sched = _to_iso(ts_m.group(1))
                        if sched:
                            try:
                                t   = datetime.fromisoformat(sched.replace("Z", "+00:00"))
                                now = datetime.now(timezone.utc)
                                age = (now - t).total_seconds()
                                if t > now or 0 <= age <= 48 * 3600:
                                    _is_past = t <= now
                                    log.info(
                                        f"[live-check] YouTube {channel_id}: "
                                        f"{'WAITING' if _is_past else 'UPCOMING'} (RSS) "
                                        f"{vtitle!r} @ {sched}"
                                    )
                                    return _base_status(
                                        is_upcoming=not _is_past, is_waiting=_is_past,
                                        video_id=vid_id, title=vtitle,
                                        url=f"https://youtube.com/watch?v={vid_id}",
                                        scheduled_at=sched,
                                    )
                            except Exception:
                                pass
                        else:
                            log.info(f"[live-check] YouTube {channel_id}: UPCOMING (RSS/no-sched) {vtitle!r}")
                            return _base_status(
                                is_upcoming=True, video_id=vid_id, title=vtitle,
                                url=f"https://youtube.com/watch?v={vid_id}",
                                scheduled_at=None,
                            )
                except Exception:
                    continue

        log.info(f"[live-check] {channel_id} returning offline (no signals found)")
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



def _fetch_all_upcoming(channel_id: str) -> list:
    """
    Scan the RSS feed and /live page for ALL upcoming streams from this channel.
    Returns a list of _base_status dicts with is_upcoming/is_waiting=True.
    Only called for YouTube channels.
    """
    results = []
    seen_ids = set()
    now      = datetime.now(timezone.utc)
    now_ts   = int(now.timestamp())

    def _try_add(vid_id, title, sched_iso, is_past):
        if vid_id in seen_ids or _is_free_chat(title or ""):
            return
        if not title:
            # No title means we couldn't verify — skip to avoid free-chat placeholders
            return
        seen_ids.add(vid_id)
        results.append(_base_status(
            is_upcoming=not is_past, is_waiting=is_past,
            video_id=vid_id, title=title or None,
            url=f"https://youtube.com/watch?v={vid_id}",
            scheduled_at=sched_iso,
        ))

    def _extract_sched(vhtml, vipr):
        """Return (sched_iso, title) from a watch page."""
        vvs  = vipr.get("videoDetails", {}) if vipr else {}
        vmf  = vipr.get("microformat", {}).get("playerMicroformatRenderer", {}) if vipr else {}
        vlbd = vmf.get("liveBroadcastDetails", {})
        vtitle = vvs.get("title") or ""
        if vlbd.get("endTimestamp"):
            return None, vtitle  # stream ended
        sched = _to_iso(vlbd.get("startTimestamp") or vvs.get("scheduledStartTime"))
        if not sched and '"scheduledStartTime"' in vhtml:
            m = re.search(r'"scheduledStartTime"\s*:\s*"([^"]+)"', vhtml)
            if m:
                sched = _to_iso(m.group(1))
        if not sched and '"upcomingEventData"' in vhtml:
            idx   = vhtml.find('"upcomingEventData"')
            chunk = vhtml[idx:idx+500]
            ts_m  = re.search(r'"startTime"\s*:\s*"(\d{9,11})"', chunk)
            if ts_m:
                sched = _to_iso(ts_m.group(1))
        return sched, vtitle

    try:
        session  = _make_session()
        rss_tree = _fetch_rss(channel_id)

        # ── 1. Scan RSS entries ───────────────────────────────────────────
        if rss_tree:
            yt_ns   = "http://www.youtube.com/xml/schemas/2015"
            entries = rss_tree.findall(f"{{{RSS_NS}}}entry")[:5]
            for entry in entries:
                vid_el = entry.find(f"{{{yt_ns}}}videoId")
                if vid_el is None:
                    continue
                vid_id = vid_el.text
                if vid_id in seen_ids:
                    continue
                try:
                    time.sleep(1.0)  # space out requests to avoid rate limiting
                    vr    = session.get(f"https://www.youtube.com/watch?v={vid_id}",
                                        timeout=REQ_TIMEOUT)
                    vhtml = vr.text
                    # Skip if clearly not upcoming (no signals, or too small = blocked)
                    if len(vhtml) < 50000:
                        continue  # bot-detection wall
                    has_sig = ('"isUpcoming":true' in vhtml
                               or '"isUpcoming": true' in vhtml
                               or '"upcomingEventData"' in vhtml)
                    if not has_sig:
                        continue
                    vipr  = _yt_parse_initial_player(vhtml)
                    vvs   = vipr.get("videoDetails", {}) if vipr else {}
                    if vvs.get("isLive"):
                        continue  # currently live, not upcoming
                    sched, vtitle = _extract_sched(vhtml, vipr)
                    if not sched:
                        continue
                    t   = datetime.fromisoformat(sched.replace("Z", "+00:00"))
                    age = (now - t).total_seconds()
                    if t > now or 0 <= age <= 48 * 3600:
                        _try_add(vid_id, vtitle, sched, t <= now)
                except Exception:
                    continue

        # ── 2. Also check /live page for members-only upcoming ────────────
        # Members-only watch pages return a consent wall (tiny HTML),
        # but the channel's /live page shows upcomingEventData for them.
        try:
            live_url  = f"https://www.youtube.com/channel/{channel_id}/live"
            lr        = session.get(live_url, timeout=REQ_TIMEOUT)
            lhtml     = lr.text
            log.info(f"[fetch_all_upcoming] {channel_id} /live page: len={len(lhtml)} has_upcoming={'upcomingEventData' in lhtml}")
            lipr      = _yt_parse_initial_player(lhtml)
            lvs       = lipr.get("videoDetails", {}) if lipr else {}
            vid_id_l  = lvs.get("videoId")
            log.info(f"[fetch_all_upcoming] {channel_id} /live videoId={vid_id_l} isLive={lvs.get('isLive')} seen={vid_id_l in seen_ids}")
            if vid_id_l and vid_id_l not in seen_ids and not lvs.get("isLive"):
                sched, vtitle = _extract_sched(lhtml, lipr)
                if not sched and '"upcomingEventData"' in lhtml:
                    idx   = lhtml.find('"upcomingEventData"')
                    chunk = lhtml[idx:idx+500]
                    ts_m  = re.search(r'"startTime"\s*:\s*"(\d{9,11})"', chunk)
                    if ts_m:
                        sched = _to_iso(ts_m.group(1))
                if sched:
                    try:
                        t   = datetime.fromisoformat(sched.replace("Z", "+00:00"))
                        age = (now - t).total_seconds()
                        if t > now or 0 <= age <= 48 * 3600:
                            if not vtitle and lipr:
                                og_m = re.search(
                                    r'<meta[^>]+property="og:title"[^>]+content="([^"]+)"', lhtml)
                                if og_m:
                                    import html as _hm_l
                                    vtitle = _hm_l.unescape(og_m.group(1))
                            _try_add(vid_id_l, vtitle, sched, t <= now)
                    except Exception:
                        pass
        except Exception:
            pass

        # ── 3. Scan /streams tab for upcoming streams ─────────────────────
        # When a channel is currently live, the /live page shows the active
        # stream. The /streams tab lists all upcoming and recent streams.
        try:
            streams_url = f"https://www.youtube.com/channel/{channel_id}/streams"
            sr          = session.get(streams_url, timeout=REQ_TIMEOUT)
            shtml       = sr.text
            log.info(f"[fetch_all_upcoming] {channel_id} /streams tab: len={len(shtml)}")
            if len(shtml) > 50000:
                # Extract all video IDs from the streams tab page
                stream_vids = list(dict.fromkeys(
                    re.findall(r'"videoId"\s*:\s*"([\w-]{11})"', shtml)
                ))[:5]
                log.info(f"[fetch_all_upcoming] {channel_id} /streams vids: {stream_vids}")
                for svid in stream_vids:
                    if svid in seen_ids:
                        continue
                    try:
                        time.sleep(1.0)  # longer delay to avoid rate limiting
                        vr    = session.get(f"https://www.youtube.com/watch?v={svid}",
                                            timeout=REQ_TIMEOUT)
                        vhtml = vr.text
                        if len(vhtml) < 50000:
                            # Members-only wall — try to extract schedule from
                            # the streams tab inline JSON for this video ID
                            vid_idx = shtml.find(f'"{svid}"')
                            if vid_idx != -1:
                                chunk = shtml[vid_idx:vid_idx + 3000]
                                # Look for scheduledStartTime in the renderer chunk
                                ms = re.search(r'"scheduledStartTime"\s*:\s*"(\d+)"', chunk)
                                if ms:
                                    sched = _to_iso(ms.group(1))
                                    # Get title from the chunk
                                    mt = re.search(r'"title"\s*:\s*\{"runs"\s*:\s*\[\{"text"\s*:\s*"([^"]+)"', chunk)
                                    vtitle = mt.group(1) if mt else ""
                                    if not _is_free_chat(vtitle):
                                        try:
                                            t   = datetime.fromisoformat(sched.replace("Z", "+00:00"))
                                            age = (now - t).total_seconds()
                                            if t > now or 0 <= age <= 48 * 3600:
                                                log.info(f"[fetch_all_upcoming] {channel_id} members-only upcoming via /streams inline: {svid} {vtitle!r:.40}")
                                                _try_add(svid, vtitle, sched, t <= now)
                                        except Exception:
                                            pass
                            continue
                        vipr  = _yt_parse_initial_player(vhtml)
                        vvs   = vipr.get("videoDetails", {}) if vipr else {}
                        vtitle = vvs.get("title") or ""
                        if _is_free_chat(vtitle) or vvs.get("isLive"):
                            continue
                        if not vvs.get("isUpcoming"):
                            continue
                        sched, vtitle = _extract_sched(vhtml, vipr)
                        if not sched:
                            continue
                        t   = datetime.fromisoformat(sched.replace("Z", "+00:00"))
                        age = (now - t).total_seconds()
                        if t > now or 0 <= age <= 48 * 3600:
                            log.info(f"[fetch_all_upcoming] {channel_id} found via /streams: {svid} {vtitle!r:.40}")
                            _try_add(svid, vtitle, sched, t <= now)
                    except Exception:
                        continue
        except Exception:
            pass

    except Exception:
        pass

    # Sort by scheduled_at ascending
    results.sort(key=lambda x: x.get("scheduled_at") or "")
    return results


def check_live(ch: dict) -> dict:
    if ch.get("platform") == "twitch":
        return check_twitch_live(ch["channel_id"])
    return check_youtube_live(ch["channel_id"])


# Consecutive offline checks needed before confirming a live stream ended.
# Prevents notification spam from transient scrape failures.
_OFFLINE_CONFIRM = 2

# Max concurrent channel checks. Keep moderate to avoid hammering YouTube/Twitch.
_CHECK_WORKERS = 10

def _check_one_channel(ch: dict) -> None:
    """Check a single channel and update its status in-place. Thread-safe."""
    prev         = ch.get("last_status", {})
    was_live     = prev.get("is_live", False)
    was_upcoming = prev.get("is_upcoming", False)
    was_waiting  = prev.get("is_waiting", False)
    prev_started = prev.get("started_at")

    status = check_live(ch)
    now    = datetime.now(timezone.utc)

    # ── Fallback: if offline but prev video_id known, probe it directly ───────
    # Handles the case where the /live page is occupied by a free-chat
    # placeholder, hiding an actual live or upcoming members-only stream.
    if (not status.get("is_live") and not status.get("is_upcoming")
            and not status.get("is_waiting")
            and ch.get("platform", "youtube") == "youtube"):
        prev_vid = prev.get("video_id")
        if prev_vid:
            try:
                session = _make_session()
                vr    = session.get(f"https://www.youtube.com/watch?v={prev_vid}",
                                    timeout=REQ_TIMEOUT)
                vhtml = vr.text
                if len(vhtml) >= 50000:
                    vipr  = _yt_parse_initial_player(vhtml)
                    vvs   = vipr.get("videoDetails", {}) if vipr else {}
                    vtitle = vvs.get("title") or ""
                    if not _is_free_chat(vtitle):
                        if vvs.get("isLive"):
                            log.info(f"[check_one] {ch.get('name','?')}: found LIVE via prev video_id {prev_vid}")
                            vmf  = vipr.get("microformat", {}).get("playerMicroformatRenderer", {}) if vipr else {}
                            status = _base_status(
                                is_live=True, video_id=prev_vid, title=vtitle,
                                url=f"https://youtube.com/watch?v={prev_vid}",
                                started_at=_find_live_start_time(vhtml, vipr, prev_vid) or None,
                            )
            except Exception:
                pass

    # ── Hysteresis: live → offline requires _OFFLINE_CONFIRM consecutive fails ──
    if was_live and not status.get("is_live"):
        miss = ch.get("_offline_misses", 0) + 1
        ch["_offline_misses"] = miss
        if miss < _OFFLINE_CONFIRM:
            ch["last_checked"] = now.isoformat()
            return
    else:
        ch["_offline_misses"] = 0

    # ── Preserve started_at across checks while live ──────────────────────────
    # Never stamp "now" — only use real startTimestamp from the scraper or
    # carry the previously stored value forward. This ensures the timer shows
    # actual stream duration, not time-since-detection.
    if status.get("is_live"):
        if prev_started and not status.get("started_at"):
            # Carry forward real start time from a previous check
            status["started_at"] = prev_started
        elif status.get("started_at"):
            # Normalise whatever the scraper returned (may be Unix ts or ISO)
            status["started_at"] = _to_iso(status["started_at"])
        # Fetch live viewer count (YouTube only)
        if ch.get("platform", "youtube") == "youtube" and status.get("video_id"):
            try:
                vc = _get_viewer_count(status["video_id"])
                log.info(f"[viewer] {ch.get('name','?')}: {vc}")
                status["viewer_count"] = vc
            except Exception:
                pass

    # Fetch waiting count for upcoming/waiting streams (YouTube only)
    if (status.get("is_upcoming") or status.get("is_waiting")) and \
            ch.get("platform", "youtube") == "youtube" and status.get("video_id"):
        try:
            vc = _get_viewer_count(status["video_id"])
            log.info(f"[waiting] {ch.get('name','?')}: {vc}")
            status["viewer_count"] = vc
        except Exception:
            pass

    ch["last_status"]  = status
    ch["last_checked"] = now.isoformat()
    if status.get("is_upcoming") or status.get("is_waiting"):
        log.info(
            f"[check_one] {ch.get('name','?')}: stored "
            f"{'UPCOMING' if status.get('is_upcoming') else 'WAITING'} "
            f"title={status.get('title','')!r:.50} sched={status.get('scheduled_at','')}"
        )

    # Fetch all upcoming streams (YouTube only) — store extras in upcoming_statuses.
    # Only scan channels that are live or upcoming — offline channels don't need
    # this since check_youtube_live already handles their primary upcoming stream.
    # Throttle to every 10 cycles even for active channels to avoid rate limiting.
    if ch.get("platform", "youtube") == "youtube":
        is_active = status.get("is_live") or status.get("is_upcoming") or status.get("is_waiting")
        if is_active:
            n = ch.get("_upcoming_check_n", 0)
            if n == 0:
                all_up = _fetch_all_upcoming(ch["channel_id"])
                primary_vid = status.get("video_id")
                ch["upcoming_statuses"] = [u for u in all_up if u.get("video_id") != primary_vid]
            else:
                # Re-filter cached results every cycle in case patterns changed
                primary_vid = status.get("video_id")
                ch["upcoming_statuses"] = [
                    u for u in ch.get("upcoming_statuses", [])
                    if u.get("video_id") != primary_vid
                    and not _is_free_chat(u.get("title") or "")
                ]
            ch["_upcoming_check_n"] = (n + 1) % 10
        else:
            # Channel is offline — clear any stale upcoming_statuses
            ch["upcoming_statuses"] = []
            ch["_upcoming_check_n"] = 0  # Reset so it scans immediately when channel goes live

    # ── Notifications ─────────────────────────────────────────────────────────
    name = ch.get("name", ch.get("channel_id", ""))
    if not was_live and status.get("is_live"):
        _push_notif("live", f"🔴 {name} is live",
                    status.get("title") or "Started streaming",
                    status.get("url") or "")
    elif not was_upcoming and not was_waiting and status.get("is_upcoming"):
        _push_notif("upcoming", f"📅 {name} scheduled a stream",
                    status.get("title") or "",
                    status.get("url") or "")


def check_all_channels():
    from concurrent.futures import ThreadPoolExecutor, as_completed
    with _lock:
        channels = list(state["channels"])

    t0 = time.time()
    with ThreadPoolExecutor(max_workers=_CHECK_WORKERS) as pool:
        futures = {pool.submit(_check_one_channel, ch): ch for ch in channels}
        for fut in as_completed(futures):
            try:
                fut.result()
            except Exception as e:
                ch = futures[fut]
                log.warning(f"[check_all] error checking {ch.get('name','?')}: {e}")

    elapsed = time.time() - t0
    log.info(f"[check_all] {len(channels)} channels checked in {elapsed:.1f}s")
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


@app.route("/api/check/upcoming", methods=["POST"])
def api_check_upcoming_now():
    """Force _fetch_all_upcoming to run on next cycle for all channels."""
    with _lock:
        for ch in state["channels"]:
            ch["_upcoming_check_n"] = 0
    threading.Thread(target=check_all_channels, daemon=True).start()
    return jsonify({"ok": True, "message": "Upcoming scan forced for all channels"})


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
                sched_raw = re.search(r'"scheduledStartTime"\s*:\s*"([^"]+)"', vhtml)
                signals["rss_upcoming_check"].append({
                    "videoId":            vid_id,
                    "title":              entry_info.get("title", "")[:60],
                    "isUpcoming":         vvs.get("isUpcoming"),
                    "isLive":             vvs.get("isLive"),
                    "startTimestamp":     vlbd.get("startTimestamp"),
                    "scheduledStartTime": vvs.get("scheduledStartTime") or (sched_raw.group(1) if sched_raw else None),
                    "raw_isUpcoming_true": is_up,
                    "has_startTimestamp": has_sched,
                    "has_scheduledStartTime": '"scheduledStartTime"' in vhtml,
                })
            except Exception as e:
                signals["rss_upcoming_check"].append({"videoId": vid_id, "error": str(e)})

    fresh = check_live(ch) if ch else check_youtube_live(channel_id)

    # Streams tab — check for upcoming streams not in RSS
    try:
        session = _make_session()
        sr = session.get(f"https://www.youtube.com/channel/{channel_id}/streams",
                         timeout=REQ_TIMEOUT)
        shtml = sr.text
        # ytInitialData contains the video grid — extract all videoId values
        yid = _yt_parse_initial_data(shtml)
        # Flatten all videoId strings from the full ytInitialData blob
        shtml_vids = list(dict.fromkeys(
            re.findall(r'"videoId"\s*:\s*"([\w-]{11})"', shtml)
        ))[:15]
        upcoming_in_streams = []
        for svid in shtml_vids:
            vid_chunk_start = shtml.find(f'"{svid}"')
            if vid_chunk_start != -1:
                vid_chunk = shtml[vid_chunk_start:vid_chunk_start+2000]
                has_up = ('"isUpcoming":true' in vid_chunk
                          or '"upcomingEventData"' in vid_chunk
                          or '"scheduledStartTime"' in vid_chunk)
                if has_up:
                    upcoming_in_streams.append(svid)
        signals["streams_tab_len"] = len(shtml)
        signals["streams_tab_video_ids"] = shtml_vids
        signals["streams_tab_upcoming_ids"] = upcoming_in_streams
        signals["streams_tab_has_ytInitialData"] = '"ytInitialData"' in shtml or 'var ytInitialData' in shtml
        # Show first occurrence of videoId to diagnose format
        vid_idx = shtml.find('"videoId"')
        if vid_idx != -1:
            signals["streams_tab_videoid_sample"] = shtml[vid_idx:vid_idx+60]
    except Exception as e:
        signals["streams_tab_error"] = str(e)

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

    # 3. Check cookie authentication — fetch a YouTube page and check if logged in
    try:
        session = _make_session()
        r = session.get("https://www.youtube.com/",
                        timeout=10, allow_redirects=True)
        html = r.text
        import re as _re
        # Check for account name in various locations YouTube embeds it
        m = (_re.search(r'"accountName"\s*:\s*\{"simpleText"\s*:\s*"([^"]+)"', html)
          or _re.search(r'"displayName"\s*:\s*\{"simpleText"\s*:\s*"([^"]+)"', html)
          or _re.search(r'"email"\s*:\s*"([^"@]+@[^"]+)"', html))
        if m:
            result["cookie_auth"] = f"logged in as: {m.group(1)}"
        elif '"LOGGED_IN":true' in html or '"isSignedIn":true' in html:
            result["cookie_auth"] = "authenticated (account name not found in page)"
        elif '"LOGGED_IN":false' in html:
            result["cookie_auth"] = "NOT authenticated — cookies expired or invalid"
        else:
            # Try checking if the response contains personalised content
            if "subscriptions" in html.lower() or "history" in html.lower():
                result["cookie_auth"] = "likely authenticated (personalised content detected)"
            else:
                result["cookie_auth"] = "unknown — could not determine login state"
        result["cookie_count"] = len(list(session.cookies))
        result["yt_response_len"] = len(html)
    except Exception as e:
        result["cookie_auth"] = f"error: {e}"

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

@app.route("/api/cookies/debug")
def api_cookies_debug():
    """Show which cookies are loaded (names only, no values for security)."""
    try:
        from http.cookiejar import MozillaCookieJar
        jar = MozillaCookieJar()
        jar.load(str(_COOKIES_PATH), ignore_discard=True, ignore_expires=True)
        yt_cookies  = [(c.name, c.domain, c.expires) for c in jar if "youtube" in c.domain]
        key_cookies = ["SID", "SSID", "HSID", "LOGIN_INFO", "SAPISID",
                       "__Secure-1PSID", "__Secure-3PSID", "VISITOR_INFO1_LIVE"]
        present = [c[0] for c in yt_cookies]
        missing = [k for k in key_cookies if k not in present]
        return jsonify({
            "total_cookies": len(list(jar)),
            "youtube_cookies": len(yt_cookies),
            "youtube_cookie_names": present,
            "key_auth_cookies_present": [k for k in key_cookies if k in present],
            "key_auth_cookies_missing": missing,
            "auth_ok": len(missing) == 0,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

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
    # Migrate old last_status dicts that predate is_upcoming / is_waiting / is_waiting fields
    _now_iso = datetime.now(timezone.utc).isoformat()
    for _i, _ch in enumerate(state["channels"]):
        _st = _ch.get("last_status", {})
        _st.setdefault("is_upcoming", False)
        _st.setdefault("is_waiting",  False)
        _st.setdefault("scheduled_at", None)
        _ch.setdefault("upcoming_statuses", [])
        # Stagger _upcoming_check_n so channels don't all scan at once.
        # Spread 39 channels across 10 slots: channel i starts at slot i%10.
        # This means at most ~4 channels scan per cycle instead of all 39.
        _ch["_upcoming_check_n"] = _i % 10
        # Do not backfill started_at — only real scraper values are used
    print("=" * 52)
    print("  Livestream Monitor (YouTube + Twitch)")
    print("  No API keys required")
    print("  Open: http://localhost:5000")
    print("=" * 52)
    app.run(host="0.0.0.0", debug=False, port=5000)
