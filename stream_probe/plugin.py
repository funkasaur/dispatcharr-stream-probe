"""Stream Probe: checks channels through Dispatcharr's own proxy and records what they really carry.

With the Proxy stream profile Dispatcharr never runs FFmpeg, so a stream's resolution, codec, frame rate and
bitrate stay empty unless someone transcodes it. IPTV Checker fills them by opening the provider URL directly,
which bypasses Dispatcharr's connection counting and can kick a viewer off a one-connection account.

This plugin plays each channel through the proxy as a dedicated XC user for a few seconds, one channel at a
time, in a night window. Dispatcharr applies every account's connection limit and its M3U profiles, so a busy
account answers 503 and the channel is logged "busy" instead of "dead". The sampled bytes go to ffprobe and the
result is written to the stream that actually served the channel (Stream.stream_stats), the same fields the UI
shows. Nothing is renamed, moved or deleted.

Needs: a Dispatcharr user for the probe (its XC password in the settings) and 127.0.0.0/8 in
Settings > Network Access > XC API, because the probe connects from inside the container.

Accounts that share one provider login (for example a provider's own XC account and an uploaded M3U built from
it) share its connections. keep_free counts the free connections of the whole login, found by applying each M3U
profile's search/replace to one of the account's stream URLs and comparing the username/password in the result.
"""
import datetime
import json
import logging
import os
import re
import shutil
import smtplib
import ssl
import subprocess
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from email.mime.text import MIMEText

from celery import shared_task

PLUGIN_KEY = "stream_probe"
TASK_NAME = "stream_probe.run_window"
DATA_DIR = "/data/plugin_data/stream_probe"
STATE = os.path.join(DATA_DIR, "state.json")
LOCK_KEY = "stream_probe:lock"
STOP_KEY = "stream_probe:stop"
DEFAULT_PROXY_BASE = "http://127.0.0.1:9191"
LOGGER = logging.getLogger("plugins.stream_probe")
TS_PACKET = 188
NULL_PID = 0x1FFF

NOTIFY_FIELDS = [
    {"id": "info_notify", "type": "info", "label": "Alerts",
     "description": "After a scheduled run that finds newly dead channels, one summary goes to Telegram and/or email. "
                    "Channels that were already dead on the previous check are counted, not listed again."},
    {"id": "alert_exclude_groups", "label": "Probe but never alert on these groups (comma-separated, exact names)",
     "type": "string", "default": "",
     "help_text": "For large optional groups where dead channels are expected. They still show in Report."},
    {"id": "server_label", "label": "Server label", "type": "string", "default": "",
     "help_text": "Name used in alerts. Empty = hostname."},
    {"id": "telegram_bot_token", "label": "Telegram bot token", "type": "string", "input_type": "password", "default": ""},
    {"id": "telegram_chat_id", "label": "Telegram chat id", "type": "string", "default": ""},
    {"id": "notify_email", "label": "Email to (comma-separated)", "type": "string", "default": ""},
    {"id": "smtp_host", "label": "SMTP host", "type": "string", "default": ""},
    {"id": "smtp_port", "label": "SMTP port", "type": "number", "default": 587},
    {"id": "smtp_security", "label": "SMTP security", "type": "select", "default": "starttls",
     "options": [{"value": "starttls", "label": "STARTTLS"}, {"value": "ssl", "label": "SSL/TLS"}, {"value": "none", "label": "None"}]},
    {"id": "smtp_username", "label": "SMTP username", "type": "string", "default": ""},
    {"id": "smtp_password", "label": "SMTP password", "type": "string", "input_type": "password", "default": ""},
]

FIELDS = [
    {"id": "xc_username", "label": "Probe user (Dispatcharr username)", "type": "string", "default": "",
     "help_text": "A dedicated non-admin user, so its traffic shows separately in usage counts."},
    {"id": "xc_password", "label": "Probe user's XC password", "type": "string", "input_type": "password", "default": ""},
    {"id": "include_groups", "label": "Only these channel groups (comma-separated, exact names)", "type": "string", "default": "",
     "help_text": "Empty = every group except the excluded ones."},
    {"id": "exclude_groups", "label": "Skip these channel groups (comma-separated, exact names)", "type": "string", "default": "",
     "help_text": "Event-slot groups are empty most of the day; probing them says nothing."},
    {"id": "max_channels", "label": "Channels per run (max)", "type": "number", "default": 150},
    {"id": "recheck_days", "label": "Re-check a working channel after (days)", "type": "number", "default": 7,
     "help_text": "Never-checked channels go first, then the oldest. Failed ones are retried on the next run."},
    {"id": "sample_seconds", "label": "Seconds of video to sample", "type": "number", "default": 8},
    {"id": "connect_timeout", "label": "Give up on a channel after (seconds without video)", "type": "number", "default": 30},
    {"id": "pause_seconds", "label": "Pause between channels (seconds)", "type": "number", "default": 3},
    {"id": "keep_free", "label": "Leave this many provider connections free for viewers", "type": "number", "default": 1,
     "help_text": "Before each channel the probe counts free connections on the provider login(s) behind its first "
                  "stream's M3U account, including other M3U accounts that use the same login; at or below this "
                  "number the channel is logged as busy and retried next run."},
    {"id": "start_time", "label": "Nightly start (HH:MM, server time zone)", "type": "string", "default": "03:30"},
    {"id": "window_minutes", "label": "Stop starting new channels after (minutes)", "type": "number", "default": 150},
    {"id": "schedule_enabled", "label": "Run nightly", "type": "boolean", "default": False},
    {"id": "proxy_base", "label": "Dispatcharr address seen from inside the container", "type": "string",
     "default": DEFAULT_PROXY_BASE,
     "help_text": "Where the probe opens /live/... URLs. The default fits the standard image (uWSGI on port 9191)."},
] + NOTIFY_FIELDS

ACTIONS = [
    {"id": "probe_now", "label": "Probe now", "description": "Start a run now in the background (uses the limits above; "
     "params: limit, group, channel_ids). Watch progress with Status.", "button_label": "Start", "button_color": "blue"},
    {"id": "stop", "label": "Stop", "description": "Stop the running probe after the current channel.", "button_label": "Stop",
     "button_color": "red"},
    {"id": "status", "label": "Status", "description": "Current or last run, schedule.", "button_label": "Status"},
    {"id": "report", "label": "Report", "description": "Dead/busy channels and the quality of everything probed.",
     "button_label": "Show"},
    {"id": "dead_list", "label": "Dead list (JSON)", "description": "Channels whose last check failed, with how many checks "
     "in a row failed, for scripts.", "button_label": "Get JSON"},
    {"id": "apply_schedule", "label": "Apply schedule", "description": "Create, update or switch off the nightly run.",
     "button_label": "Apply"},
    {"id": "test_alert", "label": "Send test alert", "description": "Send a test message to the configured Telegram chat and email.",
     "button_label": "Test"},
]


def _now():
    return datetime.datetime.now(datetime.timezone.utc)


def _load_json(path, default):
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return default


def _save_json(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(data, fh, separators=(",", ":"), default=str)
    os.replace(tmp, path)


def _settings_from_db():
    from apps.plugins.models import PluginConfig
    cfg = PluginConfig.objects.filter(key=PLUGIN_KEY).first()
    merged = {f["id"]: f.get("default") for f in FIELDS if f.get("type") != "info"}
    if cfg and cfg.settings:
        merged.update(cfg.settings)
    return merged, bool(cfg and cfg.enabled)


def _label(settings):
    return settings.get("server_label") or os.uname().nodename


def _names(value):
    return [v.strip() for v in str(value or "").split(",") if v.strip()]


def _notify(settings, subject, text):
    """Send to Telegram and/or email; return list of delivery errors (never raises)."""
    errors = []
    token, chat = (settings.get("telegram_bot_token") or "").strip(), str(settings.get("telegram_chat_id") or "").strip()
    if token and chat:
        body = f"{subject}\n\n{text}"
        for i in range(0, len(body), 3900):  # Telegram limit 4096; split, never drop
            data = urllib.parse.urlencode({"chat_id": chat, "text": body[i:i + 3900], "disable_web_page_preview": "true"}).encode()
            try:
                urllib.request.urlopen(f"https://api.telegram.org/bot{token}/sendMessage", data=data, timeout=20).read()
            except Exception as e:
                errors.append(f"telegram: {e}")
                break
    to = [a.strip() for a in (settings.get("notify_email") or "").split(",") if a.strip()]
    if to and settings.get("smtp_host"):
        msg = MIMEText(text, "plain", "utf-8")
        sender = settings.get("smtp_username") or "dispatcharr@localhost"
        msg["Subject"], msg["From"], msg["To"] = subject, sender, ", ".join(to)
        try:
            host, port, sec = settings["smtp_host"], int(settings.get("smtp_port") or 587), settings.get("smtp_security", "starttls")
            server = smtplib.SMTP_SSL(host, port, timeout=30, context=ssl.create_default_context()) if sec == "ssl" else smtplib.SMTP(host, port, timeout=30)
            if sec == "starttls":
                server.starttls(context=ssl.create_default_context())
            if settings.get("smtp_username"):
                server.login(settings["smtp_username"], settings.get("smtp_password") or "")
            server.sendmail(sender, to, msg.as_string())
            server.quit()
        except Exception as e:
            errors.append(f"email: {e}")
    for e in errors:
        LOGGER.warning("Stream Probe alert delivery failed: %s", e)
    return errors


# --- pure helpers (unit-tested) ---

def ts_payload_bytes(buf):
    """Bytes of MPEG-TS packets in buf that are not null (keep-alive) packets. buf must start on a packet boundary."""
    n = 0
    for i in range(0, len(buf) - TS_PACKET + 1, TS_PACKET):
        if buf[i] != 0x47:
            return len(buf)  # not aligned TS: count everything
        pid = ((buf[i + 1] & 0x1F) << 8) | buf[i + 2]
        if pid != NULL_PID:
            n += TS_PACKET
    return n


def _rate(value):
    try:
        num, den = str(value).split("/")
        num, den = float(num), float(den)
        return round(num / den, 2) if num > 0 and den > 0 else None
    except Exception:
        return None


def stats_from_ffprobe(info, kbps):
    """Map ffprobe -show_streams JSON to Dispatcharr's stream_stats keys."""
    streams = info.get("streams") or []
    video = next((s for s in streams if s.get("codec_type") == "video" and s.get("width")), None)
    audio = next((s for s in streams if s.get("codec_type") == "audio"), None)
    out = {"stream_type": (info.get("format") or {}).get("format_name") or "mpegts"}
    if video:
        out["video_codec"] = video.get("codec_name")
        out["resolution"] = f"{video['width']}x{video['height']}"
        fps = _rate(video.get("avg_frame_rate")) or _rate(video.get("r_frame_rate"))
        if fps and fps < 200:
            out["source_fps"] = fps
        if video.get("pix_fmt"):
            out["pixel_format"] = video["pix_fmt"]
        if str(video.get("bit_rate") or "").isdigit():
            out["video_bitrate"] = round(int(video["bit_rate"]) / 1000, 1)
    if audio:
        out["audio_codec"] = audio.get("codec_name")
        if str(audio.get("sample_rate") or "").isdigit():
            out["sample_rate"] = int(audio["sample_rate"])
        layout = audio.get("channel_layout") or ({1: "mono", 2: "stereo", 6: "5.1"}.get(audio.get("channels")))
        if layout:
            out["audio_channels"] = layout
        if str(audio.get("bit_rate") or "").isdigit():
            out["audio_bitrate"] = round(int(audio["bit_rate"]) / 1000, 1)
    if kbps:
        out["probe_kbps"] = round(kbps, 1)
    return {k: v for k, v in out.items() if v is not None}


XC_PATH = re.compile(r"^/(?:live/|movie/|series/)?([^/]+)/([^/]+)/[^/]+$")


def js_replace(pattern):
    """Dispatcharr profile replace patterns use $1 / $<name>; convert to Python's re syntax."""
    return re.sub(r"\$(\d+)", r"\\\1", re.sub(r"\$<([^>]+)>", r"\\g<\1>", pattern or ""))


def login_key(sample_url, search="", replace=""):
    """(username, password) a profile really uses: its search/replace applied to a stream URL. None = not an XC URL.

    The host is ignored on purpose: providers hand out several host names for the same login."""
    url = sample_url or ""
    if search and replace:
        try:
            url = re.sub(search, js_replace(replace), url)
        except Exception:
            return None
    m = XC_PATH.match(re.sub(r"^[a-z]+://[^/]+", "", url, flags=re.I).split("?")[0])
    return m.groups() if m else None


def line_free(account_id, profiles, used):
    """Free connections for a channel on account_id, counting every account that shares a login.

    profiles = [(profile_id, account_id, max_streams, login_key)] for active profiles; used = {profile_id: in use}.
    A login's capacity is the smallest non-zero max_streams among its profiles and its use is the sum over all of
    them. Returns None when the account has an unlimited profile or no profiles."""
    lines = {}
    for pid, acct, limit, key in profiles:
        lines.setdefault(key if key else ("profile", pid), []).append((pid, acct, int(limit or 0)))
    mine = [k for k, rows in lines.items() if any(a == account_id for _, a, _ in rows)]
    if not mine:
        return None
    total = 0
    for k in mine:
        rows = lines[k]
        own_limits = [lim for _, a, lim in rows if a == account_id]
        if any(lim == 0 for lim in own_limits):
            return None
        caps = [lim for _, _, lim in rows if lim]
        total += max(0, min(caps) - sum(int(used.get(pid) or 0) for pid, _, _ in rows))
    return total


def shared_logins(profiles):
    """{login: sorted account ids} for logins used by more than one M3U account (shown in Status)."""
    accts = {}
    for _, acct, _, key in profiles:
        if key:
            accts.setdefault(key, set()).add(acct)
    return [sorted(a) for a in accts.values() if len(a) > 1]


def dead_streak(prev, result, at):
    """Fields carried on a record: how many checks in a row failed and since when."""
    if result != "dead":
        return {}
    if prev and prev.get("result") == "dead":
        return {"dead_count": int(prev.get("dead_count") or 1) + 1, "dead_since": prev.get("dead_since") or prev.get("at")}
    return {"dead_count": 1, "dead_since": at}


def alert_lines(dead, previous, skip_groups):
    """Split this run's dead records into (newly dead, already dead) for the alert, leaving out skip_groups.

    dead = [(channel_id, record)], previous = {channel_id: record before this run}."""
    new, old = [], []
    for cid, rec in dead:
        if rec.get("group") in skip_groups:
            continue
        (old if (previous.get(cid) or {}).get("result") == "dead" else new).append(rec)
    return new, old


def classify_http(code, body):
    """Result for a non-200 answer from the proxy."""
    text = (body or "").lower()
    if code == 503 and ("no available" in text or "connection" in text or "waited" in text or "resources" in text):
        return "busy"
    if code in (401, 403):
        return "auth"
    if code == 429:
        return "busy"
    return "dead"


def pick_channels(channels, state, now, recheck_days, limit):
    """Order: never probed, then failed last time, then oldest OK older than recheck_days. channels = [(id, ...)]."""
    fresh, failed, stale = [], [], []
    cutoff = now - datetime.timedelta(days=float(recheck_days or 7))
    for ch in channels:
        rec = (state or {}).get(str(ch[0]))
        if not rec:
            fresh.append(ch)
        elif rec.get("result") != "ok":
            failed.append(ch)
        else:
            try:
                at = datetime.datetime.fromisoformat(rec["at"])
            except Exception:
                at = None
            if at is None or at < cutoff:
                stale.append((rec.get("at") or "", ch))
    stale.sort(key=lambda x: x[0])
    return (fresh + failed + [c for _, c in stale])[:max(0, int(limit))]


def quality_label(rec):
    try:
        h = int(str(rec.get("resolution", "")).split("x")[1])
    except Exception:
        return "unknown"
    return "UHD" if h >= 2000 else "FHD" if h >= 1000 else "HD" if h >= 700 else "SD"


# --- Dispatcharr side ---

def _redis():
    from core.utils import RedisClient
    return RedisClient.get_client()


def _channels(settings):
    """[(id, number, name, group, uuid, first_stream_id)] in scope."""
    from apps.channels.models import Channel, ChannelStream
    inc, exc = set(_names(settings.get("include_groups"))), set(_names(settings.get("exclude_groups")))
    firsts = {}
    for cid, sid in ChannelStream.objects.order_by("channel_id", "order").values_list("channel_id", "stream_id"):
        firsts.setdefault(cid, sid)
    out = []
    for ch in Channel.objects.select_related("channel_group").order_by("channel_number"):
        group = ch.channel_group.name if ch.channel_group else ""
        if (inc and group not in inc) or group in exc:
            continue
        first = firsts.get(ch.id)
        if first is None:
            continue  # no streams: nothing to probe
        out.append((ch.id, ch.channel_number, ch.name, group, str(ch.uuid), first))
    return out


def _channel_meta(uuid):
    try:
        raw = _redis().hgetall(f"live:channel:{uuid}:metadata") or {}
        return {(k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v) for k, v in raw.items()}
    except Exception:
        return {}


def _profiles():
    """[(profile_id, account_id, max_streams, login_key)] for active profiles of active accounts (once per run)."""
    from apps.channels.models import Stream
    from apps.m3u.models import M3UAccountProfile
    samples, out = {}, []
    for p in M3UAccountProfile.objects.filter(is_active=True, m3u_account__is_active=True):
        acct = p.m3u_account_id
        if acct not in samples:
            samples[acct] = Stream.objects.filter(m3u_account_id=acct).values_list("url", flat=True).first() or ""
        out.append((p.id, acct, p.max_streams, login_key(samples[acct], p.search_pattern, p.replace_pattern)))
    return out


def _account_free(stream_id, profiles):
    """Free connections on the provider login(s) behind stream_id's account, or None when unlimited/unknown."""
    try:
        from apps.channels.models import Stream
        acct = Stream.objects.filter(id=stream_id).values_list("m3u_account_id", flat=True).first()
        if not acct:
            return None
        r = _redis()
        used = {pid: r.get(f"profile_connections:{pid}") for pid, *_ in profiles}
        return line_free(acct, profiles, used)
    except Exception:
        LOGGER.exception("Stream Probe: could not count free connections")
        return None


def _wait_released(uuid, max_wait=45):
    end = time.time() + max_wait
    while time.time() < end:
        try:
            if not _redis().exists(f"live:channel:{uuid}:metadata"):
                return True
        except Exception:
            return False
        time.sleep(1)
    return False


def _ffprobe(path):
    exe = shutil.which("ffprobe") or "/usr/local/bin/ffprobe"
    res = subprocess.run([exe, "-v", "error", "-of", "json", "-show_streams", "-show_format", path],
                         capture_output=True, text=True, timeout=40)
    return json.loads(res.stdout or "{}")


def probe_channel(settings, ch):
    """Play one channel through the proxy; return a result record (never raises)."""
    cid, num, name, group, uuid, first_stream = ch
    rec = {"at": _now().isoformat(), "name": name, "number": num, "group": group}
    user, pw = settings.get("xc_username"), settings.get("xc_password")
    base = (settings.get("proxy_base") or DEFAULT_PROXY_BASE).rstrip("/")
    url = f"{base}/live/{urllib.parse.quote(user)}/{urllib.parse.quote(pw)}/{cid}.ts"
    sample, timeout = float(settings.get("sample_seconds") or 8), float(settings.get("connect_timeout") or 30)
    buf, real, served, started = bytearray(), 0, None, time.time()
    first_real = None
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "StreamProbe/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            carry = b""
            while True:
                chunk = resp.read1(65536)
                if not chunk:
                    break
                data = carry + chunk
                cut = len(data) - len(data) % TS_PACKET
                block, carry = data[:cut], data[cut:]
                n = ts_payload_bytes(block)
                if n:
                    if first_real is None:
                        first_real = time.time()
                        rec["wait_s"] = round(first_real - started, 1)
                    real += n
                    buf += block
                if served is None:
                    served = _channel_meta(uuid).get("stream_id")
                if first_real is None and time.time() - started > timeout:
                    break
                if first_real is not None and time.time() - first_real >= sample:
                    break
    except urllib.error.HTTPError as e:
        body = e.read(2000).decode("utf-8", "replace")
        rec.update(result=classify_http(e.code, body), note=f"HTTP {e.code}: {body[:160]}")
        return rec
    except Exception as e:
        if first_real is None:
            rec.update(result="dead", note=f"{type(e).__name__}: {str(e)[:160]}")
            return rec
    if first_real is None or real < 50 * TS_PACKET:
        rec.update(result="dead", note=f"no video within {int(timeout)} s")
        return rec
    elapsed = max(0.5, time.time() - first_real)
    kbps = real * 8 / 1000 / elapsed
    try:
        with tempfile.NamedTemporaryFile(suffix=".ts", delete=False) as fh:
            fh.write(buf)
            path = fh.name
        try:
            info = _ffprobe(path)
        finally:
            os.unlink(path)
    except Exception as e:
        info = {}
        rec["note"] = f"ffprobe failed: {str(e)[:120]}"
    stats = stats_from_ffprobe(info, kbps)
    stream_id = int(served) if str(served or "").isdigit() else None
    rec.update(result="ok" if stats.get("resolution") else "no_video", stream_id=stream_id,
               backup=bool(stream_id and stream_id != first_stream), **{k: stats.get(k) for k in
               ("resolution", "source_fps", "video_codec", "audio_codec", "audio_channels", "probe_kbps")})
    if stream_id:
        stats["probe_at"] = rec["at"]
        try:
            try:
                from apps.proxy.live_proxy.services.channel_service import ChannelService
            except ImportError:
                from apps.proxy.ts_proxy.services.channel_service import ChannelService
            ChannelService._update_stream_stats_in_db(stream_id, **stats)
        except Exception as e:
            rec["note"] = f"stats not saved: {str(e)[:120]}"
    return rec


def _do_run(settings, trigger, limit=None, group=None, channel_ids=None):
    r = _redis()
    window = float(settings.get("window_minutes") or 150) * 60
    if not r.set(LOCK_KEY, trigger, nx=True, ex=int(window + 3600)):
        return "already running"
    r.delete(STOP_KEY)
    st = _load_json(STATE, {})
    results = st.setdefault("channels", {})
    run = {"trigger": trigger, "started": _now().isoformat(), "done": 0, "counts": {}, "finished": None}
    st["run"] = run
    try:
        if not settings.get("xc_username") or not settings.get("xc_password"):
            run["finished"], run["error"] = _now().isoformat(), "probe user / XC password not set"
            _save_json(STATE, st)
            return run["error"]
        chans = _channels(dict(settings, include_groups=group) if group else settings)
        if channel_ids:
            wanted = {int(c) for c in channel_ids}
            chans = [c for c in chans if c[0] in wanted]
            limit = limit or len(chans)
        previous = {k: dict(v) for k, v in results.items()}
        try:
            profiles = _profiles()
        except Exception:
            LOGGER.exception("Stream Probe: could not read M3U profiles")
            profiles = []
        todo = pick_channels(chans, results, _now(), settings.get("recheck_days"), limit or settings.get("max_channels") or 150)
        run["planned"] = len(todo)
        _save_json(STATE, st)
        deadline = time.time() + window
        for ch in todo:
            if r.get(STOP_KEY) or time.time() > deadline:
                run["stopped"] = "stop requested" if r.get(STOP_KEY) else "window ended"
                break
            free = _account_free(ch[5], profiles)
            keep = int(float(settings.get("keep_free") if settings.get("keep_free") is not None else 1))
            if free is not None and free <= keep:
                rec = {"at": _now().isoformat(), "name": ch[2], "number": ch[1], "group": ch[3], "result": "busy",
                       "note": f"{free} connection(s) free, kept for viewers"}
            else:
                rec = probe_channel(settings, ch)
            rec.update(dead_streak(previous.get(str(ch[0])), rec["result"], rec["at"]))
            results[str(ch[0])] = rec
            run["done"] += 1
            run["counts"][rec["result"]] = run["counts"].get(rec["result"], 0) + 1
            run["current"] = f"{ch[1]} {ch[2]}"
            _save_json(STATE, st)
            _wait_released(ch[4])
            time.sleep(float(settings.get("pause_seconds") or 3))
        run["finished"] = _now().isoformat()
        run.pop("current", None)
        st.setdefault("history", []).insert(0, dict(run))
        st["history"] = st["history"][:20]
        _save_json(STATE, st)
        dead = [(k, v) for k, v in results.items() if v.get("result") == "dead" and v.get("at", "") >= run["started"]]
        new, old = alert_lines(dead, previous, set(_names(settings.get("alert_exclude_groups"))))
        run["newly_dead"] = len(new)
        if new and trigger == "schedule":
            lines = "\n".join(f"• {d.get('number')} {d.get('name')} [{d.get('group')}]: {d.get('note', '')[:80]}" for d in new[:40])
            if len(new) > 40:
                lines += f"\n… and {len(new) - 40} more (see Report)"
            if old:
                lines += f"\n\nStill dead from earlier checks: {len(old)}"
            _notify(settings, f"📺 Stream Probe ({_label(settings)}): {len(new)} newly dead of {run['done']}", lines)
        return f"{run['done']} probed: {run['counts']}"
    finally:
        r.delete(LOCK_KEY)


@shared_task(name=TASK_NAME)
def run_window(trigger="schedule", limit=None, group=None, channel_ids=None):
    settings, enabled = _settings_from_db()
    if not enabled:
        return "disabled"
    return _do_run(settings, trigger, limit=limit, group=group, channel_ids=channel_ids)


def _ensure_schedule(settings):
    from django_celery_beat.models import CrontabSchedule, PeriodicTask
    if not settings.get("schedule_enabled"):
        n = PeriodicTask.objects.filter(name="Stream Probe nightly").update(enabled=False)
        return "Nightly run is off" + (" (schedule disabled)." if n else ".")
    try:
        from core.models import CoreSettings
        tz = CoreSettings.get_system_time_zone()
    except Exception:
        tz = "UTC"
    hh, mm = (str(settings.get("start_time") or "03:30").split(":") + ["0"])[:2]
    hour, minute = max(0, min(23, int(hh))), max(0, min(59, int(mm)))
    cron, _ = CrontabSchedule.objects.get_or_create(minute=str(minute), hour=str(hour), day_of_week="*", day_of_month="*",
                                                    month_of_year="*", timezone=tz)
    # queue dvr: thread-pool worker, so a run of a few hours holds one of its threads and nothing else.
    PeriodicTask.objects.update_or_create(name="Stream Probe nightly",
                                          defaults={"task": TASK_NAME, "crontab": cron, "interval": None, "enabled": True,
                                                    "queue": "dvr", "kwargs": json.dumps({"trigger": "schedule"})})
    return f"Nightly run at {hour:02d}:{minute:02d} ({tz}), up to {settings.get('max_channels')} channels / {settings.get('window_minutes')} min."


def _report():
    st = _load_json(STATE, {})
    chans = st.get("channels") or {}
    if not chans:
        return {"status": "ok", "message": "Nothing probed yet."}
    by = {}
    for v in chans.values():
        by.setdefault(v.get("result"), []).append(v)
    ok = by.get("ok", [])
    q = {}
    for v in ok:
        q[quality_label(v)] = q.get(quality_label(v), 0) + 1
    lines = [f"Probed {len(chans)} channels: " + ", ".join(f"{k} {len(v)}" for k, v in sorted(by.items(), key=lambda x: str(x[0]))),
             "Quality of working ones: " + ", ".join(f"{k} {n}" for k, n in sorted(q.items())),
             f"Served by a backup stream: {sum(1 for v in ok if v.get('backup'))}",
             f"Low frame rate (<24 fps): {sum(1 for v in ok if (v.get('source_fps') or 99) < 24)}"]
    for res in ("dead", "no_video", "busy", "auth"):
        rows = sorted(by.get(res, []), key=lambda v: (v.get("number") or 0))
        if rows:
            lines.append(f"\n{res.upper()} ({len(rows)}):")
            lines += [f"• {v.get('number')} {v.get('name')} [{v.get('group')}] {v.get('note', '')[:90]}" for v in rows[:60]]
    return {"status": "ok", "message": "\n".join(lines)}


def _dead_list():
    st = _load_json(STATE, {})
    rows = [{"channel_id": int(k), "number": v.get("number"), "name": v.get("name"), "group": v.get("group"),
             "result": v.get("result"), "at": v.get("at"), "dead_count": v.get("dead_count", 1),
             "dead_since": v.get("dead_since") or v.get("at"), "note": (v.get("note") or "")[:120]}
            for k, v in (st.get("channels") or {}).items() if v.get("result") in ("dead", "no_video") and k.isdigit()]
    rows.sort(key=lambda r: (r["number"] or 0))
    return {"status": "ok", "message": f"{len(rows)} channels failed their last check.", "dead": rows}


def _status():
    st = _load_json(STATE, {})
    run = st.get("run") or {}
    try:
        from django_celery_beat.models import PeriodicTask
        pt = PeriodicTask.objects.filter(task=TASK_NAME).first()
        sched = f"{pt.crontab} enabled={pt.enabled} last_run={pt.last_run_at}" if pt else "not scheduled"
    except Exception as e:
        sched = f"unknown ({e})"
    try:
        running = bool(_redis().get(LOCK_KEY))
    except Exception:
        running = False
    state = "RUNNING" if running else "idle"
    try:
        shared = shared_logins(_profiles())
    except Exception:
        shared = []
    msg = (f"{state}. Run ({run.get('trigger')}) started {str(run.get('started'))[:19]}: {run.get('done', 0)}/{run.get('planned', '?')} "
           f"{run.get('counts', {})}" + (f", now {run['current']}" if running and run.get("current") else "") +
           (f", finished {str(run['finished'])[:19]}" if run.get("finished") else "") +
           (f", {run['stopped']}" if run.get("stopped") else "") + (f", error: {run['error']}" if run.get("error") else "") +
           (f", newly dead {run['newly_dead']}" if run.get("newly_dead") is not None else "") +
           f"\nSchedule: {sched}" +
           (f"\nM3U accounts sharing a provider login (keep_free counts them together): {shared}" if shared else ""))
    return {"status": "ok", "message": msg}


class Plugin:
    name = "Stream Probe"
    version = "1.2.0"
    description = ("Plays each channel for a few seconds through Dispatcharr's own proxy, one at a time, and records "
                   "resolution, codec, frame rate and bitrate on the stream that served it. Connection limits are "
                   "respected, so a busy account is skipped, never kicked. Report-only.")
    author = "funkasaur"
    license = "MIT"
    help_url = "https://github.com/funkasaur/dispatcharr-stream-probe"
    repo_url = "https://github.com/funkasaur/dispatcharr-stream-probe"
    manifest_version = 2
    min_dispatcharr_version = "v0.34.0"
    # background_tasks: the run is a Celery task (nightly via beat); proxy_internals: channel metadata and connection
    # counts in Redis, writes stream_stats through the proxy's ChannelService; subprocess: ffprobe; outbound_network:
    # the probe's own HTTP request to the local proxy plus optional Telegram/SMTP alerts; filesystem_write: state file
    # under /data/plugin_data/stream_probe.
    capabilities = ["background_tasks", "proxy_internals", "subprocess", "outbound_network", "filesystem_write"]
    fields = FIELDS
    actions = ACTIONS

    def run(self, action, params, context):
        settings = context.get("settings", {})
        params = params or {}
        if action == "probe_now":
            limit = int(params["limit"]) if str(params.get("limit") or "").isdigit() else None
            ids = [int(c) for c in re.split(r"[,\s]+", str(params.get("channel_ids") or "")) if c.isdigit()] or None
            run_window.apply_async(kwargs={"trigger": "manual", "limit": limit, "group": params.get("group") or None,
                                           "channel_ids": ids}, queue="dvr")
            return {"status": "ok", "message": "Probe started in the background; use Status to follow it."}
        if action == "stop":
            _redis().set(STOP_KEY, "1", ex=3600)
            return {"status": "ok", "message": "Stop requested; the run ends after the current channel."}
        if action == "status":
            return _status()
        if action == "report":
            return _report()
        if action == "dead_list":
            return _dead_list()
        if action == "apply_schedule":
            return {"status": "ok", "message": _ensure_schedule(settings)}
        if action == "test_alert":
            if not ((settings.get("telegram_bot_token") and settings.get("telegram_chat_id")) or
                    (settings.get("notify_email") and settings.get("smtp_host"))):
                return {"status": "error", "message": "No Telegram chat or email configured."}
            errs = _notify(settings, f"✅ Stream Probe ({_label(settings)}) test", "Test alert from the Stream Probe plugin.")
            return {"status": "error" if errs else "ok", "message": "; ".join(errs) or "Sent."}
        return {"status": "error", "message": f"Unknown action {action}"}
