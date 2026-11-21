<h1 align="center">Stream Probe for Dispatcharr</h1>

<p align="center">Measure playback through the Dispatcharr proxy.</p>

<p align="center">
  <a href="LICENSE"><img alt="License" src="https://img.shields.io/github/license/funkasaur/dispatcharr-stream-probe"></a>
  <a href="https://github.com/funkasaur/dispatcharr-stream-probe/commits/"><img alt="Last commit" src="https://img.shields.io/github/last-commit/funkasaur/dispatcharr-stream-probe"></a>
  <a href="https://github.com/funkasaur/dispatcharr-stream-probe/releases/latest"><img alt="Latest release" src="https://img.shields.io/github/v/release/funkasaur/dispatcharr-stream-probe"></a>
</p>

## Overview

A [Dispatcharr](https://github.com/Dispatcharr/Dispatcharr) plugin that checks your live channels the way a viewer
would: it plays each channel for a few seconds through Dispatcharr's own proxy, runs `ffprobe` on what came back,
and writes resolution, codec, frame rate, audio and bitrate onto the stream that served it (the same
`stream_stats` fields the Dispatcharr UI shows). Channels that send no video are reported as dead.

With the default **Proxy** stream profile Dispatcharr never runs FFmpeg, so those fields stay empty. Tools that
open the provider URL directly fill them, but they bypass Dispatcharr's connection counting and can kick a viewer
off a one-connection account. Stream Probe goes through the proxy as a normal (dedicated) user instead, so every
account's connection limit and M3U profile applies: a busy account is skipped, never kicked.

Nothing is renamed, moved or deleted. The plugin only reads channels and writes `stream_stats`.

## Features

- Runs ffprobe on the stream served by Dispatcharr.
- Records stream statistics and reports channels that return no video.

## Repository structure

```text
dispatcharr-stream-probe/
├── stream_probe/  # Plugin source, manifest and logo
├── build.py       # Versioned plugin ZIP builder
└── tests/         # Offline unit tests
```

## How it differs from IPTV Checker and VOD Probe

Two plugins in the registry already use `ffprobe` on streams. Stream Probe does a different job from both:

| | **Stream Probe** | **IPTV Checker** | **VOD Probe** |
|---|---|---|---|
| What it checks | Live channels | Live channels | VOD (movies and episodes) |
| How it connects | Through Dispatcharr's own proxy, as a dedicated user | Opens the provider stream URLs directly | Probes VOD relations |
| Connection limits | Counts like any viewer: each M3U profile's limit applies, it skips a channel when the provider login has too few free connections, and accounts sharing one login are counted together | Outside Dispatcharr's connection counting | n/a (VOD) |
| What it writes | Resolution, codec, fps, audio and bitrate into the stream's `stream_stats` (the fields the UI shows) | Its own results, then renames, moves, restores or deletes channels by result | Quality into each VOD relation's `custom_properties` |
| Changes to your lineup | None. It only reports | Yes, by design | None |
| Schedule | Nightly window with a per-night cap; rechecks each channel every N days | — | Background task |
| Alerts | Only newly dead channels; groups can be "probe but don't alert" | — | — |

**Why it is useful:**
- **Safe on 1-connection providers.** A checker that opens provider URLs directly can take the only connection a viewer needs, or get an account flagged for too many connections. Stream Probe asks Dispatcharr for the stream like any client, so the provider never sees more connections than your limits allow. It also waits instead of competing when a line is busy.
- **Fills the empty stream stats.** With the default Proxy stream profile, `stream_stats` stays empty, so the UI shows no resolution or codec. Stream Probe fills those fields for every channel from a real playback sample.
- **Finds dead channels before viewers do,** with an alert that lists only what newly broke, not the same list every night.
- **Reports, never edits.** It complements IPTV Checker rather than replacing it: use IPTV Checker when you want automatic cleanup, and Stream Probe when you want quality data and dead-channel alerts without touching connections or the lineup.

## How a run works

- One channel at a time, in a nightly window (or on demand). Never-checked channels go first, then channels that
  failed last time, then working ones older than *Re-check after (days)*.
- Before each channel it counts free connections on the provider login behind the channel's first stream. If
  *Leave this many connections free* or fewer are left, the channel is logged as **busy** and retried next run.
- **Shared logins:** if two M3U accounts use the same provider login (for example a provider's XC account and an
  M3U playlist built from it), their connections are counted together. The plugin finds this by applying each M3U
  profile's search/replace to one of the account's stream URLs and comparing the username/password in the result;
  host names are ignored, since providers often hand out several. *Status* lists the accounts it grouped.
- It samples about 8 s of real video (MPEG-TS null packets don't count), runs `ffprobe`, then waits until
  Dispatcharr has released the channel before starting the next one.
- Results: `ok`, `dead` (no video within the timeout, or an HTTP error), `busy`, `auth` (wrong probe password or
  network access), `no_video` (audio only).

## Install

1. Install **Stream Probe** from the Dispatcharr plugin registry, or download `stream_probe-<version>.zip` from the
   [latest release](https://github.com/funkasaur/dispatcharr-stream-probe/releases) and import it under
   Dispatcharr → Plugins (or build it with `python3 build.py`).
2. Approve the requested capabilities and enable **Stream Probe**.
3. Create a **dedicated Dispatcharr user** for the probe (non-admin, with access to the channels you want checked)
   and give it an XC password. Its traffic then shows up under its own name in usage statistics.
4. **Allow the probe to use the XC API from inside the container:** Settings → Network Access → **XC API**, add
   `127.0.0.0/8`. See the next section for why this is needed and why it is safe.
5. Enter the probe user and its XC password in the plugin settings, press **Probe now** with a small limit
   (for example `{"limit": 5}`), and follow it with **Status**.
6. To run nightly: set the start time and window, turn on *Run nightly*, press **Apply schedule**.

Requires Dispatcharr 0.34.0 or later and `ffprobe` in the container (the official image has it).

### Why 127.0.0.0/8 in Network Access → XC API

The probe opens `http://127.0.0.1:9191/live/<user>/<password>/<channel>.ts` from inside the Dispatcharr container,
exactly like an XC client would. Dispatcharr's Network Access setting decides which client addresses may use the XC
API, and a request from the container itself comes from 127.0.0.1, which is not in the default list.

Adding `127.0.0.0/8` only allows clients **on the same host/container** (the loopback network). It does not open
anything to your LAN or the internet, and those clients still need a valid username and XC password. If your
container uses a different address or port for uWSGI, change *Dispatcharr address seen from inside the container*.

### Capabilities

| Capability | Why |
|---|---|
| Run background tasks | A run takes minutes to hours, so it runs as a Celery task (nightly via Celery beat). |
| Use live proxy internals | Reads channel metadata and per-profile connection counts in Redis to see which stream served the channel and how many connections are free; saves stats through the proxy's own stream-stats update. |
| Run host processes | Runs `ffprobe` on the sampled bytes. |
| Access external network services | The probe's own HTTP request to the local proxy, plus the optional Telegram/email alerts. Nothing else is contacted; there is no telemetry. |
| Write outside plugin storage | Keeps its state file in `/data/plugin_data/stream_probe`. |

## Settings

| Setting | Default | Meaning |
|---|---|---|
| Probe user / XC password | blank | The dedicated user from step 3. |
| Only these channel groups | blank | Comma-separated exact group names; blank = all groups. |
| Skip these channel groups | blank | For example event-slot groups that are empty most of the day. |
| Channels per run (max) | 150 | Upper limit per run. |
| Re-check a working channel after (days) | 7 | A full cycle takes about channel count ÷ channels per run nights. |
| Seconds of video to sample | 8 | Longer = steadier bitrate, longer run. |
| Give up after (seconds without video) | 30 | Time to first real video before a channel counts as dead. |
| Pause between channels | 3 | Seconds. |
| Leave this many provider connections free | 1 | Set 0 to use every connection (only if nobody watches during the window). |
| Nightly start / window | 03:30 / 150 min | Server time zone (System Settings). No new channel starts after the window. |
| Run nightly | off | Press **Apply schedule** after changing. |
| Dispatcharr address seen from inside the container | `http://127.0.0.1:9191` | Change only for non-standard images. |
| Probe but never alert on these groups | blank | Large optional groups where dead channels are expected; still in Report. |
| Alerts | blank | Optional Telegram bot token + chat id and/or SMTP settings. |

## Actions

- **Probe now**: start a run now. Optional params: `limit`, `group` (comma-separated group names), `channel_ids`
  (comma-separated channel ids, for rechecking specific channels).
- **Stop**: stop after the current channel.
- **Status**: current or last run, schedule, and the M3U accounts found to share a provider login.
- **Report**: counts, quality summary (UHD/FHD/HD/SD), low frame rates, and the dead/busy channels.
- **Dead list (JSON)**: channels whose last check failed, with `dead_count` (checks in a row) and `dead_since`,
  for scripts.
- **Apply schedule**, **Send test alert**.

After a scheduled run, alerts list only **newly** dead channels. Channels that were already dead on the previous
check are counted ("still dead: N"), not listed again.

## Notes and limits

- The bitrate is measured over a short sample that includes the proxy's start-up burst, so treat it as rough.
- A single sample can catch a short outage. Failed channels are retried first on the next run.
- The plugin uses Dispatcharr internals (Redis key names, the proxy's stream-stats update). A Dispatcharr update can
  change them; check **Status** and **Report** after updating.
- State lives in `/data/plugin_data/stream_probe/state.json`.

## Development

```
python3 -m unittest discover -s tests   # no Django, Redis or Celery needed
python3 build.py                        # dist/stream_probe-<version>.zip
```

## License

MIT
