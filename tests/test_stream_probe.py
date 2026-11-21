"""Stream Probe tests; no Django, Redis or Celery needed. Run: python3 -m unittest discover -s tests"""
import datetime
import importlib.util
import pathlib
import sys
import types
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]
celery = types.ModuleType("celery")
celery.shared_task = lambda *a, **k: (lambda f: f)
sys.modules.setdefault("celery", celery)
spec = importlib.util.spec_from_file_location("_t_stream_probe", ROOT / "stream_probe" / "plugin.py")
sp = importlib.util.module_from_spec(spec)
spec.loader.exec_module(sp)


class StreamProbeLogic(unittest.TestCase):
    def pkt(self, pid):
        return bytes([0x47, (pid >> 8) & 0x1F, pid & 0xFF]) + bytes(185)

    def test_null_packets_do_not_count(self):
        buf = self.pkt(0x1FFF) * 3 + self.pkt(0x100) * 2
        self.assertEqual(sp.ts_payload_bytes(buf), 2 * 188)

    def test_unaligned_counts_everything(self):
        self.assertEqual(sp.ts_payload_bytes(b"x" * 400), 400)

    def test_ffprobe_mapping(self):
        info = {"format": {"format_name": "mpegts"}, "streams": [
            {"codec_type": "video", "codec_name": "h264", "width": 1920, "height": 1080, "avg_frame_rate": "30000/1001",
             "pix_fmt": "yuv420p"},
            {"codec_type": "audio", "codec_name": "aac", "sample_rate": "48000", "channel_layout": "stereo", "bit_rate": "128000"}]}
        st = sp.stats_from_ffprobe(info, 6123.46)
        self.assertEqual((st["resolution"], st["source_fps"], st["video_codec"], st["audio_channels"], st["audio_bitrate"]),
                         ("1920x1080", 29.97, "h264", "stereo", 128.0))
        self.assertEqual(st["probe_kbps"], 6123.5)
        self.assertNotIn("video_bitrate", st)

    def test_ffprobe_no_video(self):
        st = sp.stats_from_ffprobe({"streams": [{"codec_type": "audio", "codec_name": "aac", "channels": 2}]}, None)
        self.assertNotIn("resolution", st)
        self.assertEqual(st["audio_channels"], "stereo")

    def test_classify(self):
        self.assertEqual(sp.classify_http(503, '{"error": "No available streams for this channel", "waited": "0s"}'), "busy")
        self.assertEqual(sp.classify_http(502, '{"error": "All available streams failed validation"}'), "dead")
        self.assertEqual(sp.classify_http(403, "Forbidden"), "auth")

    def test_pick_order(self):
        now = datetime.datetime(2026, 11, 18, tzinfo=datetime.timezone.utc)
        chans = [(i, i, f"c{i}", "g", f"u{i}", i) for i in range(1, 6)]
        state = {"1": {"result": "ok", "at": "2026-11-17T00:00:00+00:00"},       # fresh OK: skipped
                 "2": {"result": "dead", "at": "2026-11-17T00:00:00+00:00"},     # failed: retried
                 "3": {"result": "ok", "at": "2026-11-01T00:00:00+00:00"},       # stale
                 "4": {"result": "ok", "at": "2026-10-01T00:00:00+00:00"}}       # staler
        self.assertEqual([c[0] for c in sp.pick_channels(chans, state, now, 7, 10)], [5, 2, 4, 3])
        self.assertEqual(len(sp.pick_channels(chans, state, now, 7, 2)), 2)

    def test_login_key(self):
        self.assertEqual(sp.login_key("http://a.example:80/live/u1/p1/7.ts"), ("u1", "p1"))
        self.assertEqual(sp.login_key("http://b.example/u1/p1/7"), ("u1", "p1"))       # other host, same login
        self.assertEqual(sp.login_key("http://a.example/live/u1/p1/7.ts", "/u1/p1/", "/u2/p2/"), ("u2", "p2"))
        self.assertEqual(sp.login_key("http://a.example/live/u1/p1/7.ts", "^(.*)$", "$1"), ("u1", "p1"))
        self.assertIsNone(sp.login_key("http://a.example/playlist.m3u8"))
        self.assertIsNone(sp.login_key("http://a.example/live/u1/p1/7.ts", "(", "x"))   # bad regex

    def test_line_free_single_account(self):
        profiles = [(1, 10, 1, ("a", "x")), (2, 10, 1, ("b", "x"))]
        self.assertEqual(sp.line_free(10, profiles, {1: b"1", 2: None}), 1)
        self.assertEqual(sp.line_free(10, profiles, {1: b"1", 2: b"1"}), 0)
        self.assertIsNone(sp.line_free(10, [(1, 10, 0, ("a", "x"))], {1: b"5"}))
        self.assertIsNone(sp.line_free(99, profiles, {}))

    def test_line_free_shared_login(self):
        # raw XC account 28 and an uploaded copy 33 use the same 2-connection login: a viewer on 28 counts for 33
        profiles = [(33, 28, 2, ("u", "p")), (38, 33, 2, ("u", "p")), (5, 7, 1, ("other", "p"))]
        self.assertEqual(sp.line_free(33, profiles, {33: b"1", 38: b"0"}), 1)
        self.assertEqual(sp.line_free(33, profiles, {33: b"1", 38: b"1"}), 0)
        self.assertEqual(sp.line_free(28, profiles, {33: b"2", 38: None}), 0)
        # two logins behind one account (second profile rewrites the login), each shared with another account
        two = [(31, 26, 1, ("d1", "p")), (39, 26, 1, ("d2", "p")), (37, 32, 1, ("d1", "p")), (40, 32, 1, ("d2", "p"))]
        self.assertEqual(sp.line_free(32, two, {31: b"1"}), 1)
        self.assertEqual(sp.line_free(32, two, {31: b"1", 40: b"1"}), 0)
        # unknown logins (non-XC URLs) stay per profile
        self.assertEqual(sp.line_free(5, [(1, 5, 2, None), (2, 6, 2, None)], {2: b"2"}), 2)

    def test_shared_logins(self):
        profiles = [(33, 28, 2, ("u", "p")), (38, 33, 2, ("u", "p")), (5, 7, 1, ("o", "p")), (6, 8, 1, None)]
        self.assertEqual(sp.shared_logins(profiles), [[28, 33]])

    def test_dead_streak(self):
        self.assertEqual(sp.dead_streak(None, "dead", "t2"), {"dead_count": 1, "dead_since": "t2"})
        self.assertEqual(sp.dead_streak({"result": "ok", "at": "t1"}, "dead", "t2"), {"dead_count": 1, "dead_since": "t2"})
        self.assertEqual(sp.dead_streak({"result": "dead", "at": "t1"}, "dead", "t2"), {"dead_count": 2, "dead_since": "t1"})
        self.assertEqual(sp.dead_streak({"result": "dead", "at": "t1", "dead_count": 3, "dead_since": "t0"}, "dead", "t2"),
                         {"dead_count": 4, "dead_since": "t0"})
        self.assertEqual(sp.dead_streak({"result": "dead"}, "ok", "t2"), {})

    def test_alert_only_newly_dead(self):
        dead = [("1", {"name": "A", "group": "News"}), ("2", {"name": "B", "group": "News"}),
                ("3", {"name": "C", "group": "Optional"})]
        previous = {"1": {"result": "dead"}, "2": {"result": "ok"}}
        new, old = sp.alert_lines(dead, previous, {"Optional"})
        self.assertEqual([r["name"] for r in new], ["B"])
        self.assertEqual([r["name"] for r in old], ["A"])

    def test_manifest_v2(self):
        p = sp.Plugin
        self.assertEqual((p.manifest_version, p.version), (2, "1.2.0"))
        self.assertTrue({"background_tasks", "subprocess", "outbound_network", "proxy_internals"} <= set(p.capabilities))
        self.assertIn("proxy_base", [f["id"] for f in p.fields])

    def test_quality(self):
        self.assertEqual([sp.quality_label({"resolution": r}) for r in ("3840x2160", "1920x1080", "1280x720", "720x480", "")],
                         ["UHD", "FHD", "HD", "SD", "unknown"])


if __name__ == "__main__":
    unittest.main()
