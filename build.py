"""Build dist/stream_probe-<version>.zip for Dispatcharr's plugin import.

plugin.json is regenerated from plugin.py (name, version, fields, actions, capabilities), so the manifest never
drifts from the code. Packs plugin.py, plugin.json and logo.png (when present). Usage: python3 build.py
"""
import importlib.util
import json
import pathlib
import sys
import types
import zipfile

ROOT = pathlib.Path(__file__).resolve().parent
KEY = "stream_probe"

celery = types.ModuleType("celery")
celery.shared_task = lambda *a, **k: (lambda f: f)
sys.modules.setdefault("celery", celery)

spec = importlib.util.spec_from_file_location("_build", ROOT / KEY / "plugin.py")
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)
p = mod.Plugin
manifest = {"name": p.name, "version": p.version, "description": p.description, "author": p.author,
            "help_url": p.help_url, "license": p.license, "repo_url": p.repo_url,
            "manifest_version": p.manifest_version, "min_dispatcharr_version": p.min_dispatcharr_version,
            "capabilities": p.capabilities, "fields": p.fields, "actions": p.actions}
(ROOT / KEY / "plugin.json").write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n")
out = ROOT / "dist" / f"{KEY}-{p.version}.zip"
out.parent.mkdir(exist_ok=True)
with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as z:
    for name in ("plugin.py", "plugin.json", "logo.png"):
        if (ROOT / KEY / name).exists():
            z.write(ROOT / KEY / name, f"{KEY}/{name}")
print(f"{out.relative_to(ROOT)}  {p.name} {p.version}")
