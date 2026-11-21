# Agent instructions for dispatcharr-stream-probe

## Scope

Probes live channels through the Dispatcharr proxy and writes media statistics to the stream
that served them.

## Repository layout

- `stream_probe/plugin.py` contains the runtime plugin and its metadata.
- `stream_probe/plugin.json` is the generated Dispatcharr manifest.
- `stream_probe/logo.png` is bundled with the plugin.
- `build.py` regenerates the manifest and packages a versioned ZIP.
- `tests/test_stream_probe.py` contains the offline unit tests.

## Build and test

- From the root, run `python3 -m unittest discover -s tests`.
- Run `python3 build.py` to write `dist/stream_probe-<version>.zip`.
- The offline tests use the repository's stubs rather than a running Dispatcharr stack.
- The builder gets plugin version, declared capabilities, settings and actions from the
  `Plugin` class.
- Review generated manifest changes alongside edits to plugin metadata.

## Runtime conventions

- Probe through the Dispatcharr proxy as the dedicated user so connection limits and
  profiles apply.
- Skip busy accounts and preserve active viewer connections.
- Write only `stream_stats`. Preserve channel names, membership and placement.
- Treat a probe with no video as dead and account for ffprobe and proxy failures.

## Install and check

- The README documents the plugin registry and release ZIP import in Dispatcharr's Plugins
  UI.
- For a local build, import the versioned ZIP after running the test suite.
- Use the settings and actions documented in the README when verifying a deployment.
- Follow the documented reload steps so the workers use the imported version.
