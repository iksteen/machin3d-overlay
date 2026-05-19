# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```sh
cargo build --release            # production binary (single deployable; assets are embedded via include_str!)
cargo build                      # dev build
cargo test                       # all Rust tests (lib unit tests + tests/fixtures.rs integration tests)
cargo test <name>                # filter by test name
cargo test -- --nocapture        # show stdout/stderr
cargo clippy --all-targets       # lint
cargo fmt                        # format

# Frontend overlay JS test (tests/overlay.test.mjs uses Node's built-in test runner; no npm install):
node --test tests/overlay.test.mjs
```

On Linux the video TLS path uses `native-tls`, so OpenSSL dev headers are required at build time (`pkg-config` + `libssl-dev` on Debian/Ubuntu).

Runtime entry points come from the `bambu-overlay` CLI: `login`, `devices`, `mqtt`, `serve`. See `README.md` for end-user invocation patterns; that file is the source of truth for user-visible behavior such as `--cloud-device` / `--local-device` / `--video-device` semantics and `/bind` enumeration rules.

## Repository agent gates (read before refactoring)

`AGENTS.md` defines two non-negotiable gates that apply to refactors and concept removals in this repo:

- **Refactor Architecture Gate** — the goal is a clearer ownership model, not file moves. Do not introduce generic shape-named modules (`payload`, `runtime`, `helper`, `utils`) unless they own a clearly named domain concept with more than one credible consumer.
- **Refactor Cleanup Gate** — when removing or simplifying a concept, `rg` for it across the codebase and classify every remaining hit; remove dead surface before declaring done. Passing tests and clippy are necessary but insufficient.

When the user states a domain invariant, implement around it directly unless local code or primary documentation contradicts it.

## Architecture

Single-process Tokio service; one Axum HTTP server fronts several long-lived subsystems that all share a resolved device catalog. Startup flow (entry: `src/server.rs::serve` → `ServiceGraph::build`):

1. **CLI → ServerConfig** (`src/cli.rs`, `src/server.rs`). `cli::run` dispatches to one of four subcommands. `serve` builds a `ServerConfig` and optionally constructs a `CloudSession` from the token file (skipped entirely if the token file does not exist — pure local-only operation is supported).
2. **Device resolution** (`src/devices/`). `resolve_devices` is the *only* place that converts CLI args + cloud `/bind` metadata into the runtime `DeviceRegistry`. It enforces uniqueness, infers local-device IDs from the printer MQTT certificate's CN, hydrates access codes from `/bind` when only the LAN host was provided, and probes explicit `--video-device` endpoints. **`/bind` is called lazily**: only when no explicit cloud/local devices were configured, or when an access code lookup actually needs it (`should_enumerate_cloud_catalog`, `BindCatalog`).
3. **DeviceRegistry** (`src/devices/registry.rs`) is the authoritative startup-stable catalog — type-level, not just by convention. Mutation during resolution goes through `DeviceRegistryBuilder`; `build()` consumes the builder and returns a `DeviceRegistry` whose public API is `&self`-only. Outside `crate::devices`, `&mut DeviceEntry` is unreachable. Local devices override same-ID cloud entries because LAN MQTT owns the live path in that scenario. Access codes live behind accessors so web payloads cannot accidentally serialize them; `/api/devices` deliberately omits them.
4. **Background subsystems** (spawned by `BackgroundServices::spawn` against a single `Shutdown` broadcast):
   - **MqttRuntime** (`src/mqtt/`) — shared snapshot state with a revision counter that increments on every mutation. Subscribers use the revision to order derived work (e.g., thumbnail fetch scheduling) against later disconnects. One supervisor per cloud MQTT broker + one supervisor per local device.
   - **VideoStreams** (`src/video/`) — at most one upstream TLS connection per printer. Browser/OBS clients on the same `/devices/<id>/video.mjpeg` share frames via `broadcast`; the upstream closes after the last client disconnects. TLS uses `native-tls` with **only Bambu's BBL CA** trusted and hostname verification disabled — instead, after handshake the code matches the cert CN to the requested device ID before sending the camera access code. Successful endpoints are remembered for the process lifetime; mismatched endpoint/device pairs are also remembered to skip on retry.
   - **ThumbnailService** (`src/thumbnail/`) — triggered by MQTT task changes (not polling). Cloud devices fetch via Bambu Cloud; local devices download the active `.3mf` over LAN FTPS and extract the embedded thumbnail. Concurrency capped by a semaphore.
5. **Axum router** (`src/web.rs`). Routes are registered in `router()`; per-device routes follow the `/devices/{device_id}/...` pattern and the no-device aliases default to `registry.first()`. SSE endpoint `/api/current-print/events` emits on MQTT change *and* at least once per second; while serving, the overlay never polls Bambu Cloud current-print APIs — status is built from the device catalog plus MQTT reports.
6. **Shutdown** (`src/service.rs`). HTTP server, MQTT supervisors, video workers, and thumbnail watcher all observe the same `Shutdown` token. `serve` waits for any of (HTTP done, background failure, process signal), then triggers shutdown and joins with a grace period.

### Embedded assets

`src/assets.rs` pulls `assets/static/*` (CSS/JS) and `assets/templates/overlay.html` in with `include_str!`, so the release binary ships with no companion files. Changing assets means a rebuild.

### Tests layout

- Inline `#[cfg(test)] mod tests` in most modules — these are the bulk of coverage.
- `tests/fixtures.rs` + `tests/fixtures/*.json` — integration tests that assert the Bambu Cloud response shapes the code depends on (`bind.json`, `tasks.json`, `mqtt_report.json`, `preference.json`).
- `tests/overlay.test.mjs` — DOM-fake harness for `assets/static/overlay.js`, run via `node --test`.

### Things easy to get wrong

- Don't add Bambu Cloud calls into the request hot path. The overlay-while-serving contract is: catalog + MQTT only.
- Don't serialize access codes into web responses; route credentials through `DeviceEntry::access_code()` accessors and keep them out of `/api/devices` and similar payloads. Credentials (access codes, access tokens) are wrapped in `Secret<T>` (`src/secret.rs`) so `Debug` and `Display` always render `<redacted>` — a stray `tracing::warn!(?device, ...)` cannot leak them. Use `Secret<String>` for any new credential field.
- Device IDs come from TLS certificate CNs for local printers — do not bypass the CN check on either the MQTT or video path. The FTPS thumbnail path is *deliberately* asymmetric: it trusts the BBL CA chain only, with no per-fetch CN check. This is intentional (suppaftp 8.0.3 cannot expose the peer cert from a live connection, and BBL device certs are X.509 v1 which rules out switching to rustls). Do not "fix" it without revisiting the threat model.
- `cloud_devices` (id-only) and `local_devices` (host + optional access code) are *not* interchangeable; `/bind` is the only source that can fill in missing local access codes.
