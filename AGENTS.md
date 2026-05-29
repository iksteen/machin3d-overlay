# Agent Instructions

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

Runtime entry points come from the `machin3d-overlay` CLI: `bbl-login`, `bbl-devices`, `bbl-mqtt`, `serve`, `snap-pair`. See `README.md` for end-user invocation patterns; that file is the source of truth for user-visible behavior such as `--bbl-cloud-device` / `--bbl-local-device` / `--bbl-video-device` / `--snap-device` semantics, `/bind` enumeration rules, and Snapmaker pairing.

## Architecture

Single-process Tokio service; one Axum HTTP server fronts several long-lived subsystems that all share a resolved device catalog. Two printer vendors are peers — Bambu Lab (cloud or LAN MQTT) and Snapmaker U1 (Moonraker WebSocket plus optional bespoke mTLS MQTT for camera control). Startup flow (entry: `src/server.rs::serve` → `ServiceGraph::build`):

1. **CLI → ServerConfig** (`src/cli.rs`, `src/server.rs`). `cli::run` dispatches to one of five subcommands. `serve` builds a `ServerConfig` and optionally constructs a `CloudSession` from the Bambu token file (skipped entirely if the token file does not exist — pure local-only operation is supported, and is required for Snapmaker-only deployments).
2. **Device resolution** (`src/devices/`). `resolve_devices` is the *only* place that converts CLI args + cloud `/bind` metadata + Snapmaker `machine/system_info` probes into the runtime `DeviceRegistry`. It enforces uniqueness, infers Bambu LAN device IDs from the printer MQTT certificate's CN, hydrates Bambu access codes from `/bind` when only the LAN host was provided, probes explicit `--bbl-video-device` endpoints, probes each `--snap-device` host for its serial number and friendly name, and attaches `snap-pair` mTLS material from the snap-tokens file when present. **`/bind` is called lazily**: only when no explicit cloud/local/Snapmaker devices were configured, or when an access code lookup actually needs it (`should_enumerate_cloud_catalog`, `BindCatalog`).
3. **DeviceRegistry** (`src/devices/registry.rs`) is the authoritative startup-stable catalog — type-level, not just by convention. Mutation during resolution goes through `DeviceRegistryBuilder`; `build()` consumes the builder and returns a `DeviceRegistry` whose public API is `&self`-only. Outside `crate::devices`, `&mut DeviceEntry` is unreachable. Each `DeviceEntry` carries a `DeviceCapabilities` enum (`Bambu { cloud, local }` or `Moonraker { … }`) that gates vendor-specific dispatch — there is no flat option-bag of "maybe this, maybe that". Local Bambu devices override same-ID cloud entries because LAN MQTT owns the live path in that scenario. Access codes and mTLS keys live behind accessors so web payloads cannot accidentally serialize them; `/api/devices` deliberately omits them.
4. **Background subsystems** (spawned by `BackgroundServices::spawn` against a single `Shutdown` broadcast):
   - **Bambu MqttRuntime** (`src/bambu/mqtt/`) — shared snapshot state with a revision counter that increments on every mutation. Subscribers use the revision to order derived work (e.g., thumbnail fetch scheduling) against later disconnects. One supervisor per cloud MQTT broker + one supervisor per local Bambu device.
   - **Moonraker workers** (`src/moonraker/backend.rs`) — one WebSocket session per Moonraker device (Snapmaker U1 today, any conformant Moonraker/Klipper printer in principle), subscribed to `print_stats`, `display_status`, `extruder[N]`, `heater_bed`, `fan`, `virtual_sdcard`, `print_task_config`, `gcode_move`, and `toolhead`. `notify_status_update` events are merged into a status map and converted to vendor-neutral `PrinterReport`s by `moonraker/report.rs`. The WS client itself is `moonraker/client.rs`; Snapmaker-U1-only material (mTLS pairing, the camera control plane) is quarantined under `moonraker/u1/`. Both vendor backends publish into the shared `LiveStateStore`.
   - **VideoStreams** (`src/video/`) — at most one upstream connection per printer. Browser/OBS clients on the same `/devices/<id>/video.mjpeg` share frames via `broadcast`; the upstream closes after the last client disconnects. Per-vendor sources (`BambuVideoSource` / `MoonrakerVideoSource`) decide how frames arrive: Bambu reads an authenticated TLS stream of raw JPEG frames; Moonraker polls the camera-proxy JPEG (generic, in `video/moonraker.rs`) and, on paired Snapmaker U1s, holds a long-lived mTLS MQTT session that publishes `camera.start_monitor` / `camera.stop_monitor` to wake and stop the on-device camera daemon (`video/u1_camera.rs` — the only Snapmaker-specific part of the video path). Bambu TLS uses `native-tls` with **only Bambu's BBL CA** trusted and hostname verification disabled — instead, after handshake the code matches the cert CN to the requested device ID before sending the camera access code. Successful Bambu endpoints are remembered for the process lifetime; mismatched endpoint/device pairs are also remembered to skip on retry.
   - **ThumbnailService** (`src/thumbnail/`) — triggered by MQTT/Moonraker task changes (not polling). Bambu cloud devices fetch via Bambu Cloud (`thumbnail/bambu_cloud.rs`); Bambu local devices download the active `.3mf` over LAN FTPS and extract the embedded thumbnail (`thumbnail/bambu_local.rs`); Moonraker devices download the thumbnail through Moonraker's HTTP file API (`thumbnail/moonraker.rs`). Concurrency capped by a semaphore.
5. **Axum router** (`src/web.rs`). Routes are registered in `router()`; per-device routes follow the `/devices/{device_id}/...` pattern and the no-device aliases default to `registry.first()`. SSE endpoint `/api/current-print/events` emits on backend change *and* at least once per second; while serving, the overlay never polls Bambu Cloud current-print APIs — status is built from the device catalog plus live MQTT/Moonraker reports.
6. **Shutdown** (`src/service.rs`). HTTP server, MQTT supervisors, Moonraker workers, video workers, and thumbnail watcher all observe the same `Shutdown` token. `serve` waits for any of (HTTP done, background failure, process signal), then triggers shutdown and joins with a grace period. The Snapmaker camera worker uses its drop path to publish `camera.stop_monitor` under a tight budget so it does not blow past the parent shutdown grace.

### Embedded assets

`src/assets.rs` pulls `assets/static/*` (CSS/JS) and `assets/templates/overlay.html` in with `include_str!`, so the release binary ships with no companion files. Changing assets means a rebuild.

### Tests layout

- Inline `#[cfg(test)] mod tests` in most modules — these are the bulk of coverage and where all the Snapmaker module tests live (parse paths, PKCS#1→PKCS#8 key conversion, Moonraker status mapping, token persistence).
- `tests/fixtures.rs` + `tests/fixtures/*.json` — integration tests that assert the Bambu Cloud response shapes the code depends on (`bind.json`, `tasks.json`, `mqtt_report.json`, `preference.json`).
- `tests/overlay.test.mjs` — DOM-fake harness for `assets/static/overlay.js`, run via `node --test`.

### Things easy to get wrong

- Don't add Bambu Cloud calls into the request hot path. The overlay-while-serving contract is: catalog + MQTT/Moonraker only.
- Don't serialize access codes or mTLS keys into web responses; route credentials through dedicated accessors and keep them out of `/api/devices` and similar payloads. Credentials (Bambu access codes, Bambu access tokens, Snapmaker private keys) are wrapped in `Secret<T>` (`src/secret.rs`) so `Debug` and `Display` always render `<redacted>` — a stray `tracing::warn!(?device, ...)` cannot leak them. Use `Secret<String>` for any new credential field.
- Bambu device IDs come from TLS certificate CNs for local printers — do not bypass the CN check on either the MQTT or video path. The Bambu FTPS thumbnail path is *deliberately* asymmetric: it trusts the BBL CA chain only, with no per-fetch CN check. This is intentional (suppaftp 8.0.3 cannot expose the peer cert from a live connection, and BBL device certs are X.509 v1 which rules out switching to rustls). Do not "fix" it without revisiting the threat model.
- Moonraker device IDs come from the printer's `machine/system_info` `serial_number` field — not a TLS cert. The mTLS material a Snapmaker U1 issues during `snap-pair` uses PKCS#1 RSA keys; the `moonraker/u1/mtls.rs` PKCS#1→PKCS#8 conversion via the `rsa` crate is required because `native-tls`'s `Identity::from_pkcs8` rejects the PKCS#1 PEM label. Everything U1-specific (pairing, mTLS, camera wake) lives under `moonraker/u1/` and `video/u1_camera.rs`; the rest of the Moonraker path is vendor-neutral and would drive any conformant Moonraker printer — except camera frames, which currently use the U1's legacy `monitor.jpg` poll URL rather than a generic `/server/webcams/list` lookup (the one piece still missing for plain-Klipper support, deliberately unbuilt because there is no non-U1 printer to test it against).
- Moonraker devices are LAN-only in the registry — there is no cloud variant. Don't add an `is_cloud` branch on `DeviceCapabilities::Moonraker`; if a cloud path ever materializes, model it as a new variant or sub-discriminator. Likewise, the U1 camera quirk is a sub-discriminator (`mtls: Option<…>`), not a vendor of its own.
- The Snapmaker U1 camera daemon only writes fresh frames to `monitor.jpg` while in monitor mode; paired devices wake it via mTLS `camera.start_monitor` (`domain: "lan"`, `expect_pw: true`). The HTTP poll URL is the legacy `/server/files/camera/monitor.jpg?_nocache=<unix_ms>` path — not what the daemon's `start_monitor` response advertises (that's an internal moonraker mount path). Unpaired Snapmaker devices still serve `/devices/<id>/video.mjpeg` but get whatever the printer is willing to return; frames may appear frozen unless a print or another tool is keeping the daemon awake.

## Refactor Architecture Gate

When a user asks for a refactor, do not treat moving files or flattening directories as the goal. The goal is a clearer ownership model.

Before editing:

1. Identify the responsibility boundary first: what concept owns the code, what code is only an implementation detail, and what code is coupled to a single caller.
2. Keep tightly coupled request/response mapping, formatting, and small helper logic with the endpoint or service that owns it unless it has a real independent domain role.
3. Do not create or preserve generic modules named only for shape, such as `payload`, `runtime`, `helper`, or `utils`, unless the module owns a clearly named concept and has more than one credible consumer.
4. Prefer names that describe domain responsibility over infrastructure shape. If a name would still make sense after moving the file, it is probably too vague.

Before finalizing:

1. Do a naming and ownership pass, not only a compile/test pass.
2. Check whether every new type, module, function, and public item is necessary. Remove temporary scaffolding that no longer carries its weight.
3. Review the diff for semantic clarity: the result should make the code easier to understand without already knowing the old layout.
4. Treat passing tests, clippy, and review output as necessary but insufficient. The agent owns judging whether the abstraction is justified.
5. Mention any intentionally retained awkwardness or boundary tradeoff in the final answer.

## Refactor Cleanup Gate

When a user asks to remove, simplify, or stop modeling a concept, the change is not done until the concept has been traced through the codebase.

Before finalizing:

1. Search for the removed concept and adjacent names with `rg`.
2. Classify every remaining hit as one of:
   - required runtime/API contract
   - raw fixture input that is required by a test
   - test-only helper
   - dead or unnecessary surface
3. Remove dead or unnecessary surface before running final verification. Raw fixture input is not automatically justified; keep it only when the test specifically needs that ignored or unknown input.
4. In the final answer, explicitly mention any intentional leftovers and why they remain.
5. Do not treat review output as proof that the patch is clean. Reviews can miss unnecessary additions; the agent owns this cleanup check.

When the user states a domain invariant, implement around it directly unless local code or primary documentation gives concrete evidence against it.
