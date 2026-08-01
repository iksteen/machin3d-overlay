# machin3d-overlay

Self-hosted 3D printer overlay for OBS or any browser. Supports Bambu Lab
printers (cloud or LAN MQTT) and the Snapmaker U1 (over Moonraker). Ships as a
single static binary — HTML, CSS, and overlay JavaScript are embedded at
compile time with `include_str!` and need no companion files at deploy time.

## Build

```sh
cargo build --release
```

On Linux, the printer-facing TLS transport uses `native-tls`, which links
against OpenSSL. Install the OpenSSL development package for your build target,
for example `pkg-config` and `libssl-dev` on Debian/Ubuntu.

Deploy:

```sh
target/release/machin3d-overlay
```

## Quick start

```sh
# Bambu printers: log in once, then serve.
machin3d-overlay bbl-login
machin3d-overlay serve

# Snapmaker U1: pair once (tap Approve on the printer), then serve.
machin3d-overlay snap-pair 192.168.1.60
machin3d-overlay serve --snap-device 192.168.1.60

# Mixed setup — both vendors at once.
machin3d-overlay serve --snap-device 192.168.1.60
```

`serve` works without any Bambu Cloud token if you only configure Snapmaker
devices (or Bambu LAN devices with access codes provided on the CLI). The
token file is loaded only when it exists and only when a Bambu code path needs
it.

## Overlay URLs and JSON API

Open `http://127.0.0.1:8765/horizontal` for the horizontal overlay or
`http://127.0.0.1:8765/vertical` for the vertical overlay. Both default to the
first printer in the configured device list.

Select a specific printer with a device-scoped path:
`http://127.0.0.1:8765/devices/<DEVICE_ID>/horizontal` /
`/devices/<DEVICE_ID>/vertical`. Device IDs come from each printer's stable
identifier: a Bambu serial for Bambu printers, the Snapmaker `serial_number`
field for the U1.

List configured device IDs (vendor-neutral):

```sh
curl http://127.0.0.1:8765/api/devices
```

`/api/devices` includes name, online status, and device-specific overlay /
thumbnail / video paths. It includes a video path only when the service has a
validated explicit video endpoint, a successful local startup video probe, or
(for Snapmaker) a reachable Moonraker camera endpoint. Access codes and mTLS
keys are never included.

The browser uses server-sent events from `/api/current-print/events`. The
server emits after MQTT/Moonraker reports and at least once per second. While
serving, the overlay does not poll Bambu Cloud current-print APIs; it builds
status from the configured device catalog plus live MQTT and Moonraker
reports. Thumbnail refreshes are triggered by job-state changes.

Fetch the active print thumbnail with `/devices/<DEVICE_ID>/thumbnail`. Bambu
cloud devices resolve the current task through Bambu Cloud and cache the
downloaded thumbnail. Bambu local devices download the active `.3mf` from the
printer over LAN FTPS and cache the embedded thumbnail. Snapmaker devices
download the thumbnail through Moonraker's HTTP file API.

`machin3d-overlay serve --help` lists all runtime options. Per-vendor flags are
namespaced: Bambu Lab uses `--bbl-…`, Snapmaker uses `--snap-…`.

## Bambu Lab printers

Log in once. The token, API base, and Bambu MQTT user ID are stored in the
token file:

```sh
machin3d-overlay bbl-login
```

If you ever upgrade from a token created by 1.x, re-run `login` to populate
the stored user ID. The token file defaults to a path under
`$XDG_DATA_HOME`; override it with `--bbl-token-file PATH`.

List bound Bambu printers in the token account:

```sh
machin3d-overlay bbl-devices
```

Serve. With nothing else specified, `serve` enumerates the account's bound
printers via `/bind`:

```sh
machin3d-overlay serve
machin3d-overlay serve --bind 0.0.0.0:8765
```

### Hybrid mode and explicit devices

Hybrid mode is automatic. `serve` calls Bambu Cloud `/bind` only when it needs
device data from it. If a token file exists and no `--bbl-cloud-device` or
`--bbl-local-device` is provided, `/bind` is used as the cloud device catalog.
If any `--bbl-cloud-device <DEVICE_ID>` entry is provided, that explicit list
is the complete cloud catalog and `/bind` is not used for enumeration.

```sh
machin3d-overlay serve --bbl-cloud-device 00M123456789012
machin3d-overlay serve --bbl-local-device 192.168.1.50,12345678,Office
machin3d-overlay serve --bbl-local-device 192.168.1.50
machin3d-overlay serve --bbl-local-device 192.168.1.50,12345678,Office \
                    --bbl-local-device 192.168.1.51,87654321,Garage
```

`--bbl-cloud-device` entries are id-only. Standalone cloud devices still
require a Bambu Cloud token for the MQTT UID lookup and MQTT authentication.

`--bbl-local-device` accepts `HOST[:MQTT_PORT][,ACCESS_CODE[,NAME]]`. Startup
connects to the printer's local MQTT TLS port (`8883` by default) and uses the
device certificate's common name as the device ID before MQTT authentication.
If `ACCESS_CODE` is omitted, startup looks up the matching Bambu Cloud `/bind`
entry when a token is available; otherwise startup fails. Use an empty field
when omitting the code but setting a name, e.g.
`--bbl-local-device <HOST>,,<NAME>`.

To run without any Bambu Cloud API calls, provide only `--bbl-local-device`
entries that include access codes.

### Bambu MQTT monitoring

`mqtt` prints raw MQTT report payloads for one Bambu printer (one event per
line, useful for debugging):

```sh
machin3d-overlay bbl-mqtt
machin3d-overlay bbl-mqtt --device <DEVICE_ID>
machin3d-overlay bbl-mqtt --bbl-cloud-device <DEVICE_ID>
machin3d-overlay bbl-mqtt --bbl-local-device <HOST[:MQTT_PORT]>[,<ACCESS_CODE>[,<NAME>]]
```

Same cloud enumeration and local resolution rules as `serve`. If no
`--device` is provided, it monitors the first resolved device. Only the
selected printer's `device/<DEVICE_ID>/report` payloads are written.

## Snapmaker U1

The U1 (and any other Moonraker-driven Snapmaker printer) is configured per
device with `--snap-device`:

```sh
machin3d-overlay serve --snap-device 192.168.1.60
machin3d-overlay serve --snap-device 192.168.1.60 --snap-device 192.168.1.61:80
```

`HOST` is the printer's LAN address; `PORT` defaults to `80` (Moonraker proxied
through nginx). Startup probes `http://HOST:PORT/machine/system_info` to learn
the printer's serial number (used as the device ID) and friendly name. Each
entry then spawns a Moonraker WebSocket worker that subscribes to
`print_stats`, `display_status`, `extruder[0..3]`, `heater_bed`, `fan`,
`virtual_sdcard`, `print_task_config`, `gcode_move`, and `toolhead` and feeds
the shared overlay state.

State, thumbnails, and the camera stream are all wired up. Bambu Cloud is not
required for Snapmaker — `serve` works without a token file when only
`--snap-device` is configured.

### Pairing for reliable camera wake-up

The U1's on-device camera daemon only writes fresh frames to `monitor.jpg`
while it is in "monitor" mode; by default the file is frozen on the last
captured frame, and the daemon disarms itself roughly six minutes after the
last request, so the overlay re-arms it every two minutes while a viewer is
connected.

**Pairing is optional.** From a LAN address the overlay wakes the daemon
through Moonraker's own `camera.*` JSON-RPC repeater on
`ws://HOST/websocket` — the U1 ships with the private address ranges in
Moonraker's `trusted_clients`, so no certificate and no API key are needed.
Pair only if you have narrowed `trusted_clients` (or turned on forced API-key
auth) on the printer, or want the credentialed path Snapmaker Orca uses:

```sh
# On the printer: switch to LAN mode (Settings → Network) so the approval popup can appear.
machin3d-overlay snap-pair 192.168.1.60          # tap "Approve" on the printer screen
# You can switch the printer back to cloud mode now — the issued cert keeps working.
machin3d-overlay serve --snap-device 192.168.1.60   # HOST must match the snap-pair value
```

`snap-pair` runs the LAN bootstrap on the printer's cleartext MQTT broker
(`:1884`), prints a "tap Approve" prompt, and on approval writes the
printer-issued client cert/key/CA, SN, and stable client ID to a JSON file
(default: `$XDG_STATE_HOME/machin3d-overlay/snap-tokens.json`, mode `0600`).
Override the location with `--snap-token-file PATH` on both `snap-pair` and
`serve` if you want a non-default path (the two commands must agree).

`serve` loads the snap-tokens file at startup and uses each token's cert/key/CA
to open a long-lived mTLS session that publishes `camera.start_monitor` and
`camera.stop_monitor` on the printer's mTLS broker (`:8883`). Reusing the same
client ID across `snap-pair` runs keeps the printer's auth DB warm so
subsequent pairings (e.g. after rotating tokens) do not require a second tap.

**LAN mode is only required for the initial pairing**, to expose the approval
popup. Once you have a token on disk, the printer can be in cloud mode for
normal operation and the overlay's mTLS connection still works.

Without `snap-pair` — or when the mTLS session cannot be opened — the overlay
falls back to the unauthenticated repeater described above, which works from
any client IP the printer trusts. If that is refused too (a hardened printer,
or a plain non-Snapmaker Moonraker device), frames only update while something
else keeps the daemon active; otherwise you get the last-captured frame,
frozen.

## Camera streaming

The video transport is per-vendor; the served endpoint is identical for both.
Select a camera with `/devices/<DEVICE_ID>/video.mjpeg`. Without a device
path, `/video.mjpeg` uses the first printer from the configured device list.
Only one upstream video connection per printer is open at a time — multiple
OBS or browser clients connected to the same per-device endpoint share that
connection, and the upstream closes after the last client disconnects.

### Bambu A1 and P1 series

A1 and P1 series printers expose their camera as raw JPEG frames over an
authenticated TLS socket:

```sh
machin3d-overlay serve --bbl-video-device 192.168.1.50
machin3d-overlay serve --bbl-video-device 192.168.1.50:6000,12345678
```

`--bbl-video-device` accepts a printer LAN IP or hostname, optionally followed
by `:PORT` and `,ACCESS_CODE`. The printer video server uses port `6000` when
no port is specified. Repeat the flag once per printer when serving multiple
cameras.

`serve` probes each explicit `--bbl-video-device` endpoint at startup, reads
the device ID from the printer certificate's common name, and fails if that
device is not present in the known device catalog. Known devices include cloud
`/bind` devices when enumeration is active, plus explicit `--bbl-cloud-device`
and `--bbl-local-device` options. For cloud devices, `--bbl-video-device`
provides the LAN camera endpoint and the access code can be provided on
`--bbl-video-device` or come from `/bind` metadata. For local devices, the
access code comes from the matching `--bbl-local-device` entry or
`--bbl-video-device` entry.

For local devices, `serve` also probes `<HOST>:6000` at startup. If it can
complete a Bambu device TLS handshake and the printer certificate common name
matches the local device ID, that endpoint is added automatically. No camera
access code is sent during startup probes. `--bbl-video-device` remains useful
for cloud devices and for overriding or adding camera endpoints explicitly.

For each selected device, `machin3d-overlay` tries the configured endpoints with
that device ID as TLS SNI. The printer certificate common name is the device
serial number, so `machin3d-overlay` uses the certificate to reject mismatched
endpoints before sending the camera access code. It remembers mismatched
endpoint/device pairs it discovers while probing, then remembers the endpoint
that successfully streams frames for the rest of the process.

The Bambu video connection uses `native-tls` with only Bambu's BBL CA
certificate trusted for this transport. The TLS backend verifies the
certificate chain, certificate validity, signatures, and handshake. Hostname
verification is disabled because some printer firmware serves CN-only
certificates; after the TLS handshake, `machin3d-overlay` checks that the
certificate common name matches the requested device ID before sending the
camera access code.

### Snapmaker U1

The U1 streams JPEGs through Moonraker's HTTP camera proxy. There is no
explicit `--snap-video-device`: configuring `--snap-device <HOST>` is
sufficient. Frame freshness depends on whether you have paired the printer
(see [Pairing for reliable camera wake-up](#pairing-for-reliable-camera-wake-up)
above).

## systemd

An example service unit is at `examples/systemd/machin3d-overlay.service`. Adjust
the `User`, `Group`, `ExecStart`, and token file path for your host before
installing it.

The example stores the Bambu token at `/var/lib/machin3d-overlay/token.json` and
runs as the unprivileged `machin3d-overlay` user. On systemd versions that
support `StateDirectory=`, systemd creates `/var/lib/machin3d-overlay` with the
correct owner when the service starts.

If you create the service user and state directory manually, keep the
directory private and writable only by that service account:

```sh
sudo useradd --system --home-dir /var/lib/machin3d-overlay --shell /usr/sbin/nologin machin3d-overlay
sudo install -d -o machin3d-overlay -g machin3d-overlay -m 0700 /var/lib/machin3d-overlay
```

Create the Bambu token as that user so the resulting file is owned correctly:

```sh
sudo -u machin3d-overlay /usr/local/bin/machin3d-overlay bbl-login --bbl-token-file /var/lib/machin3d-overlay/token.json
sudo chmod 0600 /var/lib/machin3d-overlay/token.json
```

For Snapmaker, run `snap-pair` as the service user too so the resulting
snap-tokens file is owned correctly:

```sh
sudo -u machin3d-overlay /usr/local/bin/machin3d-overlay snap-pair \
    --snap-token-file /var/lib/machin3d-overlay/snap-tokens.json 192.168.1.60
```

Point `serve` at the matching path with `--snap-token-file
/var/lib/machin3d-overlay/snap-tokens.json`.
