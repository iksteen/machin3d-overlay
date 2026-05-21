# bambu-overlay

Rust rewrite of the Bambu overlay prototype.

The release build is a single deployable binary. HTML, CSS, and browser
JavaScript are embedded at compile time with `include_str!`; no external static
files are needed next to the binary.

## Build

```sh
cargo build --release
```

On Linux, the video TLS transport uses `native-tls`, which links against
OpenSSL. Install the OpenSSL development package for your build target, for
example `pkg-config` and `libssl-dev` on Debian/Ubuntu.

Deploy:

```sh
target/release/bambu-overlay
```

## Usage

Log in once:

```sh
bambu-overlay login
```

Run the overlay server:

```sh
bambu-overlay serve
```

Open `http://127.0.0.1:8765/` or `http://127.0.0.1:8765/horizontal`
for the horizontal overlay, or `http://127.0.0.1:8765/vertical` for the
vertical overlay.

When the token account has more than one printer, list the available device IDs:

```sh
bambu-overlay devices
```

Select a printer in the overlay with a device-specific path:
`http://127.0.0.1:8765/devices/<DEVICE_ID>/horizontal` or
`http://127.0.0.1:8765/devices/<DEVICE_ID>/vertical`. The default layout paths
use the first printer from the configured device list.

The browser uses server-sent events from `/api/current-print/events`. The server
emits after MQTT messages and at least once per second. While serving, the
overlay does not poll Bambu Cloud current-print APIs; it builds status from the
configured device catalog plus MQTT reports. Thumbnail refreshes are triggered
by MQTT task changes.

The current device catalog is available as JSON at `/api/devices`. It includes
known device metadata and device-specific layout and thumbnail paths. It
includes a video path only when the service has a validated explicit video
endpoint or a successful local startup video probe for that device. Access codes
are never included in this response.

Fetch the active print thumbnail with `/devices/<DEVICE_ID>/thumbnail`.
Without a device path, `/thumbnail` uses the first printer from the
configured device list.
Cloud devices resolve the current task through Bambu Cloud and cache the
downloaded thumbnail. Local devices download the active `.3mf` from the printer
over LAN FTPS and cache the embedded thumbnail.

Useful commands:

```sh
bambu-overlay serve --bind 0.0.0.0:8765
bambu-overlay serve --bbl-cloud-device 00M123456789012
bambu-overlay serve --bbl-local-device 192.168.1.50,12345678,Office
bambu-overlay serve --bbl-local-device 192.168.1.50
bambu-overlay serve --bbl-local-device 192.168.1.50,12345678,Office --bbl-local-device 192.168.1.51,87654321,Garage
bambu-overlay serve --bbl-video-device 192.168.1.50
bambu-overlay serve --bbl-video-device 192.168.1.50 --bbl-video-device 192.168.1.51:6001
bambu-overlay mqtt --device 00M123456789012
bambu-overlay mqtt --bbl-local-device 192.168.1.50,12345678
```

Configuration is handled with command-line options. Use `--help` on any command
to see the available options. `login` stores the access token, API base, and
Bambu MQTT user ID in the token file; re-run `login` if the token file predates
the stored user ID. `serve` reads that token data in cloud mode and only exposes
runtime settings such as `--bind`, `--bbl-token-file`, `--timeout`, `--bbl-cloud-mqtt`,
`--bbl-local-device`, `--bbl-cloud-device`, and `--bbl-video-device`.
`--bbl-local-device`, `--bbl-cloud-device`, and `--bbl-video-device` can be repeated.

## MQTT monitoring

Use `mqtt` to print raw MQTT report payloads for one printer:

```sh
bambu-overlay mqtt
bambu-overlay mqtt --device <DEVICE_ID>
bambu-overlay mqtt --bbl-cloud-device <DEVICE_ID>
bambu-overlay mqtt --bbl-local-device <HOST[:MQTT_PORT]>[,<ACCESS_CODE>[,<NAME>]]
```

The command uses the same cloud enumeration and local-device resolution rules as
`serve`. If no `--device` is provided, it monitors the first resolved device;
in cloud enumeration mode that is the first device returned by `/bind`. Only the
selected printer's `device/<DEVICE_ID>/report` payloads are written to stdout,
one event per line.

## Local devices

To add printers directly over LAN MQTT, configure them with `--bbl-local-device`:

```sh
bambu-overlay serve --bbl-local-device <HOST[:MQTT_PORT]>[,<ACCESS_CODE>[,<NAME>]]
```

`HOST` is the printer LAN address, and `ACCESS_CODE` is the LAN access code shown
on the printer. Startup connects to the printer's local MQTT TLS port and uses
the device certificate common name as the device ID before MQTT authentication.
The MQTT port defaults to `8883`. If `ACCESS_CODE` is omitted, startup looks up
the matching Bambu Cloud `/bind` entry when a token is available. Otherwise
startup fails. Use an empty field when omitting the code but setting a name, for
example `--bbl-local-device <HOST>,,<NAME>`. Repeat
`--bbl-local-device` for multiple printers.

Hybrid mode is automatic. `serve` calls Bambu Cloud `/bind` only when it needs
device data from it. If a token file exists and no `--bbl-cloud-device` or
`--bbl-local-device` is provided, `/bind` is used as the cloud device catalog. If any
`--bbl-cloud-device <DEVICE_ID>` entry is provided, that explicit list is the
complete cloud device catalog and `/bind` is not used for enumeration.

`--bbl-cloud-device` entries are id-only. Standalone cloud devices still require a
Bambu Cloud token for the MQTT UID lookup and MQTT authentication. Local devices
with complete access codes do not trigger `/bind`; local devices missing an
access code and explicit cloud video devices without an access code look up
`/bind` only when that code is actually needed.

To run without any Bambu Cloud API calls, provide only `--bbl-local-device` entries
that include access codes.

Select a local printer the same way as cloud printers:
`http://127.0.0.1:8765/devices/<DEVICE_ID>/horizontal`.

## Snapmaker printers

`bambu-overlay` also drives Snapmaker printers that run Moonraker (e.g. the
Snapmaker U1). Add each printer with `--snap-device`:

```sh
bambu-overlay serve --snap-device <HOST[:PORT]>
```

`HOST` is the printer's LAN address; `PORT` defaults to `80` (Moonraker proxied
through nginx). Startup probes `http://HOST:PORT/machine/system_info` to learn
the printer's serial number (used as the device id) and its friendly name.
Repeat `--snap-device` for multiple printers. Each entry spawns a Moonraker
WebSocket worker that subscribes to `print_stats`, `display_status`,
`extruder[0..3]`, `heater_bed`, `virtual_sdcard`, and `print_task_config`, and
feeds the shared overlay state.

State, thumbnails, and the camera stream are all wired up. Bambu Cloud is not
required for Snapmaker — `serve` works without a token file when only
`--snap-device` is configured.

### Camera caveat

The U1's camera only writes fresh frames to `monitor.jpg` while its camera
daemon is in "monitor" mode. The overlay polls that file and serves it as
MJPEG, which means fresh frames flow only while the daemon is awake.

In practice the daemon is awake during a print and while Snapmaker Orca's
camera viewer is open on the same network; outside those, `monitor.jpg`
stays frozen on the last captured frame, and so does the overlay's video.
The overlay does send `camera.start_monitor` when it begins streaming, but
that request alone does not reliably wake the daemon — the conditions that
do are still being investigated.

## Video

A1 and P1 series printers can expose their camera as MJPEG at `/video.mjpeg`:

```sh
bambu-overlay serve --bbl-video-device 192.168.1.50
```

`--bbl-video-device` accepts a printer LAN IP address or hostname, optionally
followed by `:PORT` and `,ACCESS_CODE`, for example
`--bbl-video-device 192.168.1.50:6000,12345678`. Repeat it once per printer when
serving multiple cameras. The printer video server uses port `6000` when no port
is specified. `serve` probes each explicit
`--bbl-video-device` endpoint at startup, reads the device ID from the printer
certificate common name, and fails if that device is not present in the known
device catalog. Known devices include cloud `/bind` devices when enumeration is
active, plus explicit `--bbl-cloud-device` and `--bbl-local-device` options. For cloud
devices, `--bbl-video-device` provides the LAN camera endpoint and the access code
can be provided on `--bbl-video-device` or come from `/bind` metadata. For local
devices, the access code comes from the matching `--bbl-local-device` entry or
`--bbl-video-device` entry.

For local devices, `serve` probes `<HOST>:6000` at startup. If it can complete a
Bambu device TLS handshake and the printer certificate common name matches the
local device ID, that endpoint is added automatically. No camera access code is
sent during startup video probes. `--bbl-video-device` remains useful for cloud
devices and for overriding or adding camera endpoints explicitly.

Select a camera with `/devices/<DEVICE_ID>/video.mjpeg`. Without a device path,
`/video.mjpeg` uses the first printer from the configured device list. For each
selected device, `bambu-overlay` tries the configured video
endpoints with that device ID as TLS SNI. The printer certificate common name is
the device serial number, so `bambu-overlay` uses the certificate to reject
mismatched endpoints before sending the camera access code. It also remembers
mismatched endpoint/device pairs it discovers while probing, then remembers the
endpoint that successfully streams frames for the rest of the process.

The video connection uses `native-tls` with only Bambu's BBL CA certificate
trusted for this transport. The TLS backend verifies the certificate chain,
certificate validity, signatures, and handshake. Hostname verification is
disabled because some printer firmware serves CN-only certificates; after the
TLS handshake, `bambu-overlay` checks that the certificate common name matches
the requested device ID before sending the camera access code.

Only one upstream video connection to the printer is opened. Multiple OBS or
browser clients connected to the same `/devices/<DEVICE_ID>/video.mjpeg`
stream share that connection, and the printer connection is closed after the
last MJPEG client disconnects.

## systemd

An example service unit is available at
`examples/systemd/bambu-overlay.service`. Adjust the `User`, `Group`,
`ExecStart`, and token file path for your host before installing it.

The example stores the token at `/var/lib/bambu-overlay/token.json` and runs as
the unprivileged `bambu-overlay` user. On systemd versions that support
`StateDirectory=`, systemd creates `/var/lib/bambu-overlay` with the correct
owner when the service starts.

If you create the service user and state directory manually, keep the directory
private and writable only by that service account:

```sh
sudo useradd --system --home-dir /var/lib/bambu-overlay --shell /usr/sbin/nologin bambu-overlay
sudo install -d -o bambu-overlay -g bambu-overlay -m 0700 /var/lib/bambu-overlay
```

Create the token as that user so the resulting file is owned correctly:

```sh
sudo -u bambu-overlay /usr/local/bin/bambu-overlay login --bbl-token-file /var/lib/bambu-overlay/token.json
sudo chmod 0600 /var/lib/bambu-overlay/token.json
```
