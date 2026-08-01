# Snapmaker U1 — Webcam Enablement Protocol

How to get a live camera image out of a Snapmaker U1 from a third-party tool.

**This is NOT the standard Moonraker/Fluidd webcam flow.** There is no persistent
MJPEG stream. The camera pipeline is off until you send an authenticated
`camera.start_monitor` request over the printer's MQTT bus. Only then does the
firmware start capturing frames and publishing them as a JPEG that refreshes on
an interval. The session self-terminates after ~6 minutes unless you keep it
alive.

Everything below was reverse-engineered from firmware `1.5.2.12` (`unisrv`
binary + the `u1-moonraker` source) and **verified against a live, locally-paired
printer**.

> **TL;DR — there are two ways in.** The mTLS MQTT bus (§3) is the "official"
> paired path. But there is also a **no-credentials LAN path** (§0) via
> Moonraker's WebSocket: on the default config any LAN IP is a trusted client, so
> `camera.start_monitor` can be called with no cert and no API key. Both are
> verified working on the live device.

---

## 0. Unauthenticated LAN path (no cert, no API key)

**You do not need the mTLS pairing to turn the camera on.** Enabling the webcam
only requires getting one `start_monitor` to `unisrv`, and Moonraker exposes that
over its WebSocket JSON-RPC — which, on the U1's stock config, is **open to every
device on the LAN**.

Why it's open: `moonraker.conf` sets `trusted_clients` to `10.0.0.0/8`,
`172.16.0.0/12`, and `192.0.0.0/8` (which covers all `192.168.x.x`). Moonraker
treats a trusted-IP connection as an authenticated user, so `auth_required`
endpoints pass without an API key. Frame fetches over HTTP are trusted the same way.

```
1. ws://<printer>/websocket           (plain WS, no auth headers)
2. → {"jsonrpc":"2.0","method":"camera.start_monitor",
      "params":{"req_id":1,"domain":"lan","interval":1,"expect_pw":false},"id":1}
3. GET http://<printer>/server/files/camera/monitor.jpg     (plain HTTP, no auth)
4. heartbeat: re-send the WS call every ~60 s; stop with camera.stop_monitor
   (do not wait for a response to any of these — see below)
```

Caveats specific to this path:
- **Method name uses dots, not slashes**: `camera.start_monitor` (a `camera/…`
  path 404s as `-32601 Method not found`). Moonraker derives the RPC name by
  stripping the leading `/` and replacing `/` with `.`.
- **Fire-and-forget — success is silent.** The Moonraker handler (`repeater.py`)
  publishes to the MQTT bus and sends **nothing at all** back over the WebSocket:
  no `result`, no `url`/`pw`, ever. Verified by holding the socket open for 20 s
  after an accepted `camera.start_monitor` — only unsolicited `notify_*` traffic
  arrived, while `monitor.jpg` started refreshing once a second. **Do not wait for
  a reply**: a client that blocks on one will always time out on the happy path.
  Failures *do* answer, and immediately (a bogus `camera.*` method came back
  `{"error":{"code":-32601,...},"id":1}` in 40 ms), so the workable shape is:
  send, listen ~1 s for an objection, treat silence as acceptance. That's fine —
  the URL is always the fixed path `/server/files/camera/monitor.jpg`, so the
  reply is not needed. (The camera's real response goes onto the MQTT
  `camera/response` topic; verified it carries your request's id and
  `{"url":"/files/camera/monitor.jpg", ...}`.)
- Only `camera.*` / `system.*` repeater methods are exposed this way, and **only
  over WebSocket** — the HTTP transport for these endpoints is explicitly disabled,
  so you can't `POST /camera/start_monitor`. The trigger must be WebSocket; the
  frame fetch is HTTP.
- Works only from a **trusted client IP**. If an operator narrows `trusted_clients`
  or turns on forced API-key auth, this path closes and you must use the mTLS MQTT
  path (§3). The frame URL and session semantics are identical either way.

Verified: an unauthenticated `ws://` `camera.start_monitor` produced a
`camera/response` on the MQTT bus (id echoed) and a fresh `1920×1080` JPEG at the
HTTP path — with no client certificate and no API key.

---

## 1. Architecture

```
        mTLS MQTT (8883)                      HTTP (80, via nginx → moonraker :7125)
 client ───────────────► mosquitto ──► unisrv ──► /tmp/.monitor.jpg
   │                     (broker)     (camera     → /home/lava/printer_data/camera/monitor.jpg
   │                                   service)     served at /server/files/camera/monitor.jpg
   └── poll frame ◄──────────────────────────────────────────────────┘
```

- **`unisrv`** (`/usr/bin/unisrv`) is the camera/system service daemon. It owns the
  MIPI camera, spawns `ffmpeg` to capture, and exposes a JSON-RPC 2.0 API over MQTT.
- The MQTT broker is **mosquitto** with three listeners:
  | Port | Transport | Auth | Use |
  |------|-----------|------|-----|
  | 1883 | localhost only | anonymous | moonraker ↔ unisrv internal |
  | 8883 | LAN | **mTLS client cert** | paired external clients ← *use this* |
  | 1884 | LAN | anonymous | pairing/config handshake only (`+/config/*`) |
- The camera JPEG is served by **Moonraker's file API** over plain HTTP; nginx
  proxies `^/(printer|api|access|machine|server)/` to Moonraker on `:7125`.
- The stock Fluidd nginx config still has `/webcam/ → 127.0.0.1:8080`, but **nothing
  listens on :8080** on the U1 (it returns `502`). Ignore it; it's vestigial upstream config.

---

## 2. Authentication

Two independent trust layers exist; you only need the MQTT one.

### MQTT (mTLS) — required to send camera commands
Port 8883 requires a client certificate signed by the printer's CA. You obtain
this cert/key/CA during LAN pairing (the printer displays an access code; the
pairing exchange over listener 1884 `+/config/request` returns your credentials).
A paired client config looks like:

```json
{
  "host": "192.168.0.110",
  "mqtt_port": 8883,
  "sn": "8110026040110371KB88",
  "clientid": "n3o-...",
  "ca":   "-----BEGIN CERTIFICATE-----...",   // printer CA (CN=mqtt-broker)
  "cert": "-----BEGIN CERTIFICATE-----...",   // your client cert (CN=mqtt_cli1)
  "key":  "-----BEGIN RSA PRIVATE KEY-----..."
}
```

The broker sets `use_identity_as_username false`, so the cert CN doesn't matter for
ACL; **any** valid client cert gets the default TLS ACL:

```
topic write +/request        # camera/request, system/request, ...
topic read  +/response
topic read  +/status
topic read  +/notification
```

> The server cert's SAN is only `localhost / 127.0.0.1 / ::1`, so when you connect
> to it by IP you must **disable hostname verification** (still verify the CA
> chain). With `mosquitto_*` that's `--insecure`; in code, keep CA verification on
> but turn off host/SAN checking.

### HTTP (Moonraker) — required to fetch frames
Moonraker gates HTTP by `trusted_clients` IP ranges (LAN/private nets are trusted
by default) — no token needed from a LAN client. The camera JPEG is fetched over
plain HTTP.

This mTLS path is the credentialed option (works from anywhere that can reach
:8883 with a valid client cert, and gives you the full response incl. `pw`/`url`).
For a LAN client you usually **don't need it** — see §0 for the no-credentials
WebSocket path. (Earlier I mis-tested the WS method as `camera/start_monitor` with
a slash and got `-32601`; the correct dotted name `camera.start_monitor` does work.)

---

## 3. The camera API (JSON-RPC 2.0 over MQTT)

- **Publish** requests to topic **`camera/request`**.
- **Subscribe** to **`camera/response`** for replies (matched by `id`) and
  **`camera/notification`** for async state changes.
- QoS 1. Responses are correlated by the JSON-RPC `id` you send.

Methods (from `unisrv`): `camera.start_monitor`, `camera.stop_monitor`,
`camera.get_status`, `camera.take_a_photo`, `camera.detect_capture`,
`camera.start_timelapse`, `camera.stop_timelapse`,
`camera.{get,delete,upload}_timelapse_instance`.

### `camera.get_status`
```json
→ {"jsonrpc":"2.0","method":"camera.get_status","params":{},"id":1}
← {"jsonrpc":"2.0","id":1,"result":{
     "interface_type":"MIPI","monitoring":false,"timelapse":false,"state":"success"}}
```
When monitoring, the result also carries `"monitor_domain":"lan"|"wan"`.

### `camera.start_monitor` — the enablement call
```json
→ {"jsonrpc":"2.0","method":"camera.start_monitor",
   "params":{"domain":"lan","interval":1,"expect_pw":false},"id":2}
```

| Param | Type | Meaning |
|-------|------|---------|
| `domain` | `"lan"` \| `"wan"` | `lan` = serve frame locally. `wan` = also validate cloud binding and push an encrypted frame to Snapmaker cloud (needs a bound account + access token). **Use `lan`.** Any other value → `"Domain is not 'wan' or 'lan'."` |
| `interval` | int (seconds) | How often a fresh frame is captured/written. `1` works. |
| `expect_pw` | bool | Whether to also return password/key material for the encrypted variant (see §5). Irrelevant for plain LAN viewing — set `false`. |

Response with `expect_pw:false`:
```json
← {"jsonrpc":"2.0","id":2,"result":{
     "state":"success","url":"/files/camera/monitor.jpg"}}
```
Response with `expect_pw:true` additionally returns PBKDF2 material for the
encrypted frame (see §5):
```json
← {"jsonrpc":"2.0","id":2,"result":{
     "state":"success","url":"/files/camera/monitor.jpg",
     "pw":"ad9bc8...(48 bytes hex)", "salt":"30e8286a32a5b748", "iterations":1336}}
```

On success you also get an async notification:
```json
camera/notification {"jsonrpc":"2.0","method":"notify_camera_status_change",
  "params":[{"monitor_domain":"lan","monitoring":true,"timestamp":"2026-07-17 10:45:05"}]}
```

### `camera.stop_monitor`
```json
→ {"jsonrpc":"2.0","method":"camera.stop_monitor","params":{"domain":"lan"},"id":3}
← {"jsonrpc":"2.0","id":3,"result":{"state":"success"}}
```

---

## 4. Fetching frames

`result.url` is Moonraker-relative. The real HTTP path is:

```
GET http://<printer>/server/files/camera/monitor.jpg
```

- Returns a **plaintext baseline JPEG, 1920×1080** (ffmpeg/`Lavc58`-encoded).
  On LAN the frame is **not encrypted** — decode it directly.
- The file is overwritten every `interval` seconds; **poll it** to build a
  pseudo-stream (confirmed: consecutive GETs return different images). This is a
  snapshot-refresh model, not a true MJPEG multipart stream.
- Don't cache: send `Cache-Control: no-cache` or a cache-buster query param.

`camera.take_a_photo` can capture a single still on demand if you don't want a
running monitor; `monitor.jpg` is the right primitive for a live view.

---

## 5. Session lifetime & keep-alive (important)

The capture loop in `unisrv` runs a watchdog:

- **Hard stop at ~361 s** (`360999999999 ns`): monitoring flag clears, capture ends.
- **Warning/refresh window at ~311 s**: it starts counting down to stop.
- Each incoming `camera.start_monitor` **resets the watchdog timer**.

So to keep a live view alive, **re-send `camera.start_monitor` as a heartbeat**
well within the timeout — every ~30–120 s is comfortable. Send
`camera.stop_monitor` when you're done to release the camera (it's a shared
single-camera resource; timelapse/defect-detection use it too).

---

## 6. Minimal client recipe

```
1. TLS-connect to mqtts://<printer>:8883 with (ca, client cert, client key),
   hostname/SAN verification OFF, CA verification ON.
2. SUBSCRIBE camera/response, camera/notification   (QoS 1)
3. PUBLISH  camera/request  {start_monitor, domain:"lan", interval:1, expect_pw:false}
4. Wait for response → note result.url.
5. Loop:
     GET http://<printer>/server/files/camera/monitor.jpg   → decode/display JPEG
     every ~60 s: re-PUBLISH start_monitor (heartbeat)
6. On exit: PUBLISH camera/request {stop_monitor, domain:"lan"}
```

### Verified with the mosquitto CLI
```bash
CA=ca.pem CERT=client.crt KEY=client.key H=192.168.0.110
COMMON="-h $H -p 8883 --cafile $CA --cert $CERT --key $KEY --insecure -q 1"

# watch replies
mosquitto_sub $COMMON -t camera/response -t camera/notification -v &

# enable
mosquitto_pub $COMMON -t camera/request \
  -m '{"jsonrpc":"2.0","method":"camera.start_monitor","params":{"domain":"lan","interval":1,"expect_pw":false},"id":1}'

# fetch a frame
curl -s -o frame.jpg http://$H/server/files/camera/monitor.jpg

# stop
mosquitto_pub $COMMON -t camera/request \
  -m '{"jsonrpc":"2.0","method":"camera.stop_monitor","params":{"domain":"lan"},"id":2}'
```

---

## 6b. WAN / cloud path (for reference, not needed on LAN)

With `domain:"wan"` (or `expect_pw:true`), `unisrv` AES-encrypts each frame to
`monitor.enc` and uploads it to the Snapmaker cloud, returning a cloud URL. The
`pw`/`salt`/`iterations` fields are PBKDF2 parameters for that encrypted frame:

- `expect_pw:true` → a per-session key is generated (`RAND_bytes`, encryptor tag
  `_cap_enc_random`) and returned as `pw`; derive the AES key via
  `PBKDF2(pw, salt, iterations)`.
- `expect_pw:false` → a fixed device-derived key is used (encryptor tag
  `_cap_enc_fix`, obfuscated in the binary via XOR with `0xefc7bf81a1d573c5`).

A LAN third-party viewer never needs this — the LAN `monitor.jpg` is already
plaintext. Documented only so the extra response fields aren't mistaken for a
requirement.

---

## 7. Gotchas

- **No stream, only snapshots.** Build the "stream" by polling `monitor.jpg`.
- **`/webcam/` (:8080) is dead** on the U1 — don't point clients at it.
- **Session times out (~361 s).** Heartbeat with `start_monitor` or the image goes stale.
- **Server cert SAN is `localhost` only** → disable hostname verification (keep CA verification).
- **Single shared camera.** `start_timelapse` / defect detection contend for it; expect
  `camera.detect_capture`/`noodle` result messages on `camera/response` while monitoring.
- **`stop_monitor` isn't always immediate**: `get_status` may still report
  `monitoring:true` right after a successful stop (in-flight capture cycle / another
  active viewer). The ~361 s watchdog is the backstop.
- Two entry points: **mTLS MQTT** (§3, credentialed) and **unauthenticated LAN
  WebSocket** (§0, trusted-IP). The WS method is `camera.start_monitor` (dots) and
  is fire-and-forget; the HTTP frame path is the same for both.
```
