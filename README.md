# micontrol_canbus

Rust service that bridges CAN bus traffic with MQTT and Redis using DBC-defined message/signal schemas.

## What it does

- Loads one or more CAN bus mappings from `config.toml`.
- Resolves and parses DBC files (`*.dbc`) for each mapping.
- Reads CAN frames from SocketCAN interfaces and decodes signal values.
- Publishes changed signal values to MQTT measurement topics.
- Accepts MQTT control commands and converts them to CAN transmit frames.
- Stores signal snapshots and connection health in Redis (when available).
- Supports multiplexed CAN signals when encoding control frames.
- Runs each configured controller as its own async runtime task.

## Repository layout

- `src/main.rs` – app bootstrap, config loading, DBC/message store creation, CAN runtime loop, Redis connection status updates.
- `src/mqtt_integration.rs` – MQTT client lifecycle, topic subscriptions, control payload validation/parsing, control->CAN command handling.
- `src/message_type.rs` – CAN signal encode/decode logic, scaling, signed/unsigned conversion, mux-aware frame construction.
- `src/bus.rs` – shared bus state, frame construction helpers, topic/bus registry.
- `src/identifiers.rs` – topic and Redis hash identifier derivation/sanitization.
- `config.toml` – runtime hardware mappings.
- `epc.dbc`, `epc-2.dbc` – CAN schema files used by mappings.
- `python_test/` – manual scripts for CAN bus diagnostics and heartbeat publishing.

## Runtime architecture

1. Load `.env` (if present), then parse `config.toml`.
2. For each `[[hardware_mappings]]` entry:
   - Resolve DBC path from `hardware_configurations`.
   - Build per-message signal state from DBC messages.
   - Derive identifiers:
     - Redis hash key
     - MQTT control topic
     - MQTT measurement topic
   - Start CAN RX/TX task for that interface.
   - Start per-bus Redis signal writer task (if Redis is connected).
3. Start one MQTT service:
   - Subscribe to all control topics.
   - Process control payloads into CAN TX frames.
   - Publish measurement payloads for changed CAN signals.

## Configuration

### `config.toml`

The service expects:

```toml
[[hardware_mappings]]
hardware_configurations = ["epc-2"] # can be ["name"] or ["name.dbc"]
controller = "can0"
protocol = "CANBUS"
hardware_type = "bams"
hardware_id = "1"
auto_invalidation_interval = 30
```

Notes:

- `controller` is required and is used as the CAN interface name (e.g. `can0`, `vcan0`).
- DBC resolution from `hardware_configurations`:
  - If an entry already ends with `.dbc`, it is used directly.
  - Otherwise `<entry>.dbc` is tried.
- `protocol` and `auto_invalidation_interval` are parsed but not currently used in runtime logic.

### Environment variables

| Variable | Default | Purpose |
|---|---|---|
| `SYSTEM_NAME` | `micontrol` | Prefix used to derive MQTT topics |
| `REDIS_URL` | `redis://localhost:6379` | Redis connection URL |
| `MQTT_HOST` | `localhost` | MQTT broker host |
| `MQTT_PORT` | `1884` | MQTT broker port (code default; set to your broker port, commonly `1883`/`8883`) |
| `MQTT_USERNAME` | `iot_platform` | MQTT username |
| `MQTT_PASSWORD` | `123456` | MQTT password |
| `RUST_LOG` | `micontrol_canbus=info` (via fallback filter) | Logging level/filter |

Use `.env.example` as a starting point and create a local `.env`.

Note: `1884` is the current application default in code for this project; treat it as deployment-specific and set `MQTT_PORT` explicitly for your environment (`1883`/`8883` are common standard ports).

⚠️ WARNING: the documented default MQTT credentials (`iot_platform` / `123456`) are insecure development defaults and must never be used in production.
Use environment-specific secret injection (for example, CI/CD secrets, container/orchestrator secret stores, or OS-level secret management) instead of hardcoded credentials.

## MQTT contract

### Control topic subscription

For each mapping, subscribe to:

```text
<sanitized_system_name>/<sanitized_hardware_type>/<sanitized_hardware_id>/controls
```

### Accepted control payload

```json
{
  "control_id": "optional-id",
  "control_requested_time_utc": "optional-timestamp",
  "control": {
    "MessageName.SignalName": 12.3
  }
}
```

Validation rules:

- `control` must exist and be a JSON object.
- `control` must contain exactly one key/value pair.
- Key format must be `MessageName.SignalName` with non-empty parts.
- Value must be numeric, finite, and not NaN.

### Measurement topic publish

For each mapping, publish changed signals to:

```text
<sanitized_system_name>/<sanitized_hardware_type>/<sanitized_hardware_id>/measurements
```

Payload contains changed signal fields plus:

- `fetched_time_utc` (RFC3339 timestamp, millisecond precision)
- `status: "ok"`

## Redis data model

When Redis is available:

- Per-bus hash key (`redis_hash`) stores signal values as:
  - field: `MessageName.SignalName`
  - value: numeric signal value
- Global hash `connections` stores per-bus connection state JSON:
  - `connection_status` (`Connected`/`Disconnected`)
  - `last_updated`
  - `last_online`

Identifier derivation:

- `redis_hash`:
  - `hardware_type_hardware_id` when both are present
  - `hardware_type` when only type is present
  - `canbus_<hardware_id>` when only id is present
  - sanitized controller otherwise
- non-alphanumeric characters are replaced with `_`.

## Build, check, and test

Using `just`:

- `just build` – cross build for `aarch64-unknown-linux-gnu` (release)
- `just check` – cross check for `aarch64-unknown-linux-gnu`
- `just test` – cross tests for `aarch64-unknown-linux-gnu`
- `just build_ubuntu` – cross build for `x86_64-unknown-linux-gnu`
- `just build_windows` – cross build for `x86_64-pc-windows-gnu`
- `just test_heartbeat` – run MQTT heartbeat script with preset host/port

Using Cargo locally:

- `cargo build`
- `cargo test`

## Running locally

1. Configure CAN interface (example helper script):
   - `sudo ./python_test/reset_can_interface.sh`
   - `sudo` is required because bringing network interfaces down/up needs elevated privileges.
2. Configure environment:
   - `cp .env.example .env`
   - add MQTT and SYSTEM_NAME values as needed
3. Ensure `config.toml` points to valid mappings/DBC files.
4. Start service:
   - `cargo run`

## Manual utility scripts (`python_test/`)

- `heartbeat_mqtt.py`
  - Publishes periodic control heartbeat commands through MQTT.
  - Auto-derives default control topic from `SYSTEM_NAME` + `config.toml`.
- `canbus_test.py`
  - Manual SocketCAN monitor/control helper for specific SBCU/MBCU IDs.
- `scan_bus.py`
  - Scans bus IDs for a time window and reports missing expected IDs.
- `reset_can_interface.sh`
  - Brings interface down/up with bitrate configuration.

Note: despite the folder name, `python_test/` contains both Python utilities and a shell helper script.

## Logging and observability

- Structured logs via `tracing` / `tracing-subscriber`.
- Periodic (10s) CAN activity summaries include last frame metadata.
- First frame on each interface is explicitly logged.
- MQTT reconnect logic uses exponential backoff.
