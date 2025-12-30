#!/usr/bin/env python3
import argparse
import json
import os
import time

try:
    import paho.mqtt.client as mqtt
except ImportError as exc:
    raise SystemExit("Missing dependency: paho-mqtt (pip install paho-mqtt)") from exc


def load_env_file(path: str) -> None:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                if key and key not in os.environ:
                    os.environ[key] = value
    except FileNotFoundError:
        return


def sanitize_identifier(value: str) -> str:
    return "".join(ch if ch.isalnum() else "_" for ch in value)


def sanitize_optional(value: str | None, fallback: str) -> str:
    if not value:
        return fallback
    if all(ch.isalnum() for ch in value):
        return value
    return sanitize_identifier(value)


def load_config_mapping(config_path: str) -> dict | None:
    try:
        import tomllib  # Python 3.11+
    except ImportError:
        return None

    try:
        with open(config_path, "rb") as handle:
            config = tomllib.load(handle)
    except (FileNotFoundError, OSError, ValueError):
        return None

    mappings = config.get("hardware_mappings")
    if not isinstance(mappings, list) or not mappings:
        return None
    if not isinstance(mappings[0], dict):
        return None
    return mappings[0]


def default_control_topic(config_path: str) -> str:
    system_name = os.getenv("SYSTEM_NAME", "micontrol")
    mapping = load_config_mapping(config_path) or {}
    hardware_type = mapping.get("hardware_type")
    hardware_id = mapping.get("hardware_id")

    base = "/".join(
        [
            sanitize_identifier(system_name),
            sanitize_optional(hardware_type, "unknown"),
            sanitize_optional(hardware_id, "unknown"),
        ]
    )
    return f"{base}/controls"


def build_parser(default_topic: str) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Publish MBCU_Cmd_Info1.MBCU_Life heartbeat via MQTT control map."
    )
    parser.add_argument("--host", default=os.getenv("MQTT_HOST", "192.168.0.203"))
    parser.add_argument("--port", type=int, default=int(os.getenv("MQTT_PORT", "1884")))
    parser.add_argument("--username", default=os.getenv("MQTT_USERNAME", "iot_platform"))
    parser.add_argument("--password", default=os.getenv("MQTT_PASSWORD", "123456"))
    parser.add_argument("--topic", default=default_topic)
    parser.add_argument("--interval", type=float, default=0.05)
    parser.add_argument("--qos", type=int, choices=[0, 1, 2], default=1)
    parser.add_argument("--life-max", type=int, default=16)
    parser.add_argument("--start", type=int, default=0)
    parser.add_argument("--retry-delay", type=float, default=1.0)
    parser.add_argument("--max-retry-delay", type=float, default=30.0)
    parser.add_argument("--message-name", default="MBCU_Cmd_Info1")
    parser.add_argument("--signal-name", default="MBCU_Life")
    parser.add_argument("--verbose", action="store_true")
    return parser


def connect_with_retry(
    client: mqtt.Client,
    host: str,
    port: int,
    keepalive: int,
    retry_delay: float,
    max_retry_delay: float,
    verbose: bool,
) -> None:
    delay = max(retry_delay, 0.1)
    max_delay = max(max_retry_delay, delay)
    while True:
        try:
            client.connect(host, port, keepalive=keepalive)
            if verbose:
                print(f"connected to {host}:{port}")
            return
        except OSError as exc:
            if verbose:
                print(f"connect failed: {exc}; retrying in {delay:.1f}s")
            time.sleep(delay)
            delay = min(delay * 2, max_delay)


def main() -> None:
    load_env_file(".env")
    default_topic = default_control_topic("config.toml")
    args = build_parser(default_topic).parse_args()

    if args.interval <= 0:
        raise SystemExit("--interval must be > 0")
    if args.retry_delay <= 0:
        raise SystemExit("--retry-delay must be > 0")

    callback_api = getattr(mqtt, "CallbackAPIVersion", None)
    if callback_api is not None:
        client = mqtt.Client(callback_api_version=callback_api.VERSION2)
    else:
        client = mqtt.Client()
    if args.username:
        client.username_pw_set(args.username, args.password)
    client.reconnect_delay_set(min_delay=args.retry_delay, max_delay=args.max_retry_delay)
    connect_with_retry(
        client,
        args.host,
        args.port,
        keepalive=60,
        retry_delay=args.retry_delay,
        max_retry_delay=args.max_retry_delay,
        verbose=args.verbose,
    )
    client.loop_start()

    heartbeat = args.start % args.life_max
    key = f"{args.message_name}.{args.signal_name}"
    next_tick = time.monotonic()

    try:
        while True:
            if client.is_connected():
                payload = {"control": {key: heartbeat}}
                info = client.publish(args.topic, json.dumps(payload), qos=args.qos, retain=False)
                if info.rc != mqtt.MQTT_ERR_SUCCESS and args.verbose:
                    print(f"publish failed: {mqtt.error_string(info.rc)}")
            if args.verbose:
                print(f"topic={args.topic} {key}={heartbeat}")
            heartbeat = (heartbeat + 1) % args.life_max
            next_tick += args.interval
            sleep_for = next_tick - time.monotonic()
            if sleep_for > 0:
                time.sleep(sleep_for)
    except KeyboardInterrupt:
        pass
    finally:
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()
