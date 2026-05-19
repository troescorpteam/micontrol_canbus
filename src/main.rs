use anyhow::{Context, Result};
use bus::{BusManager, BusState, RedisCommand};
use can_dbc::Dbc;
use chrono::{DateTime, Duration as ChronoDuration, SecondsFormat, Utc};
use dashmap::DashMap;
use dotenv::dotenv;
use futures::{StreamExt, future};
use identifiers::derive_identifiers;
use message_type::MessageData;
use redis::aio::ConnectionManager;
use serde::Deserialize;
use serde_json::{Map, Value as JsonValue, json};
use socketcan::{CanFrame, EmbeddedFrame, Id, tokio::CanSocket};
use std::collections::HashMap;
use std::env;
use std::path::Path;
use std::sync::Arc;
use tokio::fs;
use tokio::sync::mpsc;
use tokio::time::{self, Duration, Instant, MissedTickBehavior};
use tracing::{debug, error, info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

mod bus;
mod identifiers;
mod message_type;
mod mqtt_integration;

const EFF_FLAG: u32 = 0x80000000; // Extended Frame Format flag
const CONNECTIONS_HASH: &str = "connections";
const DEFAULT_AUTO_INVALIDATION_INTERVAL_SECS: u64 = 30;
const DEFAULT_CAN_BITRATE: u32 = 125_000;
const CAN_ACTIVITY_LOG_INTERVAL_SECS: u64 = 10;

#[derive(Debug, Default, Deserialize)]
struct AppConfig {
    #[serde(default)]
    hardware_mappings: Vec<HardwareMapping>,
}

#[derive(Clone, Debug, Deserialize)]
struct HardwareMapping {
    #[serde(default)]
    hardware_configurations: Vec<String>,
    controller: String,
    #[serde(default)]
    protocol: Option<String>,
    #[serde(default)]
    hardware_type: Option<String>,
    #[serde(default)]
    hardware_id: Option<String>,
    #[serde(default)]
    auto_invalidation_interval: Option<u64>,
    #[serde(default, alias = "can_bitrate")]
    bitrate: Option<u32>,
}

impl AppConfig {
    fn all_mappings(&self) -> Vec<&HardwareMapping> {
        self.hardware_mappings.iter().collect()
    }
}

impl HardwareMapping {
    fn matches_interface(&self, interface: &str) -> bool {
        self.controller == interface
    }

    fn dbc(&self) -> Option<String> {
        if let Some(entry) = self
            .hardware_configurations
            .iter()
            .find(|cfg| cfg.ends_with(".dbc"))
        {
            return Some(entry.clone());
        }

        self.hardware_configurations.iter().find_map(|cfg| {
            let candidate = format!("{}.dbc", cfg);
            if Path::new(&candidate).exists() {
                Some(candidate)
            } else {
                None
            }
        })
    }

    fn stale_after_secs(&self) -> u64 {
        normalize_auto_invalidation_interval(self.auto_invalidation_interval)
    }

    fn bitrate(&self) -> u32 {
        normalize_can_bitrate(self.bitrate)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "micontrol_canbus=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    dotenv().ok();

    let config = load_config("config.toml").await?;
    if config.hardware_mappings.is_empty() {
        anyhow::bail!("No hardware_mappings defined in config.toml");
    }

    let mappings_to_run = config.all_mappings();
    let system_name = env::var("SYSTEM_NAME").unwrap_or_else(|_| "micontrol".to_string());

    let redis_manager = match init_redis_connection().await {
        Ok(manager) => {
            info!("Redis connection initialized successfully");
            Some(manager)
        }
        Err(err) => {
            warn!(
                error = %err,
                "Failed to initialize Redis. Continuing without Redis."
            );
            None
        }
    };

    let bus_manager = Arc::new(BusManager::new());
    let mqtt_service = Arc::new(mqtt_integration::MqttService::new());

    for (interface, bitrate) in collect_interface_bitrates(&mappings_to_run)? {
        info!(interface = %interface, bitrate, "Configuring CAN interface bitrate");
        configure_can_interface(&interface, bitrate).await?;
    }

    for mapping in mappings_to_run {
        let interface = mapping.controller.clone();
        let dbc_path = mapping.dbc().ok_or_else(|| {
            anyhow::anyhow!(
                "No DBC file specified for controller '{}' in config.toml",
                mapping.controller
            )
        })?;

        info!(
            interface = %interface,
            dbc = %dbc_path,
            "Initializing CAN bus runtime"
        );

        let dbc = load_dbc(&dbc_path).await?;
        let (message_store, message_index) = build_message_store(&dbc);

        let message_data = Arc::new(message_store);
        let message_index = Arc::new(message_index);

        let topic_info = derive_identifiers(
            &system_name,
            &interface,
            mapping.hardware_type.as_deref(),
            mapping.hardware_id.as_deref(),
        );

        let (tx_sender, tx_receiver) = mpsc::unbounded_channel::<CanFrame>();
        let (redis_sender, redis_rx) = if redis_manager.is_some() {
            let (tx, rx) = mpsc::unbounded_channel::<RedisCommand>();
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };

        let bus_id = interface.clone();
        let bus_state = Arc::new(BusState::new(
            bus_id.clone(),
            mapping.controller.clone(),
            interface.clone(),
            topic_info.redis_hash.clone(),
            topic_info.control_topic.clone(),
            topic_info.measurement_topic.clone(),
            mapping.hardware_type.clone(),
            mapping.hardware_id.clone(),
            Arc::clone(&message_data),
            Arc::clone(&message_index),
            tx_sender.clone(),
            redis_sender.clone(),
        ));

        bus_manager.insert(Arc::clone(&bus_state)).await;

        if let (Some(manager), Some(rx)) = (redis_manager.clone(), redis_rx) {
            spawn_redis_worker(manager, rx, topic_info.redis_hash.clone());
        }

        spawn_can_runtime(
            bus_state,
            tx_receiver,
            interface,
            mapping.stale_after_secs(),
            redis_manager.clone(),
            Arc::clone(&mqtt_service),
        );
    }

    // Start MQTT service AFTER all buses are registered
    {
        let service = Arc::clone(&mqtt_service);
        let manager = Arc::clone(&bus_manager);
        tokio::spawn(async move {
            service.run(manager).await;
        });
    }

    info!("Runtime initialized for all configured CAN buses");
    future::pending::<()>().await;
    Ok(())
}

async fn load_config(path: &str) -> Result<AppConfig> {
    let contents = fs::read_to_string(path)
        .await
        .with_context(|| format!("failed to read configuration file '{path}'"))?;

    let config = toml::from_str::<AppConfig>(&contents)
        .with_context(|| format!("failed to parse configuration file '{path}'"))?;

    Ok(config)
}

async fn init_redis_connection() -> Result<ConnectionManager> {
    let redis_url = env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string());
    let client = redis::Client::open(redis_url)?;
    let manager = ConnectionManager::new(client).await?;

    let mut ping_conn = manager.clone();
    let _: String = redis::cmd("PING").query_async(&mut ping_conn).await?;

    Ok(manager)
}

async fn load_dbc(path: &str) -> Result<Dbc> {
    let bytes = fs::read(path)
        .await
        .with_context(|| format!("Failed to read DBC file '{path}'"))?;
    let content = String::from_utf8(bytes)
        .with_context(|| format!("DBC file '{path}' is not valid UTF-8"))?;
    Dbc::try_from(content.as_str())
        .map_err(|err| anyhow::anyhow!("Failed to parse DBC file '{path}': {:?}", err))
}

fn build_message_store(dbc: &Dbc) -> (DashMap<u32, MessageData>, HashMap<String, u32>) {
    let message_map = DashMap::new();
    let mut name_index = HashMap::new();

    for msg in &dbc.messages {
        let message_id: u32 = match msg.id {
            can_dbc::MessageId::Standard(id) => id.into(),
            can_dbc::MessageId::Extended(id) => id,
        };
        let id = message_id & !EFF_FLAG;
        name_index.insert(msg.name.clone(), id);
        message_map.insert(
            id,
            MessageData::new(
                msg.name.clone(),
                msg.signals.clone(),
                msg.size as u8,
                matches!(msg.id, can_dbc::MessageId::Extended(_)),
            ),
        );
    }

    (message_map, name_index)
}

fn normalize_auto_invalidation_interval(configured: Option<u64>) -> u64 {
    configured
        .filter(|interval| *interval > 0)
        .unwrap_or(DEFAULT_AUTO_INVALIDATION_INTERVAL_SECS)
}

fn normalize_can_bitrate(configured: Option<u32>) -> u32 {
    configured
        .filter(|bitrate| *bitrate > 0)
        .unwrap_or(DEFAULT_CAN_BITRATE)
}

fn is_valid_controller_name(interface: &str) -> bool {
    !interface.is_empty()
        && !interface.starts_with('-')
        && interface.len() <= 15
        && interface
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.')
}

fn collect_interface_bitrates(mappings: &[&HardwareMapping]) -> Result<Vec<(String, u32)>> {
    let mut configured = HashMap::new();

    for mapping in mappings {
        let interface = mapping.controller.as_str();
        if !is_valid_controller_name(interface) {
            anyhow::bail!(
                "Invalid controller '{}' in config.toml; expected Linux interface name characters [A-Za-z0-9_.-], max length 15, and must not start with '-'",
                interface
            );
        }

        let bitrate = mapping.bitrate();

        if configured.insert(interface.to_string(), bitrate).is_some() {
            anyhow::bail!(
                "Duplicate controller '{}' found in config.toml; each controller must be defined only once",
                interface
            );
        }
    }

    Ok(configured.into_iter().collect())
}

async fn run_ip_link(args: &[String]) -> Result<()> {
    let output = tokio::process::Command::new("ip")
        .args(args)
        .output()
        .await
        .with_context(|| format!("failed to run ip {}", args.join(" ")))?;

    if output.status.success() {
        return Ok(());
    }

    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    anyhow::bail!(
        "ip {} failed with status {}: {}",
        args.join(" "),
        output.status,
        stderr
    );
}

fn can_interface_setup_steps(interface: &str, bitrate: u32) -> [Vec<String>; 3] {
    [
        vec!["link".into(), "set".into(), interface.into(), "down".into()],
        vec![
            "link".into(),
            "set".into(),
            interface.into(),
            "type".into(),
            "can".into(),
            "bitrate".into(),
            bitrate.to_string(),
        ],
        vec!["link".into(), "set".into(), interface.into(), "up".into()],
    ]
}

async fn configure_can_interface(interface: &str, bitrate: u32) -> Result<()> {
    let mut interface_brought_down = false;

    for (index, step) in can_interface_setup_steps(interface, bitrate).into_iter().enumerate() {
        if let Err(err) = run_ip_link(&step).await {
            if interface_brought_down {
                let cleanup_step =
                    vec!["link".into(), "set".into(), interface.into(), "up".into()];
                let _ = run_ip_link(&cleanup_step).await;
            }
            return Err(err);
        }

        if index == 0 {
            interface_brought_down = true;
        }
    }

    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CanHealthStatus {
    WaitingForData,
    Connected,
    Stale,
    Disconnected,
}

impl CanHealthStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::WaitingForData => "WaitingForData",
            Self::Connected => "Connected",
            Self::Stale => "Stale",
            Self::Disconnected => "Disconnected",
        }
    }
}

fn health_status_for_frame_age(
    now: DateTime<Utc>,
    last_frame_at: Option<DateTime<Utc>>,
    stale_after: ChronoDuration,
) -> CanHealthStatus {
    match last_frame_at {
        Some(timestamp) if now.signed_duration_since(timestamp) <= stale_after => {
            CanHealthStatus::Connected
        }
        Some(_) => CanHealthStatus::Stale,
        None => CanHealthStatus::WaitingForData,
    }
}

fn frame_age_ms(now: DateTime<Utc>, last_frame_at: Option<DateTime<Utc>>) -> Option<i64> {
    last_frame_at.map(|timestamp| {
        now.signed_duration_since(timestamp)
            .num_milliseconds()
            .max(0)
    })
}

struct ConnectionStatusSnapshot {
    status: CanHealthStatus,
    reason: &'static str,
    now: DateTime<Utc>,
    last_frame_at: Option<DateTime<Utc>>,
    total_frames: u64,
    interface: String,
}

fn connection_status_payload(snapshot: &ConnectionStatusSnapshot) -> JsonValue {
    let mut payload = Map::new();
    payload.insert("connection_status".into(), json!(snapshot.status.as_str()));
    payload.insert("status".into(), json!(snapshot.status.as_str()));
    payload.insert("reason".into(), json!(snapshot.reason));
    payload.insert("interface".into(), json!(snapshot.interface));
    payload.insert("total_frames".into(), json!(snapshot.total_frames));
    payload.insert(
        "last_updated".into(),
        json!(snapshot.now.to_rfc3339_opts(SecondsFormat::Nanos, true)),
    );
    payload.insert(
        "fetched_time_utc".into(),
        json!(snapshot.now.to_rfc3339_opts(SecondsFormat::Millis, true)),
    );

    if let Some(last_frame_at) = snapshot.last_frame_at {
        let timestamp = last_frame_at.to_rfc3339_opts(SecondsFormat::Nanos, true);
        payload.insert("last_online".into(), json!(timestamp));
        payload.insert("last_frame_at".into(), json!(timestamp));
        payload.insert(
            "frame_age_ms".into(),
            json!(frame_age_ms(snapshot.now, snapshot.last_frame_at)),
        );
    } else {
        payload.insert("last_frame_at".into(), JsonValue::Null);
        payload.insert("frame_age_ms".into(), JsonValue::Null);
    }

    JsonValue::Object(payload)
}

async fn update_connection_status(
    manager: &ConnectionManager,
    bus: &BusState,
    snapshot: &ConnectionStatusSnapshot,
) -> Result<()> {
    let payload = connection_status_payload(snapshot);

    let mut conn = manager.clone();
    redis::cmd("HSET")
        .arg(CONNECTIONS_HASH)
        .arg(bus.redis_hash())
        .arg(payload.to_string())
        .query_async::<()>(&mut conn)
        .await?;

    Ok(())
}

async fn publish_connection_status(
    redis_manager: Option<&ConnectionManager>,
    mqtt_service: &mqtt_integration::MqttService,
    bus: &BusState,
    snapshot: &ConnectionStatusSnapshot,
    publish_redis: bool,
    publish_mqtt: bool,
) {
    if publish_redis && let Some(manager) = redis_manager {
        if let Err(err) = update_connection_status(manager, bus, snapshot).await {
            warn!(
                interface = %snapshot.interface,
                status = snapshot.status.as_str(),
                error = %err,
                "Failed to publish connection status to Redis"
            );
        }
    }

    if publish_mqtt {
        mqtt_service
            .publish_measurement(bus.measurement_topic(), connection_status_payload(snapshot))
            .await;
    }
}

struct LastFrameSummary {
    timestamp: DateTime<Utc>,
    raw_id: u32,
    is_extended: bool,
    dlc: usize,
    message_name: Option<String>,
    changed_signals: Vec<(String, f32)>,
    data: Vec<u8>,
}

fn format_frame_bytes(data: &[u8]) -> String {
    if data.is_empty() {
        return "empty".to_string();
    }

    data.iter()
        .map(|byte| format!("{:02X}", byte))
        .collect::<Vec<_>>()
        .join(" ")
}

fn format_frame_id(raw_id: u32, is_extended: bool) -> String {
    if is_extended {
        format!("0x{:08X}", raw_id)
    } else {
        format!("0x{:03X}", raw_id)
    }
}

fn format_changed_signals(changed: &[(String, f32)]) -> String {
    if changed.is_empty() {
        return "none".to_string();
    }

    changed
        .iter()
        .map(|(name, value)| format!("{}={:.3}", name, value))
        .collect::<Vec<_>>()
        .join(", ")
}

fn spawn_redis_worker(
    manager: ConnectionManager,
    mut receiver: mpsc::UnboundedReceiver<RedisCommand>,
    hash_key: String,
) {
    tokio::spawn(async move {
        while let Some(command) = receiver.recv().await {
            let mut conn = manager.clone();
            if let Err(err) = redis::cmd("HSET")
                .arg(&hash_key)
                .arg(&command.field_key)
                .arg(command.value)
                .query_async::<()>(&mut conn)
                .await
            {
                warn!(
                    redis_hash = %hash_key,
                    field = %command.field_key,
                    error = %err,
                    "Failed to store signal in Redis"
                );
            }
        }
    });
}

fn spawn_can_runtime(
    bus_state: Arc<BusState>,
    mut tx_receiver: mpsc::UnboundedReceiver<CanFrame>,
    interface: String,
    stale_after_secs: u64,
    redis_manager: Option<ConnectionManager>,
    mqtt_service: Arc<mqtt_integration::MqttService>,
) {
    let frame_store = bus_state.frame_store();
    let message_data = frame_store.data();
    let bus_state_handle = Arc::clone(&bus_state);
    tokio::spawn(async move {
        let bus_state = bus_state_handle;
        let stale_after = ChronoDuration::seconds(stale_after_secs as i64);
        let mut last_frame_at: Option<DateTime<Utc>> = None;
        let mut total_frames: u64 = 0;
        let mut unknown_frames: u64 = 0;
        let mut frames_since_log: u64 = 0;

        let mut socket = match CanSocket::open(&interface) {
            Ok(socket) => socket,
            Err(err) => {
                error!(interface = %interface, error = %err, "Failed to open CAN interface");
                let snapshot = ConnectionStatusSnapshot {
                    status: CanHealthStatus::Disconnected,
                    reason: "socket_open_failed",
                    now: Utc::now(),
                    last_frame_at,
                    total_frames,
                    interface: interface.clone(),
                };
                publish_connection_status(
                    redis_manager.as_ref(),
                    &mqtt_service,
                    &bus_state,
                    &snapshot,
                    true,
                    true,
                )
                .await;
                return;
            }
        };

        let initial_snapshot = ConnectionStatusSnapshot {
            status: CanHealthStatus::WaitingForData,
            reason: "socket_opened",
            now: Utc::now(),
            last_frame_at,
            total_frames,
            interface: interface.clone(),
        };
        publish_connection_status(
            redis_manager.as_ref(),
            &mqtt_service,
            &bus_state,
            &initial_snapshot,
            true,
            true,
        )
        .await;
        let mut last_status = Some(initial_snapshot.status);

        let mut log_interval = time::interval_at(
            Instant::now() + Duration::from_secs(CAN_ACTIVITY_LOG_INTERVAL_SECS),
            Duration::from_secs(CAN_ACTIVITY_LOG_INTERVAL_SECS),
        );
        log_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        let mut last_frame_info: Option<LastFrameSummary> = None;

        loop {
            tokio::select! {
                frame_result = socket.next() => {
                    match frame_result {
                        Some(Ok(frame)) => {
                            let raw_id = match frame.id() {
                                Id::Standard(id) => id.as_raw() as u32,
                                Id::Extended(id) => id.as_raw(),
                            };
                            let id = raw_id & !EFF_FLAG;
                            let is_extended = matches!(frame.id(), Id::Extended(_));
                            let dlc = frame.data().len();
                            let frame_bytes = frame.data().to_vec();
                            let now = Utc::now();

                            let (message_name, changed_signals) =
                                if let Some(mut msg_data) = message_data.get_mut(&id) {
                                    let message_name = msg_data.name.clone();
                                    let changes = msg_data.update_from_frame(&frame);
                                    (Some(message_name), changes)
                                } else {
                                    debug!(
                                        can_id = id,
                                        interface = %interface,
                                        "Received frame for unknown CAN ID"
                                    );
                                    unknown_frames += 1;
                                    (None, Vec::new())
                                };

                            if let Some(message_name) = message_name.as_ref() {
                                let mut measurement_payload = Map::new();

                                for (signal_name, value) in &changed_signals {
                                    let field_key = format!("{}.{}", message_name, signal_name);
                                    bus_state.enqueue_redis(RedisCommand {
                                        field_key,
                                        value: *value as f64,
                                    });

                                    let measurement_key =
                                        format!("{}.{}", message_name, signal_name);
                                    measurement_payload
                                        .insert(measurement_key, json!(f64::from(*value)));
                                }

                                if !measurement_payload.is_empty() {
                                    let timestamp_str =
                                        now.to_rfc3339_opts(SecondsFormat::Millis, true);
                                    measurement_payload
                                        .insert("fetched_time_utc".into(), json!(timestamp_str));
                                    measurement_payload
                                        .insert("status".into(), json!("ok"));

                                    mqtt_service
                                        .publish_measurement(
                                            bus_state.measurement_topic(),
                                            JsonValue::Object(measurement_payload),
                                        )
                                        .await;
                                }
                            }

                            let first_frame = total_frames == 0;
                            total_frames += 1;
                            frames_since_log += 1;
                            last_frame_at = Some(now);

                            let summary = LastFrameSummary {
                                timestamp: now,
                                raw_id,
                                is_extended,
                                dlc,
                                message_name: message_name.clone(),
                                changed_signals: changed_signals.clone(),
                                data: frame_bytes,
                            };
                            last_frame_info = Some(summary);

                            if first_frame {
                                if let Some(summary) = last_frame_info.as_ref() {
                                    let id_str = format_frame_id(summary.raw_id, summary.is_extended);
                                    let message = summary.message_name.as_deref().unwrap_or("unknown");
                                    let data = format_frame_bytes(&summary.data);
                                    let changed = format_changed_signals(&summary.changed_signals);
                                    info!(
                                        interface = %interface,
                                        can_id = %id_str,
                                        message = %message,
                                        dlc = summary.dlc,
                                        changed_signals = %changed,
                                        frame_data = %data,
                                        "First CAN frame received"
                                    );
                                }
                            }

                            let snapshot = ConnectionStatusSnapshot {
                                status: CanHealthStatus::Connected,
                                reason: if last_status == Some(CanHealthStatus::Stale) {
                                    "can_data_recovered"
                                } else if first_frame {
                                    "first_can_frame"
                                } else {
                                    "can_frame_received"
                                },
                                now,
                                last_frame_at,
                                total_frames,
                                interface: interface.clone(),
                            };
                            let status_changed = last_status != Some(snapshot.status);
                            publish_connection_status(
                                redis_manager.as_ref(),
                                &mqtt_service,
                                &bus_state,
                                &snapshot,
                                status_changed,
                                status_changed,
                            )
                            .await;
                            last_status = Some(snapshot.status);
                        }
                        Some(Err(err)) => {
                            warn!(interface = %interface, error = %err, "CAN socket error");
                        }
                        None => {
                            warn!(interface = %interface, "CAN socket stream ended");
                            break;
                        }
                    }
                }
                maybe_frame = tx_receiver.recv() => {
                    match maybe_frame {
                        Some(frame) => {
                            let data = frame.data().to_vec();
                            if let Err(err) = socket.write_frame(frame).await {
                                error!(interface = %interface, error = %err, "Failed to send CAN frame");
                            } else {
                                debug!(interface = %interface, frame_data = ?data, "Sent CAN frame");
                            }
                        }
                        None => {
                            warn!(interface = %interface, "CAN TX channel closed");
                            break;
                        }
                    }
                }
                _ = log_interval.tick() => {
                    let now = Utc::now();
                    let status = health_status_for_frame_age(now, last_frame_at, stale_after);
                    let status_changed = last_status != Some(status);
                    let snapshot = ConnectionStatusSnapshot {
                        status,
                        reason: match (status, last_status) {
                            (CanHealthStatus::Stale, Some(CanHealthStatus::Connected)) => "can_data_stale",
                            (CanHealthStatus::Stale, _) => "can_data_still_stale",
                            (CanHealthStatus::WaitingForData, _) => "waiting_for_first_can_frame",
                            (CanHealthStatus::Connected, _) => "can_data_fresh",
                            (CanHealthStatus::Disconnected, _) => "disconnected",
                        },
                        now,
                        last_frame_at,
                        total_frames,
                        interface: interface.clone(),
                    };
                    publish_connection_status(
                        redis_manager.as_ref(),
                        &mqtt_service,
                        &bus_state,
                        &snapshot,
                        true,
                        status_changed,
                    )
                    .await;
                    last_status = Some(status);

                    if frames_since_log > 0 {
                        if let Some(summary) = last_frame_info.as_ref() {
                            let id_str = format_frame_id(summary.raw_id, summary.is_extended);
                            let message = summary.message_name.as_deref().unwrap_or("unknown");
                            let data = format_frame_bytes(&summary.data);
                            let changed = format_changed_signals(&summary.changed_signals);
                            let timestamp = summary.timestamp.to_rfc3339_opts(SecondsFormat::Millis, true);
                            info!(
                                interface = %interface,
                                health_status = status.as_str(),
                                stale_after_secs = stale_after_secs,
                                frames_in_interval = frames_since_log,
                                total_frames = total_frames,
                                unknown_frames = unknown_frames,
                                last_frame_timestamp = %timestamp,
                                last_frame_id = %id_str,
                                last_frame_message = %message,
                                last_frame_dlc = summary.dlc,
                                last_frame_changed = %changed,
                                last_frame_data = %data,
                                "CAN activity in the last 10s"
                            );
                        } else {
                            info!(
                                interface = %interface,
                                health_status = status.as_str(),
                                stale_after_secs = stale_after_secs,
                                frames_in_interval = frames_since_log,
                                total_frames = total_frames,
                                unknown_frames = unknown_frames,
                                "Received CAN frames but missing summary metadata"
                            );
                        }
                        frames_since_log = 0;
                    } else if let Some(summary) = last_frame_info.as_ref() {
                        let timestamp = summary.timestamp.to_rfc3339_opts(SecondsFormat::Millis, true);
                        info!(
                            interface = %interface,
                            health_status = status.as_str(),
                            stale_after_secs = stale_after_secs,
                            total_frames = total_frames,
                            unknown_frames = unknown_frames,
                            last_frame_timestamp = %timestamp,
                            "No CAN frames received in the last 10s"
                        );
                    } else {
                        info!(
                            interface = %interface,
                            health_status = status.as_str(),
                            stale_after_secs = stale_after_secs,
                            "No CAN frames received yet on this interface"
                        );
                    }
                }
            }
        }

        let snapshot = ConnectionStatusSnapshot {
            status: CanHealthStatus::Disconnected,
            reason: "can_runtime_stopped",
            now: Utc::now(),
            last_frame_at,
            total_frames,
            interface,
        };
        publish_connection_status(
            redis_manager.as_ref(),
            &mqtt_service,
            &bus_state,
            &snapshot,
            true,
            true,
        )
        .await;
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn test_time(seconds: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(seconds, 0).single().unwrap()
    }

    #[test]
    fn auto_invalidation_interval_defaults_when_missing_or_zero() {
        assert_eq!(
            normalize_auto_invalidation_interval(None),
            DEFAULT_AUTO_INVALIDATION_INTERVAL_SECS
        );
        assert_eq!(
            normalize_auto_invalidation_interval(Some(0)),
            DEFAULT_AUTO_INVALIDATION_INTERVAL_SECS
        );
        assert_eq!(normalize_auto_invalidation_interval(Some(45)), 45);
    }

    #[test]
    fn can_bitrate_defaults_when_missing_or_zero() {
        assert_eq!(normalize_can_bitrate(None), DEFAULT_CAN_BITRATE);
        assert_eq!(normalize_can_bitrate(Some(0)), DEFAULT_CAN_BITRATE);
        assert_eq!(normalize_can_bitrate(Some(500_000)), 500_000);
    }

    #[test]
    fn can_interface_setup_steps_match_expected_ip_link_sequence() {
        let steps = can_interface_setup_steps("can0", 125_000);
        assert_eq!(
            steps[0],
            vec![
                "link".to_string(),
                "set".to_string(),
                "can0".to_string(),
                "down".to_string()
            ]
        );
        assert_eq!(
            steps[1],
            vec![
                "link".to_string(),
                "set".to_string(),
                "can0".to_string(),
                "type".to_string(),
                "can".to_string(),
                "bitrate".to_string(),
                "125000".to_string()
            ]
        );
        assert_eq!(
            steps[2],
            vec![
                "link".to_string(),
                "set".to_string(),
                "can0".to_string(),
                "up".to_string()
            ]
        );
    }

    #[test]
    fn collect_interface_bitrates_rejects_duplicate_controllers() {
        let first = HardwareMapping {
            hardware_configurations: vec!["epc-2".to_string()],
            controller: "can0".to_string(),
            protocol: None,
            hardware_type: None,
            hardware_id: None,
            auto_invalidation_interval: None,
            bitrate: Some(125_000),
        };
        let second = HardwareMapping {
            hardware_configurations: vec!["epc-3".to_string()],
            controller: "can0".to_string(),
            protocol: None,
            hardware_type: None,
            hardware_id: None,
            auto_invalidation_interval: None,
            bitrate: Some(500_000),
        };

        let mappings = vec![&first, &second];
        let err = collect_interface_bitrates(&mappings).unwrap_err().to_string();
        assert!(err.contains("Duplicate controller 'can0'"));
    }

    #[test]
    fn collect_interface_bitrates_rejects_invalid_controller_names() {
        let invalid = HardwareMapping {
            hardware_configurations: vec!["epc-2".to_string()],
            controller: "-can0".to_string(),
            protocol: None,
            hardware_type: None,
            hardware_id: None,
            auto_invalidation_interval: None,
            bitrate: Some(125_000),
        };

        let mappings = vec![&invalid];
        let err = collect_interface_bitrates(&mappings).unwrap_err().to_string();
        assert!(err.contains("Invalid controller '-can0'"));
    }

    #[test]
    fn controller_name_validation_covers_edge_cases() {
        assert!(is_valid_controller_name("can0"));
        assert!(is_valid_controller_name("can0.100"));
        assert!(!is_valid_controller_name(""));
        assert!(!is_valid_controller_name("-can0"));
        assert!(!is_valid_controller_name("can 0"));
        assert!(!is_valid_controller_name("can0@"));
        assert!(!is_valid_controller_name("can0123456789012"));
    }

    #[test]
    fn no_frame_waits_for_data_before_any_inbound_can_frame() {
        let now = test_time(100);

        assert_eq!(
            health_status_for_frame_age(now, None, ChronoDuration::seconds(30)),
            CanHealthStatus::WaitingForData
        );
    }

    #[test]
    fn fresh_frame_is_connected_until_stale_threshold_expires() {
        let last_frame = test_time(100);

        assert_eq!(
            health_status_for_frame_age(
                test_time(130),
                Some(last_frame),
                ChronoDuration::seconds(30)
            ),
            CanHealthStatus::Connected
        );
        assert_eq!(
            health_status_for_frame_age(
                test_time(131),
                Some(last_frame),
                ChronoDuration::seconds(30)
            ),
            CanHealthStatus::Stale
        );
    }

    #[test]
    fn new_inbound_frame_after_stale_recovers_to_connected() {
        let now = test_time(200);

        assert_eq!(
            health_status_for_frame_age(now, Some(now), ChronoDuration::seconds(30)),
            CanHealthStatus::Connected
        );
    }

    #[test]
    fn outbound_activity_does_not_refresh_inbound_health() {
        let last_inbound_frame = test_time(100);
        let outbound_tx_time = test_time(160);

        assert_eq!(
            health_status_for_frame_age(
                outbound_tx_time,
                Some(last_inbound_frame),
                ChronoDuration::seconds(30)
            ),
            CanHealthStatus::Stale
        );
    }

    #[test]
    fn connection_status_payload_omits_last_online_until_real_frame_arrives() {
        let snapshot = ConnectionStatusSnapshot {
            status: CanHealthStatus::WaitingForData,
            reason: "socket_opened",
            now: test_time(100),
            last_frame_at: None,
            total_frames: 0,
            interface: "can0".into(),
        };

        let payload = connection_status_payload(&snapshot);
        let object = payload.as_object().unwrap();

        assert_eq!(object["connection_status"], json!("WaitingForData"));
        assert_eq!(object["status"], json!("WaitingForData"));
        assert_eq!(object["reason"], json!("socket_opened"));
        assert_eq!(object["interface"], json!("can0"));
        assert_eq!(object["total_frames"], json!(0));
        assert_eq!(object["last_frame_at"], JsonValue::Null);
        assert_eq!(object["frame_age_ms"], JsonValue::Null);
        assert!(!object.contains_key("last_online"));
    }

    #[test]
    fn connection_status_payload_includes_frame_freshness_metadata() {
        let snapshot = ConnectionStatusSnapshot {
            status: CanHealthStatus::Connected,
            reason: "first_can_frame",
            now: test_time(105),
            last_frame_at: Some(test_time(100)),
            total_frames: 12,
            interface: "can0".into(),
        };

        let payload = connection_status_payload(&snapshot);
        let object = payload.as_object().unwrap();

        assert_eq!(object["connection_status"], json!("Connected"));
        assert_eq!(object["last_online"], object["last_frame_at"]);
        assert_eq!(object["frame_age_ms"], json!(5000));
        assert_eq!(object["total_frames"], json!(12));
    }
}
