use anyhow::{Context, Result};
use bus::{BusManager, BusState, RedisCommand};
use can_dbc::Dbc;
use chrono::{DateTime, SecondsFormat, Utc};
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
use tokio::sync::{RwLock, mpsc};
use tokio::time::{self, Duration, Instant, MissedTickBehavior};
use tracing::{debug, error, info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

mod bus;
mod identifiers;
mod message_type;
mod mqtt_integration;

const EFF_FLAG: u32 = 0x80000000; // Extended Frame Format flag
const CONNECTIONS_HASH: &str = "connections";

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

        let message_data = Arc::new(RwLock::new(message_store));
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

fn build_message_store(dbc: &Dbc) -> (HashMap<u32, MessageData>, HashMap<String, u32>) {
    let mut message_map = HashMap::new();
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

async fn update_connection_status(
    manager: &ConnectionManager,
    bus: &BusState,
    status: &str,
    last_online: Option<DateTime<Utc>>,
) -> Result<()> {
    let now = Utc::now();
    let last_online = last_online.unwrap_or(now);
    let payload = json!({
        "connection_status": status,
        "last_updated": now.to_rfc3339_opts(SecondsFormat::Nanos, true),
        "last_online": last_online.to_rfc3339_opts(SecondsFormat::Nanos, true),
    });

    let mut conn = manager.clone();
    redis::cmd("HSET")
        .arg("connections")
        .arg(bus.redis_hash())
        .arg(payload.to_string())
        .query_async::<()>(&mut conn)
        .await?;

    Ok(())
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
    redis_manager: Option<ConnectionManager>,
    mqtt_service: Arc<mqtt_integration::MqttService>,
) {
    let frame_store = bus_state.frame_store();
    let message_data = frame_store.data();
    let bus_state_handle = Arc::clone(&bus_state);
    tokio::spawn(async move {
        let mut socket = match CanSocket::open(&interface) {
            Ok(socket) => socket,
            Err(err) => {
                error!(interface = %interface, error = %err, "Failed to open CAN interface");
                return;
            }
        };

        let bus_state = bus_state_handle;

        let mut last_online: Option<DateTime<Utc>> = None;

        if let Some(manager) = redis_manager.clone() {
            if let Err(err) =
                update_connection_status(&manager, &bus_state, "Connected", None).await
            {
                warn!(
                    interface = %interface,
                    error = %err,
                    "Failed to publish initial connection status"
                );
            } else {
                last_online = Some(Utc::now());
            }
        }

        let mut log_interval = time::interval_at(
            Instant::now() + Duration::from_secs(10),
            Duration::from_secs(10),
        );
        log_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        let mut last_frame_info: Option<LastFrameSummary> = None;
        let mut frames_since_log: u64 = 0;
        let mut total_frames: u64 = 0;

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

                            let (message_name, changed_signals) = {
                                let mut data_guard = message_data.write().await;
                                if let Some(msg_data) = data_guard.get_mut(&id) {
                                    let message_name = msg_data.name.clone();
                                    let changes = msg_data.update_from_frame(&frame);
                                    (Some(message_name), changes)
                                } else {
                                    debug!(
                                        can_id = id,
                                        interface = %interface,
                                        "Received frame for unknown CAN ID"
                                    );
                                    (None, Vec::new())
                                }
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
                            last_online = Some(now);

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
                                last_online = Some(Utc::now());
                            }
                        }
                        None => {
                            warn!(interface = %interface, "CAN TX channel closed");
                            break;
                        }
                    }
                }
                _ = log_interval.tick() => {
                    if frames_since_log > 0 {
                        if let Some(summary) = last_frame_info.as_ref() {
                            let id_str = format_frame_id(summary.raw_id, summary.is_extended);
                            let message = summary.message_name.as_deref().unwrap_or("unknown");
                            let data = format_frame_bytes(&summary.data);
                            let changed = format_changed_signals(&summary.changed_signals);
                            let timestamp = summary.timestamp.to_rfc3339_opts(SecondsFormat::Millis, true);
                            info!(
                                interface = %interface,
                                frames_in_interval = frames_since_log,
                                total_frames = total_frames,
                                last_frame_timestamp = %timestamp,
                                last_frame_id = %id_str,
                                last_frame_message = %message,
                                last_frame_dlc = summary.dlc,
                                last_frame_changed = %changed,
                                last_frame_data = %data,
                                "CAN activity in the last 10s"
                            );

                            if let Some(manager) = redis_manager.clone() {
                                if let Err(err) = update_connection_status(
                                    &manager,
                                    &bus_state,
                                    "Connected",
                                    Some(summary.timestamp),
                                )
                                .await
                                {
                                    warn!(
                                        interface = %interface,
                                        error = %err,
                                        "Failed to refresh connection status"
                                    );
                                }
                            }
                        } else {
                            info!(
                                interface = %interface,
                                frames_in_interval = frames_since_log,
                                total_frames = total_frames,
                                "Received CAN frames but missing summary metadata"
                            );
                        }
                        frames_since_log = 0;
                    } else if let Some(summary) = last_frame_info.as_ref() {
                        let timestamp = summary.timestamp.to_rfc3339_opts(SecondsFormat::Millis, true);
                        info!(
                            interface = %interface,
                            total_frames = total_frames,
                            last_frame_timestamp = %timestamp,
                            "No CAN frames received in the last 10s"
                        );

                        if let Some(manager) = redis_manager.clone() {
                            if let Err(err) = update_connection_status(
                                &manager,
                                &bus_state,
                                "Connected",
                                Some(summary.timestamp),
                            )
                            .await
                            {
                                warn!(
                                    interface = %interface,
                                    error = %err,
                                    "Failed to refresh connection status"
                                );
                            }
                        }
                    } else {
                        info!(
                            interface = %interface,
                            "No CAN frames received yet on this interface"
                        );

                        if let Some(manager) = redis_manager.clone() {
                            if let Err(err) = update_connection_status(
                                &manager,
                                &bus_state,
                                "Connected",
                                None,
                            )
                            .await
                            {
                                warn!(
                                    interface = %interface,
                                    error = %err,
                                    "Failed to refresh connection status"
                                );
                            }
                        }
                    }
                }
            }
        }

        if let Some(manager) = redis_manager {
            if let Err(err) =
                update_connection_status(&manager, &bus_state, "Disconnected", last_online).await
            {
                warn!(
                    interface = %interface,
                    error = %err,
                    "Failed to publish disconnect status"
                );
            }
        }
    });
}
