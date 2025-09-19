use anyhow::{Context, Result};
use bus::{BusManager, BusState, RedisCommand};
use can_dbc::DBC;
use chrono::{DateTime, SecondsFormat, Utc};
use dotenv::dotenv;
use futures::{StreamExt, future};
use message_type::MessageData;
use redis::aio::ConnectionManager;
use serde::Deserialize;
use serde_json::json;
use socketcan::{CanFrame, EmbeddedFrame, Id, tokio::CanSocket};
use std::collections::HashMap;
use std::env;
use std::path::Path;
use std::sync::Arc;
use tokio::fs;
use tokio::sync::{RwLock, mpsc};
use tracing::{debug, error, info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

mod bus;
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

        let (redis_hash, mqtt_topic) = derive_identifiers(mapping, &interface);

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
            redis_hash.clone(),
            mqtt_topic.clone(),
            mapping.hardware_type.clone(),
            mapping.hardware_id.clone(),
            Arc::clone(&message_data),
            Arc::clone(&message_index),
            tx_sender.clone(),
            redis_sender.clone(),
        ));

        bus_manager.insert(Arc::clone(&bus_state)).await;

        if let (Some(manager), Some(rx)) = (redis_manager.clone(), redis_rx) {
            spawn_redis_worker(manager, rx, redis_hash.clone());
        }

        spawn_can_runtime(
            bus_state,
            message_data,
            tx_receiver,
            interface,
            redis_manager.clone(),
        );
    }

    let mqtt_bus_manager = Arc::clone(&bus_manager);
    tokio::spawn(async move {
        mqtt_integration::MqttIntegration::setup_mqtt_client(mqtt_bus_manager).await;
    });

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

async fn load_dbc(path: &str) -> Result<DBC> {
    let bytes = fs::read(path)
        .await
        .with_context(|| format!("Failed to read DBC file '{path}'"))?;
    DBC::from_slice(&bytes)
        .map_err(|err| anyhow::anyhow!("Failed to parse DBC file '{path}': {:?}", err))
}

fn build_message_store(dbc: &DBC) -> (HashMap<u32, MessageData>, HashMap<String, u32>) {
    let mut message_map = HashMap::new();
    let mut name_index = HashMap::new();

    for msg in dbc.messages() {
        let message_id: u32 = match msg.message_id() {
            can_dbc::MessageId::Standard(id) => (*id).into(),
            can_dbc::MessageId::Extended(id) => *id,
        };
        let id = message_id & !EFF_FLAG;
        name_index.insert(msg.message_name().clone(), id);
        message_map.insert(
            id,
            MessageData::new(
                msg.message_name().clone(),
                msg.signals().clone(),
                *msg.message_size() as u8,
                matches!(msg.message_id(), can_dbc::MessageId::Extended(_)),
            ),
        );
    }

    (message_map, name_index)
}

fn derive_identifiers(mapping: &HardwareMapping, controller: &str) -> (String, String) {
    let hash_base = match (
        mapping.hardware_type.as_deref(),
        mapping.hardware_id.as_deref(),
    ) {
        (Some(ht), Some(id)) if !ht.is_empty() && !id.is_empty() => {
            format!("{}_{}", sanitize_identifier(ht), sanitize_identifier(id))
        }
        (Some(ht), _) if !ht.is_empty() => sanitize_identifier(ht),
        (_, Some(id)) if !id.is_empty() => format!("canbus_{}", sanitize_identifier(id)),
        _ => sanitize_identifier(controller),
    };

    let topic = format!(
        "canbus/{}/{}/controls",
        sanitize_identifier(mapping.hardware_type.as_deref().unwrap_or("unknown")),
        sanitize_identifier(mapping.hardware_id.as_deref().unwrap_or("unknown"))
    );

    (hash_base, topic)
}

fn sanitize_identifier(input: &str) -> String {
    input
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect()
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
    message_data: Arc<RwLock<HashMap<u32, MessageData>>>,
    mut tx_receiver: mpsc::UnboundedReceiver<CanFrame>,
    interface: String,
    redis_manager: Option<ConnectionManager>,
) {
    tokio::spawn(async move {
        let mut socket = match CanSocket::open(&interface) {
            Ok(socket) => socket,
            Err(err) => {
                error!(interface = %interface, error = %err, "Failed to open CAN interface");
                return;
            }
        };

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

                            let mut data_guard = message_data.write().await;
                            if let Some(msg_data) = data_guard.get_mut(&id) {
                                let message_name = msg_data.name.clone();
                                let changed = msg_data.update_from_frame(&frame);
                                drop(data_guard);

                                for (signal_name, value) in changed {
                                    let field_key = format!("{}_{}", message_name, signal_name);
                                    bus_state.enqueue_redis(RedisCommand {
                                        field_key,
                                        value: value as f64,
                                    });
                                }

                                last_online = Some(Utc::now());
                            } else {
                                drop(data_guard);
                                debug!(
                                    can_id = id,
                                    interface = %interface,
                                    "Received frame for unknown CAN ID"
                                );
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
