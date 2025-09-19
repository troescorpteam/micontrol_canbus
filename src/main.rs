use crate::message_type::MessageData;
use crate::message_type::{CAN_TX_SENDER, GLOBAL_MESSAGE_DATA};
use anyhow::Result;
use dotenv::dotenv;
use futures::StreamExt;
use redis::aio::ConnectionManager;
use serde::Deserialize;
use socketcan::{CanFrame, EmbeddedFrame, tokio::CanSocket};
use std::collections::HashMap;
use std::env;
use std::io::ErrorKind;
use tokio::fs::{self, File};
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

mod message_type;
mod mqtt_integration;

// Standard CAN constants (missing from socketcan crate)
const EFF_FLAG: u32 = 0x80000000; // Extended Frame Format flag
const REDIS_HASH_KEY: &str = "epc_canbus";

// Global Redis connection manager and worker channel
static REDIS_MANAGER: tokio::sync::OnceCell<ConnectionManager> = tokio::sync::OnceCell::const_new();
static REDIS_COMMAND_SENDER: tokio::sync::OnceCell<mpsc::UnboundedSender<RedisCommand>> =
    tokio::sync::OnceCell::const_new();

#[derive(Debug)]
struct RedisCommand {
    field_key: String,
    value: f64,
}

#[derive(Debug, Default, Deserialize)]
struct AppConfig {
    #[serde(default)]
    buses: Vec<BusConfig>,
}

#[derive(Clone, Debug, Deserialize)]
struct BusConfig {
    interface: String,
    dbc: String,
}

impl AppConfig {
    fn dbc_for(&self, interface: &str) -> Option<&str> {
        self.buses
            .iter()
            .find(|bus| bus.interface == interface)
            .map(|bus| bus.dbc.as_str())
    }

    fn default_bus(&self) -> Option<&BusConfig> {
        self.buses.first()
    }
}

// Initialize Redis client
async fn init_redis() -> Result<()> {
    let redis_url = env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string());
    let client = redis::Client::open(redis_url)?;
    let manager = ConnectionManager::new(client).await?;

    // Test connection asynchronously
    let mut ping_conn = manager.clone();
    let _: String = redis::cmd("PING").query_async(&mut ping_conn).await?;

    // Spawn worker task for Redis writes
    let (sender, mut receiver) = mpsc::unbounded_channel::<RedisCommand>();
    tokio::spawn({
        let mut worker_manager = manager.clone();
        async move {
            while let Some(command) = receiver.recv().await {
                let mut conn = worker_manager.clone();
                if let Err(err) = redis::cmd("HSET")
                    .arg(REDIS_HASH_KEY)
                    .arg(&command.field_key)
                    .arg(command.value)
                    .query_async::<()>(&mut conn)
                    .await
                {
                    error!(
                        signal = %command.field_key,
                        error = %err,
                        "Failed to store signal in Redis"
                    );
                }
            }
        }
    });

    REDIS_MANAGER
        .set(manager)
        .map_err(|_| anyhow::anyhow!("Failed to initialize Redis manager - already initialized"))?;
    REDIS_COMMAND_SENDER.set(sender).map_err(|_| {
        anyhow::anyhow!("Failed to initialize Redis command sender - already initialized")
    })?;

    info!("Redis connection initialized successfully");
    Ok(())
}

async fn load_config(path: &str) -> Result<AppConfig> {
    match fs::read_to_string(path).await {
        Ok(contents) => {
            let config = toml::from_str::<AppConfig>(&contents)?;
            Ok(config)
        }
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(AppConfig::default()),
        Err(err) => Err(err.into()),
    }
}

// Get Redis connection manager clone
async fn get_redis_connection() -> Result<ConnectionManager> {
    let manager = REDIS_MANAGER
        .get()
        .ok_or_else(|| anyhow::anyhow!("Redis client not initialized"))?;
    Ok(manager.clone())
}

// Store signal value in Redis hash epc_canbus with key as messagename_signalname
async fn store_signal_in_redis(message_name: &str, signal_name: &str, value: f32) -> Result<()> {
    if let Some(sender) = REDIS_COMMAND_SENDER.get() {
        let field_key = format!("{}_{}", message_name, signal_name);
        sender
            .send(RedisCommand {
                field_key,
                value: value as f64,
            })
            .map_err(|err| anyhow::anyhow!("Failed to queue Redis command: {}", err))?;
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing subscriber
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "micontrol_canbus=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Load environment variables from .env file
    dotenv().ok();

    // Set up signal update channel for MQTT integration FIRST

    tokio::spawn(async {
        info!("Starting MQTT client setup...");

        // Setup MQTT client
        mqtt_integration::MqttIntegration::setup_mqtt_client().await;
    });

    // Load optional configuration mapping CAN ports to DBC files
    let config = match load_config("config.toml").await {
        Ok(cfg) => cfg,
        Err(err) => {
            warn!(error = %err, "Failed to load config.toml; falling back to defaults");
            AppConfig::default()
        }
    };

    // Resolve CAN interface and DBC file based on environment overrides and configuration
    let mut can_interface = env::var("CAN_INTERFACE").ok();
    let mut dbc_file = env::var("DBC_FILE").ok();

    if let Some(ref interface) = can_interface {
        if dbc_file.is_none() {
            if let Some(cfg_dbc) = config.dbc_for(interface) {
                dbc_file = Some(cfg_dbc.to_owned());
            }
        }
    } else if let Some(default_bus) = config.default_bus() {
        can_interface = Some(default_bus.interface.clone());
        if dbc_file.is_none() {
            dbc_file = Some(default_bus.dbc.clone());
        }
    }

    let can_interface = can_interface.unwrap_or_else(|| "can0".to_string());
    let dbc_file = dbc_file.unwrap_or_else(|| "epc.dbc".to_string());

    info!(dbc_file = %dbc_file, "Using DBC file");
    info!(can_interface = %can_interface, "Using CAN interface");

    // Initialize Redis connection
    if let Err(e) = init_redis().await {
        warn!(
            error = %e,
            "Failed to initialize Redis. Continuing without Redis."
        );
    }

    let mut socket_rx = CanSocket::open(&can_interface).unwrap();

    // Create CAN transmission channel
    let (can_tx_sender, mut can_tx_receiver) = mpsc::unbounded_channel::<CanFrame>();
    CAN_TX_SENDER
        .set(can_tx_sender)
        .map_err(|_| anyhow::anyhow!("Failed to initialize CAN TX sender - already initialized"))?;

    // Read DBC file and create message data storage
    let mut f = File::open(&dbc_file).await?;
    let mut buffer = Vec::new();
    f.read_to_end(&mut buffer).await?;
    let dbc = can_dbc::DBC::from_slice(&buffer).expect("Failed to parse DBC");

    // Initialize global message data from DBC
    {
        let mut message_data = HashMap::new();
        for msg in dbc.messages() {
            let message_id: u32 = match msg.message_id() {
                can_dbc::MessageId::Standard(id) => (*id).into(),
                can_dbc::MessageId::Extended(id) => *id,
            };
            let id = message_id & !EFF_FLAG;
            let dlc = *msg.message_size() as u8;
            let is_extended = matches!(msg.message_id(), can_dbc::MessageId::Extended(_));
            message_data.insert(
                id,
                MessageData::new(
                    msg.message_name().clone(),
                    msg.signals().clone(),
                    dlc,
                    is_extended,
                ),
            );
        }
        *GLOBAL_MESSAGE_DATA.write().await = message_data;
    }

    loop {
        tokio::select! {
            socket_result = socket_rx.next() => {
                match socket_result {
                    Some(Ok(frame)) => {
                        let raw_id = match frame.id() {
                            socketcan::Id::Standard(id) => id.as_raw() as u32,
                            socketcan::Id::Extended(id) => id.as_raw(),
                        };
                        let id = raw_id & !EFF_FLAG;

                        // Update message data if we have it
                        if let Some(msg_data) = GLOBAL_MESSAGE_DATA.write().await.get_mut(&id) {
                            let changed_signals = msg_data.update_from_frame(&frame);

                            // Store only changed signal values in Redis hash
                            for (signal_name, value) in changed_signals {
                                if let Err(e) = store_signal_in_redis(&msg_data.name, &signal_name, value).await {
                                    error!(
                                        message_name = %msg_data.name,
                                        signal_name = %signal_name,
                                        error = %e,
                                        "Failed to store signal in Redis"
                                    );
                                }
                            }
                        }
                    }
                    Some(Err(err)) => {
                        error!(error = %err, "IO error reading CAN frame");
                    }
                    None => break,
                }
            }
            // Handle outgoing CAN frames from the channel
            frame_to_send = can_tx_receiver.recv() => {
                match frame_to_send {
                    Some(frame) => {
                        if let Err(e) = socket_rx.write_frame(frame).await {
                            error!(error = %e, "Failed to send CAN frame");
                        } else {
                            debug!(frame_data = ?frame.data(), "Sent CAN frame");
                        }
                    }
                    None => {
                        warn!("CAN TX channel closed");
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}

// Get all signals from Redis hash epc_canbus
async fn get_all_signals_from_redis() -> Result<HashMap<String, f32>> {
    let mut conn = get_redis_connection().await?;
    let result: HashMap<String, f32> = redis::cmd("HGETALL")
        .arg(REDIS_HASH_KEY)
        .query_async(&mut conn)
        .await?;

    Ok(result)
}

// Get specific signal value from Redis hash
async fn get_signal_from_redis(message_name: &str, signal_name: &str) -> Result<Option<f32>> {
    let mut conn = get_redis_connection().await?;
    let field_key = format!("{}_{}", message_name, signal_name);

    let result: Option<f32> = redis::cmd("HGET")
        .arg(REDIS_HASH_KEY)
        .arg(&field_key)
        .query_async(&mut conn)
        .await?;

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mqtt_integration::construct_frame_with_signal;

    async fn load_test_dbc() -> Result<can_dbc::DBC> {
        let mut f = File::open("epc.dbc").await?;
        let mut buffer = Vec::new();
        f.read_to_end(&mut buffer).await?;
        Ok(can_dbc::DBC::from_slice(&buffer).expect("Failed to parse DBC"))
    }

    #[tokio::test]
    async fn test_phase_sequence_frame_construction() {
        // Load the actual DBC file to get real message structures
        let dbc = load_test_dbc().await.unwrap();

        // Find StatusGridMonitorLoc message
        let status_grid_msg = dbc
            .messages()
            .iter()
            .find(|msg| msg.message_name() == "StatusGridMonitorLoc")
            .expect("StatusGridMonitorLoc message not found in DBC");

        let message_id: u32 = match status_grid_msg.message_id() {
            can_dbc::MessageId::Standard(id) => (*id).into(),
            can_dbc::MessageId::Extended(id) => *id,
        };
        let can_id = message_id & !EFF_FLAG;

        // Create MessageData with real signals from DBC
        let mut msg_data = MessageData::new(
            status_grid_msg.message_name().clone(),
            status_grid_msg.signals().clone(),
            *status_grid_msg.message_size() as u8,
            matches!(
                status_grid_msg.message_id(),
                can_dbc::MessageId::Extended(_)
            ),
        );

        // Check if PhaseSequence signal exists
        let phase_sequence_signal = status_grid_msg
            .signals()
            .iter()
            .find(|signal| signal.name() == "PhaseSequence");

        assert!(
            phase_sequence_signal.is_some(),
            "PhaseSequence signal not found in StatusGridMonitorLoc"
        );

        // Set PhaseSequence to 1.0
        let result = msg_data.set_signal_value("PhaseSequence", 1.0);
        assert!(
            result.is_ok(),
            "Failed to set PhaseSequence value: {:?}",
            result
        );

        // Try to construct the frame
        let frame_result = msg_data.construct_frame(can_id);
        assert!(
            frame_result.is_ok(),
            "Failed to construct frame: {:?}",
            frame_result
        );

        let frame = frame_result.unwrap();
        info!(
            can_id = %format!("0x{:08x}", can_id),
            "Successfully constructed frame"
        );
        debug!(frame_data = ?frame.data(), "Frame data");

        // Verify the frame has the expected properties
        assert_eq!(frame.data().len(), 8, "Frame should have 8 bytes of data");
    }

    #[tokio::test]
    async fn test_multiple_signal_frame_construction() {
        let dbc = load_test_dbc().await.unwrap();

        // Test with different messages to ensure the frame construction works generally
        let test_messages = [
            "StatusGridMonitorLoc",
            "StatusInverterPhaseArm",
            "StatusInverterAcuate",
        ];

        for msg_name in &test_messages {
            if let Some(msg) = dbc.messages().iter().find(|m| m.message_name() == msg_name) {
                let message_id: u32 = match msg.message_id() {
                    can_dbc::MessageId::Standard(id) => (*id).into(),
                    can_dbc::MessageId::Extended(id) => *id,
                };
                let can_id = message_id & !EFF_FLAG;

                let mut msg_data = MessageData::new(
                    msg.message_name().clone(),
                    msg.signals().clone(),
                    *msg.message_size() as u8,
                    matches!(msg.message_id(), can_dbc::MessageId::Extended(_)),
                );

                // Set first signal to a test value if it exists
                if let Some(first_signal) = msg.signals().first() {
                    let result = msg_data.set_signal_value(first_signal.name(), 42.0);
                    assert!(
                        result.is_ok(),
                        "Failed to set signal value for {}",
                        first_signal.name()
                    );

                    let frame_result = msg_data.construct_frame(can_id);
                    assert!(
                        frame_result.is_ok(),
                        "Failed to construct frame for message {}: {:?}",
                        msg_name,
                        frame_result
                    );

                    info!(message_name = %msg_name, "Successfully constructed frame for message");
                }
            }
        }
    }

    #[tokio::test]
    async fn test_signal_encoding_decoding_roundtrip() {
        let dbc = load_test_dbc().await.unwrap();

        if let Some(msg) = dbc
            .messages()
            .iter()
            .find(|m| m.message_name() == "StatusGridMonitorLoc")
        {
            let message_id: u32 = match msg.message_id() {
                can_dbc::MessageId::Standard(id) => (*id).into(),
                can_dbc::MessageId::Extended(id) => *id,
            };
            let can_id = message_id & !EFF_FLAG;

            let mut msg_data = MessageData::new(
                msg.message_name().clone(),
                msg.signals().clone(),
                *msg.message_size() as u8,
                matches!(msg.message_id(), can_dbc::MessageId::Extended(_)),
            );

            // Set all signals to known test values
            let test_values = vec![
                ("PhaseSequence", 1.0),
                ("NetworkVoltage", 230.5),
                ("NetworkFrequency", 50.2),
            ];

            for (signal_name, value) in &test_values {
                if msg_data.signal_values.contains_key(*signal_name) {
                    msg_data.set_signal_value(signal_name, *value).unwrap();
                }
            }

            // Construct frame
            let frame = msg_data.construct_frame(can_id).unwrap();

            // Create a new MessageData and decode the frame
            let mut decoded_msg_data = MessageData::new(
                msg.message_name().clone(),
                msg.signals().clone(),
                *msg.message_size() as u8,
                matches!(msg.message_id(), can_dbc::MessageId::Extended(_)),
            );
            decoded_msg_data.update_from_frame(&frame);

            // Check that we can retrieve the values (allowing for some precision loss)
            for (signal_name, expected_value) in &test_values {
                if let Some(decoded_value) = decoded_msg_data.get_signal_value(signal_name) {
                    let diff = (decoded_value - expected_value).abs();
                    assert!(
                        diff < 0.1,
                        "Signal {} roundtrip failed: expected {}, got {}",
                        signal_name,
                        expected_value,
                        decoded_value
                    );
                    info!(
                        signal_name = %signal_name,
                        expected_value = %expected_value,
                        decoded_value = %decoded_value,
                        "Signal roundtrip successful"
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn test_frame_construction_helper_functions() {
        // Test the helper functions without requiring a real CAN interface
        // This test verifies the logic of the helper functions even if we can't test actual frame construction
        // due to missing DBC or CAN interface in test environment

        // We can test that the functions handle missing messages correctly
        let result = construct_frame_with_signal("NonExistentMessage", "TestSignal", 1.0).await;
        assert!(result.is_err(), "Should fail when message doesn't exist");

        let error_msg = result.unwrap_err();
        assert!(
            error_msg.contains("not found"),
            "Error should mention message not found"
        );
    }

    #[tokio::test]
    async fn test_debug_phase_sequence_issue() {
        // This test specifically debugs the PhaseSequence issue
        let dbc = load_test_dbc().await.unwrap();

        // Print all messages to understand the structure
        info!("Available messages in DBC:");
        for msg in dbc.messages() {
            info!(
                message_name = %msg.message_name(),
                message_id = ?msg.message_id(),
                "DBC message"
            );
        }

        // Find and analyze StatusGridMonitorLoc
        if let Some(msg) = dbc
            .messages()
            .iter()
            .find(|m| m.message_name() == "StatusGridMonitorLoc")
        {
            info!("StatusGridMonitorLoc signals:");
            for signal in msg.signals() {
                info!(
                    signal_name = %signal.name(),
                    start_bit = signal.start_bit(),
                    size = signal.signal_size(),
                    byte_order = ?signal.byte_order(),
                    factor = signal.factor(),
                    offset = signal.offset(),
                    "Signal details"
                );
            }

            let message_id: u32 = match msg.message_id() {
                can_dbc::MessageId::Standard(id) => (*id).into(),
                can_dbc::MessageId::Extended(id) => *id,
            };
            let can_id = message_id & !EFF_FLAG;
            info!(
                can_id = %format!("0x{:08x}", can_id),
                raw_message_id = %format!("0x{:08x}", message_id),
                "CAN ID details"
            );

            // Try the frame construction that's failing in main
            let mut message_data: HashMap<u32, MessageData> = HashMap::new();
            message_data.insert(
                can_id,
                MessageData::new(
                    msg.message_name().clone(),
                    msg.signals().clone(),
                    *msg.message_size() as u8,
                    matches!(msg.message_id(), can_dbc::MessageId::Extended(_)),
                ),
            );

            let result =
                construct_frame_with_signal("StatusGridMonitorLoc", "PhaseSequence", 1.0).await;

            match result {
                Ok((returned_can_id, frame)) => {
                    info!("SUCCESS: Constructed frame!");
                    info!(
                        returned_can_id = %format!("0x{:08x}", returned_can_id),
                        "Returned CAN ID"
                    );
                    debug!(frame_data = ?frame.data(), "Frame data");
                }
                Err(e) => {
                    error!(error = %e, "ERROR constructing frame");

                    // Debug further - check if the signal exists
                    if let Some(msg_data) = message_data.get(&can_id) {
                        info!("Signal values in message:");
                        for (name, value) in &msg_data.signal_values {
                            info!(
                                signal_name = %name,
                                signal_value = %value,
                                "Signal value"
                            );
                        }
                    }
                }
            }
        } else {
            error!("StatusGridMonitorLoc message not found!");
        }
    }
}
