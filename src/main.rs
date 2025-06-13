use anyhow::Result;
use can_dbc::{ByteOrder, Signal};
use dotenv::dotenv;
use futures::StreamExt;
use lazy_static::lazy_static;
use micontrol_canbus::CAN_TX_SENDER;
use micontrol_canbus::{SignalUpdatePayload, set_signal_update_sender};
use serde::{Deserialize, Serialize};
use socketcan::{CanFrame, EmbeddedFrame, ExtendedId, Id, StandardId, tokio::CanSocket};
use std::collections::HashMap;
use std::convert::TryInto;
use std::env;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::sync::{RwLock, mpsc};

mod mqtt_integration;

// Standard CAN constants (missing from socketcan crate)
const EFF_FLAG: u32 = 0x80000000; // Extended Frame Format flag

// Global message data storage using lazy_static
lazy_static! {
    pub static ref GLOBAL_MESSAGE_DATA: Arc<RwLock<HashMap<u32, MessageData>>> =
        Arc::new(RwLock::new(HashMap::new()));
}

// Global Redis connection
static REDIS_CLIENT: tokio::sync::OnceCell<redis::Client> = tokio::sync::OnceCell::const_new();

// Initialize Redis client
async fn init_redis() -> Result<()> {
    let redis_url = env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string());
    let client = redis::Client::open(redis_url)?;

    // Test connection
    let mut conn = client.get_connection()?;
    let _: String = redis::cmd("PING").query(&mut conn)?;

    REDIS_CLIENT
        .set(client)
        .map_err(|_| anyhow::anyhow!("Failed to initialize Redis client - already initialized"))?;

    println!("Redis connection initialized successfully");
    Ok(())
}

// Get Redis connection
async fn get_redis_connection() -> Result<redis::Connection> {
    let client = REDIS_CLIENT
        .get()
        .ok_or_else(|| anyhow::anyhow!("Redis client not initialized"))?;
    Ok(client.get_connection()?)
}

// Store signal value in Redis hash epc_canbus with key as messagename_signalname
async fn store_signal_in_redis(message_name: &str, signal_name: &str, value: f32) -> Result<()> {
    let mut conn = get_redis_connection().await?;
    let hash_key = "epc_canbus";
    let field_key = format!("{}_{}", message_name, signal_name);

    let _: () = redis::cmd("HSET")
        .arg(hash_key)
        .arg(&field_key)
        .arg(value)
        .query(&mut conn)?;

    Ok(())
}

// Structure to store message signals and their current values
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageData {
    name: String,
    #[serde(skip)] // Skip Signal as it may not be serializable
    signals: Vec<Signal>,
    signal_values: HashMap<String, f32>,
}

impl MessageData {
    fn new(name: String, signals: Vec<Signal>) -> Self {
        let signal_values = signals
            .iter()
            .map(|signal| (signal.name().clone(), 0.0))
            .collect();

        Self {
            name,
            signals,
            signal_values,
        }
    }

    fn update_from_frame(&mut self, frame: &CanFrame) -> Vec<(String, f32)> {
        let mut changed_signals = Vec::new();

        for signal in &self.signals {
            let frame_data: [u8; 8] = frame
                .data()
                .try_into()
                .expect("slice with incorrect length");

            let signal_value: u64 = if *signal.byte_order() == ByteOrder::LittleEndian {
                u64::from_le_bytes(frame_data)
            } else {
                u64::from_be_bytes(frame_data)
            };

            // Calculate signal value
            let bit_mask: u64 = 2u64.pow(*signal.signal_size() as u32) - 1;
            let calculated_value = ((signal_value >> signal.start_bit()) & bit_mask) as f32
                * *signal.factor() as f32
                + *signal.offset() as f32;

            // Check if the value has changed
            let current_value = self
                .signal_values
                .get(signal.name())
                .copied()
                .unwrap_or(0.0);
            if (calculated_value - current_value).abs() > f32::EPSILON {
                changed_signals.push((signal.name().clone(), calculated_value));
                self.signal_values
                    .insert(signal.name().clone(), calculated_value);
            }
        }

        changed_signals
    }

    fn get_signal_value(&self, signal_name: &str) -> Option<f32> {
        self.signal_values.get(signal_name).copied()
    }

    fn print_all_signals(&self) {
        println!("\n=== {} ===", self.name);
        for (signal_name, value) in &self.signal_values {
            println!("{} → {:.4}", signal_name, value);
        }
    }

    fn set_signal_value(&mut self, signal_name: &str, value: f32) -> Result<(), String> {
        if self.signal_values.contains_key(signal_name) {
            self.signal_values.insert(signal_name.to_string(), value);
            Ok(())
        } else {
            Err(format!(
                "Signal '{}' not found in message '{}'",
                signal_name, self.name
            ))
        }
    }

    fn construct_frame(&self, can_id: u32) -> Result<CanFrame, String> {
        let mut frame_data = [0u8; 8];

        // Encode all signals into the frame data
        for signal in &self.signals {
            let signal_value = self
                .signal_values
                .get(signal.name())
                .ok_or_else(|| format!("Signal '{}' value not found", signal.name()))?;

            // Convert float value back to raw integer
            let raw_value =
                ((*signal_value - *signal.offset() as f32) / *signal.factor() as f32) as u64;

            // Encode this signal into the frame data
            self.encode_signal_to_frame(&mut frame_data, signal, raw_value);
        }

        // Create the CAN frame with the encoded data
        let id = if can_id > 0x7FF || (can_id & EFF_FLAG != 0) {
            // Extended CAN ID (29-bit) - remove EFF_FLAG if present
            let extended_id =
                ExtendedId::new(can_id & !EFF_FLAG).ok_or("Invalid extended CAN ID")?;
            Id::Extended(extended_id)
        } else {
            // Standard CAN ID (11-bit)
            let standard_id = StandardId::new(can_id as u16).ok_or("Invalid standard CAN ID")?;
            Id::Standard(standard_id)
        };

        CanFrame::new(id, &frame_data).ok_or_else(|| "Failed to create CAN frame".to_string())
    }

    fn encode_signal_to_frame(&self, frame_data: &mut [u8; 8], signal: &Signal, value: u64) {
        let start_bit = *signal.start_bit() as usize;
        let signal_size = *signal.signal_size() as usize;

        match signal.byte_order() {
            ByteOrder::LittleEndian => {
                // For little endian, we need to place bits starting from the start_bit
                let mut remaining_bits = signal_size;
                let mut current_value = value;
                let mut bit_position = start_bit;

                while remaining_bits > 0 {
                    let byte_index = bit_position / 8;
                    let bit_offset = bit_position % 8;
                    let bits_in_byte = std::cmp::min(remaining_bits, 8 - bit_offset);

                    // Fix overflow issue by using u16 for intermediate calculations
                    let byte_mask = if bits_in_byte == 8 {
                        0xFFu8
                    } else {
                        ((1u16 << bits_in_byte) - 1) as u8
                    } << bit_offset;

                    let byte_value = ((current_value as u8) << bit_offset) & byte_mask;

                    frame_data[byte_index] &= !byte_mask;
                    frame_data[byte_index] |= byte_value;

                    current_value >>= bits_in_byte;
                    bit_position += bits_in_byte;
                    remaining_bits -= bits_in_byte;
                }
            }
            ByteOrder::BigEndian => {
                // For big endian, similar logic but different bit ordering
                let mut remaining_bits = signal_size;
                let mut current_value = value;
                let mut bit_position = start_bit;

                while remaining_bits > 0 {
                    let byte_index = bit_position / 8;
                    let bit_offset = bit_position % 8;
                    let bits_in_byte = std::cmp::min(remaining_bits, 8 - bit_offset);

                    // Fix overflow issue by using u16 for intermediate calculations
                    let byte_mask = if bits_in_byte == 8 {
                        0xFFu8
                    } else {
                        ((1u16 << bits_in_byte) - 1) as u8
                    } << (8 - bit_offset - bits_in_byte);

                    let byte_value =
                        ((current_value as u8) << (8 - bit_offset - bits_in_byte)) & byte_mask;

                    frame_data[byte_index] &= !byte_mask;
                    frame_data[byte_index] |= byte_value;

                    current_value >>= bits_in_byte;
                    bit_position += bits_in_byte;
                    remaining_bits -= bits_in_byte;
                }
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Load environment variables from .env file
    dotenv().ok();

    tokio::spawn(async {
        // Setup MQTT client
        mqtt_integration::MqttIntegration::setup_mqtt_client().await;
    });

    // Read configuration from environment variables with fallback defaults
    let dbc_file = env::var("DBC_FILE").unwrap_or_else(|_| "epc.dbc".to_string());
    let can_interface = env::var("CAN_INTERFACE").unwrap_or_else(|_| "can0".to_string());

    println!("Using DBC file: {}", dbc_file);
    println!("Using CAN interface: {}", can_interface);

    // Initialize Redis connection
    if let Err(e) = init_redis().await {
        eprintln!(
            "Warning: Failed to initialize Redis: {}. Continuing without Redis.",
            e
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
            message_data.insert(
                id,
                MessageData::new(msg.message_name().clone(), msg.signals().clone()),
            );
        }
        *GLOBAL_MESSAGE_DATA.write().await = message_data;
    }

    loop {
        tokio::select! {
            // Handle incoming CAN frames
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
                                    eprintln!("Failed to store signal {}_{} in Redis: {}", msg_data.name, signal_name, e);
                                }
                            }
                        }
                    }
                    Some(Err(err)) => {
                        eprintln!("IO error: {}", err);
                    }
                    None => break,
                }
            }
            // Handle outgoing CAN frames from the channel
            frame_to_send = can_tx_receiver.recv() => {
                match frame_to_send {
                    Some(frame) => {
                        if let Err(e) = socket_rx.write_frame(frame).await {
                            eprintln!("Failed to send CAN frame: {}", e);
                        } else {
                            println!("Sent CAN frame: {:02x?}", frame.data());
                        }
                    }
                    None => {
                        eprintln!("CAN TX channel closed");
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}

// Helper function to find a message by name and construct a frame with modified signal value
pub async fn construct_frame_with_signal(
    message_name: &str,
    signal_name: &str,
    new_value: f32,
) -> Result<(u32, CanFrame), String> {
    let mut message_data = GLOBAL_MESSAGE_DATA.write().await;

    // Find the message by name
    let (can_id, msg_data) = message_data
        .iter_mut()
        .find(|(_, msg)| msg.name == message_name)
        .ok_or_else(|| format!("Message '{}' not found", message_name))?;

    // Set the new signal value
    msg_data.set_signal_value(signal_name, new_value)?;

    // Store the signal update in Redis hash
    if let Err(e) = store_signal_in_redis(&msg_data.name, signal_name, new_value).await {
        eprintln!("Failed to store signal in Redis: {}", e);
    }

    // Construct the frame
    let frame = msg_data.construct_frame(*can_id)?;

    Ok((*can_id, frame))
}

/// Write a CAN frame to the socket
pub fn write_frame(frame: CanFrame) -> Result<(), String> {
    let sender = CAN_TX_SENDER
        .get()
        .ok_or_else(|| "CAN TX sender not initialized".to_string())?;
    sender
        .send(frame)
        .map_err(|e| format!("Failed to send CAN frame: {}", e))
}

// Get all signals from Redis hash epc_canbus
async fn get_all_signals_from_redis() -> Result<HashMap<String, f32>> {
    let mut conn = get_redis_connection().await?;
    let hash_key = "epc_canbus";

    let result: HashMap<String, f32> = redis::cmd("HGETALL").arg(hash_key).query(&mut conn)?;

    Ok(result)
}

// Get specific signal value from Redis hash
async fn get_signal_from_redis(message_name: &str, signal_name: &str) -> Result<Option<f32>> {
    let mut conn = get_redis_connection().await?;
    let hash_key = "epc_canbus";
    let field_key = format!("{}_{}", message_name, signal_name);

    let result: Option<f32> = redis::cmd("HGET")
        .arg(hash_key)
        .arg(&field_key)
        .query(&mut conn)?;

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

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
        println!(
            "Successfully constructed frame with CAN ID: 0x{:08x}",
            can_id
        );
        println!("Frame data: {:02x?}", frame.data());

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

                let mut msg_data =
                    MessageData::new(msg.message_name().clone(), msg.signals().clone());

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

                    println!("Successfully constructed frame for message: {}", msg_name);
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

            let mut msg_data = MessageData::new(msg.message_name().clone(), msg.signals().clone());

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
            let mut decoded_msg_data =
                MessageData::new(msg.message_name().clone(), msg.signals().clone());
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
                    println!(
                        "Signal {} roundtrip successful: {} -> {}",
                        signal_name, expected_value, decoded_value
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
        println!("Available messages in DBC:");
        for msg in dbc.messages() {
            println!("  {}: ID {:?}", msg.message_name(), msg.message_id());
        }

        // Find and analyze StatusGridMonitorLoc
        if let Some(msg) = dbc
            .messages()
            .iter()
            .find(|m| m.message_name() == "StatusGridMonitorLoc")
        {
            println!("\nStatusGridMonitorLoc signals:");
            for signal in msg.signals() {
                println!(
                    "  {}: start_bit={}, size={}, byte_order={:?}, factor={}, offset={}",
                    signal.name(),
                    signal.start_bit(),
                    signal.signal_size(),
                    signal.byte_order(),
                    signal.factor(),
                    signal.offset()
                );
            }

            let message_id: u32 = match msg.message_id() {
                can_dbc::MessageId::Standard(id) => (*id).into(),
                can_dbc::MessageId::Extended(id) => *id,
            };
            let can_id = message_id & !EFF_FLAG;
            println!("\nCAN ID: 0x{:08x} (raw: 0x{:08x})", can_id, message_id);

            // Try the frame construction that's failing in main
            let mut message_data: HashMap<u32, MessageData> = HashMap::new();
            message_data.insert(
                can_id,
                MessageData::new(msg.message_name().clone(), msg.signals().clone()),
            );

            let result =
                construct_frame_with_signal("StatusGridMonitorLoc", "PhaseSequence", 1.0).await;

            match result {
                Ok((returned_can_id, frame)) => {
                    println!("SUCCESS: Constructed frame!");
                    println!("  Returned CAN ID: 0x{:08x}", returned_can_id);
                    println!("  Frame data: {:02x?}", frame.data());
                }
                Err(e) => {
                    println!("ERROR: {}", e);

                    // Debug further - check if the signal exists
                    if let Some(msg_data) = message_data.get(&can_id) {
                        println!("Signal values in message:");
                        for (name, value) in &msg_data.signal_values {
                            println!("  {}: {}", name, value);
                        }
                    }
                }
            }
        } else {
            println!("StatusGridMonitorLoc message not found!");
        }
    }
}
