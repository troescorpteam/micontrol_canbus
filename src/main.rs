use anyhow::Result;
use can_dbc::{ByteOrder, Signal};
use dotenv::dotenv;
use futures::StreamExt;
use lazy_static::lazy_static;
use socketcan::{CanFrame, EmbeddedFrame, ExtendedId, Id, StandardId, tokio::CanSocket};
use std::collections::HashMap;
use std::convert::TryInto;
use std::env;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::sync::RwLock;
use tokio::time::{Duration, interval};

// Standard CAN constants (missing from socketcan crate)
const EFF_FLAG: u32 = 0x80000000; // Extended Frame Format flag

// Global message data storage using lazy_static
lazy_static! {
    pub static ref GLOBAL_MESSAGE_DATA: Arc<RwLock<HashMap<u32, MessageData>>> =
        Arc::new(RwLock::new(HashMap::new()));
}

// Structure to store message signals and their current values
#[derive(Debug, Clone)]
pub struct MessageData {
    name: String,
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

    fn update_from_frame(&mut self, frame: &CanFrame) {
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

            self.signal_values
                .insert(signal.name().clone(), calculated_value);
        }
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

        for signal in &self.signals {
            let signal_value = self
                .signal_values
                .get(signal.name())
                .ok_or_else(|| format!("Signal '{}' value not found", signal.name()))?;

            // Convert float value back to raw integer
            let raw_value =
                ((*signal_value - *signal.offset() as f32) / *signal.factor() as f32) as u64;

            // Create bit mask for the signal
            let bit_mask: u64 = (1u64 << *signal.signal_size()) - 1;
            let masked_value = raw_value & bit_mask;

            // Encode the signal into the frame data
            self.encode_signal_to_frame(&mut frame_data, signal, masked_value);
        }

        // Create the CAN frame
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

    // Read configuration from environment variables with fallback defaults
    let dbc_file = env::var("DBC_FILE").unwrap_or_else(|_| "epc.dbc".to_string());
    let can_interface = env::var("CAN_INTERFACE").unwrap_or_else(|_| "can0".to_string());

    println!("Using DBC file: {}", dbc_file);
    println!("Using CAN interface: {}", can_interface);

    let mut socket_rx = CanSocket::open(&can_interface).unwrap();

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

    // Create a timer for periodic printing
    let mut print_timer = interval(Duration::from_secs(10));

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
                            msg_data.update_from_frame(&frame);
                        }
                    }
                    Some(Err(err)) => {
                        eprintln!("IO error: {}", err);
                    }
                    None => break,
                }
            }
            _ = print_timer.tick() => {
                println!("\n========== CAN Signal Values ==========");
                {
                    let message_data = GLOBAL_MESSAGE_DATA.read().await;
                    for (can_id, msg_data) in message_data.iter() {
                        println!("\nCAN ID: 0x{:08x}", can_id);
                        msg_data.print_all_signals();
                    }
                }
                println!("=======================================\n");

                // Demonstrate frame construction by modifying PhaseSequence
                if let Ok((can_id, frame)) = construct_frame_with_signal(
                    &mut *GLOBAL_MESSAGE_DATA.write().await,
                    "StatusGridMonitorLoc",
                    "PhaseSequence",
                    1.0  // Set PhaseSequence to 1.0
                ) {
                    println!("Constructed frame for PhaseSequence modification:");
                    println!("CAN ID: 0x{:08x}", can_id);
                    match frame.id() {
                        socketcan::Id::Standard(id) => {
                            println!("Frame ID (Standard): 0x{:03x}", id.as_raw());
                        }
                        socketcan::Id::Extended(id) => {
                            println!("Frame ID (Extended): 0x{:08x}", id.as_raw());
                        }
                    }
                    println!("Frame data: {:02x?}", frame.data());
                    // socket_rx.write_frame(frame).await?;
                    println!("-------------------------------------------\n");
                } else {
                    println!("Failed to construct frame for PhaseSequence\n");
                }
            }
        }
    }

    Ok(())
}

// Helper function to find a message by name and construct a frame with modified signal value
fn construct_frame_with_signal(
    message_data: &mut HashMap<u32, MessageData>,
    message_name: &str,
    signal_name: &str,
    new_value: f32,
) -> Result<(u32, CanFrame), String> {
    // Find the message by name
    let (can_id, msg_data) = message_data
        .iter_mut()
        .find(|(_, msg)| msg.name == message_name)
        .ok_or_else(|| format!("Message '{}' not found", message_name))?;

    // Set the new signal value
    msg_data.set_signal_value(signal_name, new_value)?;

    // Construct the frame
    let frame = msg_data.construct_frame(*can_id)?;

    Ok((*can_id, frame))
}

// Helper function to construct frame by CAN ID with modified signal value
fn construct_frame_with_signal_by_id(
    message_data: &mut HashMap<u32, MessageData>,
    can_id: u32,
    signal_name: &str,
    new_value: f32,
) -> Result<CanFrame, String> {
    let msg_data = message_data
        .get_mut(&can_id)
        .ok_or_else(|| format!("Message with CAN ID 0x{:08x} not found", can_id))?;

    // Set the new signal value
    msg_data.set_signal_value(signal_name, new_value)?;

    // Construct the frame
    msg_data.construct_frame(can_id)
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

    #[test]
    fn test_frame_construction_helper_functions() {
        // Test the helper functions without requiring a real CAN interface
        let mut message_data: HashMap<u32, MessageData> = HashMap::new();

        // This test verifies the logic of the helper functions even if we can't test actual frame construction
        // due to missing DBC or CAN interface in test environment

        // We can test that the functions handle missing messages correctly
        let result =
            construct_frame_with_signal_by_id(&mut message_data, 0x12345678, "TestSignal", 1.0);
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

            let result = construct_frame_with_signal(
                &mut message_data,
                "StatusGridMonitorLoc",
                "PhaseSequence",
                1.0,
            );

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
