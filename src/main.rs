use anyhow::Result;
use can_dbc::{ByteOrder, Signal};
use futures::StreamExt;
use socketcan::{CanFrame, EmbeddedFrame, ExtendedId, Id, StandardId, tokio::CanSocket};
use std::collections::HashMap;
use std::convert::TryInto;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::time::{Duration, interval};

// Standard CAN constants (missing from socketcan crate)
const EFF_FLAG: u32 = 0x80000000; // Extended Frame Format flag

// Structure to store message signals and their current values
#[derive(Debug, Clone)]
struct MessageData {
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
        let id = if can_id & EFF_FLAG != 0 {
            let extended_id =
                ExtendedId::new(can_id & !EFF_FLAG).ok_or("Invalid extended CAN ID")?;
            Id::Extended(extended_id)
        } else {
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

                    let byte_mask = ((1u8 << bits_in_byte) - 1) << bit_offset;
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

                    let byte_mask = ((1u8 << bits_in_byte) - 1) << (8 - bit_offset - bits_in_byte);
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
    // Use default values: epc.dbc file and can0 interface
    let dbc_file = "epc.dbc";
    let can_interface = "can0";

    let mut socket_rx = CanSocket::open(can_interface).unwrap();

    // Read DBC file and create message data storage
    let mut f = File::open(dbc_file).await?;
    let mut buffer = Vec::new();
    f.read_to_end(&mut buffer).await?;
    let dbc = can_dbc::DBC::from_slice(&buffer).expect("Failed to parse DBC");

    // HashMap to store all message data by CAN ID
    let mut message_data: HashMap<u32, MessageData> = HashMap::new();

    // Initialize message data from DBC
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
                        if let Some(msg_data) = message_data.get_mut(&id) {
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
                for (can_id, msg_data) in &message_data {
                    println!("\nCAN ID: 0x{:08x}", can_id);
                    msg_data.print_all_signals();
                }
                println!("=======================================\n");

                // Demonstrate frame construction by modifying PhaseSequence
                if let Ok((can_id, frame)) = construct_frame_with_signal(
                    &mut message_data,
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
