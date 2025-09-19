use bitvec::prelude::{Lsb0, Msb0};
use bitvec::view::BitView;
use can_dbc::{ByteOrder, MultiplexIndicator, Signal, ValueType};
use lazy_static::lazy_static;
use serde::Deserialize;
use serde::Serialize;
use socketcan::CanFrame;
use socketcan::EmbeddedFrame;
use socketcan::ExtendedId;
use socketcan::Id;
use socketcan::StandardId;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::sync::mpsc;
use tracing::{debug, info};

// Standard CAN constants (missing from socketcan crate)
const EFF_FLAG: u32 = 0x80000000; // Extended Frame Format flag

// Structure to store message signals and their current values
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageData {
    pub name: String,
    #[serde(skip)] // Skip Signal as it may not be serializable
    pub signals: Vec<Signal>,
    pub signal_values: HashMap<String, f32>,
    pub default_signal_values: HashMap<String, f32>,
    pub dlc: u8,
    pub is_extended: bool,
}

impl MessageData {
    pub fn new(name: String, signals: Vec<Signal>, dlc: u8, is_extended: bool) -> Self {
        let mut signal_values = HashMap::new();
        let mut default_signal_values = HashMap::new();

        for signal in &signals {
            let default_value = *signal.offset() as f32;
            signal_values.insert(signal.name().clone(), default_value);
            default_signal_values.insert(signal.name().clone(), default_value);
        }

        Self {
            name,
            signals,
            signal_values,
            default_signal_values,
            dlc,
            is_extended,
        }
    }

    pub fn update_from_frame(&mut self, frame: &CanFrame) -> Vec<(String, f32)> {
        let mut changed_signals = Vec::new();

        for signal in &self.signals {
            if let Some(calculated_value) = decode_signal(signal, frame.data()) {
                let calculated_value = calculated_value as f32;
                let current_value = self
                    .signal_values
                    .get(signal.name())
                    .copied()
                    .unwrap_or_default();

                if (calculated_value - current_value).abs() > f32::EPSILON {
                    changed_signals.push((signal.name().clone(), calculated_value));
                    self.signal_values
                        .insert(signal.name().clone(), calculated_value);
                }
            }
        }

        changed_signals
    }

    pub fn get_signal_value(&self, signal_name: &str) -> Option<f32> {
        self.signal_values.get(signal_name).copied()
    }

    #[allow(dead_code)]
    fn print_all_signals(&self) {
        info!(message_name = %self.name, "Message signals");
        for (signal_name, value) in &self.signal_values {
            info!(
                signal_name = %signal_name,
                signal_value = %format!("{:.4}", value),
                "Signal value"
            );
        }
    }

    pub fn set_signal_value(&mut self, signal_name: &str, value: f32) -> Result<(), String> {
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

    pub fn construct_frame(&self, can_id: u32) -> Result<CanFrame, String> {
        let mut frame_data = vec![0u8; self.effective_dlc()];
        let mux_values = self.active_multiplex_values();

        for signal in &self.signals {
            if !self.should_encode_signal(signal, &mux_values) {
                continue;
            }

            let signal_value = self
                .signal_values
                .get(signal.name())
                .ok_or_else(|| format!("Signal '{}' value not found", signal.name()))?;

            encode_signal(signal, *signal_value as f64, &mut frame_data)
                .map_err(|e| format!("Failed to encode signal '{}': {}", signal.name(), e))?;
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

    fn effective_dlc(&self) -> usize {
        let required = self
            .signals
            .iter()
            .map(required_bytes_for_signal)
            .max()
            .unwrap_or(0);

        let declared = self.dlc as usize;
        let chosen = declared.max(required);

        chosen.clamp(1, 8)
    }

    // Determine if a signal should be encoded based on the current MUX value
    fn should_encode_signal(&self, signal: &Signal, mux_values: &HashMap<String, u64>) -> bool {
        match signal.multiplexer_indicator() {
            MultiplexIndicator::Plain => true,
            MultiplexIndicator::Multiplexor => true,
            MultiplexIndicator::MultiplexedSignal(expected) => {
                self.mux_matches(*expected, mux_values)
            }
            MultiplexIndicator::MultiplexorAndMultiplexedSignal(expected) => {
                mux_values.contains_key(signal.name()) || self.mux_matches(*expected, mux_values)
            }
        }
    }

    fn active_multiplex_values(&self) -> HashMap<String, u64> {
        let mut values = HashMap::new();

        for signal in &self.signals {
            match signal.multiplexer_indicator() {
                MultiplexIndicator::Multiplexor
                | MultiplexIndicator::MultiplexorAndMultiplexedSignal(_) => {
                    if let Some(value) = self.signal_values.get(signal.name()) {
                        let bit_length = *signal.signal_size() as usize;
                        if let Ok(raw) = convert_physical_to_raw(signal, *value as f64, bit_length)
                        {
                            values.insert(signal.name().clone(), raw);
                        }
                    }
                }
                _ => {}
            }
        }

        values
    }

    fn mux_matches(&self, expected: u64, mux_values: &HashMap<String, u64>) -> bool {
        if mux_values.is_empty() {
            expected == 0
        } else {
            mux_values.values().any(|current| *current == expected)
        }
    }
}

// Global CAN transmission channel
pub static CAN_TX_SENDER: tokio::sync::OnceCell<mpsc::UnboundedSender<CanFrame>> =
    tokio::sync::OnceCell::const_new();

// Global message data storage using lazy_static
lazy_static! {
    pub static ref GLOBAL_MESSAGE_DATA: Arc<RwLock<HashMap<u32, MessageData>>> =
        Arc::new(RwLock::new(HashMap::new()));
}

fn decode_signal(signal: &Signal, data: &[u8]) -> Option<f64> {
    let bit_length = *signal.signal_size() as usize;
    if bit_length == 0 {
        return None;
    }

    match signal.byte_order() {
        ByteOrder::LittleEndian => {
            let bits = data.view_bits::<Lsb0>();
            let start = *signal.start_bit() as usize;
            let end = start.checked_add(bit_length)?;
            if end > bits.len() {
                return None;
            }

            let mut raw = 0u64;
            for (idx, bit_index) in (start..end).enumerate() {
                if bits[bit_index] {
                    raw |= 1u64 << idx;
                }
            }

            Some(apply_signal_scaling(signal, raw))
        }
        ByteOrder::BigEndian => {
            let bits = data.view_bits::<Msb0>();
            let start = motorola_start_bit_index(*signal.start_bit() as usize);
            let end = start.checked_add(bit_length)?;
            if end > bits.len() {
                return None;
            }

            let mut raw = 0u64;
            for bit_index in start..end {
                raw <<= 1;
                if bits[bit_index] {
                    raw |= 1;
                }
            }

            Some(apply_signal_scaling(signal, raw))
        }
    }
}

fn encode_signal(signal: &Signal, value: f64, frame_data: &mut [u8]) -> Result<(), &'static str> {
    let bit_length = *signal.signal_size() as usize;
    if bit_length == 0 {
        return Ok(());
    }
    if bit_length > 64 {
        return Err("unsupported signal size");
    }

    if value.is_nan() {
        return Err("value is NaN");
    }

    let raw = convert_physical_to_raw(signal, value, bit_length)?;

    match signal.byte_order() {
        ByteOrder::LittleEndian => {
            let bits = frame_data.view_bits_mut::<Lsb0>();
            let start = *signal.start_bit() as usize;
            let end = start
                .checked_add(bit_length)
                .ok_or("signal exceeds limits")?;
            if end > bits.len() {
                return Err("signal exceeds frame size");
            }

            for (idx, bit_index) in (start..end).enumerate() {
                let bit = ((raw >> idx) & 1) == 1;
                bits.set(bit_index, bit);
            }
        }
        ByteOrder::BigEndian => {
            let bits = frame_data.view_bits_mut::<Msb0>();
            let start = motorola_start_bit_index(*signal.start_bit() as usize);
            let end = start
                .checked_add(bit_length)
                .ok_or("signal exceeds limits")?;
            if end > bits.len() {
                return Err("signal exceeds frame size");
            }

            for (idx, bit_index) in (start..end).enumerate() {
                let shift = bit_length - 1 - idx;
                let bit = ((raw >> shift) & 1) == 1;
                bits.set(bit_index, bit);
            }
        }
    }

    Ok(())
}

fn apply_signal_scaling(signal: &Signal, raw: u64) -> f64 {
    let bit_length = *signal.signal_size() as usize;

    let value = if matches!(signal.value_type(), ValueType::Signed) {
        let signed = sign_extend(raw, bit_length);
        signed as f64
    } else {
        raw as f64
    };

    value * *signal.factor() + *signal.offset()
}

fn convert_physical_to_raw(
    signal: &Signal,
    value: f64,
    bit_length: usize,
) -> Result<u64, &'static str> {
    if bit_length == 0 {
        return Ok(0);
    }

    let factor = *signal.factor();
    let offset = *signal.offset();

    let raw_unbounded = if factor.abs() < f64::EPSILON {
        0.0
    } else {
        (value - offset) / factor
    };

    if matches!(signal.value_type(), ValueType::Signed) {
        let max = if bit_length == 64 {
            i64::MAX as i128
        } else {
            (1i128 << (bit_length - 1)) - 1
        };
        let min = if bit_length == 64 {
            i64::MIN as i128
        } else {
            -(1i128 << (bit_length - 1))
        };

        let mut raw_i = raw_unbounded.round() as i128;
        raw_i = raw_i.clamp(min, max);

        let raw_i64 = raw_i as i64;
        Ok(if bit_length == 64 {
            raw_i64 as u64
        } else {
            let mask = (1u64 << bit_length) - 1;
            (raw_i64 as u64) & mask
        })
    } else {
        let mut raw_u = raw_unbounded.round();
        if raw_u.is_sign_negative() {
            raw_u = 0.0;
        }

        let max = if bit_length == 64 {
            u64::MAX as f64
        } else {
            ((1u128 << bit_length) - 1) as f64
        };
        if raw_u > max {
            raw_u = max;
        }

        Ok(raw_u as u64)
    }
}

fn required_bytes_for_signal(signal: &Signal) -> usize {
    let bits = match signal.byte_order() {
        ByteOrder::LittleEndian => *signal.start_bit() as usize + *signal.signal_size() as usize,
        ByteOrder::BigEndian => {
            let start = motorola_start_bit_index(*signal.start_bit() as usize);
            start + *signal.signal_size() as usize
        }
    };

    if bits == 0 {
        1
    } else {
        ((bits + 7) / 8).clamp(1, 8)
    }
}

fn motorola_start_bit_index(start_bit: usize) -> usize {
    let byte_index = start_bit / 8;
    let bit_in_byte = start_bit % 8;
    byte_index * 8 + (7 - bit_in_byte)
}

fn sign_extend(value: u64, bits: usize) -> i64 {
    if bits == 0 {
        return 0;
    }

    if bits >= 64 {
        value as i64
    } else {
        let shift = 64 - bits;
        ((value << shift) as i64) >> shift
    }
}
