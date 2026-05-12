use bitvec::prelude::{Lsb0, Msb0};
use bitvec::view::BitView;
use can_dbc::{ByteOrder, MultiplexIndicator, Signal, ValueType};
use serde::Deserialize;
use serde::Serialize;
use socketcan::CanFrame;
use socketcan::EmbeddedFrame;
use socketcan::ExtendedId;
use socketcan::Id;
use socketcan::StandardId;
use std::collections::HashMap;
use tracing::info;

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
            let default_value = signal.offset as f32;
            signal_values.insert(signal.name.clone(), default_value);
            default_signal_values.insert(signal.name.clone(), default_value);
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
                    .get(&signal.name)
                    .copied()
                    .unwrap_or_default();

                if (calculated_value - current_value).abs() > f32::EPSILON {
                    changed_signals.push((signal.name.clone(), calculated_value));
                    self.signal_values
                        .insert(signal.name.clone(), calculated_value);
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
                .get(&signal.name)
                .ok_or_else(|| format!("Signal '{}' value not found", signal.name))?;

            encode_signal(signal, *signal_value as f64, &mut frame_data)
                .map_err(|e| format!("Failed to encode signal '{}': {}", signal.name, e))?;
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
        match signal.multiplexer_indicator {
            MultiplexIndicator::Plain => true,
            MultiplexIndicator::Multiplexor => true,
            MultiplexIndicator::MultiplexedSignal(expected) => {
                self.mux_matches(expected, mux_values)
            }
            MultiplexIndicator::MultiplexorAndMultiplexedSignal(expected) => {
                mux_values.contains_key(&signal.name) || self.mux_matches(expected, mux_values)
            }
        }
    }

    fn active_multiplex_values(&self) -> HashMap<String, u64> {
        let mut values = HashMap::new();

        for signal in &self.signals {
            match signal.multiplexer_indicator {
                MultiplexIndicator::Multiplexor
                | MultiplexIndicator::MultiplexorAndMultiplexedSignal(_) => {
                    if let Some(value) = self.signal_values.get(&signal.name) {
                        let bit_length = signal.size as usize;
                        if let Ok(raw) = convert_physical_to_raw(signal, *value as f64, bit_length)
                        {
                            values.insert(signal.name.clone(), raw);
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

fn decode_signal(signal: &Signal, data: &[u8]) -> Option<f64> {
    let bit_length = signal.size as usize;
    if bit_length == 0 {
        return None;
    }

    match signal.byte_order {
        ByteOrder::LittleEndian => {
            let bits = data.view_bits::<Lsb0>();
            let start = signal.start_bit as usize;
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
            let start = motorola_start_bit_index(signal.start_bit as usize);
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
    let bit_length = signal.size as usize;
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

    match signal.byte_order {
        ByteOrder::LittleEndian => {
            let bits = frame_data.view_bits_mut::<Lsb0>();
            let start = signal.start_bit as usize;
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
            let start = motorola_start_bit_index(signal.start_bit as usize);
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
    let bit_length = signal.size as usize;

    let value = if matches!(signal.value_type, ValueType::Signed) {
        let signed = sign_extend(raw, bit_length);
        signed as f64
    } else {
        raw as f64
    };

    value * signal.factor + signal.offset
}

fn convert_physical_to_raw(
    signal: &Signal,
    value: f64,
    bit_length: usize,
) -> Result<u64, &'static str> {
    if bit_length == 0 {
        return Ok(0);
    }

    let factor = signal.factor;
    let offset = signal.offset;

    let raw_unbounded = if factor.abs() < f64::EPSILON {
        0.0
    } else {
        (value - offset) / factor
    };

    if matches!(signal.value_type, ValueType::Signed) {
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
    let bits = match signal.byte_order {
        ByteOrder::LittleEndian => signal.start_bit as usize + signal.size as usize,
        ByteOrder::BigEndian => {
            let start = motorola_start_bit_index(signal.start_bit as usize);
            start + signal.size as usize
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

#[cfg(test)]
mod tests {
    use super::*;
    use can_dbc::{Dbc, MessageId};

    const DCDC_DBC: &str = include_str!("../dcdc.dbc");

    fn build_signal(
        name: &str,
        start_bit: u64,
        size: u64,
        byte_order: ByteOrder,
        multiplexer_indicator: MultiplexIndicator,
        factor: f64,
        offset: f64,
    ) -> Signal {
        Signal {
            name: name.to_string(),
            multiplexer_indicator,
            start_bit,
            size,
            byte_order,
            value_type: ValueType::Unsigned,
            factor,
            offset,
            min: 0.0,
            max: 0.0,
            unit: "".to_string(),
            receivers: vec![],
        }
    }

    fn dcdc_message(name: &str) -> (u32, MessageData) {
        let dbc = Dbc::try_from(DCDC_DBC).expect("DCDC DBC should parse");
        let message = dbc
            .messages
            .iter()
            .find(|message| message.name == name)
            .unwrap_or_else(|| panic!("message '{name}' should exist in DCDC DBC"));

        let raw_id = match message.id {
            MessageId::Standard(id) => id.into(),
            MessageId::Extended(id) => id,
        };
        let id = raw_id & !EFF_FLAG;
        let data = MessageData::new(
            message.name.clone(),
            message.signals.clone(),
            message.size as u8,
            matches!(message.id, MessageId::Extended(_)),
        );

        (id, data)
    }

    fn assert_dcdc_frame(
        message_name: &str,
        signal_values: &[(&str, f32)],
        expected_id: u32,
        expected_data: &[u8],
    ) {
        let (id, mut message) = dcdc_message(message_name);
        assert_eq!(id, expected_id, "unexpected CAN ID for {message_name}");

        for (signal, value) in signal_values {
            message
                .set_signal_value(signal, *value)
                .unwrap_or_else(|err| panic!("failed to set {message_name}.{signal}: {err}"));
        }

        let frame = message
            .construct_frame(id)
            .unwrap_or_else(|err| panic!("failed to construct {message_name}: {err}"));

        let actual_id = match frame.id() {
            Id::Standard(id) => id.as_raw() as u32,
            Id::Extended(id) => id.as_raw(),
        };

        assert_eq!(
            actual_id, expected_id,
            "unexpected frame ID for {message_name}"
        );
        assert!(
            matches!(frame.id(), Id::Extended(_)),
            "{message_name} should use an extended CAN ID"
        );
        assert_eq!(
            frame.data().len(),
            expected_data.len(),
            "unexpected DLC for {message_name}"
        );
        assert_eq!(
            frame.data(),
            expected_data,
            "unexpected raw bytes for {message_name}"
        );
    }

    #[test]
    fn encode_decode_roundtrip_little_endian() {
        let signal = build_signal(
            "Speed",
            0,
            16,
            ByteOrder::LittleEndian,
            MultiplexIndicator::Plain,
            0.1,
            0.0,
        );

        let expected_value = 12.3_f32;
        let mut encoder = MessageData::new("Msg".into(), vec![signal.clone()], 2, false);
        encoder
            .set_signal_value("Speed", expected_value)
            .expect("signal exists");

        let frame = encoder.construct_frame(0x123).expect("frame created");

        let mut decoder = MessageData::new("Msg".into(), vec![signal], 2, false);
        let changes = decoder.update_from_frame(&frame);

        let decoded = decoder.get_signal_value("Speed").unwrap();
        assert!(
            (decoded - expected_value).abs() < 0.001,
            "expected {expected_value}, got {decoded}"
        );
        assert_eq!(
            changes,
            vec![("Speed".to_string(), decoded)],
            "expected change was reported"
        );
    }

    #[test]
    fn encode_decode_roundtrip_big_endian() {
        let signal = build_signal(
            "Speed",
            0,
            16,
            ByteOrder::BigEndian,
            MultiplexIndicator::Plain,
            0.1,
            0.0,
        );

        let expected_value = 45.6_f32;
        let mut encoder = MessageData::new("Msg".into(), vec![signal.clone()], 2, false);
        encoder
            .set_signal_value("Speed", expected_value)
            .expect("signal exists");

        let frame = encoder.construct_frame(0x123).expect("frame created");

        let mut decoder = MessageData::new("Msg".into(), vec![signal], 2, false);
        let changes = decoder.update_from_frame(&frame);

        let decoded = decoder.get_signal_value("Speed").unwrap();
        assert!(
            (decoded - expected_value).abs() < 0.001,
            "expected {expected_value}, got {decoded}"
        );
        assert_eq!(
            changes,
            vec![("Speed".to_string(), decoded)],
            "expected change was reported"
        );
    }

    #[test]
    fn multiplexed_signal_respects_mux_value() {
        let mux_signal = build_signal(
            "Mux",
            0,
            4,
            ByteOrder::LittleEndian,
            MultiplexIndicator::Multiplexor,
            1.0,
            0.0,
        );
        let muxed_one = build_signal(
            "SignalA",
            8,
            8,
            ByteOrder::LittleEndian,
            MultiplexIndicator::MultiplexedSignal(1),
            1.0,
            0.0,
        );
        let muxed_two = build_signal(
            "SignalB",
            16,
            8,
            ByteOrder::LittleEndian,
            MultiplexIndicator::MultiplexedSignal(2),
            1.0,
            0.0,
        );

        let mut encoder = MessageData::new(
            "MuxedMsg".into(),
            vec![mux_signal.clone(), muxed_one.clone(), muxed_two.clone()],
            3,
            false,
        );
        encoder
            .set_signal_value("Mux", 1.0)
            .expect("mux signal exists");
        encoder
            .set_signal_value("SignalA", 99.0)
            .expect("muxed signal exists");
        encoder
            .set_signal_value("SignalB", 55.0)
            .expect("muxed signal exists");

        let frame = encoder.construct_frame(0x1FF).expect("frame created");

        let mut decoder = MessageData::new(
            "MuxedMsg".into(),
            vec![mux_signal, muxed_one, muxed_two],
            3,
            false,
        );
        let changes = decoder.update_from_frame(&frame);

        let mux_value = decoder.get_signal_value("Mux").unwrap();
        let a_value = decoder.get_signal_value("SignalA").unwrap();
        let b_value = decoder.get_signal_value("SignalB").unwrap();

        assert_eq!(mux_value, 1.0);
        assert_eq!(a_value, 99.0);
        assert_eq!(
            b_value, 0.0,
            "SignalB should be ignored for mux value 1 and stay at default"
        );

        assert!(
            changes.iter().any(|(name, _)| name == "SignalA"),
            "SignalA should report a change"
        );
        assert!(
            changes.iter().all(|(name, _)| name != "SignalB"),
            "SignalB should not be reported as changed"
        );
    }

    #[test]
    fn dcdc_startup_frames_match_busmaster_config_bytes() {
        let cases: &[(&str, &[(&str, f32)], u32, &[u8])] = &[
            (
                "EPC_meas_config",
                &[
                    ("Enable_FB01_FB02_msg", 1.0),
                    ("FB01_FB02_msg_period", 50.0),
                ],
                0xEB02,
                &[0x01, 0x32, 0x00],
            ),
            (
                "EPC_control",
                &[
                    ("Enable", 0.0),
                    ("Power_direction", 0.0),
                    ("Current_ref_HS", 0.0),
                    ("Current_ref_LS", 0.0),
                ],
                0xEB00,
                &[0x00, 0x00, 0x00, 0x00, 0x00],
            ),
            (
                "EPC_LSVCMode_control",
                &[
                    ("LS_voltage_reference", 48.0),
                    ("HVDC_max_voltage", 700.0),
                    ("HVDC_min_voltage", 535.0),
                    ("HVDC_max_volt_hysteresis", 5.0),
                    ("HVDC_min_volt_hysteresis", 5.0),
                ],
                0xEB06,
                &[0xC0, 0x12, 0x58, 0x1B, 0xE6, 0x14, 0x05, 0x05],
            ),
            (
                "EPC_ext_config",
                &[
                    ("Extended_mode", 2.0),
                    ("Power_flow_direction", 1.0),
                    ("CAN_net_check_mode", 0.0),
                ],
                0xEB03,
                &[0x42, 0x00],
            ),
            (
                "EPC_currents_lims_config",
                &[
                    ("HVDC_charge_current_lim", 120.0),
                    ("HVDC_discharge_current_lim", 120.0),
                    ("LVDC_charge_current_lim", 120.0),
                    ("LVDC_discharge_current_lim", 120.0),
                ],
                0xEB04,
                &[0xE0, 0x2E, 0xE0, 0x2E, 0xB0, 0x04, 0xB0, 0x04],
            ),
            (
                "EPC_config",
                &[
                    ("Mode", 3.0),
                    ("HS_max_voltage", 700.0),
                    ("HS_min_voltage", 530.0),
                    ("LS_max_voltage", 55.0),
                    ("LS_min_voltage", 0.0),
                    ("Charge_power_limit", 5500.0),
                    ("Discharge_power_limit", 10.0),
                ],
                0xEB01,
                &[0xF3, 0x2A, 0xA1, 0x89, 0x00, 0x98, 0x18, 0x00],
            ),
            (
                "EPC_info_request",
                &[("EPC_info_request", 0.0)],
                0xEF0E,
                &[0x00],
            ),
        ];

        for (message_name, signal_values, expected_id, expected_data) in cases {
            assert_dcdc_frame(message_name, signal_values, *expected_id, expected_data);
        }
    }
}
