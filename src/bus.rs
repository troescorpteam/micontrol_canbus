use crate::message_type::MessageData;
use can_dbc::MultiplexIndicator;
use socketcan::CanFrame;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{RwLock, mpsc};
use tracing::{info, warn};

#[derive(Clone, Debug)]
pub struct RedisCommand {
    pub field_key: String,
    pub value: f64,
}

pub struct BusState {
    pub id: String,
    pub controller: String,
    pub interface: String,
    pub redis_hash: String,
    pub mqtt_topic: String,
    pub hardware_type: Option<String>,
    pub hardware_id: Option<String>,
    message_data: Arc<RwLock<HashMap<u32, MessageData>>>,
    message_index: Arc<HashMap<String, u32>>,
    tx_sender: mpsc::UnboundedSender<CanFrame>,
    redis_sender: Option<mpsc::UnboundedSender<RedisCommand>>,
}

impl BusState {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: String,
        controller: String,
        interface: String,
        redis_hash: String,
        mqtt_topic: String,
        hardware_type: Option<String>,
        hardware_id: Option<String>,
        message_data: Arc<RwLock<HashMap<u32, MessageData>>>,
        message_index: Arc<HashMap<String, u32>>,
        tx_sender: mpsc::UnboundedSender<CanFrame>,
        redis_sender: Option<mpsc::UnboundedSender<RedisCommand>>,
    ) -> Self {
        Self {
            id,
            controller,
            interface,
            redis_hash,
            mqtt_topic,
            hardware_type,
            hardware_id,
            message_data,
            message_index,
            tx_sender,
            redis_sender,
        }
    }

    pub fn redis_hash(&self) -> &str {
        &self.redis_hash
    }

    pub fn mqtt_topic(&self) -> &str {
        &self.mqtt_topic
    }

    pub fn controller(&self) -> &str {
        &self.controller
    }

    pub fn interface(&self) -> &str {
        &self.interface
    }

    #[allow(dead_code)]
    pub fn hardware_type(&self) -> Option<&str> {
        self.hardware_type.as_deref()
    }

    #[allow(dead_code)]
    pub fn hardware_id(&self) -> Option<&str> {
        self.hardware_id.as_deref()
    }

    #[allow(dead_code)]
    pub fn message_data(&self) -> Arc<RwLock<HashMap<u32, MessageData>>> {
        Arc::clone(&self.message_data)
    }

    pub fn enqueue_redis(&self, command: RedisCommand) {
        if let Some(sender) = &self.redis_sender {
            let _ = sender.send(command);
        }
    }

    pub fn tx_sender(&self) -> mpsc::UnboundedSender<CanFrame> {
        self.tx_sender.clone()
    }

    pub async fn construct_frame_with_signal(
        &self,
        message_name: &str,
        signal_name: &str,
        new_value: f32,
    ) -> Result<(u32, CanFrame), String> {
        let message_id = self
            .message_index
            .get(message_name)
            .ok_or_else(|| format!("Message '{}' not found", message_name))?;

        let mut message_data = self.message_data.write().await;
        let msg_data = message_data
            .get_mut(message_id)
            .ok_or_else(|| format!("Message '{}' not found", message_name))?;

        let indicator = msg_data
            .signals
            .iter()
            .find(|signal| signal.name() == signal_name)
            .map(|signal| *signal.multiplexer_indicator())
            .ok_or_else(|| {
                format!(
                    "Signal '{}' not found in message '{}'",
                    signal_name, message_name
                )
            })?;

        let mux_adjustment = match indicator {
            MultiplexIndicator::MultiplexedSignal(expected)
            | MultiplexIndicator::MultiplexorAndMultiplexedSignal(expected) => msg_data
                .signals
                .iter()
                .find(|candidate| {
                    matches!(
                        candidate.multiplexer_indicator(),
                        MultiplexIndicator::Multiplexor
                            | MultiplexIndicator::MultiplexorAndMultiplexedSignal(_)
                    )
                })
                .map(|mux_signal| {
                    let factor = *mux_signal.factor();
                    let offset = *mux_signal.offset();
                    let physical = (expected as f64) * factor + offset;
                    (mux_signal.name().clone(), physical as f32, expected)
                }),
            _ => None,
        };

        msg_data.set_signal_value(signal_name, new_value)?;

        match (indicator, mux_adjustment) {
            (
                MultiplexIndicator::MultiplexedSignal(expected)
                | MultiplexIndicator::MultiplexorAndMultiplexedSignal(expected),
                Some((mux_name, mux_value, resolved_expected)),
            ) => {
                info!(
                    message = %message_name,
                    signal = %signal_name,
                    value = new_value,
                    multiplexer_signal = %mux_name,
                    multiplexer_value = %mux_value,
                    expected_mux = expected,
                    resolved_expected = resolved_expected,
                    "Applied multiplexer adjustment for CAN signal update"
                );
                msg_data.signal_values.insert(mux_name, mux_value);
            }
            (
                MultiplexIndicator::MultiplexedSignal(expected)
                | MultiplexIndicator::MultiplexorAndMultiplexedSignal(expected),
                None,
            ) => {
                warn!(
                    message = %message_name,
                    signal = %signal_name,
                    value = new_value,
                    expected_mux = expected,
                    "Unable to locate multiplexer signal for multiplexed update"
                );
            }
            (MultiplexIndicator::Multiplexor, _) => {
                info!(
                    message = %message_name,
                    signal = %signal_name,
                    value = new_value,
                    "Prepared multiplexer signal update"
                );
            }
            (MultiplexIndicator::Plain, _) => {
                info!(
                    message = %message_name,
                    signal = %signal_name,
                    value = new_value,
                    "Prepared CAN signal update"
                );
            }
        }

        let frame = msg_data.construct_frame(*message_id)?;

        Ok((*message_id, frame))
    }
}

pub struct BusManager {
    buses: RwLock<HashMap<String, Arc<BusState>>>,
    topics: RwLock<HashMap<String, String>>, // topic -> bus id
}

impl BusManager {
    pub fn new() -> Self {
        Self {
            buses: RwLock::new(HashMap::new()),
            topics: RwLock::new(HashMap::new()),
        }
    }

    pub async fn insert(&self, bus: Arc<BusState>) {
        let mut buses = self.buses.write().await;
        let mut topics = self.topics.write().await;
        topics.insert(bus.mqtt_topic.clone(), bus.id.clone());
        buses.insert(bus.id.clone(), bus);
    }

    #[allow(dead_code)]
    pub async fn bus_by_id(&self, id: &str) -> Option<Arc<BusState>> {
        let buses = self.buses.read().await;
        buses.get(id).cloned()
    }

    pub async fn bus_by_topic(&self, topic: &str) -> Option<Arc<BusState>> {
        let topics = self.topics.read().await;
        let buses = self.buses.read().await;
        topics
            .get(topic)
            .and_then(|bus_id| buses.get(bus_id))
            .cloned()
    }

    #[allow(dead_code)]
    pub async fn all_buses(&self) -> Vec<Arc<BusState>> {
        let buses = self.buses.read().await;
        buses.values().cloned().collect()
    }

    pub async fn all_topics(&self) -> Vec<String> {
        let topics = self.topics.read().await;
        topics.keys().cloned().collect()
    }
}
