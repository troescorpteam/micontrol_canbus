use crate::bus::{BusManager, BusState, RedisCommand};
use anyhow::Result;
use rumqttc::v5::mqttbytes::QoS;
use rumqttc::v5::mqttbytes::v5::{Packet, Publish, SubscribeProperties};
use rumqttc::v5::{AsyncClient, Event, MqttOptions};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::{env, sync::Arc};
use tokio::sync::Mutex;
use tokio::time::Duration;
use tracing::{debug, error, info, warn};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalUpdatePayload {
    #[serde(default)]
    pub message_name: Option<String>,
    #[serde(default)]
    pub signal_name: Option<String>,
    #[serde(default)]
    pub new_value: Option<String>,
    #[serde(default)]
    pub control_id: Option<String>,
    #[serde(default)]
    pub control: Option<serde_json::Value>,
    #[serde(default)]
    pub control_requested_time_utc: Option<String>,
}

pub struct MqttService {
    client: Arc<Mutex<Option<AsyncClient>>>,
}

impl MqttService {
    pub fn new() -> Self {
        Self {
            client: Arc::new(Mutex::new(None)),
        }
    }

    pub async fn run(self: Arc<Self>, bus_manager: Arc<BusManager>) {
        const MAX_RETRIES: u32 = 5;
        const INITIAL_RETRY_DELAY_MS: u64 = 1000;
        const MAX_RETRY_DELAY_MS: u64 = 30000;

        info!("Setting up MQTT client...");

        loop {
            let mqtt_host = env::var("MQTT_HOST").unwrap_or_else(|_| "localhost".to_string());
            let mqtt_port = env::var("MQTT_PORT")
                .unwrap_or_else(|_| "1884".to_string())
                .parse::<u16>()
                .unwrap_or(1884);
            let mqtt_username =
                env::var("MQTT_USERNAME").unwrap_or_else(|_| "iot_platform".to_string());
            let mqtt_password = env::var("MQTT_PASSWORD").unwrap_or_else(|_| "123456".to_string());

            info!(
                mqtt_host = %mqtt_host,
                mqtt_port = mqtt_port,
                mqtt_username = %mqtt_username,
                "Connecting to MQTT broker"
            );

            let mut mqtt_options = MqttOptions::new("micontrol-canbus", mqtt_host, mqtt_port);
            mqtt_options.set_credentials(mqtt_username, mqtt_password);
            mqtt_options.set_keep_alive(Duration::from_secs(60));
            mqtt_options.set_clean_start(true);
            mqtt_options.set_connection_timeout(10);
            mqtt_options.set_manual_acks(false);

            let (mqtt_client, mut mqtt_eventloop) = AsyncClient::new(mqtt_options, 30);
            {
                let mut guard = self.client.lock().await;
                *guard = Some(mqtt_client.clone());
            }

            let topics = bus_manager.all_topics().await;
            info!(topics = ?topics, "MQTT client created, setting up subscriptions...");
            let subscription_service = Arc::clone(&self);
            tokio::spawn(async move {
                subscription_service
                    .setup_subscriptions(mqtt_client, topics)
                    .await;
            });

            let mut retry_count = 0;
            let mut retry_delay = INITIAL_RETRY_DELAY_MS;

            loop {
                match mqtt_eventloop.poll().await {
                    Ok(Event::Incoming(Packet::Publish(publish))) => {
                        let manager = Arc::clone(&bus_manager);
                        if let Err(err) = self.process_publish(manager, publish).await {
                            error!(error = %err, "Failed to process MQTT publish packet");
                        }

                        retry_count = 0;
                        retry_delay = INITIAL_RETRY_DELAY_MS;
                    }
                    Ok(_) => {
                        retry_count = 0;
                        retry_delay = INITIAL_RETRY_DELAY_MS;
                    }
                    Err(e) => {
                        error!(error = %e, retry_count, "Error in MQTT event loop");

                        if retry_count >= MAX_RETRIES {
                            error!("Max retries reached, attempting full reconnection");
                            break;
                        }

                        retry_count += 1;
                        warn!(
                            retry_count,
                            retry_delay_ms = retry_delay,
                            "Attempting to reconnect to MQTT broker"
                        );

                        tokio::time::sleep(Duration::from_millis(retry_delay)).await;

                        retry_delay = (retry_delay * 2).min(MAX_RETRY_DELAY_MS);
                    }
                }
            }

            {
                let mut guard = self.client.lock().await;
                guard.take();
            }

            tokio::time::sleep(Duration::from_millis(MAX_RETRY_DELAY_MS)).await;
            warn!("Attempting to establish new MQTT connection");
        }
    }

    async fn setup_subscriptions(&self, client: AsyncClient, topics: Vec<String>) {
        if topics.is_empty() {
            warn!("No MQTT topics configured; MQTT integration will remain idle");
            return;
        }

        let props = SubscribeProperties {
            id: Some(1),
            user_properties: vec![],
        };
        let qos = QoS::ExactlyOnce;

        for topic in topics {
            match client
                .subscribe_with_properties(topic.clone(), qos, props.clone())
                .await
            {
                Ok(_) => info!(topic = %topic, "Subscribed to MQTT topic"),
                Err(e) => error!(topic = %topic, error = %e, "Failed to subscribe to MQTT topic"),
            }
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
        info!("MQTT subscription setup complete");
    }

    async fn process_publish(&self, bus_manager: Arc<BusManager>, publish: Publish) -> Result<()> {
        let topic = String::from_utf8_lossy(&publish.topic).to_string();
        let payload = String::from_utf8_lossy(&publish.payload).to_string();

        if let Some(bus) = bus_manager.bus_by_topic(&topic).await {
            debug!(topic = %topic, payload = %payload, "Received MQTT payload");
            let parsed = serde_json::from_str::<SignalUpdatePayload>(&payload)?;
            handle_payload(bus, parsed).await
        } else {
            debug!(topic = %topic, "Ignoring MQTT payload for unrecognised topic");
            Ok(())
        }
    }

    pub async fn publish_measurement(&self, topic: &str, payload: JsonValue) {
        let payload_string = payload.to_string();
        let client = {
            let guard = self.client.lock().await;
            guard.as_ref().cloned()
        };

        if let Some(client) = client {
            if let Err(err) = client
                .publish(
                    topic.to_string(),
                    QoS::AtLeastOnce,
                    false,
                    payload_string.clone(),
                )
                .await
            {
                warn!(topic = %topic, error = %err, "Failed to publish measurement payload to MQTT");
            } else {
                debug!(topic = %topic, payload = %payload_string, "Published measurement payload to MQTT");
            }
        } else {
            debug!(topic = %topic, "MQTT client not available for publishing measurement payload");
        }
    }
}

async fn process_can_signal_update(
    bus: Arc<BusState>,
    message_name: &str,
    signal_name: &str,
    new_value: f32,
) -> Result<()> {
    let (can_id, frame) = bus
        .construct_frame_with_signal(message_name, signal_name, new_value)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to construct CAN frame: {}", e))?;

    bus.enqueue_redis(RedisCommand {
        field_key: format!("{}.{}", message_name, signal_name),
        value: new_value as f64,
    });

    bus.tx_sender()
        .send(frame)
        .map_err(|e| anyhow::anyhow!("Failed to queue CAN frame: {}", e))?;

    info!(
        bus = %bus.controller(),
        message_name = %message_name,
        signal_name = %signal_name,
        can_id = can_id,
        "Enqueued CAN frame from MQTT command"
    );

    Ok(())
}

async fn handle_payload(bus: Arc<BusState>, payload: SignalUpdatePayload) -> Result<()> {
    if let (Some(message_name), Some(signal_name), Some(new_value)) =
        (payload.message_name, payload.signal_name, payload.new_value)
    {
        info!(
            bus = %bus.controller(),
            message_name = %message_name,
            signal_name = %signal_name,
            new_value = %new_value,
            "Parsed legacy MQTT payload"
        );

        let value = new_value
            .parse::<f32>()
            .map_err(|e| anyhow::anyhow!("Failed to parse new_value '{new_value}': {e}"))?;

        return process_can_signal_update(bus, &message_name, &signal_name, value).await;
    }

    if let Some(control) = payload.control {
        let control_map = control
            .as_object()
            .ok_or_else(|| anyhow::anyhow!("Expected 'control' to be a JSON object"))?;

        if control_map.is_empty() {
            anyhow::bail!("'control' object was empty");
        }

        let (message_signal, value) = control_map
            .iter()
            .next()
            .map(|(key, val)| {
                let value = val
                    .as_f64()
                    .or_else(|| val.as_i64().map(|v| v as f64))
                    .unwrap_or(0.0);
                (key.clone(), value as f32)
            })
            .ok_or_else(|| anyhow::anyhow!("Failed to extract control payload"))?;

        let mut parts = message_signal.splitn(2, '.');
        let message = parts
            .next()
            .ok_or_else(|| anyhow::anyhow!("Control key missing message prefix (expected 'MessageName.SignalName')"))?;
        let signal = parts
            .next()
            .ok_or_else(|| anyhow::anyhow!("Control key missing signal suffix (expected 'MessageName.SignalName')"))?;

        info!(
            bus = %bus.controller(),
            control_id = %payload.control_id.unwrap_or_default(),
            message = %message,
            signal = %signal,
            value = value,
            "Parsed control command"
        );

        return process_can_signal_update(bus, message, signal, value).await;
    }

    anyhow::bail!("Unsupported MQTT payload format")
}
