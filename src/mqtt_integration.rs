use anyhow::Result;
use once_cell::sync::Lazy;
use rumqttc::v5::mqttbytes::QoS;
use rumqttc::v5::mqttbytes::v5::SubscribeProperties;
use rumqttc::v5::{AsyncClient, MqttOptions};
use serde::{Deserialize, Serialize};
use std::{env, sync::Arc};
use tokio::sync::Mutex;
use tokio::time::Duration;
use tracing::{error, warn};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalUpdatePayload {
    pub message_name: String,
    pub signal_name: String,
    pub new_value: String,
}

// Global signal update channel sender (will be set from main.rs)
static SIGNAL_UPDATE_SENDER: tokio::sync::OnceCell<
    tokio::sync::mpsc::UnboundedSender<SignalUpdatePayload>,
> = tokio::sync::OnceCell::const_new();

// Set the signal update sender (called from main.rs)
pub fn set_signal_update_sender(sender: tokio::sync::mpsc::UnboundedSender<SignalUpdatePayload>) {
    let _ = SIGNAL_UPDATE_SENDER.set(sender);
}

// Process signal update from JSON string (for MQTT integration)
async fn process_signal_update_payload(json_payload: &str) -> Result<()> {
    let payload: SignalUpdatePayload = serde_json::from_str(json_payload)
        .map_err(|e| anyhow::anyhow!("Failed to parse JSON payload: {}", e))?;

    println!(
        "Processing signal update: {} -> {} = {}",
        payload.message_name, payload.signal_name, payload.new_value
    );

    // Send the payload through the channel
    if let Some(sender) = SIGNAL_UPDATE_SENDER.get() {
        sender
            .send(payload)
            .map_err(|e| anyhow::anyhow!("Failed to send signal update payload: {}", e))?;
    } else {
        return Err(anyhow::anyhow!("Signal update channel not initialized"));
    }

    Ok(())
}

/// Lazy static initialization of the MQTT client using Arc for thread safety.
pub static MQTT_CLIENT: Lazy<Arc<Mutex<Option<AsyncClient>>>> =
    Lazy::new(|| Arc::new(Mutex::new(None)));

pub struct MqttIntegration;

impl MqttIntegration {
    pub async fn setup_mqtt_client() {
        const MAX_RETRIES: u32 = 5;
        const INITIAL_RETRY_DELAY_MS: u64 = 1000;
        const MAX_RETRY_DELAY_MS: u64 = 30000;

        println!("Setting up MQTT client...");

        loop {
            let mqtt_host = env::var("MQTT_HOST").unwrap_or_else(|_| "localhost".to_string());
            let mqtt_port = env::var("MQTT_PORT")
                .unwrap_or_else(|_| "1884".to_string())
                .parse::<u16>()
                .unwrap_or(1884);
            let mqtt_username =
                env::var("MQTT_USERNAME").unwrap_or_else(|_| "iot_platform".to_string());
            let mqtt_password = env::var("MQTT_PASSWORD").unwrap_or_else(|_| "123456".to_string());

            println!(
                "Connecting to MQTT broker at {}:{} with username: {}",
                mqtt_host, mqtt_port, mqtt_username
            );

            let mut mqtt_options = MqttOptions::new("new-automatic-client", mqtt_host, mqtt_port);
            mqtt_options.set_credentials(mqtt_username, mqtt_password);
            mqtt_options.set_keep_alive(Duration::from_secs(60));
            mqtt_options.set_clean_start(true);
            // Add automatic reconnect options
            mqtt_options.set_connection_timeout(10);
            mqtt_options.set_manual_acks(false);

            let (mqtt_client, mut mqtt_eventloop) = AsyncClient::new(mqtt_options, 30);
            *MQTT_CLIENT.lock().await = Some(mqtt_client.clone());

            println!("MQTT client created, setting up subscriptions...");
            tokio::spawn(async move {
                Self::setup_subscriptions(mqtt_client).await;
            });

            let mut retry_count = 0;
            let mut retry_delay = INITIAL_RETRY_DELAY_MS;

            loop {
                match mqtt_eventloop.poll().await {
                    Ok(rumqttc::v5::Event::Incoming(
                        rumqttc::v5::mqttbytes::v5::Packet::Publish(publish),
                    )) => {
                        let topic = String::from_utf8_lossy(&publish.topic).to_string();
                        let payload = String::from_utf8_lossy(&publish.payload).to_string();

                        // If topic is canbus/test then we get the payload
                        if topic == "canbus/test" {
                            println!("Received MQTT payload on topic {}: {}", topic, payload);

                            // For now, just print the payload - we'll integrate with CAN later
                            println!("Processing signal update payload: {}", payload);

                            // Parse and validate the JSON
                            match serde_json::from_str::<SignalUpdatePayload>(&payload) {
                                Ok(parsed_payload) => {
                                    println!(
                                        "Successfully parsed: message={}, signal={}, value={}",
                                        parsed_payload.message_name,
                                        parsed_payload.signal_name,
                                        parsed_payload.new_value
                                    );
                                }
                                Err(e) => {
                                    eprintln!("Failed to parse JSON payload: {}", e);
                                }
                            }
                        }

                        // Reset retry count on successful operation
                        retry_count = 0;
                        retry_delay = INITIAL_RETRY_DELAY_MS;
                    }
                    Ok(_) => {
                        // Reset retry count on any successful operation
                        retry_count = 0;
                        retry_delay = INITIAL_RETRY_DELAY_MS;
                    }
                    Err(e) => {
                        error!(error = %e, retry_count, "Error in MQTT event loop");

                        if retry_count >= MAX_RETRIES {
                            error!("Max retries reached, attempting full reconnection");
                            break; // Break inner loop to recreate client
                        }

                        retry_count += 1;
                        warn!(
                            retry_count,
                            retry_delay_ms = retry_delay,
                            "Attempting to reconnect to MQTT broker"
                        );

                        tokio::time::sleep(Duration::from_millis(retry_delay)).await;

                        // Exponential backoff with max delay
                        retry_delay = (retry_delay * 2).min(MAX_RETRY_DELAY_MS);
                    }
                }
            }

            // Wait before attempting full reconnection
            tokio::time::sleep(Duration::from_millis(MAX_RETRY_DELAY_MS)).await;
            warn!("Attempting to establish new MQTT connection");
        }
    }

    async fn setup_subscriptions(client: AsyncClient) {
        println!("Setting up MQTT subscriptions...");
        let props = SubscribeProperties {
            id: Some(1),
            user_properties: vec![],
        };
        let qos = QoS::ExactlyOnce;

        match client
            .subscribe_with_properties("#", qos, props.clone())
            .await
        {
            Ok(_) => println!("Successfully subscribed to MQTT topic '#'"),
            Err(e) => eprintln!("Failed to subscribe to MQTT topic: {}", e),
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
        println!("MQTT subscription setup complete");
    }
}
