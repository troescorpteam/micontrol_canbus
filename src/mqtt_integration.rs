use crate::message_type::CAN_TX_SENDER;
use crate::message_type::GLOBAL_MESSAGE_DATA;
use anyhow::Result;
use once_cell::sync::Lazy;
use rumqttc::v5::mqttbytes::QoS;
use rumqttc::v5::mqttbytes::v5::SubscribeProperties;
use rumqttc::v5::{AsyncClient, MqttOptions};
use serde::{Deserialize, Serialize};
use serde_json;
use socketcan::CanFrame;
use std::{env, sync::Arc};
use tokio::sync::Mutex;
use tokio::time::Duration;
use tracing::{debug, error, info, warn};

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
    match SIGNAL_UPDATE_SENDER.set(sender) {
        Ok(_) => {
            info!("Signal update sender initialized successfully");
            // Test if we can access it immediately
            if SIGNAL_UPDATE_SENDER.get().is_some() {
                debug!("Signal update sender verification: OK");
            } else {
                error!("Signal update sender verification: FAILED");
            }
        }
        Err(_) => warn!("Signal update sender was already initialized"),
    }
}

// Process signal update from JSON string (for MQTT integration)
#[allow(dead_code)]
async fn process_signal_update_payload(json_payload: &str) -> Result<()> {
    let payload: SignalUpdatePayload = serde_json::from_str(json_payload)
        .map_err(|e| anyhow::anyhow!("Failed to parse JSON payload: {}", e))?;

    info!(
        message_name = %payload.message_name,
        signal_name = %payload.signal_name,
        new_value = %payload.new_value,
        "Processing signal update"
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

            let mut mqtt_options = MqttOptions::new("new-automatic-client", mqtt_host, mqtt_port);
            mqtt_options.set_credentials(mqtt_username, mqtt_password);
            mqtt_options.set_keep_alive(Duration::from_secs(60));
            mqtt_options.set_clean_start(true);
            // Add automatic reconnect options
            mqtt_options.set_connection_timeout(10);
            mqtt_options.set_manual_acks(false);

            let (mqtt_client, mut mqtt_eventloop) = AsyncClient::new(mqtt_options, 30);
            *MQTT_CLIENT.lock().await = Some(mqtt_client.clone());

            info!("MQTT client created, setting up subscriptions...");
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
                            debug!(
                                topic = %topic,
                                payload = %payload,
                                "Received MQTT payload"
                            );

                            // For now, just print the payload - we'll integrate with CAN later
                            debug!(payload = %payload, "Processing signal update payload");

                            // Parse and validate the JSON
                            match serde_json::from_str::<SignalUpdatePayload>(&payload) {
                                Ok(parsed_payload) => {
                                    info!(
                                        message_name = %parsed_payload.message_name,
                                        signal_name = %parsed_payload.signal_name,
                                        new_value = %parsed_payload.new_value,
                                        "Successfully parsed MQTT payload"
                                    );

                                    // Parse the new value as float and process directly
                                    match parsed_payload.new_value.parse::<f32>() {
                                        Ok(new_value) => {
                                            info!(
                                                message_name = %parsed_payload.message_name,
                                                signal_name = %parsed_payload.signal_name,
                                                new_value = new_value,
                                                "Processing CAN signal update"
                                            );

                                            // Call the main processing function directly
                                            // We'll implement this next
                                            if let Err(e) = process_can_signal_update(
                                                &parsed_payload.message_name,
                                                &parsed_payload.signal_name,
                                                new_value,
                                            )
                                            .await
                                            {
                                                error!(
                                                    error = %e,
                                                    "Failed to process CAN signal update"
                                                );
                                            }
                                        }
                                        Err(e) => {
                                            error!(
                                                value = %parsed_payload.new_value,
                                                error = %e,
                                                "Failed to parse value as float"
                                            );
                                        }
                                    }
                                }
                                Err(e) => {
                                    error!(
                                        error = %e,
                                        "Failed to parse JSON payload"
                                    );
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
        info!("Setting up MQTT subscriptions...");
        let props = SubscribeProperties {
            id: Some(1),
            user_properties: vec![],
        };
        let qos = QoS::ExactlyOnce;

        match client
            .subscribe_with_properties("#", qos, props.clone())
            .await
        {
            Ok(_) => info!("Successfully subscribed to MQTT topic '#'"),
            Err(e) => error!(error = %e, "Failed to subscribe to MQTT topic"),
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
        info!("MQTT subscription setup complete");
    }
}

/// Process CAN signal update (to be implemented)
async fn process_can_signal_update(
    message_name: &str,
    signal_name: &str,
    new_value: f32,
) -> Result<()> {
    // Here we would implement the logic to process the CAN signal update
    info!(
        message_name = %message_name,
        signal_name = %signal_name,
        new_value = new_value,
        "Processing CAN signal update"
    );

    let (can_id, frame) = construct_frame_with_signal(message_name, signal_name, new_value)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to construct CAN frame: {}", e))?;

    write_frame(frame).map_err(|e| anyhow::anyhow!("Failed to write CAN frame: {}", e))?;

    info!(
        can_id = %format!("0x{:X}", can_id),
        "Successfully sent CAN frame"
    );
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
    debug!(
        message_name = %message_name,
        signal_name = %signal_name,
        new_value = new_value,
        "Setting signal value"
    );
    msg_data.set_signal_value(signal_name, new_value)?;

    // Verify the signal value was set correctly
    if let Some(current_value) = msg_data.get_signal_value(signal_name) {
        debug!(
            message_name = %message_name,
            signal_name = %signal_name,
            current_value = current_value,
            "Signal value after setting"
        );
    } else {
        warn!(
            message_name = %message_name,
            signal_name = %signal_name,
            "Signal not found after setting"
        );
    }

    // Print all signal values for debugging
    debug!("All signal values in {}:", message_name);
    for (sig_name, sig_value) in &msg_data.signal_values {
        debug!(
            signal_name = %sig_name,
            signal_value = %sig_value,
            "Signal value"
        );
    }

    // // Store the signal update in Redis hash
    // if let Err(e) = store_signal_in_redis(&msg_data.name, signal_name, new_value).await {
    //     eprintln!("Failed to store signal in Redis: {}", e);
    // }

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
