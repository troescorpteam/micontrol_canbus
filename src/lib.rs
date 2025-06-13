use socketcan::CanFrame;
use tokio::sync::mpsc;

pub mod mqtt_integration;

// Re-export dotenv for convenience
pub use dotenv;

// Re-export from mqtt_integration
pub use mqtt_integration::{SignalUpdatePayload, set_signal_update_sender};

// Global CAN transmission channel
pub static CAN_TX_SENDER: tokio::sync::OnceCell<mpsc::UnboundedSender<CanFrame>> =
    tokio::sync::OnceCell::const_new();
