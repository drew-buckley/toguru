use std::{os::linux::raw::stat, sync::Arc};

use tokio::sync::mpsc;

use crate::ToggleState;

pub mod mqtt_zigbee_switch;

pub enum Actuator {
    PhoneyBaloney,
    MqttZigbeeSwitch,
}

impl Actuator {
    pub async fn set(&mut self, state: ToggleState) -> Result<(), error::OperationError> {
        todo!()
    }

    pub fn register_change_listener(&mut self, state_tx: mpsc::Sender<Arc<ActuatorToggleState>>) {
        todo!()
    }
}

#[derive(Debug, Clone)]
pub enum ActuatorToggleState {
    Up(ActuatorToggleUpState),
    Down(Arc<error::OperationError>),
}

#[derive(Debug, Clone)]
pub struct ActuatorToggleUpState {
    pub state: ToggleState,
    pub extra_context: serde_json::Value,
}

impl ActuatorToggleUpState {
    pub fn new(state: ToggleState) -> Self {
        Self {
            state,
            extra_context: serde_json::Value::Null,
        }
    }

    pub fn add_extra_context(&mut self, extra_context: serde_json::Value) {
        self.extra_context = extra_context;
    }
}

struct StateObserverWire {
    tx_tx: mpsc::Sender<mpsc::Sender<Arc<ActuatorToggleState>>>,
}

impl StateObserverWire {
    fn new(tx_tx: mpsc::Sender<mpsc::Sender<Arc<ActuatorToggleState>>>) -> Self {
        Self { tx_tx }
    }
}

struct StateReporter {
    tx_rx: mpsc::Receiver<mpsc::Sender<Arc<ActuatorToggleState>>>,
    report_txs: Vec<mpsc::Sender<Arc<ActuatorToggleState>>>,
}

impl StateReporter {
    fn new(tx_rx: mpsc::Receiver<mpsc::Sender<Arc<ActuatorToggleState>>>) -> Self {
        Self {
            tx_rx,
            report_txs: Vec::new(),
        }
    }

    fn report_state(&mut self, state: ActuatorToggleState) {
        let state = Arc::new(state);
        while let Ok(report_tx) = self.tx_rx.try_recv() {
            self.report_txs.push(report_tx);
        }

        for report_tx in &mut self.report_txs {
            let _ = report_tx.try_send(Arc::clone(&state));
        }
    }
}

pub mod error {
    use super::*;

    #[derive(Debug, thiserror::Error)]
    pub enum OperationError {
        #[error("failed to communicate over the network")]
        Network(#[source] anyhow::Error),

        #[error("internal error")]
        Internal(#[source] anyhow::Error),
    }
}
