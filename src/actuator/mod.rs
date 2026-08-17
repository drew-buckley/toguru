use std::{borrow::Cow, sync::Arc};

use tokio::sync::mpsc;

use crate::{
    ToggleState,
    actuator::{
        mqtt_zigbee_switch::MqttZigbeeSwitchActuator, phoney_baloney::PhoneyBaloneyActuator,
    },
};

pub mod mqtt_zigbee_switch;
pub mod phoney_baloney;

pub enum Actuator {
    PhoneyBaloney(PhoneyBaloneyActuator),
    MqttZigbeeSwitch(MqttZigbeeSwitchActuator),
}

impl Actuator {
    pub async fn set(&mut self, state: ToggleState) -> Result<(), error::OperationError> {
        log::debug!("Actuator ({}) set: {}", self.id(), state);

        match self {
            Self::PhoneyBaloney(actuator) => actuator.set(state).await,
            Self::MqttZigbeeSwitch(actuator) => unimplemented!(),
        }
    }

    pub fn register_change_listener(&mut self, state_tx: mpsc::Sender<Arc<ActuatorToggleState>>) {
        log::debug!("Actuator ({}) state change listener added", self.id());

        match self {
            Self::PhoneyBaloney(actuator) => actuator.register_change_listener(state_tx),
            Self::MqttZigbeeSwitch(actuator) => unimplemented!(),
        }
    }

    pub fn id(&self) -> Cow<'_, str> {
        match self {
            Self::PhoneyBaloney(actuator) => actuator.id().into(),
            Self::MqttZigbeeSwitch(actuator) => unimplemented!(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum ActuatorToggleState {
    Up(ActuatorToggleUpState),
    Down(Arc<error::OperationError>),
}

impl From<ActuatorToggleUpState> for ActuatorToggleState {
    fn from(state: ActuatorToggleUpState) -> Self {
        Self::Up(state)
    }
}

impl From<error::OperationError> for ActuatorToggleState {
    fn from(err: error::OperationError) -> Self {
        Self::Down(Arc::new(err))
    }
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

fn state_observer() -> (StateObserverWire, StateReporter) {
    let (tx_tx, tx_rx) = mpsc::channel(1);
    (StateObserverWire::new(tx_tx), StateReporter::new(tx_rx))
}

#[derive(Clone)]
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
