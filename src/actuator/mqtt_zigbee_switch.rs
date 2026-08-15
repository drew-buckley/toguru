use std::{borrow::Cow, sync::Arc};

use rumqttc::MqttOptions;
use tokio::sync::mpsc;

use crate::{
    ToggleState,
    actuator::{
        ActuatorToggleState, StateObserverWire, StateReporter, error::OperationError,
        state_observer,
    },
};

pub struct MqttZigbeeSwitchActuator {
    id: Arc<String>,
    set_tx: mpsc::Sender<(Arc<String>, ToggleState)>,
    state_observer_wire: StateObserverWire,
}

impl MqttZigbeeSwitchActuator {
    pub async fn set(&mut self, state: ToggleState) -> Result<(), OperationError> {
        todo!()
    }

    pub fn register_change_listener(&mut self, state_tx: mpsc::Sender<Arc<ActuatorToggleState>>) {
        todo!()
    }

    pub fn id(&self) -> Cow<'_, str> {
        todo!()
    }
}

pub struct MqttBrokerConnection {
    options: MqttOptions,
    state_observer_wire: StateObserverWire,
    state_reporter: StateReporter,
    set_tx: mpsc::Sender<(Arc<String>, ToggleState)>,
    set_rx: mpsc::Receiver<(Arc<String>, ToggleState)>,
}

impl MqttBrokerConnection {
    fn new(id: impl Into<String>, host: impl Into<String>, port: u16) -> Self {
        let (state_observer_wire, state_reporter) = state_observer();
        let (set_tx, set_rx) = mpsc::channel(12);
        Self {
            options: MqttOptions::new(id, host, port),
            state_observer_wire,
            state_reporter,
            set_tx,
            set_rx,
        }
    }

    pub async fn run(self) -> Result<(), anyhow::Error> {
        todo!()
    }
}
