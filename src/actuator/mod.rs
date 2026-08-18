use std::{borrow::Cow, sync::Arc};

use tokio::sync::broadcast;

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

    pub fn subscribe_to_changes(&self) -> ActuatorChangeListener {
        match self {
            Self::PhoneyBaloney(actuator) => unimplemented!(),
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

pub struct ActuatorChangeListener {
    id: String,
    rx: broadcast::Receiver<ActuatorToggleState>,
}

impl ActuatorChangeListener {
    fn new(id: impl Into<String>, rx: broadcast::Receiver<ActuatorToggleState>) -> Self {
        Self { id: id.into(), rx }
    }

    pub async fn recv(&mut self) -> Result<ActuatorToggleState, error::ActuatorChangeRecvError> {
        self.rx.recv().await.map_err(|e| e.into())
    }
}

impl Clone for ActuatorChangeListener {
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            rx: self.rx.resubscribe(),
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

pub mod error {
    use super::*;

    #[derive(Debug, thiserror::Error)]
    pub enum OperationError {
        #[error("failed to communicate over the network")]
        Network(#[source] anyhow::Error),

        #[error("internal error")]
        Internal(#[source] anyhow::Error),
    }

    #[derive(Debug, thiserror::Error)]
    pub enum ActuatorChangeRecvError {
        #[error("actuator is closed")]
        ActuatorClosed,

        #[error("actuator changes lagged behind {0} state transitions")]
        Lagged(u64),
    }

    impl From<broadcast::error::RecvError> for ActuatorChangeRecvError {
        fn from(err: broadcast::error::RecvError) -> Self {
            match err {
                broadcast::error::RecvError::Closed => Self::ActuatorClosed,
                broadcast::error::RecvError::Lagged(lag) => Self::Lagged(lag),
            }
        }
    }
}
