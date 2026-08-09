use std::sync::Arc;

use tokio::sync::mpsc;

use crate::ToggleState;

pub enum Actuator {
    MqttZigbeeSwitch,
}

impl Actuator {
    pub async fn set(&mut self, state: ToggleState) -> Result<(), error::OperationError> {
        todo!()
    }

    pub fn register_change_listener(&mut self, state_tx: mpsc::Sender<ActuatorToggleState>) {
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
