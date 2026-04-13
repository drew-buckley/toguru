use std::sync::Arc;

use crate::{SwitchOperationalStatus, SwitchToggleState};

#[derive(Debug, Clone)]
pub enum ActuatorRequest {
    Get(String),
    Set(String, SwitchToggleState),
}

impl ActuatorRequest {
    pub fn into_switch_id(self) -> String {
        match self {
            ActuatorRequest::Get(id) => id,
            ActuatorRequest::Set(id, ..) => id,
        }
    }
}

#[derive(Debug, Clone)]
pub enum ActuatorResponse {
    Get(String, SwitchOperationalStatus),
    Set(String, SwitchOperationalStatus),
    Error(Arc<anyhow::Error>),
}
