use serde::{Deserialize, Serialize};
use strum_macros::{Display as EnumDisplay, EnumString};

pub mod api;

#[derive(Debug, Clone, Copy, EnumDisplay, EnumString, Serialize, Deserialize)]
pub enum ToggleState {
    Off,
    On,
}

pub enum ActuatorState {}
