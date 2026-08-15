use serde::{Deserialize, Serialize};
use strum_macros::{Display as EnumDisplay, EnumString};

pub mod actuator;
pub mod api;
pub mod engine;

#[derive(Debug, Clone, Copy, EnumDisplay, EnumString, Serialize, Deserialize, PartialEq, Eq)]
pub enum ToggleState {
    Off,
    On,
}

pub enum ActuatorState {}
