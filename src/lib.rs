use std::{process::Output, sync::Arc};

pub mod actuators;
pub mod api;
pub mod broker;
pub mod rrch;
pub mod server;

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub enum SwitchToggleState {
    On,
    Off,
}

#[derive(Debug, Clone)]
pub enum SwitchOperationalStatus {
    Unknown,
    Transitioning(SwitchToggleState),
    Confirmed(SwitchToggleState),
    Failure(Arc<anyhow::Error>),
}

pub trait StdTask {
    fn run(self) -> impl std::future::Future<Output = Result<(), anyhow::Error>>;
}
