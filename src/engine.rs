use std::{sync::Arc, time::SystemTime};

use anyhow::Context;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::ToggleState;

#[derive(Clone)]
pub struct Controller {
    ctrl_tx: mpsc::Sender<EngineCommand>,
}

impl Controller {
    fn new(ctrl_tx: mpsc::Sender<EngineCommand>) -> Self {
        Self { ctrl_tx }
    }

    pub async fn list_toggles(&self) -> Result<Vec<String>, error::ControllerOperationError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.ctrl_tx
            .send(ListCommand::new(resp_tx).into())
            .await
            .context("ctrl_tx channel closed")
            .map_err(error::ControllerOperationError::Internal)?;

        resp_rx
            .await
            .context("resp_rx oneshot channel closed")
            .map_err(error::ControllerOperationError::Internal)
    }

    pub async fn get(
        &self,
        id: impl Into<String>,
    ) -> Result<ToggleStatus, error::ControllerOperationError> {
        self.toggle(id, None).await
    }

    pub async fn set(
        &self,
        id: impl Into<String>,
        state: ToggleState,
    ) -> Result<ToggleStatus, error::ControllerOperationError> {
        self.toggle(id, Some(state)).await
    }

    async fn toggle(
        &self,
        id: impl Into<String>,
        set_state: Option<ToggleState>,
    ) -> Result<ToggleStatus, error::ControllerOperationError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.ctrl_tx
            .send(ToggleCommand::new(id, set_state, resp_tx).into())
            .await
            .context("ctrl_tx channel closed")
            .map_err(error::ControllerOperationError::Internal)?;

        resp_rx
            .await
            .context("resp_rx oneshot channel closed")
            .map_err(error::ControllerOperationError::Internal)
    }
}

pub struct Engine {
    cancel_token: CancellationToken,
}

enum EngineCommand {
    List(ListCommand),
    Toggle(ToggleCommand),
}

impl From<ToggleCommand> for EngineCommand {
    fn from(cmd: ToggleCommand) -> Self {
        EngineCommand::Toggle(cmd)
    }
}

impl From<ListCommand> for EngineCommand {
    fn from(cmd: ListCommand) -> Self {
        EngineCommand::List(cmd)
    }
}

struct ListCommand {
    resp_tx: oneshot::Sender<Vec<String>>,
}

impl ListCommand {
    fn new(resp_tx: oneshot::Sender<Vec<String>>) -> Self {
        Self { resp_tx }
    }
}

struct ToggleCommand {
    id: String,
    set_state: Option<ToggleState>,
    resp_tx: oneshot::Sender<ToggleStatus>,
}

impl ToggleCommand {
    fn new(
        id: impl Into<String>,
        set_state: Option<ToggleState>,
        resp_tx: oneshot::Sender<ToggleStatus>,
    ) -> Self {
        Self {
            id: id.into(),
            set_state,
            resp_tx,
        }
    }
}

#[derive(Debug, Clone)]
pub enum ToggleStatus {
    Up(ToggleUpStatus),
    Down(ToggleDownStatus),
}

#[derive(Debug, Clone)]
pub struct ToggleUpStatus {
    pub id: String,
    pub observed: ToggleStateSnapshot,
    pub commanded: ToggleStateSnapshot,
    pub extra_context: serde_json::Value,
}

impl ToggleUpStatus {
    pub fn new(
        id: impl Into<String>,
        observed: ToggleStateSnapshot,
        commanded: ToggleStateSnapshot,
    ) -> Self {
        Self {
            id: id.into(),
            observed,
            commanded,
            extra_context: serde_json::Value::Null,
        }
    }

    pub fn add_extra_context(&mut self, extra_context: serde_json::Value) {
        self.extra_context = extra_context;
    }
}

#[derive(Debug, Clone)]
pub struct ToggleDownStatus {
    pub id: String,
    pub last_operable: SystemTime,
    pub error: Option<Arc<anyhow::Error>>,
}

#[derive(Debug, Clone, Copy)]
pub struct ToggleStateSnapshot {
    pub timestamp: SystemTime,
    pub state: ToggleState,
}

impl ToggleStateSnapshot {
    pub const fn new(timestamp: SystemTime, state: ToggleState) -> Self {
        Self { timestamp, state }
    }
}

pub mod error {
    use super::*;

    #[derive(Debug, thiserror::Error)]
    pub enum ControllerOperationError {
        #[error("unknown toggle ID: {0}")]
        UnknownToggleId(String),

        #[error("internal error")]
        Internal(#[source] anyhow::Error),
    }
}
