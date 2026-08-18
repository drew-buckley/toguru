use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use ahash::{HashMap, HashMapExt};
use anyhow::Context;
use parking_lot::Mutex;
use tokio::sync::{Mutex as AsyncMutex, mpsc, oneshot};

use crate::{
    ToggleState,
    actuator::{
        Actuator, ActuatorChangeListener, ActuatorToggleState, ActuatorToggleUpState,
        error::ActuatorChangeRecvError,
    },
};

pub fn make_engine(ctrl_buffer: usize) -> (Engine, Controller) {
    let (ctrl_tx, ctrl_rx) = mpsc::channel(ctrl_buffer);
    (Engine::new(ctrl_rx), Controller::new(ctrl_tx))
}

#[derive(Clone)]
pub struct Controller {
    ctrl_tx: mpsc::Sender<Command>,
}

impl Controller {
    fn new(ctrl_tx: mpsc::Sender<Command>) -> Self {
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
    ctrl_rx: mpsc::Receiver<Command>,
    actuator_map: HashMap<String, AsyncMutex<Actuator>>,
}

impl Engine {
    fn new(ctrl_rx: mpsc::Receiver<Command>) -> Self {
        Self {
            ctrl_rx,
            actuator_map: HashMap::new(),
        }
    }

    pub async fn run(self) -> Result<(), Vec<anyhow::Error>> {
        let Self {
            mut ctrl_rx,
            actuator_map,
        } = self;

        let actuator_map = Arc::new(actuator_map);
        let state_cache = Arc::new(StateCache::from_iter(actuator_map.keys()));

        for (id, actuator) in actuator_map.iter() {
            let change_listener = actuator
                .try_lock()
                // safe to unwrap; nothing else have access to these mutexes yet
                .unwrap()
                .subscribe_to_changes();

            kick_observe(id.clone(), change_listener, Arc::clone(&state_cache));
        }

        while let Some(cmd) = ctrl_rx.recv().await {
            match cmd {
                Command::List(ListCommand { resp_tx }) => {
                    if resp_tx
                        .send(actuator_map.keys().cloned().collect())
                        .is_err()
                    {
                        log::warn!("Operation response channel closed too early for response");
                    }
                }
                Command::Toggle(cmd) => {
                    let actuator_map = Arc::clone(&actuator_map);
                    let state_cache = Arc::clone(&state_cache);
                    tokio::spawn(async move { toggle(cmd, actuator_map, state_cache).await });
                }
            }
        }

        Ok(())
    }

    pub fn register_actuator(&mut self, id: impl Into<String>, actuator: Actuator) {
        self.actuator_map
            .insert(id.into(), AsyncMutex::new(actuator));
    }
}

async fn toggle(
    cmd: ToggleCommand,
    actuator_map: Arc<HashMap<String, AsyncMutex<Actuator>>>,
    state_cache: Arc<StateCache>,
) {
    let ToggleCommand {
        id,
        set_state,
        resp_tx,
    } = cmd;

    let mut resp = None;
    if let Some(state) = set_state {
        if let Some(actuator) = actuator_map.get(&id) {
            let mut actuator = actuator.lock().await;
            match actuator.set(state).await {
                Ok(()) => {
                    state_cache
                        .set_commanded(&id, state)
                        .expect("logic error: ID not preregistered in cache");
                }
                Err(err) => {
                    resp = Some(ToggleStatus::Down(ToggleDownStatus::new(
                        &id,
                        state_cache.get_last_operable(&id).unwrap_or(UNIX_EPOCH),
                        Some(Arc::new(err.into())),
                    )));
                }
            }
        };
    };

    if resp.is_none() {
        if let (Some(commanded), Some(observed)) = (
            state_cache.get_commanded(&id),
            state_cache.get_observed(&id),
        ) {
            resp = Some(ToggleStatus::Up(ToggleUpStatus::new(
                id, observed, commanded,
            )))
        } else {
            let last_operable = state_cache.get_last_operable(&id).unwrap_or(UNIX_EPOCH);
            resp = Some(ToggleStatus::Down(ToggleDownStatus::new(
                id,
                last_operable,
                None,
            )))
        }
    }

    if resp_tx.send(resp.unwrap()).is_err() {
        log::warn!("Operation response channel closed too early for response");
    }
}

fn kick_observe(
    id: impl AsRef<str> + Send + 'static,
    change_listener: ActuatorChangeListener,
    state_cache: Arc<StateCache>,
) {
    tokio::spawn(async move {
        let id = id.as_ref();
        if let Err(err) = observe(&id, change_listener, state_cache).await {
            log::error!("Observe task failed for \"{}\": {:?}", id, err);
        } else {
            log::info!("Observe task finished for \"{}\"", id);
        }
    });
}

async fn observe(
    id: impl AsRef<str>,
    mut change_listener: ActuatorChangeListener,
    state_cache: Arc<StateCache>,
) -> Result<(), anyhow::Error> {
    let id = id.as_ref();
    while let Some(state) = change_listener
        .recv()
        .await
        .inspect_err(|err| {
            if let ActuatorChangeRecvError::Lagged(lag) = err {
                log::warn!("State change listener fell behind by {}", lag);
            }
        })
        .ok()
    {
        if let ActuatorToggleState::Up(ActuatorToggleUpState { state, .. }) = state {
            state_cache.set_observed(id, state)?;
        }
    }

    Ok(())
}

struct StateCache {
    commanded: HashMap<String, Mutex<Option<ToggleStateSnapshot>>>,
    observed: HashMap<String, Mutex<Option<ToggleStateSnapshot>>>,
}

impl StateCache {
    fn from_iter<I, T>(ids: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: Into<String>,
    {
        let commanded = HashMap::from_iter(ids.into_iter().map(|id| (id.into(), Mutex::new(None))));
        let observed =
            HashMap::from_iter(commanded.keys().map(|id| (id.clone(), Mutex::new(None))));
        Self {
            commanded,
            observed,
        }
    }

    fn get_last_operable(&self, id: impl AsRef<str>) -> Option<SystemTime> {
        let id = id.as_ref();
        match (self.get_commanded(id), self.get_observed(id)) {
            (Some(commanded), Some(observed)) => Some(commanded.timestamp.min(observed.timestamp)),
            (Some(commanded), None) => Some(commanded.timestamp),
            (None, Some(observed)) => Some(observed.timestamp),
            (None, None) => None,
        }
    }

    fn get_commanded(&self, id: impl AsRef<str>) -> Option<ToggleStateSnapshot> {
        self.commanded.get(id.as_ref()).and_then(|c| *c.lock())
    }

    fn set_commanded(&self, id: impl AsRef<str>, state: ToggleState) -> Result<(), anyhow::Error> {
        set_and_timestamp(&self.commanded, id, state)
    }

    fn get_observed(&self, id: impl AsRef<str>) -> Option<ToggleStateSnapshot> {
        self.observed.get(id.as_ref()).and_then(|c| *c.lock())
    }

    fn set_observed(&self, id: impl AsRef<str>, state: ToggleState) -> Result<(), anyhow::Error> {
        set_and_timestamp(&self.commanded, id, state)
    }
}

fn set_and_timestamp(
    cache: &HashMap<String, Mutex<Option<ToggleStateSnapshot>>>,
    id: impl AsRef<str>,
    state: ToggleState,
) -> Result<(), anyhow::Error> {
    let now = SystemTime::now();
    let id = id.as_ref();
    let mut snapshot = cache
        .get(id)
        .with_context(|| format!("{} not registered in cache", id))?
        .lock();
    *snapshot = Some(ToggleStateSnapshot::new(now, state));
    Ok(())
}

enum Command {
    List(ListCommand),
    Toggle(ToggleCommand),
}

impl From<ToggleCommand> for Command {
    fn from(cmd: ToggleCommand) -> Self {
        Command::Toggle(cmd)
    }
}

impl From<ListCommand> for Command {
    fn from(cmd: ListCommand) -> Self {
        Command::List(cmd)
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

impl ToggleDownStatus {
    fn new(
        id: impl Into<String>,
        last_operable: SystemTime,
        error: Option<Arc<anyhow::Error>>,
    ) -> Self {
        Self {
            id: id.into(),
            last_operable,
            error,
        }
    }
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
