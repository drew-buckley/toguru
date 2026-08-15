use std::{sync::Arc, time::Duration};

use tokio::sync::mpsc;

use crate::{
    ToggleState,
    actuator::{ActuatorToggleState, ActuatorToggleUpState, error::OperationError},
};

pub struct PhoneyBaloneyActuator {
    id: String,
    toggle_state: ToggleState,
    state_txs: Vec<mpsc::Sender<Arc<ActuatorToggleState>>>,
    state_change_delay: Duration,
}

impl PhoneyBaloneyActuator {
    pub fn new(
        id: impl Into<String>,
        init_state: ToggleState,
        state_change_delay: Duration,
    ) -> Self {
        Self {
            id: id.into(),
            toggle_state: init_state,
            state_txs: Vec::new(),
            state_change_delay,
        }
    }

    pub async fn set(&mut self, state: ToggleState) -> Result<(), OperationError> {
        if self.toggle_state != state {
            self.toggle_state = state;
            if self.state_change_delay == Duration::ZERO {
                send_state_changes(state, &self.state_txs);
            } else {
                let state_txs = self.state_txs.clone();
                let state_change_delay = self.state_change_delay;
                tokio::spawn(async move {
                    tokio::time::sleep(state_change_delay).await;
                    send_state_changes(state, state_txs.iter());
                });
            }
        }
        Ok(())
    }

    pub fn register_change_listener(&mut self, state_tx: mpsc::Sender<Arc<ActuatorToggleState>>) {
        self.state_txs.push(state_tx);
    }

    pub fn id(&self) -> &str {
        &self.id
    }
}

fn send_state_changes<'a, I>(state: ToggleState, state_txs: I)
where
    I: IntoIterator<Item = &'a mpsc::Sender<Arc<ActuatorToggleState>>>,
{
    let state = Arc::new(ActuatorToggleUpState::new(state).into());
    for state_tx in state_txs {
        if let Err(err) = state_tx.try_send(Arc::clone(&state)) {
            log::warn!("Failed to send state update: {:?}", err);
        }
    }
}
