use std::{sync::Arc, time::Duration};

use tokio::sync::broadcast;

use crate::{
    ToggleState,
    actuator::{
        ActuatorChangeListener, ActuatorToggleState, ActuatorToggleUpState, error::OperationError,
    },
};

pub struct PhoneyBaloneyActuator {
    id: String,
    toggle_state: ToggleState,
    state_change_tx: broadcast::Sender<ActuatorToggleState>,
    state_change_delay: Duration,
}

impl PhoneyBaloneyActuator {
    pub fn new(
        id: impl Into<String>,
        init_state: ToggleState,
        state_change_delay: Duration,
        state_change_buffer: usize,
    ) -> Self {
        let (state_change_tx, _) = broadcast::channel(state_change_buffer);
        Self {
            id: id.into(),
            toggle_state: init_state,
            state_change_tx,
            state_change_delay,
        }
    }

    pub async fn set(&mut self, state: ToggleState) -> Result<(), OperationError> {
        if self.toggle_state != state {
            self.toggle_state = state;
            if self.state_change_delay == Duration::ZERO {
                send_state_changes(state, &self.state_change_tx);
            } else {
                let state_change_tx = self.state_change_tx.clone();
                let state_change_delay = self.state_change_delay;
                tokio::spawn(async move {
                    tokio::time::sleep(state_change_delay).await;
                    send_state_changes(state, &state_change_tx);
                });
            }
        }
        Ok(())
    }

    pub fn subscribe_to_changes(&self) -> ActuatorChangeListener {
        ActuatorChangeListener::new(&self.id, self.state_change_tx.subscribe())
    }

    pub fn id(&self) -> &str {
        &self.id
    }
}

fn send_state_changes<'a>(
    state: ToggleState,
    state_change_tx: &broadcast::Sender<ActuatorToggleState>,
) {
    if let Err(err) = state_change_tx.send(ActuatorToggleUpState::new(state).into()) {
        log::warn!("Failed to send state update: {:?}", err);
    }
}
