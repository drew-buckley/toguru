use std::{
    borrow::Cow,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use ahash::{HashMap, HashMapExt};
use anyhow::Context;
use rumqttc::{AsyncClient, Event, EventLoop, MqttOptions, Packet, Publish, QoS};
use serde::Deserialize;
use tokio::sync::{broadcast, mpsc};

use crate::{
    ToggleState,
    actuator::{
        ActuatorChangeListener, ActuatorToggleState, ActuatorToggleUpState, error::OperationError,
    },
};

pub struct MqttZigbeeSwitchActuator {
    id: String,
    set_tx: mpsc::Sender<(String, ToggleState)>,
    state_change_listener: ActuatorChangeListener,
}

impl MqttZigbeeSwitchActuator {
    fn new(
        id: impl Into<String>,
        set_tx: mpsc::Sender<(String, ToggleState)>,
        state_change_listener: ActuatorChangeListener,
    ) -> Self {
        Self {
            id: id.into(),
            set_tx,
            state_change_listener,
        }
    }

    pub async fn set(&mut self, state: ToggleState) -> Result<(), OperationError> {
        self.set_tx
            .send((self.id.clone(), state))
            .await
            .context("actuator set channel closed")
            .map_err(|e| OperationError::Internal(e))
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn subscribe_to_changes(&self) -> ActuatorChangeListener {
        self.state_change_listener.clone()
    }
}

pub struct MqttBrokerConnection {
    options: MqttOptions,
    state_change_tx_map: HashMap<String, broadcast::Sender<ActuatorToggleState>>,
    set_tx: mpsc::Sender<(String, ToggleState)>,
    set_rx: mpsc::Receiver<(String, ToggleState)>,
    base_topic: String,
    mqtt_client_capacity: usize,
    switch_timeout: Duration,
}

impl MqttBrokerConnection {
    pub fn new(
        mqtt_client_id: impl Into<String>,
        host: impl Into<String>,
        port: u16,
        base_topic: impl Into<String>,
        set_buffer_capacity: usize,
        mqtt_client_capacity: usize,
        switch_timeout: Duration,
    ) -> Self {
        let (set_tx, set_rx) = mpsc::channel(set_buffer_capacity);
        Self {
            options: MqttOptions::new(mqtt_client_id, host, port),
            state_change_tx_map: HashMap::new(),
            set_tx: set_tx,
            set_rx,
            base_topic: base_topic.into(),
            mqtt_client_capacity,
            switch_timeout,
        }
    }

    pub fn new_actuator(
        &mut self,
        id: impl Into<String>,
        change_listener_capacity: usize,
    ) -> MqttZigbeeSwitchActuator {
        let id = id.into();
        let (tx, rx) = broadcast::channel(change_listener_capacity);
        self.state_change_tx_map.insert(id.clone(), tx);
        MqttZigbeeSwitchActuator::new(
            id.clone(),
            self.set_tx.clone(),
            ActuatorChangeListener::new(id, rx),
        )
    }

    pub fn set_keepalive(&mut self, keepalive: Duration) {
        self.options.set_keep_alive(keepalive);
    }

    pub async fn run(self) -> Result<(), anyhow::Error> {
        let Self {
            options,
            state_change_tx_map,
            mut set_rx,
            base_topic,
            mqtt_client_capacity,
            switch_timeout,
            ..
        } = self;
        let mut state_change_tx_map = HashMap::from_iter(
            state_change_tx_map
                .into_iter()
                .map(|(id, tx)| (id, StateWidget::new(tx))),
        );
        while let Err(err) = run_client_instance(
            options.clone(),
            &base_topic,
            mqtt_client_capacity,
            &mut set_rx,
            &mut state_change_tx_map,
            switch_timeout,
        )
        .await
        {
            log::error!("MQTT client failed: {:?}", err);
        }

        Ok(())
    }
}

async fn run_client_instance(
    options: MqttOptions,
    base_topic: impl AsRef<str>,
    mqtt_client_capacity: usize,
    set_rx: &mut mpsc::Receiver<(String, ToggleState)>,
    state_change_tx_map: &mut HashMap<String, StateWidget>,
    switch_timeout: Duration,
) -> Result<(), anyhow::Error> {
    let base_topic = base_topic.as_ref();
    let (broker_host, broker_port) = options.broker_address();
    log::info!("Connecting to MQTT broker: {}:{}", broker_host, broker_port);
    let (client, mut eventloop) = AsyncClient::new(options, mqtt_client_capacity);

    let sub_topic = format!("{}/+", base_topic);
    log::debug!("Subscribing to {}", sub_topic);
    client
        .subscribe(sub_topic, QoS::AtMostOnce)
        .await
        .context("failed to subscribe to MQTT topic")?;

    let mut last_timeout_check = Instant::now();
    loop {
        let event = tokio::select! {
            res = eventloop.poll() => {
                ClientEvent::MqttEvent(res.context("Failed to receive next MQTT event")?)
            }

            res = set_rx.recv() => {
                if let Some((id, state)) = res {
                    ClientEvent::SetCommand(id, state)
                } else {
                    return Ok(())
                }
            }
        };

        match event {
            ClientEvent::SetCommand(id, state) => {
                let topic = format!("{}/{}/set", base_topic, id);
                let state = state.to_string();
                log::debug!("Publishing MQTT message: {} {}", topic, state);
                client
                    .publish(topic, QoS::AtLeastOnce, false, state.to_string())
                    .await?;
            }
            ClientEvent::MqttEvent(event) => match event {
                Event::Incoming(Packet::Publish(msg)) => match process_msg(&msg, base_topic) {
                    Ok((id, state)) => {
                        if let Some(StateWidget {
                            last_state,
                            last_state_ts,
                            state_change_tx,
                        }) = state_change_tx_map.get_mut(id)
                        {
                            if last_state.is_none_or(|ls| ls != state) {
                                *last_state_ts = Instant::now();
                                *last_state = Some(state);
                                let _ =
                                    state_change_tx.send(ActuatorToggleUpState::new(state).into());
                            }
                        } else {
                            log::debug!("Ignoring unknown switch ID: {}", id)
                        }
                    }
                    Err(err) => log::warn!("Malformed MQTT payload: {:?}", err),
                },
                event => log::trace!("MQTT event: {:?}", event),
            },
        }

        let now = Instant::now();
        if now.duration_since(last_timeout_check) > switch_timeout {
            for StateWidget {
                last_state,
                last_state_ts,
                state_change_tx,
            } in state_change_tx_map.values_mut()
            {
                if now.duration_since(*last_state_ts) > switch_timeout {
                    let _ = state_change_tx.send(OperationError::Unresponsive.into());
                    *last_state = None;
                }
            }
            last_timeout_check = now;
        }
    }
}

fn process_msg<'a>(
    msg: &'a Publish,
    expected_base_topic: &str,
) -> Result<(&'a str, ToggleState), anyhow::Error> {
    let mut topic = msg.topic.split('/');
    let base_topic = topic.next().context("could not parse base topic")?;
    if base_topic != expected_base_topic {
        anyhow::bail!("unexpected base topic: {}", base_topic)
    }
    let id = topic.next().context("could not parse ID")?;
    let state = serde_json::from_slice::<MqttSwitchPayload>(&msg.payload)
        .context("failed to parse payload")?
        .state
        .parse()
        .context("failed to parse state")?;

    Ok((id, state))
}

enum ClientEvent {
    SetCommand(String, ToggleState),
    MqttEvent(Event),
}

#[derive(Debug, Clone, Deserialize)]
struct MqttSwitchPayload {
    state: String,
}

struct StateWidget {
    last_state: Option<ToggleState>,
    last_state_ts: Instant,
    state_change_tx: broadcast::Sender<ActuatorToggleState>,
}

impl StateWidget {
    fn new(state_change_tx: broadcast::Sender<ActuatorToggleState>) -> Self {
        Self {
            last_state: None,
            last_state_ts: Instant::now(),
            state_change_tx,
        }
    }
}
