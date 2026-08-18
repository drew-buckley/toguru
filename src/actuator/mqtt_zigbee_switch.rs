use std::{borrow::Cow, sync::Arc};

use ahash::{HashMap, HashMapExt};
use anyhow::Context;
use rumqttc::{AsyncClient, Event, EventLoop, MqttOptions, QoS};
use tokio::sync::{broadcast, mpsc};

use crate::{
    ToggleState,
    actuator::{ActuatorChangeListener, ActuatorToggleState, error::OperationError},
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

    pub fn id(&self) -> Cow<'_, str> {
        todo!()
    }
}

pub struct MqttBrokerConnection {
    state_change_tx_map: HashMap<String, broadcast::Sender<ActuatorToggleState>>,
    options: MqttOptions,
    state_change_tx_map: HashMap<String, broadcast::Sender<ActuatorToggleState>>,
    set_tx: mpsc::Sender<(String, ToggleState)>,
    set_rx: mpsc::Receiver<(String, ToggleState)>,
    base_topic: String,
    mqtt_client_capacity: usize,
}

impl MqttBrokerConnection {
    pub fn new(
        mqtt_client_id: impl Into<String>,
        host: impl Into<String>,
        port: u16,
        base_topic: impl Into<String>,
        set_buffer_capacity: usize,
        mqtt_client_capacity: usize,
    ) -> Self {
        let (set_tx, set_rx) = mpsc::channel(set_buffer_capacity);
        Self {
            options: MqttOptions::new(mqtt_client_id, host, port),
            state_change_tx_map: HashMap::new(),
            set_tx: set_tx,
            set_rx,
            base_topic: base_topic.into(),
            mqtt_client_capacity,
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

    pub async fn run(self) -> Result<(), anyhow::Error> {
        todo!()
    }
}

async fn run_client_instance(
    options: MqttOptions,
    base_topic: impl AsRef<str>,
    mqtt_client_capacity: usize,
    set_rx: mpsc::Receiver<(String, ToggleState)>,
    state_change_tx_map: Arc<HashMap<String, broadcast::Sender<ActuatorToggleState>>>,
) -> Result<(), anyhow::Error> {
    let base_topic = base_topic.as_ref();
    let (broker_host, broker_port) = options.broker_address();
    log::info!("Connecting to MQTT broker: {}:{}", broker_host, broker_port);
    let (mut client, mut eventloop) = AsyncClient::new(options, mqtt_client_capacity);

    let sub_topic = format!("{}/+", base_topic);
    log::debug!("Subscribing to {}", sub_topic);
    client
        .subscribe(sub_topic, QoS::AtMostOnce)
        .await
        .context("failed to subscribe to MQTT topic")?;

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
            ClientEvent::MqttEvent(event) => todo!(),
        }
    }
    todo!()
}

enum ClientEvent {
    SetCommand(String, ToggleState),
    MqttEvent(Event),
}

async fn run_eventloop(mut eventloop: EventLoop) -> Result<(), anyhow::Error> {
    loop {
        let event = eventloop
            .poll()
            .await
            .context("Failed to receive next MQTT event")?;
    }
}
