use std::{collections::HashMap, sync::Arc};

use tokio::sync::oneshot;

use crate::{
    StdTask, SwitchOperationalStatus, SwitchToggleState,
    actuators::{ActuatorRequest, ActuatorResponse},
    rrch,
};

pub fn broker(req_queue: usize) -> (Broker, rrch::Client<BrokerRequest, BrokerResponse>) {
    let (server, client) = rrch::server_client(req_queue);
    (Broker::new(server), client)
}

pub struct Broker {
    req_rx: rrch::Server<BrokerRequest, BrokerResponse>,
    actuator_map: HashMap<String, rrch::Client<ActuatorRequest, ActuatorResponse>>,
}

impl Broker {
    fn new(req_rx: rrch::Server<BrokerRequest, BrokerResponse>) -> Self {
        Self {
            req_rx,
            actuator_map: HashMap::new(),
        }
    }

    pub fn add(
        &mut self,
        id: impl Into<String>,
        actuator_client: rrch::Client<ActuatorRequest, ActuatorResponse>,
    ) -> Option<rrch::Client<ActuatorRequest, ActuatorResponse>> {
        self.actuator_map.insert(id.into(), actuator_client)
    }
}

impl StdTask for Broker {
    async fn run(mut self) -> Result<(), anyhow::Error> {
        let actuator_map = Arc::new(self.actuator_map);
        while let Some((req, resp_tx)) = self.req_rx.recv_req().await {
            let actuator_map = actuator_map.clone();
            tokio::spawn(async move {
                if let Err(err) = process_request(req, resp_tx, actuator_map).await {
                    log::error!("Failed to process broker request: {:?}", err);
                }
            });
        }

        Ok(())
    }
}

async fn process_request(
    req: BrokerRequest,
    resp_tx: oneshot::Sender<BrokerResponse>,
    actuator_map: Arc<HashMap<String, rrch::Client<ActuatorRequest, ActuatorResponse>>>,
) -> Result<(), anyhow::Error> {
    let resp = match req {
        BrokerRequest::List => BrokerResponse::List(actuator_map.keys().cloned().collect()),
        BrokerRequest::Get(id) => {
            if let Some(actuator_client) = actuator_map.get(&id).map(|c| c.clone()) {
                transact_actuator(actuator_client, ActuatorRequest::Get(id)).await?
            } else {
                BrokerResponse::Error(Arc::new(anyhow::anyhow!("unknown switch: \"{}\"", id)))
            }
        }
        BrokerRequest::Set(id, state) => {
            if let Some(actuator_client) = actuator_map.get(&id).map(|c| c.clone()) {
                transact_actuator(actuator_client, ActuatorRequest::Set(id, state)).await?
            } else {
                BrokerResponse::Error(Arc::new(anyhow::anyhow!("unknown switch: \"{}\"", id)))
            }
        }
    };

    resp_tx
        .send(resp)
        .map_err(|resp| anyhow::anyhow!("broker response channel closed ({:?})", resp))
}

async fn transact_actuator(
    actuator_client: rrch::Client<ActuatorRequest, ActuatorResponse>,
    req: ActuatorRequest,
) -> Result<BrokerResponse, anyhow::Error> {
    let resp = match actuator_client.send_request(req).await {
        Ok(resp_rx) => match resp_rx.await {
            Ok(resp) => match resp {
                ActuatorResponse::Get(id, status) => BrokerResponse::Get(id, status),
                ActuatorResponse::Set(id, status) => BrokerResponse::Set(id, status),
                ActuatorResponse::Error(err) => BrokerResponse::Error(err),
            },
            Err(_) => BrokerResponse::Error(Arc::new(anyhow::anyhow!(
                "actuator response channel closed before sending"
            ))),
        },
        Err(err) => BrokerResponse::Error(Arc::new(anyhow::anyhow!(
            "actuator channel mapped to \"{}\" closed",
            err.req.into_switch_id()
        ))),
    };

    Ok(resp)
}

#[derive(Debug, Clone)]
pub enum BrokerRequest {
    List,
    Get(String),
    Set(String, SwitchToggleState),
}

impl BrokerRequest {
    pub fn into_switch_id(self) -> Option<String> {
        match self {
            BrokerRequest::List => None,
            BrokerRequest::Get(id) => Some(id),
            BrokerRequest::Set(id, ..) => Some(id),
        }
    }
}

#[derive(Debug, Clone)]
pub enum BrokerResponse {
    List(Vec<String>),
    Get(String, SwitchOperationalStatus),
    Set(String, SwitchOperationalStatus),
    Error(Arc<anyhow::Error>),
}
