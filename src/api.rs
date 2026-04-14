use std::time::{Duration, Instant};

use anyhow::Context;
use tokio::sync::oneshot;

use crate::{
    SwitchOperationalStatus, SwitchToggleState,
    api::v1::SetModeBurstProperties,
    broker::{BrokerRequest, BrokerResponse},
    rrch::Client,
};

#[derive(Debug, Clone, Copy)]
pub enum ApiVersion {
    V1,
}

#[derive(Debug, Clone)]
pub enum Request {
    V1(v1::Request),
}

impl Request {
    pub fn version(&self) -> ApiVersion {
        match self {
            Request::V1(..) => ApiVersion::V1,
        }
    }
}

impl From<v1::Request> for Request {
    fn from(req: v1::Request) -> Self {
        Request::V1(req)
    }
}

#[derive(Debug, Clone)]
pub enum Response {
    V1(v1::Response),
}

impl Response {
    pub fn version(&self) -> ApiVersion {
        match self {
            Response::V1(..) => ApiVersion::V1,
        }
    }
}

impl From<v1::Response> for Response {
    fn from(req: v1::Response) -> Self {
        Response::V1(req)
    }
}

pub struct Api {
    broker_tx: Client<BrokerRequest, BrokerResponse>,
}

impl Api {
    pub async fn transact(&self, req: Request) -> Result<Response, anyhow::Error> {
        let api_version = req.version();
        let (req, burst_props) = match req {
            Request::V1(req) => match req {
                v1::Request::List => (BrokerRequest::List, None),
                v1::Request::Get(req) => (BrokerRequest::Get(req.id), None),
                v1::Request::Set(req) => {
                    (BrokerRequest::Set(req.id, req.state), req.mode.as_burst())
                }
            },
        };

        let resp = if let Some(burst_props) = burst_props
            && let BrokerRequest::Set(id, state) = req
        {
            self.transact_broker_burst_set(id, state, burst_props)
                .await?
        } else {
            self.transact_broker_simple(req).await?
        };

        let resp = match resp {
            BrokerResponse::List(switches) => match api_version {
                ApiVersion::V1 => v1::Response::List(switches).into(),
            },
            BrokerResponse::Get(id, status) => match api_version {
                ApiVersion::V1 => v1::Response::Get((id, status).into()).into(),
            },
            BrokerResponse::Set(id, status) => match api_version {
                ApiVersion::V1 => v1::Response::Set((id, status).into()).into(),
            },
            BrokerResponse::Error(error) => match api_version {
                ApiVersion::V1 => v1::Response::Error(error.to_string()).into(),
            },
        };

        Ok(resp)
    }

    async fn transact_broker_simple(
        &self,
        req: BrokerRequest,
    ) -> Result<BrokerResponse, anyhow::Error> {
        log::trace!("Sending broker request: {:?}", req);
        let resp_rx = self
            .broker_tx
            .send_request(req)
            .await
            .context("failed to send broker request")?;

        let resp = resp_rx
            .await
            .context("broker response channel closed early")?;
        log::trace!("Got broker response: {:?}", resp);

        Ok(resp)
    }

    async fn transact_broker_burst_set(
        &self,
        id: String,
        state: SwitchToggleState,
        burst_props: SetModeBurstProperties,
    ) -> Result<BrokerResponse, anyhow::Error> {
        let interval = Duration::from_millis(burst_props.interval_ms as u64);
        let mut req = Some(BrokerRequest::Set(id, state));
        let mut resp = None;
        for i in 0..burst_props.limit {
            log::trace!(
                "Sending broker request (set burst attempt #{}): {:?}",
                i,
                req
            );
            let resp_rx = self
                .broker_tx
                .send_request(req.take().unwrap())
                .await
                .context("failed to send broker request")?;

            resp = Some(
                resp_rx
                    .await
                    .context("broker response channel closed early")?,
            );
            log::trace!("Got broker response: {:?}", resp);
            if let BrokerResponse::Set(.., status) = resp.as_ref().unwrap()
                && status.is_confirmed()
            {
                break;
            } else {
                tokio::time::sleep(interval).await;
            }
        }

        resp.ok_or(anyhow::anyhow!("burst limit is 0"))
    }
}

pub mod v1 {
    use crate::SwitchOperationalStatus;

    use super::*;

    #[derive(Debug, Clone)]
    pub enum Request {
        List,
        Get(GetReq),
        Set(SetReq),
    }

    #[derive(Debug, Clone, serde::Deserialize)]
    pub struct GetReq {
        pub id: String,
    }

    #[derive(Debug, Clone, serde::Deserialize)]
    pub struct SetReq {
        pub id: String,
        pub state: SwitchToggleState,

        #[serde(default = "set_req_mode_default")]
        pub mode: SetMode,
    }

    #[derive(Debug, Clone, Copy, serde::Deserialize)]
    #[serde(tag = "type", rename_all = "snake_case")]
    pub enum SetMode {
        Oneshot,
        Burst(SetModeBurstProperties),
    }

    impl SetMode {
        pub fn as_burst(&self) -> Option<SetModeBurstProperties> {
            match self {
                SetMode::Oneshot => None,
                SetMode::Burst(burst_props) => Some(*burst_props),
            }
        }
    }

    #[derive(Debug, Clone, Copy, serde::Deserialize)]
    pub struct SetModeBurstProperties {
        pub limit: u32,
        pub interval_ms: u32,
    }

    fn set_req_mode_default() -> SetMode {
        SetMode::Oneshot
    }

    #[derive(Debug, Clone)]
    pub enum Response {
        List(Vec<String>),
        Get(GetSetResp),
        Set(GetSetResp),
        Error(String),
    }

    #[derive(Debug, Clone, serde::Serialize)]
    pub struct GetSetResp {
        pub id: String,
        pub status: String,
        pub state: Option<SwitchToggleState>,
        pub msg: Option<String>,
    }

    impl From<(String, SwitchOperationalStatus)> for GetSetResp {
        fn from((id, status): (String, SwitchOperationalStatus)) -> Self {
            match status {
                SwitchOperationalStatus::Unknown => GetSetResp {
                    id,
                    status: "unknown".into(),
                    state: None,
                    msg: None,
                },
                SwitchOperationalStatus::Transitioning(state) => GetSetResp {
                    id,
                    status: "transitioning".into(),
                    state: Some(state),
                    msg: None,
                },
                SwitchOperationalStatus::Confirmed(state) => GetSetResp {
                    id,
                    status: "confirmed".into(),
                    state: Some(state),
                    msg: None,
                },
                SwitchOperationalStatus::Failure(error) => GetSetResp {
                    id,
                    status: "failure".into(),
                    state: None,
                    msg: Some(error.to_string()),
                },
            }
        }
    }
}
