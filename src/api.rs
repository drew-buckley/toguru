use anyhow::Context;
use tokio::sync::oneshot;

use crate::{
    SwitchToggleState,
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
    pub async fn send(&self, req: Request) -> Result<ApiTransaction, anyhow::Error> {
        match req {
            Request::V1(req) => match req {
                v1::Request::List => BrokerRequest::List,
                v1::Request::Get(req) => BrokerRequest::Get(req.id),
                v1::Request::Set(req) => BrokerRequest::Set(req.id, req.state),
            },
        }
        self.broker_tx
            .send_request(req)
            .await
            .context("failed to send broker request")?;
        todo!()
    }
}

pub struct ApiTransaction {
    version: ApiVersion,
    resp_rx: oneshot::Receiver<BrokerResponse>,
}

impl ApiTransaction {
    fn new(version: ApiVersion, resp_rx: oneshot::Receiver<BrokerResponse>) -> Self {
        Self { version, resp_rx }
    }

    pub async fn recv(self) -> Result<Response, anyhow::Error> {
        let resp = self
            .resp_rx
            .await
            .context("broker response channel closed early")?;

        let resp = match resp {
            BrokerResponse::List(switches) => match self.version {
                ApiVersion::V1 => v1::Response::List(switches).into(),
            },
            BrokerResponse::Get(id, status) => match self.version {
                ApiVersion::V1 => v1::Response::Get((id, status).into()).into(),
            },
            BrokerResponse::Set(id, status) => match self.version {
                ApiVersion::V1 => v1::Response::Set((id, status).into()).into(),
            },
            BrokerResponse::Error(error) => match self.version {
                ApiVersion::V1 => v1::Response::Error(error.to_string()).into(),
            },
        };

        Ok(resp)
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
