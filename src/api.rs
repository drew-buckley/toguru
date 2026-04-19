use std::str::FromStr;

use anyhow::Context;

use crate::{
    SwitchToggleState,
    broker::{BrokerRequest, BrokerResponse},
    rrch::Client,
};

#[derive(Debug, Clone, Copy)]
pub enum ApiVersion {
    V1,
}

impl FromStr for ApiVersion {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "v1" => Ok(Self::V1),
            _ => anyhow::bail!("not a valid api version string"),
        }
    }
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

#[derive(Clone)]
pub struct Api {
    broker_tx: Client<BrokerRequest, BrokerResponse>,
}

impl Api {
    pub async fn transact(&self, req: Request) -> Result<Response, anyhow::Error> {
        let api_version = req.version();
        let req = match req {
            Request::V1(req) => match req {
                v1::Request::List => BrokerRequest::List,
                v1::Request::Get(req) => BrokerRequest::Get(req.id),
                v1::Request::Set(req) => BrokerRequest::Set(req.id, req.state),
            },
        };

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

    impl GetReq {
        pub fn new(id: String) -> Self {
            Self { id }
        }
    }

    #[derive(Debug, Clone, serde::Deserialize)]
    pub struct SetReq {
        pub id: String,
        pub state: SwitchToggleState,
    }

    impl SetReq {
        pub fn new(id: String, state: SwitchToggleState) -> Self {
            Self { id, state }
        }
    }

    #[derive(Debug, Clone)]
    pub enum Response {
        List(Vec<String>),
        Get(GetSetResp),
        Set(GetSetResp),
        Error(String),
    }

    #[derive(Debug, Clone)]
    pub struct GetSetResp {
        pub switch: String,
        pub status: SwitchOperationalStatus,
    }

    impl From<(String, SwitchOperationalStatus)> for GetSetResp {
        fn from((switch, status): (String, SwitchOperationalStatus)) -> Self {
            GetSetResp { switch, status }
        }
    }
}
