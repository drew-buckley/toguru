use serde::{Deserialize, Serialize};

use crate::ToggleState;

pub type TimestampNs = u128;

pub enum ApiPath {
    Versions,
    V1(ApiV1Path),
}

pub enum ApiV1Path {
    Meta,
    Toggle(Option<String>),
}

pub struct Api {
    v1: v1::Api,
}

impl Api {
    pub async fn request(&self, path: ApiPath) -> Response {
        match path {
            ApiPath::Versions => Response::Versions(SUPPORTED_APIS),
            ApiPath::V1(path) => self.v1.request(path).await.into(),
        }
    }
}

const SUPPORTED_APIS: &[&str] = &["v1"];

pub enum Response {
    Versions(&'static [&'static str]),
    Api(ApiResponse),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "api", rename_all = "snake_case")]
pub enum ApiResponse {
    V1(v1::Response),
}

pub mod v1 {
    use super::*;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct ToggleGetRequest {
        id: String,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct ToggleSetRequest {
        id: String,
        state: ToggleState,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct Response {
        timestamp_tsns: TimestampNs,
        #[serde(flatten)]
        body: ResponseBody,
    }

    impl Into<super::Response> for Response {
        fn into(self) -> super::Response {
            super::Response::Api(ApiResponse::V1(self))
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(tag = "op_type", rename_all = "snake_case")]
    pub enum ResponseBody {
        Null(OperationStatus<()>),
        ToggleList(OperationStatus<ToggleList>),
        ToggleGet(OperationStatus<ToggleStatus>),
        ToggleSet(OperationStatus<ToggleStatus>),
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct ToggleList {
        toggles: Vec<String>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(tag = "toggle_status", rename_all = "snake_case")]
    pub enum ToggleStatus {
        Unknown,
        Up(ToggleUpStatus),
        Down(ToggleDownStatus),
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct ToggleUpStatus {
        pub id: String,
        pub observed_tsns: TimestampNs,
        pub observed_state: ToggleState,
        pub commanded_tsns: TimestampNs,
        pub commanded_state: ToggleState,
        pub extra_context: serde_json::Value,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct ToggleDownStatus {
        pub last_operable_tsns: TimestampNs,
        pub failure_context: serde_json::Value,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(tag = "error_symbol", rename_all = "snake_case")]
    pub enum ApiError {
        Internal(serde_json::Value),
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(tag = "op_status", rename_all = "snake_case")]
    pub enum OperationStatus<T: Clone> {
        Ok(T),
        Error(ApiError),
    }

    pub struct Api {}

    impl Api {
        pub async fn request(&self, path: ApiV1Path) -> Response {
            todo!()
        }
    }
}
