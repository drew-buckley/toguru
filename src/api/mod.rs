use ahash::HashMap;
use http::StatusCode;
use serde::{Deserialize, Serialize};

use crate::ToggleState;

pub mod v1;

pub type TimestampNs = u128;

pub enum ApiPath {
    Versions,
    V1(v1::ApiPath),
}

pub struct Api {
    v1: v1::Api,
}

impl Api {
    pub async fn get(&self, path: ApiPath, params: HashMap<String, String>) -> Response {
        match path {
            ApiPath::Versions => Response::Versions(SUPPORTED_APIS),
            ApiPath::V1(path) => self.v1.get(path, params).await.into(),
        }
    }

    pub async fn set(&self, path: ApiPath, params: HashMap<String, String>) -> Response {
        match path {
            ApiPath::Versions => Response::Error(StatusCode::METHOD_NOT_ALLOWED),
            ApiPath::V1(path) => self.v1.set(path, params).await.into(),
        }
    }
}

const SUPPORTED_APIS: &[&str] = &["v1"];

pub enum Response {
    Versions(&'static [&'static str]),
    Api(ApiResponse),
    Error(StatusCode),
}

impl Response {
    pub fn get_status_code(&self) -> StatusCode {
        match self {
            Self::Versions(_) => StatusCode::OK,
            Self::Api(api_resp) => api_resp.get_status_code(),
            Self::Error(status_code) => *status_code,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "api", rename_all = "snake_case")]
pub enum ApiResponse {
    V1(v1::Response),
}

impl ApiResponse {
    fn get_status_code(&self) -> StatusCode {
        match self {
            ApiResponse::V1(v1) => v1.get_status_code(),
        }
    }
}
