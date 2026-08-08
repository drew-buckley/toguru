use ahash::HashMap;
use serde::{Deserialize, Serialize};

use crate::ToggleState;

pub mod v1;

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
    pub async fn get(&self, path: ApiPath, params: HashMap<String, String>) -> Response {
        match path {
            ApiPath::Versions => Response::Versions(SUPPORTED_APIS),
            ApiPath::V1(path) => self.v1.get(path, params).await.into(),
        }
    }

    pub async fn set(&self, path: ApiPath, params: HashMap<String, String>) -> Response {
        match path {
            ApiPath::Versions => Response::Versions(SUPPORTED_APIS),
            ApiPath::V1(path) => self.v1.get(path, params).await.into(),
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
