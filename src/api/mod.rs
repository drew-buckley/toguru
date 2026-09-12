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

#[derive(Clone)]
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

    pub async fn post(&self, path: ApiPath, params: HashMap<String, String>) -> Response {
        match path {
            ApiPath::Versions => Response::Error(StatusCode::METHOD_NOT_ALLOWED),
            ApiPath::V1(path) => self.v1.post(path, params).await.into(),
        }
    }
}

const SUPPORTED_APIS: &[&str] = &["v1"];

pub enum Response {
    Versions(&'static [&'static str]),
    Api(VersionedApiResponse),
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

impl Serialize for Response {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Self::Versions(versions) => versions.serialize(serializer),
            Self::Api(resp) => resp.serialize(serializer),
            Self::Error(_) => serde_json::Value::Null.serialize(serializer),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "api", rename_all = "snake_case")]
pub enum VersionedApiResponse {
    V1(v1::Response),
}

impl VersionedApiResponse {
    fn get_status_code(&self) -> StatusCode {
        match self {
            VersionedApiResponse::V1(v1) => v1.get_status_code(),
        }
    }
}

/*
 serde_json::Value::Array(
                versions
                    .into_iter()
                    .map(|v| serde_json::Value::String((*v).into()))
                    .collect(),
            )
*/
