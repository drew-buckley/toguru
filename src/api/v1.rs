use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ahash::HashMap;
use http::StatusCode;

use crate::engine::{
    Controller, ToggleDownStatus as ControllerDownToggleStatus,
    ToggleStatus as ControllerToggleStatus, ToggleUpStatus as ControllerUpToggleStatus,
    error::ControllerOperationError,
};

use super::*;

pub enum ApiPath {
    Meta,
    Toggle(Option<String>),
}

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

impl Response {
    fn new(timestamp_tsns: TimestampNs, body: ResponseBody) -> Self {
        Self {
            timestamp_tsns,
            body,
        }
    }

    pub fn get_status_code(&self) -> StatusCode {
        self.body.get_status_code()
    }
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

impl ResponseBody {
    fn get_status_code(&self) -> StatusCode {
        match self {
            ResponseBody::Null(OperationStatus::Error(err)) => err.get_status_code(),
            ResponseBody::ToggleList(OperationStatus::Error(err)) => err.get_status_code(),
            ResponseBody::ToggleGet(OperationStatus::Error(err)) => err.get_status_code(),
            ResponseBody::ToggleSet(OperationStatus::Error(err)) => err.get_status_code(),
            _ => StatusCode::OK,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToggleList {
    pub toggles: Vec<String>,
}

impl ToggleList {
    pub fn new(toggles: Vec<String>) -> Self {
        Self { toggles }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "toggle_status", rename_all = "snake_case")]
pub enum ToggleStatus {
    Unknown,
    Up(ToggleUpStatus),
    Down(ToggleDownStatus),
}

impl From<ControllerToggleStatus> for ToggleStatus {
    fn from(status: ControllerToggleStatus) -> Self {
        match status {
            ControllerToggleStatus::Up(up) => ToggleStatus::Up(up.into()),
            ControllerToggleStatus::Down(down) => ToggleStatus::Down(down.into()),
        }
    }
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

impl From<ControllerUpToggleStatus> for ToggleUpStatus {
    fn from(status: ControllerUpToggleStatus) -> Self {
        Self {
            id: status.id,
            observed_tsns: status
                .observed
                .timestamp
                .duration_since(UNIX_EPOCH)
                .unwrap_or(Duration::ZERO)
                .as_nanos(),
            observed_state: status.observed.state,
            commanded_tsns: status
                .commanded
                .timestamp
                .duration_since(UNIX_EPOCH)
                .unwrap_or(Duration::ZERO)
                .as_nanos(),
            commanded_state: status.commanded.state,
            extra_context: status.extra_context,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToggleDownStatus {
    pub last_operable_tsns: TimestampNs,
    pub failure_context: serde_json::Value,
}

impl From<ControllerDownToggleStatus> for ToggleDownStatus {
    fn from(status: ControllerDownToggleStatus) -> Self {
        Self {
            last_operable_tsns: status
                .last_operable
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos(),
            failure_context: status
                .error
                .map(|err| jsonify_err(err.as_ref()))
                .unwrap_or(serde_json::Value::Null),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "error_symbol", rename_all = "snake_case")]
pub enum ApiError {
    MethodNotAllowed,
    UnknownToggleId(String),
    MissingState,
    UnknownState(String),
    Internal(serde_json::Value),
}

impl ApiError {
    fn get_status_code(&self) -> StatusCode {
        match self {
            ApiError::MethodNotAllowed => StatusCode::METHOD_NOT_ALLOWED,
            ApiError::UnknownToggleId(_) => StatusCode::NOT_FOUND,
            ApiError::UnknownState(_) | ApiError::MissingState => StatusCode::BAD_REQUEST,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op_status", rename_all = "snake_case")]
pub enum OperationStatus<T: Clone> {
    Ok(T),
    Error(ApiError),
}

pub struct Api {
    controller: Controller,
}

impl Api {
    pub async fn get(&self, path: ApiPath, _: HashMap<String, String>) -> Response {
        let body = match path {
            ApiPath::Meta => todo!(),
            ApiPath::Toggle(None) => self.controller.list_toggles().await.into(),
            ApiPath::Toggle(Some(toggle_id)) => self.controller.get(toggle_id).await.into(),
        };
        Response::new(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or(Duration::ZERO)
                .as_nanos(),
            body,
        )
    }

    pub async fn set(&self, path: ApiPath, mut params: HashMap<String, String>) -> Response {
        let body = match path {
            ApiPath::Meta => method_not_allowed(),
            ApiPath::Toggle(None) => method_not_allowed(),
            ApiPath::Toggle(Some(toggle_id)) => {
                if let Some(state) = params.remove("state") {
                    if let Ok(state) = state.parse() {
                        self.controller.set(toggle_id, state).await.into()
                    } else {
                        ResponseBody::ToggleSet(OperationStatus::Error(ApiError::UnknownState(
                            state,
                        )))
                    }
                } else {
                    ResponseBody::ToggleSet(OperationStatus::Error(ApiError::MissingState))
                }
            }
        };
        Response::new(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or(Duration::ZERO)
                .as_nanos(),
            body,
        )
    }
}

impl From<Result<Vec<String>, ControllerOperationError>> for ResponseBody {
    fn from(res: Result<Vec<String>, ControllerOperationError>) -> Self {
        ResponseBody::ToggleList(match res {
            Ok(toggles) => OperationStatus::Ok(ToggleList::new(toggles)),
            Err(err) => OperationStatus::Error(err.into()),
        })
    }
}

impl From<Result<ControllerToggleStatus, ControllerOperationError>> for ResponseBody {
    fn from(res: Result<ControllerToggleStatus, ControllerOperationError>) -> Self {
        ResponseBody::ToggleGet(match res {
            Ok(status) => OperationStatus::Ok(status.into()),
            Err(err) => OperationStatus::Error(err.into()),
        })
    }
}

impl From<ControllerOperationError> for ApiError {
    fn from(err: ControllerOperationError) -> Self {
        match err {
            ControllerOperationError::UnknownToggleId(id) => ApiError::UnknownToggleId(id),
            ControllerOperationError::Internal(err) => ApiError::Internal(jsonify_err(err)),
        }
    }
}

fn jsonify_err(err: impl AsRef<dyn std::error::Error>) -> serde_json::Value {
    let mut err_msgs = Vec::with_capacity(16);
    let mut next_err = err.as_ref();
    loop {
        err_msgs.push(serde_json::Value::String(next_err.to_string()));
        if let Some(cause) = next_err.source() {
            next_err = cause;
        } else {
            break;
        }
    }

    serde_json::Value::Object(serde_json::Map::from_iter([(
        "error_stack".into(),
        serde_json::Value::Array(err_msgs),
    )]))
}

fn method_not_allowed() -> ResponseBody {
    ResponseBody::Null(OperationStatus::Error(ApiError::MethodNotAllowed))
}
