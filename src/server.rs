use std::net::SocketAddr;

use anyhow::Context;
use axum::{
    Router,
    body::Body,
    extract::{Path, State},
    response::{IntoResponse, Response},
    routing::get,
};
use http::StatusCode;
use serde_json::json;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use crate::{
    StdTask, SwitchOperationalStatus,
    api::{self, Api},
};

pub struct Server {
    bind: SocketAddr,
    api: Api,
    cancel_token: CancellationToken,
}

impl StdTask for Server {
    async fn run(self) -> Result<(), anyhow::Error> {
        let app = Router::new()
            .route("api/v1/switch", get(get_switch_list_v1))
            .with_state(self.api.clone());
        let listener = TcpListener::bind(&self.bind)
            .await
            .context("Could not bind prometheus server")?;
        tokio::select! {
            res = axum::serve(listener, app) => {
                res.context("internal axum server failed")
            }

            _ = self.cancel_token.cancelled() => {
                log::info!("Prometheus exporter server closing due to signal");
                Ok(())
            }
        }
    }
}

const STD_CONTENT_TYPE_HEADER: &str = "text/plain; version=0.0.4; charset=utf-8";

// async fn get_switch_list_v1(Path(switch): Path<String>, State(api): State<Api>) -> Response<Body> {
async fn get_switch_list_v1(State(api): State<Api>) -> Response<Body> {
    log::debug!("API V1 request: list switches");

    let (code, body) = match api.transact(api::v1::Request::List.into()).await {
        Ok(resp) => match resp {
            api::Response::V1(api::v1::Response::List(switches)) => (
                StatusCode::OK,
                json!({
                    "status" : "ok",
                    "switches" : switches
                }),
            ),
            api::Response::V1(api::v1::Response::Error(err)) => (
                StatusCode::SERVICE_UNAVAILABLE,
                error_payload_v1(format!("failed to list switches: {}", err)),
            ),
            resp => {
                log::error!("Unexpected response from API subsystem: {:?}", resp);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    error_payload_v1("unexpected response from API subsystem"),
                )
            }
        },
        Err(err) => {
            log::error!("Failed to transact with API subsystem: {:?}", err);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                error_payload_v1("failed to transact with API subsystem"),
            )
        }
    };

    finalize_response_v1(code, body)
}

async fn get_switch_state_v1(Path(switch): Path<String>, State(api): State<Api>) -> Response<Body> {
    log::debug!("API V1 request: list switches");

    let (code, body) = match api
        .transact(api::v1::Request::Get(api::v1::GetReq::new(switch)).into())
        .await
    {
        Ok(resp) => match resp {
            api::Response::V1(api::v1::Response::Get(resp)) => {
                (StatusCode::OK, switch_state_resp_payload_v1(resp))
            }
            api::Response::V1(api::v1::Response::Error(err)) => (
                StatusCode::SERVICE_UNAVAILABLE,
                error_payload_v1(format!("failed to list switches: {}", err)),
            ),
            resp => {
                log::error!("Unexpected response from API subsystem: {:?}", resp);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    error_payload_v1("unexpected response from API subsystem"),
                )
            }
        },
        Err(err) => {
            log::error!("Failed to transact with API subsystem: {:?}", err);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                error_payload_v1("failed to transact with API subsystem"),
            )
        }
    };

    finalize_response_v1(code, body)
}

fn switch_state_resp_payload_v1(resp: api::v1::GetSetResp) -> serde_json::Value {
    let (status, state, failure_reason) = match resp.status {
        SwitchOperationalStatus::Unknown => ("unknown", None, None),
        SwitchOperationalStatus::Transitioning(state) => {
            ("transitioning", Some(state.to_string()), None)
        }
        SwitchOperationalStatus::Confirmed(state) => ("confirmed", Some(state.to_string()), None),
        SwitchOperationalStatus::Failure(error) => ("failure", None, Some(error.to_string())),
    };

    match (state, failure_reason) {
        (Some(state), None) => json!({
            "status" : "ok",
            "switch" : {
                "id" : resp.switch,
                "status" : status,
                "state" : state,
            }
        }),
        (None, Some(failure_reason)) => json!({
            "status" : "ok",
            "switch" : {
                "id" : resp.switch,
                "status" : status,
                "failure_reason" : failure_reason,
            }
        }),
        _ => unreachable!(),
    }
}

fn error_payload_v1(msg: impl AsRef<str>) -> serde_json::Value {
    json!({
        "status" : "error",
        "error_msg" : msg.as_ref(),
    })
}

fn finalize_response_v1(code: StatusCode, body: serde_json::Value) -> Response<Body> {
    let (code, body) = match serde_json::to_string(&body) {
        Ok(s) => (code, Body::from(s)),
        Err(err) => {
            log::error!("Failed to serialize response body to JSON: {:?}", err);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Body::from(
                    "{ \"status\" : \"error\", \"error_msg\" : \"failed to serialize body to JSON\" }",
                ),
            )
        }
    };
    (
        code,
        [(http::header::CONTENT_TYPE, STD_CONTENT_TYPE_HEADER)],
        body,
    )
        .into_response()
}
