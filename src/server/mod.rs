use ahash::HashMap;
use anyhow::Context;
use axum::{
    Router,
    body::Body,
    extract::{OriginalUri, Path, Query, State},
    response::{IntoResponse, Response},
    routing::get,
};
use http::StatusCode;
use tokio::net::{TcpListener, ToSocketAddrs};

use crate::api::{Api, ApiPath, Response as ApiResponse, v1};

pub mod assets;

pub struct Server<B>
where
    B: ToSocketAddrs,
{
    bind: B,
    api: Api,
}

impl<B> Server<B>
where
    B: ToSocketAddrs,
{
    pub fn new(bind: B, api: Api) -> Self {
        Self { bind, api }
    }

    pub async fn run(self) -> Result<(), anyhow::Error> {
        let Self { bind, api } = self;
        let app = Router::new()
            .route("/api/versions", get(handle_get_versions))
            .route("/api/v1/meta", get(handle_get_v1_meta))
            .route("/api/v1/toggle", get(handle_get_v1_toggle_base))
            .route(
                "/api/v1/toggle/{id}",
                get(handle_get_v1_toggle).post(handle_post_v1_toggle),
            )
            .fallback(handle_fallback)
            .with_state(api);

        let listener = TcpListener::bind(bind)
            .await
            .expect("Could not bind prometheus server");
        axum::serve(listener, app)
            .await
            .context("server task failed")
    }
}

async fn handle_get_versions(
    State(api): State<Api>,
    Query(params): Query<HashMap<String, String>>,
) -> Response<Body> {
    api.get(ApiPath::Versions, params).await.into()
}

async fn handle_get_v1_meta(
    State(api): State<Api>,
    Query(params): Query<HashMap<String, String>>,
) -> Response<Body> {
    api.get(ApiPath::V1(v1::ApiPath::Meta), params).await.into()
}

async fn handle_get_v1_toggle_base(
    State(api): State<Api>,
    Query(params): Query<HashMap<String, String>>,
) -> Response<Body> {
    api.get(ApiPath::V1(v1::ApiPath::Toggle(None)), params)
        .await
        .into()
}

async fn handle_get_v1_toggle(
    State(api): State<Api>,
    Path(toggle_id): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Response<Body> {
    api.get(ApiPath::V1(v1::ApiPath::Toggle(Some(toggle_id))), params)
        .await
        .into()
}

async fn handle_post_v1_toggle(
    State(api): State<Api>,
    Path(toggle_id): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Response<Body> {
    api.post(ApiPath::V1(v1::ApiPath::Toggle(Some(toggle_id))), params)
        .await
        .into()
}

async fn handle_fallback(OriginalUri(_uri): OriginalUri) -> Response<Body> {
    (
        StatusCode::NOT_FOUND,
        [(http::header::CONTENT_TYPE, "text/html; charset=utf-8")],
        Body::from(assets::NOT_FOUND_PAGE_GENERIC),
    )
        .into_response()
}

impl From<ApiResponse> for Response<Body> {
    fn from(resp: ApiResponse) -> Self {
        (
            StatusCode::OK,
            [(
                http::header::CONTENT_TYPE,
                "application/json; charset=utf-8",
            )],
            Body::from(serde_json::to_string(&resp).unwrap_or_else(|e| {
                log::error!("Failed to serialize JSON: {:?}", e);
                "{}".into()
            })),
        )
            .into_response()
    }
}
