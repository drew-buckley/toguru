use std::net::SocketAddr;

use axum::{Router, body::Body, extract::State, response::Response, routing::get};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use crate::StdTask;

pub struct Server {
    bind: SocketAddr,
    cancel_token: CancellationToken,
}

impl StdTask for Server {
    async fn run(self) -> Result<(), anyhow::Error> {
        let app = Router::new()
            .route(&self.path, get(handle))
            .with_state(self.prometheus_registry);
        let listener = TcpListener::bind(&self.socket)
            .await
            .context("Could not bind prometheus server")?;
        tokio::select! {
            res = axum::serve(listener, app) => {
                res
            }

            _ = self.cancel_token.cancelled() => {
                log::info!("Prometheus exporter server closing due to signal");
                Ok(())
            }
        }
    }
}

async fn get_switch(State(state): State<u32>) -> Response<Body> {
    log::debug!("Handling axum request");

    // panic here since an join error, in this case, will mean the task itself panicked
    .expect("failed to join encoder blocking task");

    let buffer = match res {
        Ok(buffer) => buffer,
        Err(err) => {
            log::error!("failed to encode metrics: {:?}", err);
            return (StatusCode::INTERNAL_SERVER_ERROR).into_response();
        }
    };
    (
        StatusCode::OK,
        [(
            http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        Body::from(buffer),
    )
}
