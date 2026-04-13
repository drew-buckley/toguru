use tokio::sync::{mpsc, oneshot};

pub fn server_client<ReqT, RespT>(req_queue: usize) -> (Server<ReqT, RespT>, Client<ReqT, RespT>) {
    let (req_tx, req_rx) = mpsc::channel(req_queue);
    (Server::new(req_rx), Client::new(req_tx))
}

pub struct Server<ReqT, RespT> {
    req_rx: mpsc::Receiver<(ReqT, oneshot::Sender<RespT>)>,
}

impl<ReqT, RespT> Server<ReqT, RespT> {
    fn new(req_rx: mpsc::Receiver<(ReqT, oneshot::Sender<RespT>)>) -> Self {
        Self { req_rx }
    }

    pub async fn recv_req(&mut self) -> Option<(ReqT, oneshot::Sender<RespT>)> {
        self.req_rx.recv().await
    }
}

#[derive(Clone)]
pub struct Client<ReqT, RespT> {
    req_tx: mpsc::Sender<(ReqT, oneshot::Sender<RespT>)>,
}

impl<ReqT, RespT> Client<ReqT, RespT> {
    fn new(req_tx: mpsc::Sender<(ReqT, oneshot::Sender<RespT>)>) -> Self {
        Self { req_tx }
    }

    pub async fn send_request(
        &self,
        req: ReqT,
    ) -> Result<oneshot::Receiver<RespT>, error::SendError<ReqT>> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.req_tx
            .send((req, resp_tx))
            .await
            .map_err(|e| error::SendError::new(e.0.0))?;
        Ok(resp_rx)
    }
}

pub mod error {
    pub struct SendError<ReqT> {
        pub req: ReqT,
    }

    impl<ReqT> SendError<ReqT> {
        pub fn new(req: ReqT) -> Self {
            Self { req }
        }
    }

    impl<ReqT> std::fmt::Display for SendError<ReqT> {
        fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(fmt, "rrch server down")
        }
    }

    impl<T> std::fmt::Debug for SendError<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("SendError").finish_non_exhaustive()
        }
    }

    impl<ReqT> std::error::Error for SendError<ReqT> {}
}
