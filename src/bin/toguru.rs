use std::{net::ToSocketAddrs, path::PathBuf};

use argh::FromArgs;

#[derive(Clone, FromArgs)]
/// Reac
struct Args {}

#[tokio::main]
async fn main() {
    let args: Args = argh::from_env();
    log::info!("Toguru version: {}", env!("CARGO_PKG_VERSION"));

    test("kernel.org:21")
}

fn test(addr: impl ToSocketAddrs) {
    for addr in addr.to_socket_addrs().unwrap() {
        println!("{}", addr);
    }
}
