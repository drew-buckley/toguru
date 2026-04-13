use std::path::PathBuf;

use argh::FromArgs;
use env_logger::Env;

///
#[derive(FromArgs)]
struct Args {
    /// f
    #[argh(option, short = 'c', long = "config")]
    config: PathBuf,
}

#[tokio::main]
async fn main() {
    let args: Args = argh::from_env();
    init_log();

    log::info!("Toguru version: {}", env!("CARGO_PKG_VERSION"));
}

fn init_log() {
    use systemd_journal_logger::{JournalLog, connected_to_journal};
    const LOG_LEVEL_ENVVAR: &str = "LOG_LEVEL";
    const LOG_LEVEL_DEFAULT: &str = "info";
    let env = Env::new().filter_or(LOG_LEVEL_ENVVAR, LOG_LEVEL_DEFAULT);
    env_logger::init_from_env(env);
    if connected_to_journal() {
        JournalLog::new()
            .expect("Failed to initialize journald logger")
            .install()
            .expect("Failed install journald logger");
    }
}

mod config {
    use std::net::SocketAddr;

    use serde::Deserialize;

    #[derive(Debug, Clone, Deserialize)]
    pub struct Config {
        pub server: ServerConfig,
    }

    #[derive(Debug, Clone, Deserialize)]
    pub struct ServerConfig {
        pub bind: SocketAddr,
    }
}
