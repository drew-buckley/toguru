use serde::{Deserialize, Serialize};

pub async fn run_daemon(config: Config) -> Result<(), anyhow::Error> {
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {}
