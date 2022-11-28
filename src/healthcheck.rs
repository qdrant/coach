use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use anyhow::Result;
use qdrant_client::client::QdrantClient;
use serde_json::Value;
use tokio::task::JoinHandle;
use crate::args::Args;
use crate::get_config;

pub async fn run_healthcheck(args: Args, stopped: Arc<AtomicBool>) -> Result<JoinHandle<()>> {
    // client
    let client = QdrantClient::new(Some(get_config(&args))).await?;
    let client_arc = Arc::new(client);
    let args_arc = Arc::new(args);

    let healthcheck_task = tokio::spawn(async move {
        while !stopped.load(Ordering::Relaxed) {
            match client_arc.health_check().await {
                Ok(_) => (),
                Err(e) => {
                    eprintln!("Failed to ping health check for {} {}", args_arc.uri, e)
                },
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });
    Ok(healthcheck_task)
}

// TODO fetch peers and check that they are healthy in distributed mode?
pub async fn _distributed_mode_enabled(args: Arc<Args>) -> Result<bool> {
    let resp: Value  = reqwest::get(format!("{}/cluster", args.http_uri))
        .await?
        .json()
        .await?;

    let status = &resp["result"]["status"];
    Ok(status.as_str() != Some("disabled"))
}
