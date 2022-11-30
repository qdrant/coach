use crate::args::Args;
use crate::get_config;
use anyhow::Result;
use log::error;
use log::info;
use qdrant_client::client::QdrantClient;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::time::Instant;

pub async fn run_healthcheck(args: Args, stopped: Arc<AtomicBool>) -> Result<JoinHandle<()>> {
    let healthcheck_task = tokio::spawn(async move {
        // record errors for deduplication
        let mut last_errors: HashMap<String, anyhow::Error> = HashMap::new();
        while !stopped.load(Ordering::Relaxed) {
            // contact all input uris
            for uri in &args.uris {
                if let Ok(client) = QdrantClient::new(Some(get_config(uri))).await {
                    let execution_start = Instant::now();
                    match client.health_check().await {
                        Ok(_) => {
                            // marking it as healthy
                            if let Some(_prev) = last_errors.remove(uri) {
                                info!("{} is healthy again ({:?})", uri, execution_start.elapsed());
                            }
                        }
                        Err(e) => {
                            // do not spam logs with the same error
                            if let Some(_prev_error) = last_errors.get(uri) {
                                last_errors.insert(uri.to_string(), e);
                            } else {
                                error!(
                                    "healthcheck failed for {} after {:?} ({})",
                                    uri,
                                    execution_start.elapsed(),
                                    e
                                );
                                last_errors.insert(uri.to_string(), e);
                            }
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(args.health_check_delay_ms as u64)).await;
            }
        }
    });
    Ok(healthcheck_task)
}
