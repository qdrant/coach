use crate::args::Args;
use crate::common::histograms::render_histogram;
use crate::get_config;
use anyhow::Result;
use hdrhistogram::Histogram;
use log::error;
use log::info;
use qdrant_client::client::QdrantClient;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::time::Instant;

pub async fn run_healthcheck(args: Args, stopped: Arc<AtomicBool>) -> Result<Vec<JoinHandle<()>>> {
    let mut healthcheck_tasks = Vec::with_capacity(args.uris.len());
    // one task per uri
    for uri in args.uris {
        let stopped = stopped.clone();
        let handle = tokio::spawn(async move {
            // use 1 minute as upper bound timeout for client healthcheck
            let client_config = get_config(&uri, 60 * 1000);
            // validate timeout manually
            let max_healthcheck_timeout_ms = args.grpc_health_check_timeout_ms as u64;
            let client = QdrantClient::new(Some(client_config)).await;
            // fails only if the configuration is invalid
            if let Err(e) = client {
                error!("Failed to create healthcheck client for {}: {}", uri, e);
                return;
            }
            // safe unwrap
            let client = client.unwrap();
            let health_check_delay_ms = Duration::from_millis(args.health_check_delay_ms as u64);
            // record errors for deduplication
            let mut last_error: Option<anyhow::Error> = None;
            let mut failures = 0;
            // track latencies in the range [1 msec..1 hour]
            let mut hist = Histogram::<u64>::new_with_bounds(1, 60 * 60 * 1000, 2).unwrap();
            while !stopped.load(Ordering::Relaxed) {
                let execution_start = Instant::now();
                let health_check_result = client.health_check().await;
                let elapsed_millis = execution_start.elapsed().as_millis() as u64;
                // record latency in ms
                hist.record(elapsed_millis)
                    .expect("Failed to record latency");
                match health_check_result {
                    Ok(_) => {
                        // check if healthcheck took too long
                        if elapsed_millis <= max_healthcheck_timeout_ms {
                            // marking it as healthy by removing possible previous error
                            if let Some(_prev) = last_error.take() {
                                info!(
                                    "{} is healthy again after {} consecutive failures {}",
                                    uri,
                                    failures,
                                    render_histogram(&hist)
                                );
                                failures = 0;
                            }
                        } else {
                            // do not spam logs with errors if it has already failed
                            if last_error.is_none() {
                                error!(
                                    "healthcheck for {} took too long with {}ms {}",
                                    uri,
                                    elapsed_millis,
                                    render_histogram(&hist)
                                );
                            }
                            // always set last error and increment failures
                            last_error = Some(anyhow::Error::msg("healthcheck timeout"));
                            failures += 1;
                        }
                    }
                    Err(e) => {
                        // do not spam logs with errors if it has already failed
                        if last_error.is_none() {
                            error!(
                                "healthcheck failed for {} after {}ms {} ({})",
                                uri,
                                elapsed_millis,
                                render_histogram(&hist),
                                e
                            );
                        }
                        // always set last error and increment failures
                        last_error = Some(e);
                        failures += 1;
                    }
                }
                // delay between checks
                tokio::time::sleep(health_check_delay_ms).await;
            }
        });
        healthcheck_tasks.push(handle);
    }

    Ok(healthcheck_tasks)
}
