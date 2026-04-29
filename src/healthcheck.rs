use crate::args::Args;
use crate::common::histograms::render_histogram;
use crate::get_config;
use anyhow::Result;
use hdrhistogram::Histogram;
use log::error;
use log::info;
use qdrant_client::Qdrant;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

pub async fn run_healthcheck(
    args: Arc<Args>,
    cancel: CancellationToken,
) -> Result<Vec<JoinHandle<()>>> {
    // build one client per URI up front - propagate config errors before spawning
    // use 1 minute as upper bound timeout for client healthcheck
    let mut clients: Vec<(String, Qdrant)> = Vec::with_capacity(args.uris.len());
    for uri in &args.uris {
        let client = Qdrant::new(get_config(uri, 60 * 1000, args.api_key.as_deref()))?;
        clients.push((uri.clone(), client));
    }
    // validate timeout manually
    let max_healthcheck_timeout_ms = args.grpc_health_check_timeout_ms as u64;
    let health_check_delay_ms = Duration::from_millis(args.health_check_delay_ms as u64);

    let mut healthcheck_tasks = Vec::with_capacity(clients.len());
    // one task per uri
    for (uri, client) in clients {
        let cancel = cancel.clone();
        let handle = tokio::spawn(async move {
            // record errors for deduplication
            let mut last_error: Option<anyhow::Error> = None;
            let mut failures = 0;
            // track latencies in the range [1 msec..1 hour]
            let mut hist = Histogram::<u64>::new_with_bounds(1, 60 * 60 * 1000, 2).unwrap();
            let healthcheck_timeout = Duration::from_millis(max_healthcheck_timeout_ms);
            while !cancel.is_cancelled() {
                let execution_start = Instant::now();
                let health_check_result =
                    tokio::time::timeout(healthcheck_timeout, client.health_check()).await;
                let elapsed_millis = execution_start.elapsed().as_millis() as u64;
                // record latency in ms
                hist.record(elapsed_millis)
                    .expect("Failed to record latency");
                match health_check_result {
                    Ok(Ok(_)) => {
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
                    }
                    Ok(Err(e)) => {
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
                        last_error = Some(e.into());
                        failures += 1;
                    }
                    Err(_elapsed) => {
                        // do not spam logs with errors if it has already failed
                        if last_error.is_none() {
                            error!(
                                "healthcheck for {} timed out after {}ms {}",
                                uri,
                                max_healthcheck_timeout_ms,
                                render_histogram(&hist)
                            );
                        }
                        // always set last error and increment failures
                        last_error = Some(anyhow::Error::msg("healthcheck timeout"));
                        failures += 1;
                    }
                }
                // delay between checks - exit early on cancel
                tokio::select! {
                    _ = tokio::time::sleep(health_check_delay_ms) => {}
                    _ = cancel.cancelled() => break,
                }
            }
        });
        healthcheck_tasks.push(handle);
    }

    Ok(healthcheck_tasks)
}
