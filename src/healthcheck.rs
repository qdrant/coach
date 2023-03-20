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
            // use `grpc_health_check_timeout_ms` as client timeout
            let client_config = get_config(&uri, args.grpc_health_check_timeout_ms);
            let client = QdrantClient::new(Some(client_config)).await;
            // fails only if the configuration is invalid
            if let Err(e) = client {
                error!("Failed to create healthcheck client for {}: {}", uri, e);
                return;
            }
            let client = client.unwrap();
            // record errors for deduplication
            let mut last_errors: Option<anyhow::Error> = None;
            let mut failures = 0;
            // track latencies in the range [1 msec..1 hour]
            let mut hist = Histogram::<u64>::new_with_bounds(1, 60 * 60 * 1000, 2).unwrap();
            while !stopped.load(Ordering::Relaxed) {
                let execution_start = Instant::now();
                let health_check_result = client.health_check().await;
                let elapsed = execution_start.elapsed();
                // record latency in ms
                hist.record(elapsed.as_millis() as u64)
                    .expect("Failed to record latency");
                match health_check_result {
                    Ok(_) => {
                        // marking it as healthy
                        if let Some(_prev) = last_errors.take() {
                            info!(
                                "{} is healthy again after {} consecutive failures {}",
                                uri,
                                failures,
                                render_histogram(&hist)
                            );
                            failures = 0;
                        }
                    }
                    Err(e) => {
                        // do not spam logs with errors if it has already failed
                        if let Some(_prev_error) = &last_errors {
                            last_errors = Some(e)
                        } else {
                            error!(
                                "healthcheck failed for {} after {:?} {} ({})",
                                uri,
                                elapsed,
                                render_histogram(&hist),
                                e
                            );
                            last_errors = Some(e)
                        }
                        failures += 1;
                    }
                }
                // delay between checks
                tokio::time::sleep(Duration::from_millis(args.health_check_delay_ms as u64)).await;
            }
        });
        healthcheck_tasks.push(handle);
    }

    Ok(healthcheck_tasks)
}
