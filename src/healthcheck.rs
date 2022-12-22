use crate::args::Args;
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
    let mut healthcheck_tasks = vec![];

    // one task per uri
    for uri in args.uris {
        let stopped = stopped.clone();
        let handle = tokio::spawn(async move {
            // record errors for deduplication
            let mut last_errors: Option<anyhow::Error> = None;
            let mut failures = 0;
            // track latencies in the range [1 msec..1 hour]
            let mut hist = Histogram::<u64>::new_with_bounds(1, 60 * 60 * 1000, 2).unwrap();
            while !stopped.load(Ordering::Relaxed) {
                // contact all input uris
                if let Ok(client) =
                    QdrantClient::new(Some(get_config(&uri, args.grpc_health_check_timeout_ms)))
                        .await
                {
                    let execution_start = Instant::now();
                    match client.health_check().await {
                        Ok(_) => {
                            // marking it as healthy
                            if let Some(_prev) = last_errors.take() {
                                info!(
                                    "{} is healthy again after {} failures ({:?})",
                                    uri,
                                    failures,
                                    execution_start.elapsed()
                                );
                                failures = 0;
                            }
                        }
                        Err(e) => {
                            // do not spam logs with the same error
                            if let Some(_prev_error) = &last_errors {
                                last_errors = Some(e)
                            } else {
                                let min = hist.min();
                                let p50 = hist.value_at_quantile(0.5);
                                let p99 = hist.value_at_quantile(0.99);
                                let max = hist.max();
                                error!(
                                    "healthcheck failed for {} after {:?} [min: {}, p50: {}ms, p99: {}ms, max: {}ms] ({})",
                                    uri,
                                    execution_start.elapsed(),
                                    min,
                                    p50,
                                    p99,
                                    max,
                                    e
                                );
                                last_errors = Some(e)
                            }
                            failures += 1;
                        }
                    }
                    // record latency in ms
                    hist.record_correct(
                        execution_start.elapsed().as_millis() as u64,
                        args.health_check_delay_ms as u64,
                    )
                    .unwrap();
                }
                // delay between checks
                tokio::time::sleep(Duration::from_millis(args.health_check_delay_ms as u64)).await;
            }
        });
        healthcheck_tasks.push(handle);
    }

    Ok(healthcheck_tasks)
}
