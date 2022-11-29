use crate::args::Args;
use crate::drills::collection_churn::CollectionChurn;
use crate::drills::points_churn::PointsChurn;
use crate::drills::points_search::PointsSearch;
use crate::drills::points_upsert::PointsUpdate;
use crate::get_config;
use anyhow::Result;
use async_trait::async_trait;
use log::debug;
use log::error;
use log::info;
use log::warn;
use qdrant_client::client::QdrantClient;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio::time::Instant;

/// A drill is a single test that is run periodically
#[async_trait]
pub trait Drill: Send + Sync {
    // name of the drill
    fn name(&self) -> String;
    // delay between runs
    fn reschedule_after_sec(&self) -> u64;
    // run drill
    async fn run(&self, client: &QdrantClient, args: Arc<Args>) -> Result<()>;
    // run before the first run
    async fn before_all(&self, client: &QdrantClient, args: Arc<Args>) -> Result<()>;
}

struct DrillReport {
    uri: String,
    duration: Duration,
    error: Option<String>,
}

pub async fn run_drills(args: Args, stopped: Arc<AtomicBool>) -> Result<()> {
    // all drills to run
    let all_drills: Vec<Box<dyn Drill>> = vec![
        Box::new(CollectionChurn::new(stopped.clone())),
        Box::new(PointsSearch::new(stopped.clone())),
        Box::new(PointsChurn::new(stopped.clone())),
        Box::new(PointsUpdate::new(stopped.clone())),
    ];

    let mut drill_tasks = vec![];

    info!(
        "Coach is scheduling {} drills against {:?}:",
        all_drills.len(),
        args.uris
    );
    for drill in &all_drills {
        info!(
            "- {} (repeating after {} seconds)",
            drill.name(),
            drill.reschedule_after_sec()
        );
    }
    // control the max number of concurrent drills running
    let max_drill_semaphore = Arc::new(Semaphore::new(args.parallel_drills));
    let uris_len = args.uris.len();
    let args_arc = Arc::new(args);
    let first_uri = &args_arc.uris.first().expect("Not empty per construction");
    let before_client = QdrantClient::new(Some(get_config(first_uri))).await?;
    // pick first uri to run before_all
    let before_client_arc = Arc::new(before_client);
    // run drills
    for drill in all_drills {
        let stopped = stopped.clone();
        let drill_semaphore = max_drill_semaphore.clone();
        let args_arc = args_arc.clone();
        let before_client_arc = before_client_arc.clone();

        let task = tokio::spawn(async move {
            // before drill
            let before_res = drill.before_all(&before_client_arc, args_arc.clone()).await;
            if let Err(e) = before_res {
                error!(
                    "Drill {} failed to run before_all caused by {}",
                    drill.name(),
                    e
                );
                if args_arc.stop_at_first_error {
                    error!("Stopping coach because stop_at_first_error is set");
                    stopped.store(true, Ordering::Relaxed);
                }
                // stop drill
                return;
            }

            // record errors for deduplication
            let mut last_errors: HashMap<String, anyhow::Error> = HashMap::new();

            // loop drill run
            while !stopped.load(Ordering::Relaxed) {
                // acquire semaphore to run
                let run_permit = drill_semaphore.acquire().await.unwrap();
                let mut drill_reports = vec![];
                // run drill against all uri sequentially
                debug!("Starting {}", drill.name());
                for uri in &args_arc.uris {
                    let drill_client = QdrantClient::new(Some(get_config(uri))).await.unwrap();
                    let execution_start = Instant::now();
                    let result = drill.run(&drill_client, args_arc.clone()).await;
                    match result {
                        Ok(_) => {
                            if let Some(_prev) = last_errors.remove(uri) {
                                warn!("{} is working again for {}", drill.name(), uri);
                            }
                            drill_reports.push(DrillReport {
                                uri: uri.to_string(),
                                duration: execution_start.elapsed(),
                                error: None,
                            });
                        }
                        Err(e) => {
                            drill_reports.push(DrillReport {
                                uri: uri.to_string(),
                                duration: execution_start.elapsed(),
                                error: Some(e.to_string()),
                            });
                            // do not spam logs with the same error
                            if let Some(_prev_error) = last_errors.get(uri) {
                                last_errors.insert(uri.to_string(), e);
                            } else {
                                // print warning the first time
                                warn!("{} started to fail for {}", drill.name(), uri);
                                last_errors.insert(uri.to_string(), e);
                            }
                        }
                    }
                }

                // at least one successful run necessary
                let successful_runs = drill_reports.iter().filter(|r| r.error.is_none()).count();

                if successful_runs == 0 {
                    let mut display_report =
                        format!("{} failed for all {} uris - ", drill.name(), uris_len);
                    for r in drill_reports {
                        if let Some(e) = r.error {
                            display_report.push_str(&format!("{} ({}), ", r.uri, e));
                        }
                    }
                    error!("{}", display_report);
                    // stop coach in case pf no successful run if configured
                    if args_arc.stop_at_first_error {
                        stopped.store(true, Ordering::Relaxed);
                    }
                } else if successful_runs == uris_len {
                    info!(
                        "{} finished successfully in {} seconds",
                        drill.name(),
                        drill_reports
                            .iter()
                            .map(|r| r.duration.as_secs())
                            .sum::<u64>()
                    );
                } else {
                    let mut display_report = format!("{} failed partially - ", drill.name());
                    for r in drill_reports {
                        if let Some(e) = r.error {
                            display_report.push_str(&format!("{} ({}), ", r.uri, e));
                        }
                    }
                    info!("{}", display_report);
                }

                // release semaphore while waiting for reschedule
                drop(run_permit);
                let reschedule_start = Instant::now();
                while reschedule_start.elapsed().as_secs() < drill.reschedule_after_sec() {
                    if stopped.load(Ordering::Relaxed) {
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            }
        });
        drill_tasks.push(task);
    }

    for task in drill_tasks {
        task.await?;
    }

    Ok(())
}
