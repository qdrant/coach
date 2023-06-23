use crate::args::Args;
use crate::common::coach_errors::CoachError;
use crate::drills::collection_churn::CollectionsChurn;
use crate::drills::collection_concurrent_lifecycle::CollectionConcurrentLifecycle;
use crate::drills::collection_snapshots_churn::CollectionSnapshotsChurn;
use crate::drills::high_concurrency::HighConcurrency;
use crate::drills::large_retrieve::LargeRetrieve;
use crate::drills::points_churn::PointsChurn;
use crate::drills::points_optional_vectors::PointsOptionalVectors;
use crate::drills::points_search::PointsSearch;
use crate::drills::points_upsert::PointsUpdate;
use crate::drills::toggle_indexing::ToggleIndexing;
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
use tokio::task::JoinHandle;
use tokio::time::Instant;

/// A drill is a single test that is run periodically
#[async_trait]
pub trait Drill: Send + Sync {
    // name of the drill
    fn name(&self) -> String;
    // delay between runs
    fn reschedule_after_sec(&self) -> u64;
    // run drill
    async fn run(&self, client: &QdrantClient, args: Arc<Args>) -> Result<(), CoachError>;
    // run before the first run
    async fn before_all(&self, client: &QdrantClient, args: Arc<Args>) -> Result<(), CoachError>;
}

struct DrillReport {
    uri: String,
    duration: Duration,
    error: Option<String>,
}

pub async fn run_drills(args: Args, stopped: Arc<AtomicBool>) -> Result<Vec<JoinHandle<()>>> {
    // all drills known to coach
    let all_drills: Vec<Box<dyn Drill>> = vec![
        Box::new(CollectionsChurn::new(stopped.clone())),
        Box::new(PointsSearch::new(stopped.clone())),
        Box::new(PointsChurn::new(stopped.clone())),
        Box::new(PointsUpdate::new(stopped.clone())),
        Box::new(CollectionSnapshotsChurn::new(stopped.clone())),
        Box::new(ToggleIndexing::new(stopped.clone())),
        Box::new(HighConcurrency::new(stopped.clone())),
        Box::new(LargeRetrieve::new(stopped.clone())),
        Box::new(CollectionConcurrentLifecycle::new(stopped.clone())),
        Box::new(PointsOptionalVectors::new(stopped.clone())),
    ];

    // filter drills by name
    let drills_to_run = if !args.drills_to_run.is_empty() {
        all_drills
            .into_iter()
            .filter(|d| args.drills_to_run.contains(&d.name()))
            .collect::<Vec<_>>()
    } else {
        all_drills
            .into_iter()
            .filter(|d| !args.ignored_drills.contains(&d.name()))
            .collect::<Vec<_>>()
    };

    let mut drill_tasks = vec![];

    info!(
        "Coach is scheduling {} drills against {:?} (by batch of {}):",
        drills_to_run.len(),
        args.uris,
        args.parallel_drills,
    );
    for drill in &drills_to_run {
        info!(
            "- {} (repeating after {} seconds)",
            drill.name(),
            drill.reschedule_after_sec(),
        );
    }
    // control the max number of concurrent drills running
    let max_drill_semaphore = Arc::new(Semaphore::new(args.parallel_drills));
    let uris_len = args.uris.len();
    let args_arc = Arc::new(args);
    let first_uri = &args_arc.uris.first().expect("Not empty per construction");
    let before_client = QdrantClient::new(Some(get_config(first_uri, args_arc.grpc_timeout_ms)))?;
    // pick first uri to run before_all
    let before_client_arc = Arc::new(before_client);
    // run drills
    for drill in drills_to_run {
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
                    info!("Stopping coach because stop_at_first_error is set");
                    stopped.store(true, Ordering::Relaxed);
                }
                // stop drill
                return;
            }

            // record errors for deduplication
            let mut last_errors: HashMap<String, CoachError> = HashMap::new();

            // loop drill run
            loop {
                // acquire semaphore to run
                let run_permit = drill_semaphore.acquire().await.unwrap();
                let mut drill_reports = vec![];
                // run drill against all uri sequentially
                debug!("Starting {}", drill.name());
                for uri in &args_arc.uris {
                    if stopped.load(Ordering::Relaxed) {
                        // stop drill
                        return;
                    }
                    let drill_client =
                        QdrantClient::new(Some(get_config(uri, args_arc.grpc_timeout_ms))).unwrap();
                    let execution_start = Instant::now();
                    let result = drill.run(&drill_client, args_arc.clone()).await;
                    match result {
                        Ok(_) => {
                            if let Some(_prev) = last_errors.remove(uri) {
                                info!("{} is working again for {}", drill.name(), uri);
                            }
                            drill_reports.push(DrillReport {
                                uri: uri.to_string(),
                                duration: execution_start.elapsed(),
                                error: None,
                            })
                        }
                        Err(e @ CoachError::Client(_) | e @ CoachError::Invariant(_)) => {
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
                                warn!("{} started to fail for {} with {}", drill.name(), uri, e);
                                last_errors.insert(uri.to_string(), e);
                            }
                        }
                        Err(CoachError::Cancelled) => (),
                    };
                }

                if stopped.load(Ordering::Relaxed) {
                    // drill canceled
                    return;
                }

                // at least one successful run necessary
                let successful_runs = drill_reports.iter().filter(|r| r.error.is_none()).count();

                if successful_runs == 0 {
                    let mut display_report = format!("{} failed completely - ", drill.name());
                    for r in drill_reports {
                        if let Some(e) = r.error {
                            display_report
                                .push_str(&format!("{} in {:?} ({}), ", r.uri, r.duration, e));
                        }
                    }
                    error!("{}", display_report);
                    // stop coach in case pf no successful run if configured
                    if args_arc.stop_at_first_error {
                        info!("Stopping coach because stop_at_first_error is set");
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
                    warn!("{}", display_report);
                }

                // release semaphore while waiting for reschedule
                drop(run_permit);
                let reschedule_start = Instant::now();
                while reschedule_start.elapsed().as_secs() < drill.reschedule_after_sec() {
                    if stopped.load(Ordering::Relaxed) {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        });
        drill_tasks.push(task);
    }

    Ok(drill_tasks)
}
