use crate::args::Args;
use crate::drills::collection_churn::CollectionChurn;
use crate::drills::points_churn::PointsChurn;
use crate::drills::points_search::PointsSearch;
use crate::drills::points_upsert::PointsUpdate;
use crate::get_config;
use anyhow::Result;
use async_trait::async_trait;
use qdrant_client::client::QdrantClient;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
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

pub async fn run_drills(args: Args, stopped: Arc<AtomicBool>) -> Result<()> {
    // all drills to run
    let all_drills: Vec<Box<dyn Drill>> = vec![
        Box::new(CollectionChurn::new(stopped.clone())),
        Box::new(PointsSearch::new(stopped.clone())),
        Box::new(PointsChurn::new(stopped.clone())),
        Box::new(PointsUpdate::new(stopped.clone())),
    ];

    let mut drill_tasks = vec![];

    println!(
        "Coach scheduling {} drills against {:?}:",
        all_drills.len(),
        args.uris
    );
    for drill in &all_drills {
        println!(
            "- {} (repeating after {} seconds)",
            drill.name(),
            drill.reschedule_after_sec()
        );
    }
    println!();

    // control the max number of concurrent drills running
    let max_drill_semaphore = Arc::new(Semaphore::new(args.parallel_drills));
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
                eprintln!(
                    "ERROR: Drill {} failed to run before_all caused by {}",
                    drill.name(),
                    e
                );
                if args_arc.stop_at_first_error {
                    eprintln!("Stopping coach because stop_at_first_error is set");
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
                // at least one successful run necessary
                let mut at_least_one_successful_run = false;

                // run drill against all uri sequentially
                println!("Running {}", drill.name());
                for uri in &args_arc.uris {
                    let drill_client = QdrantClient::new(Some(get_config(uri))).await.unwrap();
                    let execution_start = Instant::now();
                    let result = drill.run(&drill_client, args_arc.clone()).await;
                    match result {
                        Ok(_) => {
                            if let Some(_prev) = last_errors.remove(uri) {
                                println!("...{} is running again for {}", drill.name(), uri);
                            }
                            println!(
                                "...{} completed for {} in {:?}",
                                drill.name(),
                                uri,
                                execution_start.elapsed()
                            );
                            at_least_one_successful_run = true;
                        }
                        Err(e) => {
                            // do not spam logs with the same error
                            if let Some(_prev_error) = last_errors.get(uri) {
                                last_errors.insert(uri.to_string(), e);
                            } else {
                                eprintln!(
                                    "ERROR: Drill {} failed for {} caused by {}",
                                    drill.name(),
                                    uri,
                                    e
                                );
                                last_errors.insert(uri.to_string(), e);
                            }
                        }
                    }
                }

                if !at_least_one_successful_run {
                    eprintln!("ERROR: No successful runs for drill {}", drill.name());
                    // stop coach in case pf no successful run if configured
                    if args_arc.stop_at_first_error {
                        stopped.store(true, Ordering::Relaxed);
                    }
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
