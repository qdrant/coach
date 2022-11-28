use crate::args::Args;
use crate::drills::collection_churn::CollectionChurn;
use crate::drills::points_churn::PointsChurn;
use crate::drills::points_search::PointsSearch;
use crate::drills::points_upsert::PointsUpdate;
use crate::get_config;
use anyhow::Result;
use async_trait::async_trait;
use qdrant_client::client::QdrantClient;
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
    async fn run(&self, args: Arc<Args>) -> Result<()>;
    // run before the first run
    async fn before_all(&self, args: Arc<Args>) -> Result<()>;
}

pub async fn run_drills(args: Args, stopped: Arc<AtomicBool>) -> Result<()> {
    // client
    let client = QdrantClient::new(Some(get_config(&args))).await?;
    let client_arc = Arc::new(client);

    // all drills to run
    let all_drills: Vec<Box<dyn Drill>> = vec![
        Box::new(CollectionChurn::new(client_arc.clone(), stopped.clone())),
        Box::new(PointsSearch::new(client_arc.clone(), stopped.clone())),
        Box::new(PointsChurn::new(client_arc.clone(), stopped.clone())),
        Box::new(PointsUpdate::new(client_arc.clone(), stopped.clone())),
    ];

    let mut drill_tasks = vec![];

    println!(
        "Coach scheduling {} drills against {}:",
        all_drills.len(),
        args.uri
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

    // run drills
    for drill in all_drills {
        let stopped = stopped.clone();
        let drill_semaphore = max_drill_semaphore.clone();
        let drill_name = drill.name();
        let delay_seconds = drill.reschedule_after_sec();
        let args_arc = args_arc.clone();
        let task = tokio::spawn(async move {
            // before drill
            let before_res = drill.before_all(args_arc.clone()).await;
            if let Err(e) = before_res {
                eprintln!(
                    "ERROR: Drill {} failed to run before_all\ncaused by {}",
                    drill_name, e
                );
                if args_arc.stop_at_first_error {
                    eprintln!("Stopping coach because stop_at_first_error is set");
                    stopped.store(true, Ordering::Relaxed);
                }
                // stop drill
                return;
            }

            // loop drill run
            while !stopped.load(Ordering::Relaxed) {
                // acquire semaphore to run
                let run_permit = drill_semaphore.acquire().await.unwrap();
                println!("Running {}", drill_name);
                let execution_start = Instant::now();
                let result = drill.run(args_arc.clone()).await;
                match result {
                    Ok(_) => {
                        println!(
                            "...{} completed: {:?}",
                            drill_name,
                            execution_start.elapsed()
                        );
                    }
                    Err(e) => {
                        eprintln!("ERROR: Drill {} failed\ncaused by {}", drill_name, e);
                        if args_arc.stop_at_first_error {
                            stopped.store(true, Ordering::Relaxed);
                        }
                    }
                }
                // release semaphore while waiting for reschedule
                drop(run_permit);
                let reschedule_start = Instant::now();
                while reschedule_start.elapsed().as_secs() < delay_seconds {
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
