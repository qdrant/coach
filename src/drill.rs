use crate::args::Args;
use crate::drills::collection_churn::CollectionChurn;
use crate::drills::search::Search;
use crate::get_config;
use anyhow::Result;
use async_trait::async_trait;
use qdrant_client::client::QdrantClient;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::time::Instant;

/// A drill is a single test that is run periodically
#[async_trait]
pub trait Drill: Send + Sync {
    // name of the drill
    fn name(&self) -> String;
    // delay between runs
    fn reschedule_after_sec(&self) -> u64;
    // run drill
    async fn run(&self) -> Result<()>;
}

pub async fn run_drills(args: Args, stopped: Arc<AtomicBool>) -> Result<()> {
    // client
    let client = QdrantClient::new(Some(get_config(&args))).await?;
    let client_arc = Arc::new(client);

    // all drills to run
    let all_drills: Vec<Box<dyn Drill>> = vec![
        Box::new(CollectionChurn::new(client_arc.clone(), stopped.clone())),
        Box::new(Search::new(client_arc, stopped.clone())),
    ];

    let mut drill_tasks = vec![];

    println!("Scheduling {} drills:", all_drills.len());
    for drill in &all_drills {
        println!(
            "{} after {} seconds",
            drill.name(),
            drill.reschedule_after_sec()
        );
    }
    println!();

    // run drills
    for drill in all_drills {
        let stopped = stopped.clone();
        let task = tokio::spawn(async move {
            while !stopped.load(Ordering::Relaxed) {
                let name = drill.name();
                let delay_seconds = drill.reschedule_after_sec();
                println!("Running {}", name);
                let execution_start = Instant::now();
                let result = drill.run().await;
                match result {
                    Ok(_) => {
                        println!("...{} completed: {:?}", name, execution_start.elapsed());
                    }
                    Err(e) => {
                        println!("Drill {} failed: {}", name, e);
                    }
                }
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
