mod args;
mod common;
mod drill_runner;
mod drills;
mod healthcheck;

use crate::drill_runner::run_drills;
use crate::healthcheck::run_healthcheck;
use args::Args;
use clap::Parser;
use env_logger::Target;
use qdrant_client::client::QdrantClientConfig;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    setup_logger();
    let stopped = Arc::new(AtomicBool::new(false));
    let r = stopped.clone();

    ctrlc::set_handler(move || {
        log::info!("Coach is stopping");
        r.store(true, Ordering::SeqCst);
    })
    .expect("Error setting Ctrl-C handler");

    let mut handles = vec![];

    // start healthcheck
    let healthcheck_handles = run_healthcheck(args.clone(), stopped.clone()).await?;
    for handle in healthcheck_handles {
        handles.push(handle);
    }

    if !args.only_healthcheck {
        // start drills
        let drill_handles = run_drills(args, stopped).await?;
        for handle in drill_handles {
            handles.push(handle);
        }
    } else {
        log::info!("Only healthcheck is enabled, no drills will be executed");
    }

    // wait for completion
    for task in handles {
        task.await?;
    }
    Ok(())
}

fn get_config(url: &str, timeout_ms: usize) -> QdrantClientConfig {
    let mut config = QdrantClientConfig::from_url(url);
    config.timeout = Duration::from_millis(timeout_ms as u64);
    config.connect_timeout = Duration::from_millis(timeout_ms as u64);

    let api_key = std::env::var("QDRANT_API_KEY").ok();

    if let Some(api_key) = api_key {
        config.set_api_key(&api_key);
    }
    config
}

pub fn setup_logger() {
    let mut log_builder = env_logger::Builder::new();

    log_builder
        .target(Target::Stdout)
        .format_timestamp_millis()
        .filter_level(log::LevelFilter::Info);

    log_builder.init();
}
