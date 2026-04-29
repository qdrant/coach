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
use qdrant_client::config::QdrantConfig;
use rand::SeedableRng;
use rand::rngs::SmallRng;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Arc::new(Args::parse());
    setup_logger();

    // resolve the seed - pick a random one and log it if not provided so failures are reproducible
    let seed = args.seed.unwrap_or_else(rand::random);
    log::info!("Using random seed {seed}");
    let master_rng = SmallRng::seed_from_u64(seed);

    let cancel = CancellationToken::new();
    let r = cancel.clone();

    ctrlc::set_handler(move || {
        log::info!("Coach is stopping");
        r.cancel();
    })
    .expect("Error setting Ctrl-C handler");

    let mut handles = vec![];

    // start healthcheck (no random use)
    let healthcheck_handles = run_healthcheck(args.clone(), cancel.clone()).await?;
    handles.extend(healthcheck_handles);

    if !args.only_healthcheck {
        // start drills
        let drill_handles = run_drills(args, cancel, master_rng).await?;
        handles.extend(drill_handles);
    } else {
        log::info!("Only healthcheck is enabled, no drills will be executed");
    }

    // wait for completion
    for task in handles {
        task.await?;
    }
    Ok(())
}

fn get_config(url: &str, timeout_ms: usize, api_key: Option<&str>) -> QdrantConfig {
    let mut config = QdrantConfig::from_url(url);
    config.timeout = Duration::from_millis(timeout_ms as u64);
    config.connect_timeout = Duration::from_millis(timeout_ms as u64);
    if let Some(api_key) = api_key {
        config.set_api_key(api_key);
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
