mod args;
mod common;
mod drill_runner;
mod drills;
mod healthcheck;

use crate::drill_runner::run_drills;
use crate::healthcheck::run_healthcheck;
use args::Args;
use clap::Parser;
use qdrant_client::client::QdrantClientConfig;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let stopped = Arc::new(AtomicBool::new(false));
    let r = stopped.clone();

    ctrlc::set_handler(move || {
        r.store(true, Ordering::SeqCst);
    })
    .expect("Error setting Ctrl-C handler");

    // start healthcheck
    let healthcheck_handle = run_healthcheck(args.clone(), stopped.clone())
        .await
        .unwrap();
    // start drills
    run_drills(args, stopped).await.unwrap();
    healthcheck_handle.await.unwrap();
}

fn get_config(url: &str) -> QdrantClientConfig {
    let mut config = QdrantClientConfig::from_url(url);
    let api_key = std::env::var("QDRANT_API_KEY").ok();

    if let Some(api_key) = api_key {
        config.set_api_key(&api_key);
    }
    config
}
