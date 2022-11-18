mod args;
mod common;
mod drill;
mod drills;

use crate::drill::run_drills;
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

    run_drills(args, stopped).await.unwrap();
}

fn get_config(args: &Args) -> QdrantClientConfig {
    let mut config = QdrantClientConfig::from_url(&args.uri);
    let api_key = std::env::var("QDRANT_API_KEY").ok();

    if let Some(api_key) = api_key {
        config.set_api_key(&api_key);
    }
    config
}
