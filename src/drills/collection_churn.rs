use crate::args::Args;
use crate::common::client::{create_collection, delete_collection};
use crate::common::coach_errors::CoachError;
use crate::common::coach_errors::CoachError::Cancelled;
use anyhow::Result;
use async_trait::async_trait;
use qdrant_client::client::QdrantClient;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

use crate::drill_runner::Drill;

/// Drill that keeps on creating and deleting the same collection
pub struct CollectionChurn {
    collection_name: String,
    stopped: Arc<AtomicBool>,
}

impl CollectionChurn {
    pub fn new(stopped: Arc<AtomicBool>) -> Self {
        let collection_name = "collection-churn-drill".to_string();
        CollectionChurn {
            collection_name,
            stopped,
        }
    }
}

#[async_trait]
impl Drill for CollectionChurn {
    fn name(&self) -> String {
        "collection_churn".to_string()
    }

    fn reschedule_after_sec(&self) -> u64 {
        10
    }

    async fn run(&self, client: &QdrantClient, args: Arc<Args>) -> Result<(), CoachError> {
        // delete if already exists
        if client.has_collection(&self.collection_name).await? {
            delete_collection(client, &self.collection_name).await?;
        }

        if self.stopped.load(Ordering::Relaxed) {
            return Err(Cancelled);
        }

        sleep(Duration::from_secs(1)).await;

        create_collection(client, &self.collection_name, 128, args.clone()).await?;

        if self.stopped.load(Ordering::Relaxed) {
            return Err(Cancelled);
        }

        sleep(Duration::from_secs(1)).await;

        delete_collection(client, &self.collection_name).await?;

        Ok(())
    }

    async fn before_all(&self, _client: &QdrantClient, _args: Arc<Args>) -> Result<(), CoachError> {
        // no need to explicitly honor args.recreate_collection
        // because we are going to delete the collection anyway
        Ok(())
    }
}
