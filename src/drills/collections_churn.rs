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

/// Drill that keeps on creating and deleting the same collections
pub struct CollectionsChurn {
    base_collection_name: String,
    collection_count: usize,
    stopped: Arc<AtomicBool>,
}

impl CollectionsChurn {
    pub fn new(stopped: Arc<AtomicBool>) -> Self {
        let base_collection_name = "collection-churn-drill_".to_string();
        let collection_count = 200;
        CollectionsChurn {
            base_collection_name,
            collection_count,
            stopped,
        }
    }
}

#[async_trait]
impl Drill for CollectionsChurn {
    fn name(&self) -> String {
        "collections_churn".to_string()
    }

    fn reschedule_after_sec(&self) -> u64 {
        10
    }

    async fn run(&self, client: &QdrantClient, args: Arc<Args>) -> Result<(), CoachError> {
        // cleanup potential left0over previous collections
        for i in 0..self.collection_count {
            if self.stopped.load(Ordering::Relaxed) {
                return Err(Cancelled);
            }
            let collection_name = format!("{}{}", self.base_collection_name, i);
            // delete if already exists
            if client.has_collection(&collection_name).await? {
                delete_collection(client, &collection_name).await?;
            }
        }

        sleep(Duration::from_secs(1)).await;

        // create new collections
        for i in 0..self.collection_count {
            if self.stopped.load(Ordering::Relaxed) {
                return Err(Cancelled);
            }
            let collection_name = format!("{}{}", self.base_collection_name, i);
            create_collection(client, &collection_name, 128, args.clone()).await?;
        }

        sleep(Duration::from_secs(1)).await;

        // create new collections
        for i in 0..self.collection_count {
            if self.stopped.load(Ordering::Relaxed) {
                return Err(Cancelled);
            }
            let collection_name = format!("{}{}", self.base_collection_name, i);
            delete_collection(client, &collection_name).await?;
        }

        Ok(())
    }

    async fn before_all(&self, _client: &QdrantClient, _args: Arc<Args>) -> Result<(), CoachError> {
        // no need to explicitly honor args.recreate_collection
        // because we are going to delete the collection anyway
        Ok(())
    }
}
