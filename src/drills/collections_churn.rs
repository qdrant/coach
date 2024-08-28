use crate::args::Args;
use crate::common::client::{
    create_collection, create_field_index, delete_collection, enable_indexing, get_collection_info,
    insert_points_batch,
};
use crate::common::coach_errors::CoachError;
use crate::common::coach_errors::CoachError::Cancelled;
use crate::common::generators::KEYWORD_PAYLOAD_KEY;
use crate::drill_runner::Drill;
use anyhow::Result;
use async_trait::async_trait;
use qdrant_client::client::QdrantClient;
use qdrant_client::qdrant::FieldType;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

/// Drill that keeps on creating and deleting the same collections
pub struct CollectionsChurn {
    base_collection_name: String,
    collection_count: usize,
    point_count: usize,
    vec_dim: usize,
    stopped: Arc<AtomicBool>,
}

impl CollectionsChurn {
    pub fn new(stopped: Arc<AtomicBool>) -> Self {
        let base_collection_name = "collection-churn-drill_".to_string();
        let collection_count = 10;
        let point_count = 10000;
        let vec_dim = 512;
        CollectionsChurn {
            base_collection_name,
            collection_count,
            point_count,
            vec_dim,
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
        // cleanup potential left-over previous collections
        for i in 0..self.collection_count {
            if self.stopped.load(Ordering::Relaxed) {
                return Err(Cancelled);
            }
            let collection_name = format!("{}{}", self.base_collection_name, i);
            // delete if already exists
            if client.collection_exists(&collection_name).await? {
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
            create_collection(client, &collection_name, self.vec_dim, args.clone()).await?;
            enable_indexing(client, &collection_name).await?;
            // insert a few points & trigger indexers
            insert_points_batch(
                client,
                &collection_name,
                self.point_count,
                self.vec_dim,
                2,
                None,
                self.stopped.clone(),
            )
            .await?;
            // create field index (non-blocking)
            create_field_index(
                client,
                &collection_name,
                KEYWORD_PAYLOAD_KEY,
                FieldType::Keyword,
            )
            .await?;
            let info = get_collection_info(client, &collection_name).await?;
            if info.is_none() {
                return Err(CoachError::Invariant(format!(
                    "Collection info for {} was not found after it was created",
                    collection_name
                )));
            }
        }

        // delete new collections
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
