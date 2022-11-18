use anyhow::Result;
use async_trait::async_trait;
use qdrant_client::client::QdrantClient;
use qdrant_client::qdrant::vectors_config::Config;
use qdrant_client::qdrant::{CreateCollection, Distance, VectorParams, VectorsConfig};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

use crate::drill::Drill;

/// Drill that keeps on creating and deleting the same collection
pub struct CollectionChurn {
    client: Arc<QdrantClient>,
    collection_name: String,
    stopped: Arc<AtomicBool>,
}

impl CollectionChurn {
    pub fn new(client: Arc<QdrantClient>, stopped: Arc<AtomicBool>) -> Self {
        let collection_name = "collection-churn-drill".to_string();
        CollectionChurn {
            client,
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

    async fn run(&self) -> Result<()> {
        // delete if already exists
        if self.client.has_collection(&self.collection_name).await? {
            match self.client.delete_collection(&self.collection_name).await {
                Ok(_) => {}
                Err(e) => {
                    return Err(anyhow::anyhow!("Failed to delete collection: {}", e));
                }
            }
        }

        if self.stopped.load(Ordering::Relaxed) {
            return Ok(());
        }

        sleep(Duration::from_secs(1)).await;

        self.client
            .create_collection(&CreateCollection {
                collection_name: self.collection_name.to_string(),
                vectors_config: Some(VectorsConfig {
                    config: Some(Config::Params(VectorParams {
                        size: 128,
                        distance: Distance::Cosine.into(),
                    })),
                }),
                ..Default::default()
            })
            .await?;

        if self.stopped.load(Ordering::Relaxed) {
            return Ok(());
        }

        sleep(Duration::from_secs(1)).await;

        match self.client.delete_collection(&self.collection_name).await {
            Ok(_) => {}
            Err(e) => {
                return Err(anyhow::anyhow!("Failed to delete collection: {}", e));
            }
        }

        Ok(())
    }
}
