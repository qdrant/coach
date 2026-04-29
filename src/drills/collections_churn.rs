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
use qdrant_client::Qdrant;
use qdrant_client::qdrant::FieldType;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

/// Drill that keeps on creating and deleting the same collections
pub struct CollectionsChurn {
    base_collection_name: String,
    collection_count: usize,
    point_count: usize,
    vec_dim: usize,
    stopped: CancellationToken,
}

impl CollectionsChurn {
    pub fn new(stopped: CancellationToken) -> Self {
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
    fn name(&self) -> &'static str {
        "collections_churn"
    }

    fn reschedule_after_sec(&self) -> u64 {
        10
    }

    async fn run(&self, client: &Qdrant, args: Arc<Args>) -> Result<(), CoachError> {
        // cleanup potential left-over previous collections
        for i in 0..self.collection_count {
            if self.stopped.is_cancelled() {
                return Err(Cancelled);
            }
            let collection_name = format!("{}{}", self.base_collection_name, i);
            // delete if already exists
            if client.collection_exists(&collection_name).await? {
                delete_collection(client, &collection_name).await?;
            }
        }

        // create new collections
        for i in 0..self.collection_count {
            if self.stopped.is_cancelled() {
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
            create_field_index(
                client,
                &collection_name,
                KEYWORD_PAYLOAD_KEY,
                FieldType::Keyword,
            )
            .await?;
            let info = get_collection_info(client, &collection_name)
                .await?
                .ok_or_else(|| {
                    CoachError::Invariant(format!(
                        "Collection info for {collection_name} was not found after it was created"
                    ))
                })?;
            // verify the field index actually registered, not just that the API call succeeded
            if !info.payload_schema.contains_key(KEYWORD_PAYLOAD_KEY) {
                return Err(CoachError::Invariant(format!(
                    "Field index for {KEYWORD_PAYLOAD_KEY} missing from {collection_name} payload_schema"
                )));
            }
        }

        // delete new collections
        for i in 0..self.collection_count {
            if self.stopped.is_cancelled() {
                return Err(Cancelled);
            }
            let collection_name = format!("{}{}", self.base_collection_name, i);
            delete_collection(client, &collection_name).await?;
            // verify the collection is actually gone
            if client.collection_exists(&collection_name).await? {
                return Err(CoachError::Invariant(format!(
                    "{collection_name} still exists after delete"
                )));
            }
        }

        Ok(())
    }

    async fn before_all(&self, _client: &Qdrant, _args: Arc<Args>) -> Result<(), CoachError> {
        // no need to explicitly honor args.recreate_collection
        // because we are going to delete the collection anyway
        Ok(())
    }
}
