use anyhow::Result;
use qdrant_client::qdrant::CollectionStatus;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::args::Args;
use crate::common::client::{
    create_collection, delete_collection, disable_indexing, enable_indexing, get_collection_status,
    get_points_count, insert_points_batch, search_points, wait_index,
};
use crate::common::coach_errors::CoachError;
use crate::common::coach_errors::CoachError::{Cancelled, Invariant};
use crate::drill_runner::Drill;
use async_trait::async_trait;
use qdrant_client::Qdrant;

/// Drill that performs index toggle on a collection.
/// The collection is always re-created and populated with random data.
pub struct ToggleIndexing {
    collection_name: String,
    search_count: usize,
    points_count: usize,
    vec_dim: usize,
    payload_count: usize,
    stopped: Arc<AtomicBool>,
}

impl ToggleIndexing {
    pub fn new(stopped: Arc<AtomicBool>) -> Self {
        let collection_name = "toggle-indexing-drill".to_string();
        let vec_dim = 128;
        let payload_count = 2;
        let search_count = 1000;
        let points_count = 20000; // large enough to trigger HNSW indexing
        ToggleIndexing {
            collection_name,
            search_count,
            points_count,
            vec_dim,
            payload_count,
            stopped,
        }
    }
}

#[async_trait]
impl Drill for ToggleIndexing {
    fn name(&self) -> String {
        "toggle_indexing".to_string()
    }

    fn reschedule_after_sec(&self) -> u64 {
        10
    }

    async fn run(&self, client: &Qdrant, args: Arc<Args>) -> Result<(), CoachError> {
        // delete if already exists
        if client.collection_exists(&self.collection_name).await? {
            delete_collection(client, &self.collection_name).await?;
        }

        // create collection
        create_collection(client, &self.collection_name, self.vec_dim, args.clone()).await?;

        // disable indexing
        disable_indexing(client, &self.collection_name).await?;

        // insert some points
        insert_points_batch(
            client,
            &self.collection_name,
            self.points_count,
            self.vec_dim,
            self.payload_count,
            None,
            self.stopped.clone(),
        )
        .await?;

        // assert point count
        let points_count = get_points_count(client, &self.collection_name).await?;
        if points_count != self.points_count {
            return Err(Invariant(format!(
                "Collection has wrong number of points after insert {} vs {}",
                points_count, self.points_count
            )));
        }

        // toggle indexing to build index in background while searching
        enable_indexing(client, &self.collection_name).await?;

        // search `search_count` times
        for _i in 0..self.search_count {
            if self.stopped.load(Ordering::Relaxed) {
                return Err(Cancelled);
            }
            let response = search_points(
                client,
                &self.collection_name,
                self.vec_dim,
                self.payload_count,
            )
            .await?;
            // assert not empty
            if response.result.is_empty() {
                return Err(Invariant("Search returned empty result".to_string()));
            }
        }

        // waiting for green status
        wait_index(client, &self.collection_name, self.stopped.clone()).await?;

        let collection_status = get_collection_status(client, &self.collection_name).await?;
        if collection_status != CollectionStatus::Green {
            return Err(Invariant(format!(
                "Collection status is not Green after indexing but {:?}",
                collection_status
            )));
        }

        delete_collection(client, &self.collection_name).await?;

        Ok(())
    }

    async fn before_all(&self, _client: &Qdrant, _args: Arc<Args>) -> Result<(), CoachError> {
        // no need to explicitly honor args.recreate_collection
        // because we are going to delete the collection anyway
        Ok(())
    }
}
