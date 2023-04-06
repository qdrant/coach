use anyhow::Result;
use qdrant_client::client::QdrantClient;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::args::Args;
use crate::common::client::{
    create_collection, delete_collection, delete_point_by_id, disable_indexing, get_point_by_id,
    search_points, set_payload, upsert_point_by_id,
};
use crate::common::coach_errors::CoachError;
use crate::drill_runner::Drill;
use async_trait::async_trait;
use futures::StreamExt;
use qdrant_client::qdrant::WriteOrdering;

/// Drill that performs operations on a collection with a high level of concurrency (without indexing).
/// Run `concurrency_level` workers which repeatedly call APIs for inserting -> searching -> set payload -> updating -> getting one -> deleting
/// The collection is created and populated with random data if it does not exist.
pub struct HighConcurrency {
    collection_name: String,
    concurrency_level: usize,
    number_iterations: usize,
    vec_dim: usize,
    payload_count: usize,
    write_concurrency: Option<WriteOrdering>,
    stopped: Arc<AtomicBool>,
}

impl HighConcurrency {
    pub fn new(stopped: Arc<AtomicBool>) -> Self {
        let collection_name = "high-concurrency".to_string();
        let concurrency_level = 300;
        let number_iterations = 30000;
        let vec_dim = 128;
        let payload_count = 2;
        let write_concurrency = None; // default
        HighConcurrency {
            collection_name,
            concurrency_level,
            number_iterations,
            vec_dim,
            payload_count,
            write_concurrency,
            stopped,
        }
    }

    async fn run_for_point(&self, client: &QdrantClient, point_id: u64) -> Result<(), CoachError> {
        // insert single point
        upsert_point_by_id(
            client,
            &self.collection_name,
            point_id,
            self.vec_dim,
            self.payload_count,
            self.write_concurrency.clone(),
        )
        .await?;

        // search random points
        search_points(
            client,
            &self.collection_name,
            self.vec_dim,
            self.payload_count,
        )
        .await?;

        // set random payload on point
        set_payload(
            client,
            &self.collection_name,
            point_id,
            self.payload_count,
            self.write_concurrency.clone(),
        )
        .await?;

        // updating point by id
        upsert_point_by_id(
            client,
            &self.collection_name,
            point_id,
            self.vec_dim,
            self.payload_count,
            self.write_concurrency.clone(),
        )
        .await?;

        // getting point by id
        let retrieved = get_point_by_id(client, &self.collection_name, point_id).await?;
        if retrieved.is_none() {
            return Err(CoachError::Invariant(format!(
                "The point {} was not found after it was inserted in {}",
                point_id, self.collection_name
            )));
        }

        // delete point by id
        delete_point_by_id(client, &self.collection_name, point_id).await?;

        Ok(())
    }
}

#[async_trait]
impl Drill for HighConcurrency {
    fn name(&self) -> String {
        "high_concurrency".to_string()
    }

    fn reschedule_after_sec(&self) -> u64 {
        10
    }

    async fn run(&self, client: &QdrantClient, args: Arc<Args>) -> Result<(), CoachError> {
        // delete if already exists
        if client.has_collection(&self.collection_name).await? {
            delete_collection(client, &self.collection_name).await?;
        }

        // create collection
        create_collection(client, &self.collection_name, args.clone()).await?;

        // disable HNSW indexing (just in case)
        disable_indexing(client, &self.collection_name).await?;

        // lazy stream of futures
        let query_stream = (0..self.number_iterations)
            .take_while(|_| !self.stopped.load(Ordering::Relaxed))
            .map(|n| self.run_for_point(client, n as u64));

        let mut upsert_stream =
            futures::stream::iter(query_stream).buffer_unordered(self.concurrency_level);
        while let Some(result) = upsert_stream.next().await {
            // stop on first error
            result?;
        }

        // delete collection to not accumulate data over time
        delete_collection(client, &self.collection_name).await?;

        Ok(())
    }

    async fn before_all(&self, _client: &QdrantClient, _args: Arc<Args>) -> Result<(), CoachError> {
        // no need to explicitly honor args.recreate_collection
        // because we are going to delete the collection anyway
        Ok(())
    }
}
