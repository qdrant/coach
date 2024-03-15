use anyhow::Result;
use qdrant_client::client::QdrantClient;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::args::Args;
use crate::common::client::{
    create_collection, delete_collection, delete_point_by_id, get_point_by_id, search_points,
    set_payload, upsert_point_by_id,
};
use crate::common::coach_errors::CoachError;
use crate::common::coach_errors::CoachError::Cancelled;
use crate::drill_runner::Drill;
use crate::get_config;
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
    write_ordering: Option<WriteOrdering>,
    stopped: Arc<AtomicBool>,
}

impl HighConcurrency {
    pub fn new(stopped: Arc<AtomicBool>) -> Self {
        let collection_name = "high-concurrency".to_string();
        let concurrency_level = 400;
        let number_iterations = 40000;
        let vec_dim = 128;
        let payload_count = 2;
        let write_ordering = None; // default
        HighConcurrency {
            collection_name,
            concurrency_level,
            number_iterations,
            vec_dim,
            payload_count,
            write_ordering,
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
            self.write_ordering.clone(),
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
            self.write_ordering.clone(),
        )
        .await?;

        // updating point by id
        upsert_point_by_id(
            client,
            &self.collection_name,
            point_id,
            self.vec_dim,
            self.payload_count,
            self.write_ordering.clone(),
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

    fn pick_random<'a>(&self, clients: &'a [QdrantClient]) -> &'a QdrantClient {
        let index = rand::random::<usize>() % clients.len();
        &clients[index]
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
        if client.collection_exists(&self.collection_name).await? {
            delete_collection(client, &self.collection_name).await?;
        }

        // create collection
        create_collection(client, &self.collection_name, self.vec_dim, args.clone()).await?;

        // workers will target random clients for all nodes to stress the cluster
        let mut target_clients = vec![];
        for uri in &args.uris {
            let target_client = QdrantClient::new(Some(get_config(uri, args.grpc_timeout_ms)))?;
            target_clients.push(target_client);
        }

        // lazy stream of futures
        let query_stream = (0..self.number_iterations)
            .map(|n| self.run_for_point(self.pick_random(&target_clients), n as u64));

        // at most self.concurrency_level futures will be running at the same time
        let mut upsert_stream =
            futures::stream::iter(query_stream).buffer_unordered(self.concurrency_level);

        // consume stream
        while let Some(result) = upsert_stream.next().await {
            // stop consuming stream on shutdown
            if self.stopped.load(Ordering::Relaxed) {
                return Err(Cancelled);
            }
            // stop consuming stream on first error
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
