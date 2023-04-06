use anyhow::Result;
use qdrant_client::client::QdrantClient;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::args::Args;
use crate::common::client::{
    create_collection, get_points_count, insert_points_batch, recreate_collection, search_points,
    wait_index,
};
use crate::common::coach_errors::CoachError;
use crate::common::coach_errors::CoachError::{Cancelled, Invariant};
use crate::drill_runner::Drill;
use async_trait::async_trait;

/// Drill that performs search requests on a collection.
/// The collection is created and populated with random data if it does not exist.
pub struct PointsSearch {
    collection_name: String,
    search_count: usize,
    points_count: usize,
    vec_dim: usize,
    payload_count: usize,
    stopped: Arc<AtomicBool>,
}

impl PointsSearch {
    pub fn new(stopped: Arc<AtomicBool>) -> Self {
        let collection_name = "points-search-drill".to_string();
        let vec_dim = 128;
        let payload_count = 2;
        let search_count = 1000;
        let points_count = 10000;
        PointsSearch {
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
impl Drill for PointsSearch {
    fn name(&self) -> String {
        "points_search".to_string()
    }

    fn reschedule_after_sec(&self) -> u64 {
        10
    }

    async fn run(&self, client: &QdrantClient, args: Arc<Args>) -> Result<(), CoachError> {
        // create and populate collection if it does not exists
        if !client.has_collection(&self.collection_name).await? {
            log::info!("The search drill needs to setup the collection first");
            create_collection(client, &self.collection_name, self.vec_dim, args.clone()).await?;

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
        }

        // waiting for green status
        wait_index(client, &self.collection_name, self.stopped.clone()).await?;

        // assert point count
        let points_count = get_points_count(client, &self.collection_name).await?;
        if points_count != self.points_count {
            return Err(Invariant(format!(
                "Collection has wrong number of points after insert {} vs {}",
                points_count, self.points_count
            )));
        }

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

        Ok(())
    }

    async fn before_all(&self, client: &QdrantClient, args: Arc<Args>) -> Result<(), CoachError> {
        // honor args.recreate_collection
        if args.recreate_collection {
            recreate_collection(client, &self.collection_name, self.vec_dim, args.clone()).await?;
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
        }
        Ok(())
    }
}
