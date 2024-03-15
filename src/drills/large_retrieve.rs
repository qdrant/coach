use anyhow::Result;
use qdrant_client::client::QdrantClient;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::args::Args;
use crate::common::client::{
    create_collection, get_points_count, insert_points_batch, recreate_collection, retrieve_points,
};
use crate::common::coach_errors::CoachError;
use crate::common::coach_errors::CoachError::{Cancelled, Invariant};
use crate::drill_runner::Drill;
use async_trait::async_trait;

/// Drill that performs large retrieve requests on a collection.
/// The collection is created and populated with random data if it does not exist.
pub struct LargeRetrieve {
    collection_name: String,
    retrieve_count: usize,
    points_count: usize,
    vec_dim: usize,
    payload_count: usize,
    stopped: Arc<AtomicBool>,
}

impl LargeRetrieve {
    pub fn new(stopped: Arc<AtomicBool>) -> Self {
        let collection_name = "large-retrieve-drill".to_string();
        let vec_dim = 1536;
        let payload_count = 2;
        let retrieve_count = 100;
        let points_count = 2000;
        LargeRetrieve {
            collection_name,
            retrieve_count,
            points_count,
            vec_dim,
            payload_count,
            stopped,
        }
    }
}

#[async_trait]
impl Drill for LargeRetrieve {
    fn name(&self) -> String {
        "large_retrieve".to_string()
    }

    fn reschedule_after_sec(&self) -> u64 {
        10
    }

    async fn run(&self, client: &QdrantClient, args: Arc<Args>) -> Result<(), CoachError> {
        // create and populate collection if it does not exist
        if !client.collection_exists(&self.collection_name).await? {
            log::info!("The large retrieve drill needs to setup the collection first");
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

        // assert point count
        let points_count = get_points_count(client, &self.collection_name).await?;
        if points_count != self.points_count {
            return Err(Invariant(format!(
                "Collection has wrong number of points after insert {} vs {}",
                points_count, self.points_count
            )));
        }

        // retrieve all points at once
        let ids: Vec<_> = (1..self.points_count - 1).collect();

        // retrieve `retrieve_count` times
        for _i in 0..self.retrieve_count {
            if self.stopped.load(Ordering::Relaxed) {
                return Err(Cancelled);
            }
            let response = retrieve_points(client, &self.collection_name, &ids).await?;
            // assert not empty
            if response.result.len() != ids.len() {
                return Err(Invariant(format!(
                    "Retrieve did not return all result {}",
                    response.result.len()
                )));
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
