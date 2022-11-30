use anyhow::Result;
use qdrant_client::client::QdrantClient;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use crate::args::Args;
use crate::common::client::{
    create_collection, delete_points, get_points_count, insert_points, recreate_collection,
    wait_index,
};
use crate::drill_runner::Drill;
use async_trait::async_trait;

/// Drill that creates and deletes points in a collection.
/// The collection is created and populated with random data if it does not exist.
pub struct PointsChurn {
    collection_name: String,
    points_count: usize,
    vec_dim: usize,
    payload_count: usize,
    stopped: Arc<AtomicBool>,
}

impl PointsChurn {
    pub fn new(stopped: Arc<AtomicBool>) -> Self {
        let collection_name = "points-churn-drill".to_string();
        let vec_dim = 128;
        let payload_count = 2;
        let points_count = 10000;
        PointsChurn {
            collection_name,
            points_count,
            vec_dim,
            payload_count,
            stopped,
        }
    }
}

#[async_trait]
impl Drill for PointsChurn {
    fn name(&self) -> String {
        "points_churn".to_string()
    }

    fn reschedule_after_sec(&self) -> u64 {
        10
    }

    async fn run(&self, client: &QdrantClient, args: Arc<Args>) -> Result<()> {
        // create and populate collection if it does not exists
        if !client.has_collection(&self.collection_name).await? {
            log::info!("The points churn drill needs to setup the collection first");
            create_collection(client, &self.collection_name, args.clone()).await?;
        }

        // insert some points
        insert_points(
            client,
            &self.collection_name,
            self.points_count,
            self.vec_dim,
            self.payload_count,
            self.stopped.clone(),
        )
        .await?;

        // waiting for green status
        wait_index(client, &self.collection_name, self.stopped.clone()).await?;

        // assert point count
        let points_count = get_points_count(client, &self.collection_name).await?;
        if points_count != self.points_count {
            return Err(anyhow::anyhow!(
                "Collection has wrong number of points after insert {} vs {}",
                points_count,
                self.points_count
            ));
        }

        // delete all points
        delete_points(client, &self.collection_name, self.points_count).await?;

        // assert point count
        let points_count = get_points_count(client, &self.collection_name).await?;
        if points_count != 0 {
            return Err(anyhow::anyhow!(
                "Collection should be empty but got {} points",
                points_count
            ));
        }

        Ok(())
    }

    async fn before_all(&self, client: &QdrantClient, args: Arc<Args>) -> Result<()> {
        // honor args.recreate_collection
        if args.recreate_collection {
            recreate_collection(client, &self.collection_name, args.clone()).await?;
        }
        Ok(())
    }
}
