use anyhow::Result;
use qdrant_client::client::QdrantClient;
use qdrant_client::qdrant::point_id::PointIdOptions;
use qdrant_client::qdrant::{PointId, PointStruct};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::args::Args;
use crate::common::client::{create_collection, delete_points, get_points_count, wait_index};
use crate::common::generators::{random_payload, random_vector};
use crate::drill::Drill;
use async_trait::async_trait;

/// Drill that creates and deletes points in a collection.
/// The collection is created and populated with random data if it does not exist.
pub struct PointsChurn {
    client: Arc<QdrantClient>,
    collection_name: String,
    points_count: usize,
    vec_dim: usize,
    payload_count: usize,
    stopped: Arc<AtomicBool>,
}

impl PointsChurn {
    pub fn new(client: Arc<QdrantClient>, stopped: Arc<AtomicBool>) -> Self {
        let collection_name = "points-churn-drill".to_string();
        let vec_dim = 128;
        let payload_count = 2;
        let points_count = 10000;
        PointsChurn {
            client,
            collection_name,
            points_count,
            vec_dim,
            payload_count,
            stopped,
        }
    }

    pub async fn insert_points(&self) -> Result<(), anyhow::Error> {
        let batch_size = 100;
        let num_batches = self.points_count / batch_size;

        for batch_id in 0..num_batches {
            let mut points = Vec::new();
            for i in 0..batch_size {
                let idx = batch_id as u64 * batch_size as u64 + i as u64;

                let point_id: PointId = PointId {
                    point_id_options: Some(PointIdOptions::Num(idx)),
                };

                points.push(PointStruct::new(
                    point_id,
                    random_vector(self.vec_dim),
                    random_payload(Some(self.payload_count)),
                ));
            }
            if self.stopped.load(Ordering::Relaxed) {
                return Ok(());
            }

            // push batch
            self.client
                .upsert_points_blocking(&self.collection_name, points)
                .await?;
        }
        Ok(())
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

    async fn run(&self, args: Arc<Args>) -> Result<()> {
        // create and populate collection if it does not exists
        if !self.client.has_collection(&self.collection_name).await? {
            println!("The points churn drill needs to setup the collection first");
            create_collection(self.client.clone(), &self.collection_name, args.clone()).await?;
        }

        // index some points
        self.insert_points().await?;

        // waiting for green status
        wait_index(
            self.client.clone(),
            &self.collection_name,
            self.stopped.clone(),
        )
        .await?;

        // assert point count
        let points_count = get_points_count(self.client.clone(), &self.collection_name).await?;
        if points_count != self.points_count {
            return Err(anyhow::anyhow!(
                "Collection has wrong number of points after insert {} vs {}",
                points_count,
                self.points_count
            ));
        }

        // delete all points
        delete_points(
            self.client.clone(),
            &self.collection_name,
            self.points_count,
        )
        .await?;

        // assert point count
        let points_count = get_points_count(self.client.clone(), &self.collection_name).await?;
        if points_count != 0 {
            return Err(anyhow::anyhow!(
                "Collection should be empty but got {} points",
                points_count
            ));
        }

        Ok(())
    }
}
