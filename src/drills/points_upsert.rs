use anyhow::Result;
use qdrant_client::client::QdrantClient;
use qdrant_client::qdrant::point_id::PointIdOptions;
use qdrant_client::qdrant::{PointId, PointStruct};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::args::Args;
use crate::common::client::{create_collection, get_points_count, recreate_collection, wait_index};
use crate::common::generators::{random_payload, random_vector};
use crate::drill_runner::Drill;
use async_trait::async_trait;

/// Drill that consistently updates the same points.
/// The collection is created and populated with random data if it does not exist.
pub struct PointsUpdate {
    collection_name: String,
    points_count: usize,
    vec_dim: usize,
    payload_count: usize,
    stopped: Arc<AtomicBool>,
}

impl PointsUpdate {
    pub fn new(stopped: Arc<AtomicBool>) -> Self {
        let collection_name = "points-update-drill".to_string();
        let vec_dim = 128;
        let payload_count = 2;
        let points_count = 10000;
        PointsUpdate {
            collection_name,
            points_count,
            vec_dim,
            payload_count,
            stopped,
        }
    }

    pub async fn insert_points(&self, client: &QdrantClient) -> Result<(), anyhow::Error> {
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
            client.upsert_points(&self.collection_name, points).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl Drill for PointsUpdate {
    fn name(&self) -> String {
        "points_update".to_string()
    }

    fn reschedule_after_sec(&self) -> u64 {
        10
    }

    async fn run(&self, client: &QdrantClient, args: Arc<Args>) -> Result<()> {
        // create if does not exists
        if !client.has_collection(&self.collection_name).await? {
            println!("The update drill needs to setup the collection first");
            create_collection(client, &self.collection_name, args.clone()).await?;

            // index some points
            self.insert_points(client).await?;
        }

        // waiting for green status
        wait_index(client, &self.collection_name, self.stopped.clone()).await?;

        // assert point count
        let collection_info = client
            .collection_info(&self.collection_name)
            .await?
            .result
            .unwrap();
        if collection_info.points_count != self.points_count as u64 {
            return Err(anyhow::anyhow!("Collection has wrong number of points"));
        }

        // update
        self.insert_points(client).await?;

        // assert point count
        let points_count = get_points_count(client, &self.collection_name).await?;
        if points_count != self.points_count {
            return Err(anyhow::anyhow!(
                "Collection has wrong number of points after insert {} vs {}",
                points_count,
                self.points_count
            ));
        }

        Ok(())
    }

    async fn before_all(&self, client: &QdrantClient, args: Arc<Args>) -> Result<()> {
        // honor args.recreate_collection
        if args.recreate_collection {
            recreate_collection(client, &self.collection_name, args.clone()).await?;
            // index some points
            self.insert_points(client).await?;
        }
        Ok(())
    }
}