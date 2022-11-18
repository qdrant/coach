use anyhow::Result;
use qdrant_client::client::QdrantClient;
use qdrant_client::qdrant::point_id::PointIdOptions;
use qdrant_client::qdrant::vectors_config::Config;
use qdrant_client::qdrant::{
    CollectionStatus, CreateCollection, Distance, PointId, PointStruct, SearchPoints,
    VectorParams, VectorsConfig,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

use crate::drill::Drill;
use crate::generators::{random_filter, random_payload, random_vector};
use async_trait::async_trait;

/// Drill that performs search requests on a collection.
/// The collection is created and populated with random data if it does not exit.
pub struct Search {
    client: Arc<QdrantClient>,
    collection_name: String,
    search_count: usize,
    vector_count: usize,
    vec_dim: usize,
    payload_count: usize,
    stopped: Arc<AtomicBool>,
}

impl Search {
    pub fn new(client: Arc<QdrantClient>, stopped: Arc<AtomicBool>) -> Self {
        let collection_name = "search-drill".to_string();
        let vec_dim = 128;
        let payload_count = 2;
        let search_count = 1000;
        let vector_count = 10000;
        Search {
            client,
            collection_name,
            search_count,
            vector_count,
            vec_dim,
            payload_count,
            stopped,
        }
    }

    pub async fn search(&self) -> Result<(), anyhow::Error> {
        let query_vector = random_vector(self.vec_dim);
        let query_filter = random_filter(Some(self.payload_count));

        self.client
            .search_points(&SearchPoints {
                collection_name: self.collection_name.to_string(),
                vector: query_vector,
                filter: query_filter,
                limit: 100,
                with_payload: Some(true.into()),
                params: None,
                score_threshold: None,
                offset: None,
                vector_name: None,
                with_vectors: None,
            })
            .await?;

        Ok(())
    }

    async fn wait_index(&self) -> Result<f64> {
        let start = std::time::Instant::now();
        let mut seen = 0;
        loop {
            if self.stopped.load(Ordering::Relaxed) {
                return Ok(0.0);
            }
            sleep(Duration::from_secs(1)).await;
            let info = self.client.collection_info(&self.collection_name).await?;
            if info.result.unwrap().status == CollectionStatus::Green as i32 {
                seen += 1;
                if seen == 3 {
                    break;
                }
            } else {
                seen = 1;
            }
        }
        Ok(start.elapsed().as_secs_f64())
    }

    pub async fn insert_points(&self) -> Result<(), anyhow::Error> {
        let batch_size = 100;
        let num_batches = self.vector_count / batch_size;

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
                .upsert_points(&self.collection_name, points)
                .await?;
        }
        Ok(())
    }
}

#[async_trait]
impl Drill for Search {
    fn name(&self) -> String {
        "search-drill".to_string()
    }

    fn reschedule_after_sec(&self) -> u64 {
        10
    }

    async fn run(&self) -> Result<()> {
        // create if does not exists
        if !self.client.has_collection(&self.collection_name).await? {
            println!("The search drills needs to setup the collection first");
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

            // index some points
            self.insert_points().await?;
        }

        self.wait_index().await?;

        // search
        for _i in 0..self.search_count {
            if self.stopped.load(Ordering::Relaxed) {
                return Ok(());
            }
            self.search().await?;
        }

        Ok(())
    }
}
