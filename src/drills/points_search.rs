use anyhow::Result;
use qdrant_client::client::QdrantClient;
use qdrant_client::qdrant::point_id::PointIdOptions;
use qdrant_client::qdrant::vectors_config::Config;
use qdrant_client::qdrant::{
    CreateCollection, Distance, PointId, PointStruct, SearchPoints, SearchResponse, VectorParams,
    VectorsConfig,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::common::client::wait_index;
use crate::common::generators::{random_filter, random_payload, random_vector};
use crate::drill::Drill;
use async_trait::async_trait;

/// Drill that performs search requests on a collection.
/// The collection is created and populated with random data if it does not exist.
pub struct PointsSearch {
    client: Arc<QdrantClient>,
    collection_name: String,
    search_count: usize,
    points_count: usize,
    vec_dim: usize,
    payload_count: usize,
    stopped: Arc<AtomicBool>,
}

impl PointsSearch {
    pub fn new(client: Arc<QdrantClient>, stopped: Arc<AtomicBool>) -> Self {
        let collection_name = "points-search-drill".to_string();
        let vec_dim = 128;
        let payload_count = 2;
        let search_count = 1000;
        let points_count = 10000;
        PointsSearch {
            client,
            collection_name,
            search_count,
            points_count,
            vec_dim,
            payload_count,
            stopped,
        }
    }

    pub async fn search(&self) -> Result<SearchResponse, anyhow::Error> {
        let query_vector = random_vector(self.vec_dim);
        let query_filter = random_filter(Some(self.payload_count));

        let response = self
            .client
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

        Ok(response)
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
                .upsert_points(&self.collection_name, points)
                .await?;
        }
        Ok(())
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

        // waiting for green status
        wait_index(
            self.client.clone(),
            &self.collection_name,
            self.stopped.clone(),
        )
        .await?;

        // assert point count
        let collection_info = self
            .client
            .collection_info(&self.collection_name)
            .await?
            .result
            .unwrap();
        if collection_info.points_count != self.points_count as u64 {
            return Err(anyhow::anyhow!("Collection has wrong number of points"));
        }

        // search
        for _i in 0..self.search_count {
            if self.stopped.load(Ordering::Relaxed) {
                return Ok(());
            }
            let response = self.search().await?;
            if response.result.is_empty() {
                return Err(anyhow::anyhow!("Search returned empty result"));
            }
        }

        Ok(())
    }
}
