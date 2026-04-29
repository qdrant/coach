use anyhow::Result;
use rand::rngs::SmallRng;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use crate::args::Args;
use crate::common::client::{
    create_collection, get_points_count, insert_points_batch, recreate_collection, search_points,
    upsert_point_by_id, wait_index,
};
use crate::common::coach_errors::CoachError;
use crate::common::coach_errors::CoachError::{Cancelled, Invariant};
use crate::drill_runner::Drill;
use async_trait::async_trait;
use qdrant_client::Qdrant;

/// Drill that performs search requests on a collection.
/// The collection is created and populated with random data if it does not exist.
pub struct PointsSearch {
    collection_name: String,
    search_count: usize,
    points_count: usize,
    vec_dim: usize,
    keyword_variants: usize,
    stopped: CancellationToken,
}

impl PointsSearch {
    pub fn new(stopped: CancellationToken) -> Self {
        let collection_name = "points-search-drill".to_string();
        let vec_dim = 128;
        let keyword_variants = 2;
        let search_count = 1000;
        let points_count = 10000;
        PointsSearch {
            collection_name,
            search_count,
            points_count,
            vec_dim,
            keyword_variants,
            stopped,
        }
    }
}

#[async_trait]
impl Drill for PointsSearch {
    fn name(&self) -> &'static str {
        "points_search"
    }

    fn reschedule_after_sec(&self) -> u64 {
        10
    }

    async fn run(
        &self,
        client: &Qdrant,
        args: Arc<Args>,
        rng: &mut SmallRng,
    ) -> Result<(), CoachError> {
        // create and populate collection if it does not exist
        if !client.collection_exists(&self.collection_name).await? {
            log::info!("The search drill needs to setup the collection first");
            create_collection(client, &self.collection_name, self.vec_dim, args.clone()).await?;

            // insert some points
            insert_points_batch(
                client,
                &self.collection_name,
                self.points_count,
                self.vec_dim,
                self.keyword_variants,
                None,
                self.stopped.clone(),
                rng,
            )
            .await?;
        }

        // refresh a slice of points each run with fresh random data so searches
        // exercise different content over time, not the same dataset on every iteration
        for point_id in 0..100u64 {
            if self.stopped.is_cancelled() {
                return Err(Cancelled);
            }
            upsert_point_by_id(
                client,
                &self.collection_name,
                point_id,
                self.vec_dim,
                self.keyword_variants,
                None,
                rng,
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
            if self.stopped.is_cancelled() {
                return Err(Cancelled);
            }
            let response = search_points(
                client,
                &self.collection_name,
                self.vec_dim,
                self.keyword_variants,
                rng,
            )
            .await?;
            // assert not empty
            if response.result.is_empty() {
                return Err(Invariant("Search returned empty result".to_string()));
            }
            // verify result content - top hit must have a valid id within the inserted range
            // and payload (search_points requests with_payload)
            let top = &response.result[0];
            if top.payload.is_empty() {
                return Err(Invariant("Search top hit has empty payload".to_string()));
            }
            let id = top.id.as_ref().and_then(|i| match i.point_id_options {
                Some(qdrant_client::qdrant::point_id::PointIdOptions::Num(n)) => Some(n),
                _ => None,
            });
            match id {
                Some(n) if (n as usize) < self.points_count => {}
                Some(n) => {
                    return Err(Invariant(format!(
                        "Search returned out-of-range point id {n} (max {})",
                        self.points_count
                    )));
                }
                None => return Err(Invariant("Search top hit missing numeric id".to_string())),
            }
        }

        Ok(())
    }

    async fn before_all(
        &self,
        client: &Qdrant,
        args: Arc<Args>,
        rng: &mut SmallRng,
    ) -> Result<(), CoachError> {
        // honor args.recreate_collection
        if args.recreate_collection {
            recreate_collection(client, &self.collection_name, self.vec_dim, args.clone()).await?;
            // insert some points
            insert_points_batch(
                client,
                &self.collection_name,
                self.points_count,
                self.vec_dim,
                self.keyword_variants,
                None,
                self.stopped.clone(),
                rng,
            )
            .await?;
        }
        Ok(())
    }
}
