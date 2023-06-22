use anyhow::Result;
use qdrant_client::client::QdrantClient;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::args::Args;
use crate::common::client::{
    create_collection, get_points_count, insert_points_batch, recreate_collection,
    set_indexing_threshold, set_payload, upsert_point_by_id, wait_index,
};
use crate::common::coach_errors::CoachError;
use crate::common::coach_errors::CoachError::{Cancelled, Invariant};
use crate::drill_runner::Drill;
use async_trait::async_trait;
use qdrant_client::qdrant::WriteOrdering;

/// Drill that creates empty points in a collection.
/// Those points are progressively filled with payload and named vectors.
pub struct PointsOptionalVectors {
    collection_name: String,
    points_count: usize,
    vec_dim: usize,
    payload_count: usize,
    write_ordering: Option<WriteOrdering>,
    stopped: Arc<AtomicBool>,
}

impl PointsOptionalVectors {
    pub fn new(stopped: Arc<AtomicBool>) -> Self {
        let collection_name = "points-optional-vectors-drill".to_string();
        let vec_dim = 768;
        let payload_count = 2;
        let points_count = 20000;
        let write_ordering = None; // default
        PointsOptionalVectors {
            collection_name,
            points_count,
            vec_dim,
            payload_count,
            write_ordering,
            stopped,
        }
    }
}

#[async_trait]
impl Drill for PointsOptionalVectors {
    fn name(&self) -> String {
        "points_optional_vectors".to_string()
    }

    fn reschedule_after_sec(&self) -> u64 {
        10
    }

    async fn run(&self, client: &QdrantClient, args: Arc<Args>) -> Result<(), CoachError> {
        // create and populate collection if it does not exists
        if !client.has_collection(&self.collection_name).await? {
            log::info!("The points optional vectors drill needs to setup the collection first");
            create_collection(client, &self.collection_name, self.vec_dim, args.clone()).await?;
        } else {
            // assert point count
            let points_count = get_points_count(client, &self.collection_name).await?;
            if points_count != self.points_count {
                return Err(Invariant(format!(
                    "Collection has wrong number of points from previous run {} vs {}",
                    points_count, self.points_count
                )));
            }
        }

        // make sure the indexer is triggered
        set_indexing_threshold(
            client,
            &self.collection_name,
            args.indexing_threshold.unwrap_or(200),
        )
        .await?;

        // insert some points without data
        insert_points_batch(
            client,
            &self.collection_name,
            self.points_count,
            0, // no vectors (workaround using the vec_dim param for that)
            0, // no payload
            None,
            self.stopped.clone(),
        )
        .await?;

        // set payload on empty points
        for point_id in 1..self.points_count {
            if self.stopped.load(Ordering::Relaxed) {
                return Err(Cancelled);
            }
            set_payload(
                client,
                &self.collection_name,
                point_id as u64,
                self.payload_count,
                self.write_ordering.clone(),
            )
            .await?;
        }

        // set vectors (no additional payloads)
        for point_id in 1..self.points_count {
            if self.stopped.load(Ordering::Relaxed) {
                return Err(Cancelled);
            }
            upsert_point_by_id(
                client,
                &self.collection_name,
                point_id as u64,
                self.vec_dim,
                0,
                self.write_ordering.clone(),
            )
            .await?;
        }

        // wait for indexing
        wait_index(client, &self.collection_name, self.stopped.clone()).await?;

        // assert point count
        let points_count = get_points_count(client, &self.collection_name).await?;
        if points_count != self.points_count {
            return Err(Invariant(format!(
                "Collection has wrong number of points after insert {} vs {}",
                points_count, self.points_count
            )));
        }

        Ok(())
    }

    async fn before_all(&self, client: &QdrantClient, args: Arc<Args>) -> Result<(), CoachError> {
        // honor args.recreate_collection
        if args.recreate_collection {
            recreate_collection(client, &self.collection_name, self.vec_dim, args.clone()).await?;
        }
        Ok(())
    }
}
