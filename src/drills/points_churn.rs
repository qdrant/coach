use anyhow::Result;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use crate::args::Args;
use crate::common::client::{
    create_collection, delete_points, get_point_by_id, get_points_count, insert_points_batch,
    recreate_collection,
};
use crate::common::coach_errors::CoachError;
use crate::common::coach_errors::CoachError::Invariant;
use crate::drill_runner::Drill;
use async_trait::async_trait;
use qdrant_client::Qdrant;

/// Drill that creates and deletes points in a collection.
/// The collection is created and populated with random data if it does not exist.
pub struct PointsChurn {
    collection_name: String,
    points_count: usize,
    vec_dim: usize,
    keyword_variants: usize,
    stopped: CancellationToken,
}

impl PointsChurn {
    pub fn new(stopped: CancellationToken) -> Self {
        let collection_name = "points-churn-drill".to_string();
        let vec_dim = 128;
        let keyword_variants = 2;
        let points_count = 10000;
        PointsChurn {
            collection_name,
            points_count,
            vec_dim,
            keyword_variants,
            stopped,
        }
    }
}

#[async_trait]
impl Drill for PointsChurn {
    fn name(&self) -> &'static str {
        "points_churn"
    }

    fn reschedule_after_sec(&self) -> u64 {
        10
    }

    async fn run(&self, client: &Qdrant, args: Arc<Args>) -> Result<(), CoachError> {
        // create and populate collection if it does not exist
        if !client.collection_exists(&self.collection_name).await? {
            log::info!("The points churn drill needs to setup the collection first");
            create_collection(client, &self.collection_name, self.vec_dim, args.clone()).await?;
        }

        // insert some points
        insert_points_batch(
            client,
            &self.collection_name,
            self.points_count,
            self.vec_dim,
            self.keyword_variants,
            None,
            self.stopped.clone(),
        )
        .await?;

        // assert point count
        let points_count = get_points_count(client, &self.collection_name).await?;
        if points_count != self.points_count {
            return Err(Invariant(format!(
                "Collection has wrong number of points after insert {} vs {}",
                points_count, self.points_count
            )));
        }

        // verify a sample point was actually written, not just that the count looks right
        if get_point_by_id(client, &self.collection_name, 0)
            .await?
            .is_none()
        {
            return Err(Invariant(
                "Sample point 0 missing despite matching point count".to_string(),
            ));
        }

        // delete all points
        delete_points(client, &self.collection_name, self.points_count).await?;

        // assert point count
        let points_count = get_points_count(client, &self.collection_name).await?;
        if points_count != 0 {
            return Err(Invariant(format!(
                "Collection should be empty but got {points_count} points"
            )));
        }

        Ok(())
    }

    async fn before_all(&self, client: &Qdrant, args: Arc<Args>) -> Result<(), CoachError> {
        // honor args.recreate_collection
        if args.recreate_collection {
            recreate_collection(client, &self.collection_name, self.vec_dim, args.clone()).await?;
        }
        Ok(())
    }
}
