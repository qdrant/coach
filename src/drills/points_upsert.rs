use anyhow::Result;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use crate::args::Args;
use crate::common::client::{
    create_collection, get_points_count, insert_points_batch, recreate_collection, retrieve_points,
    wait_index,
};
use crate::common::coach_errors::CoachError;
use crate::common::coach_errors::CoachError::Invariant;
use crate::drill_runner::Drill;
use async_trait::async_trait;
use qdrant_client::Qdrant;

/// Drill that consistently updates the same points.
/// The collection is created and populated with random data if it does not exist.
pub struct PointsUpdate {
    collection_name: String,
    points_count: usize,
    vec_dim: usize,
    keyword_variants: usize,
    stopped: CancellationToken,
}

impl PointsUpdate {
    pub fn new(stopped: CancellationToken) -> Self {
        let collection_name = "points-update-drill".to_string();
        let vec_dim = 128;
        let keyword_variants = 2;
        let points_count = 10000;
        PointsUpdate {
            collection_name,
            points_count,
            vec_dim,
            keyword_variants,
            stopped,
        }
    }
}

#[async_trait]
impl Drill for PointsUpdate {
    fn name(&self) -> &'static str {
        "points_update"
    }

    fn reschedule_after_sec(&self) -> u64 {
        10
    }

    async fn run(&self, client: &Qdrant, args: Arc<Args>) -> Result<(), CoachError> {
        // create if it does not exist
        if !client.collection_exists(&self.collection_name).await? {
            log::info!("The points update drill needs to setup the collection first");
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
            )
            .await?;
        }

        // waiting for green status
        wait_index(client, &self.collection_name, self.stopped.clone()).await?;

        // assert point count
        let collection_info = client
            .collection_info(&self.collection_name)
            .await?
            .result
            .unwrap();
        let points_count = collection_info.points_count.unwrap_or_default();
        if points_count != self.points_count as u64 {
            return Err(Invariant(format!(
                "Collection has wrong number of points after insert {} vs {}",
                points_count, self.points_count
            )));
        }

        // snapshot point 0's vector before the upsert
        let before = retrieve_points(client, &self.collection_name, &[0]).await?;
        let before_vectors = before
            .result
            .first()
            .and_then(|p| p.vectors.clone())
            .ok_or_else(|| Invariant("Sample point 0 missing vectors before upsert".to_string()))?;

        // update points by upserting on same ids (insert_points_batch generates fresh
        // random vectors per call - so the upsert should overwrite the previous content)
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
                "Collection has wrong number of points after upsert {} vs {}",
                points_count, self.points_count
            )));
        }

        // verify upsert actually wrote new content - random f32 vector collision is ~0
        let after = retrieve_points(client, &self.collection_name, &[0]).await?;
        let after_vectors = after
            .result
            .first()
            .and_then(|p| p.vectors.clone())
            .ok_or_else(|| Invariant("Sample point 0 missing vectors after upsert".to_string()))?;
        if before_vectors == after_vectors {
            return Err(Invariant(
                "Upsert did not change vector content for point 0".to_string(),
            ));
        }

        Ok(())
    }

    async fn before_all(&self, client: &Qdrant, args: Arc<Args>) -> Result<(), CoachError> {
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
            )
            .await?;
        }
        Ok(())
    }
}
