use anyhow::Result;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use crate::args::Args;
use crate::common::client::{
    create_collection, get_points_count, insert_points_batch, recreate_collection, retrieve_points,
    set_indexing_threshold, set_payload, upsert_point_by_id, wait_index,
};
use crate::common::coach_errors::CoachError;
use crate::common::coach_errors::CoachError::{Cancelled, Invariant};
use crate::drill_runner::Drill;
use async_trait::async_trait;
use qdrant_client::Qdrant;
use qdrant_client::qdrant::WriteOrdering;

/// Drill that creates empty points in a collection.
/// Those points are progressively filled with payload and named vectors.
pub struct PointsOptionalVectors {
    collection_name: String,
    points_count: usize,
    vec_dim: usize,
    keyword_variants: usize,
    write_ordering: Option<WriteOrdering>,
    stopped: CancellationToken,
}

impl PointsOptionalVectors {
    pub fn new(stopped: CancellationToken) -> Self {
        let collection_name = "points-optional-vectors-drill".to_string();
        let vec_dim = 768;
        let keyword_variants = 2;
        let points_count = 20000;
        let write_ordering = None; // default
        PointsOptionalVectors {
            collection_name,
            points_count,
            vec_dim,
            keyword_variants,
            write_ordering,
            stopped,
        }
    }
}

#[async_trait]
impl Drill for PointsOptionalVectors {
    fn name(&self) -> &'static str {
        "points_optional_vectors"
    }

    fn reschedule_after_sec(&self) -> u64 {
        10
    }

    async fn run(&self, client: &Qdrant, args: Arc<Args>) -> Result<(), CoachError> {
        // create and populate collection if it does not exist
        if !client.collection_exists(&self.collection_name).await? {
            log::info!("The points optional vectors drill needs to setup the collection first");
            create_collection(client, &self.collection_name, self.vec_dim, args.clone()).await?;
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
        for point_id in 0..self.points_count {
            if self.stopped.is_cancelled() {
                return Err(Cancelled);
            }
            set_payload(
                client,
                &self.collection_name,
                point_id as u64,
                self.keyword_variants,
                self.write_ordering,
            )
            .await?;
        }

        // set vectors (no additional payloads)
        for point_id in 0..self.points_count {
            if self.stopped.is_cancelled() {
                return Err(Cancelled);
            }
            upsert_point_by_id(
                client,
                &self.collection_name,
                point_id as u64,
                self.vec_dim,
                0,
                self.write_ordering,
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

        // sample point 0 - must have vectors set after the upsert phase (payload is
        // not checked because the empty-payload upsert in phase 3 may overwrite it)
        let sample = retrieve_points(client, &self.collection_name, &[0]).await?;
        let point = sample.result.first().ok_or_else(|| {
            Invariant("Sample point 0 missing after payload+vector phases".to_string())
        })?;
        if point.vectors.is_none() {
            return Err(Invariant(
                "Sample point 0 has no vectors after upsert phase".to_string(),
            ));
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
