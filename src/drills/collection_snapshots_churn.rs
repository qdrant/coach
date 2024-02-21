use anyhow::Result;
use qdrant_client::client::QdrantClient;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use crate::args::Args;
use crate::common::client::{
    count_collection_snapshots, create_collection, create_collection_snapshot,
    delete_collection_snapshot, disable_indexing, get_points_count, insert_points_batch,
    recreate_collection,
};
use crate::common::coach_errors::CoachError;
use crate::common::coach_errors::CoachError::Invariant;
use crate::drill_runner::Drill;
use async_trait::async_trait;

/// Drill that creates and deletes points in a collection.
/// The collection is created and populated with random data if it does not exist.
pub struct CollectionSnapshotsChurn {
    collection_name: String,
    points_count: usize,
    vec_dim: usize,
    payload_count: usize,
    stopped: Arc<AtomicBool>,
}

impl CollectionSnapshotsChurn {
    pub fn new(stopped: Arc<AtomicBool>) -> Self {
        let collection_name = "collection-snapshot-drill".to_string();
        let vec_dim = 128;
        let payload_count = 2;
        let points_count = 200_000;
        CollectionSnapshotsChurn {
            collection_name,
            points_count,
            vec_dim,
            payload_count,
            stopped,
        }
    }
}

#[async_trait]
impl Drill for CollectionSnapshotsChurn {
    fn name(&self) -> String {
        "collection_snapshots_churn".to_string()
    }

    fn reschedule_after_sec(&self) -> u64 {
        10
    }

    async fn run(&self, client: &QdrantClient, args: Arc<Args>) -> Result<(), CoachError> {
        // create and populate collection if it does not exist
        if !client.has_collection(&self.collection_name).await? {
            // create collection
            create_collection(client, &self.collection_name, self.vec_dim, args.clone()).await?;

            // disable indexing
            disable_indexing(client, &self.collection_name).await?;

            // insert some points
            insert_points_batch(
                client,
                &self.collection_name,
                self.points_count,
                self.vec_dim,
                self.payload_count,
                None,
                self.stopped.clone(),
            )
            .await?;
        }

        // assert point count
        let points_count = get_points_count(client, &self.collection_name).await?;
        if points_count != self.points_count {
            return Err(Invariant(format!(
                "Collection has wrong number of points after insert {} vs {}",
                points_count, self.points_count
            )));
        }

        // number of snapshots before
        let snapshot_count = count_collection_snapshots(client, &self.collection_name).await?;

        // create snapshot
        let snapshot = create_collection_snapshot(client, &self.collection_name).await?;

        // number of snapshots before
        let post_create_snapshot_count =
            count_collection_snapshots(client, &self.collection_name).await?;

        // assert snapshot count before
        if post_create_snapshot_count != snapshot_count + 1 {
            return Err(Invariant(format!(
                "Collection snapshot count is wrong {} vs {}",
                post_create_snapshot_count,
                snapshot_count + 1
            )));
        }

        // delete snapshot
        let snapshot_name = snapshot.snapshot_description.unwrap().name;
        delete_collection_snapshot(client, &self.collection_name, &snapshot_name).await?;

        // number of snapshots before
        let post_delete_snapshot_count =
            count_collection_snapshots(client, &self.collection_name).await?;

        // assert snapshot count after
        if post_delete_snapshot_count != snapshot_count {
            return Err(Invariant(format!(
                "Collection snapshot count is wrong {} vs {}",
                post_delete_snapshot_count, snapshot_count
            )));
        }

        Ok(())
    }

    async fn before_all(&self, client: &QdrantClient, args: Arc<Args>) -> Result<(), CoachError> {
        // honor args.recreate_collection
        if args.recreate_collection {
            recreate_collection(client, &self.collection_name, self.vec_dim, args.clone()).await?;

            // disable indexing
            disable_indexing(client, &self.collection_name).await?;

            // insert some points
            insert_points_batch(
                client,
                &self.collection_name,
                self.points_count,
                self.vec_dim,
                self.payload_count,
                None,
                self.stopped.clone(),
            )
            .await?;
        }
        Ok(())
    }
}
