use anyhow::Result;
use qdrant_client::client::QdrantClient;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use crate::args::Args;
use crate::common::client::{
    create_collection, get_points_count, insert_points, recreate_collection, wait_index,
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
        let points_count = 10000;
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
        // create and populate collection if it does not exists
        if !client.has_collection(&self.collection_name).await? {
            log::info!("The collection snapshot churn drill needs to setup the collection first");
            create_collection(client, &self.collection_name, args.clone()).await?;

            // insert some points
            insert_points(
                client,
                &self.collection_name,
                self.points_count,
                self.vec_dim,
                self.payload_count,
                self.stopped.clone(),
            )
            .await?;

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
        }

        // number of snapshots before
        let snapshot_count = client
            .list_snapshots(&self.collection_name)
            .await?
            .snapshot_descriptions
            .len();

        // create snapshot
        let snapshot = client.create_snapshot(&self.collection_name).await?;

        // number of snapshots before
        let post_create_snapshot_count = client
            .list_snapshots(&self.collection_name)
            .await?
            .snapshot_descriptions
            .len();
        if post_create_snapshot_count != snapshot_count + 1 {
            return Err(Invariant(format!(
                "Collection snapshot count is wrong {} vs {}",
                post_create_snapshot_count,
                snapshot_count + 1
            )));
        }

        // delete snapshot
        client
            .delete_snapshot(
                &self.collection_name,
                &snapshot.snapshot_description.unwrap().name,
            )
            .await?;

        // number of snapshots before
        let post_delete_snapshot_count = client
            .list_snapshots(&self.collection_name)
            .await?
            .snapshot_descriptions
            .len();
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
            recreate_collection(client, &self.collection_name, args.clone()).await?;
        }
        Ok(())
    }
}
