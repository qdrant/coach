use anyhow::Result;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::args::Args;
use crate::common::client::{
    create_collection, create_field_index, delete_collection, insert_points_batch,
};
use crate::common::coach_errors::CoachError;
use crate::common::coach_errors::CoachError::{Cancelled, Invariant};
use crate::common::generators::KEYWORD_PAYLOAD_KEY;
use crate::drill_runner::Drill;
use async_trait::async_trait;
use futures::stream::FuturesUnordered;
use futures::{StreamExt, stream};
use qdrant_client::Qdrant;
use qdrant_client::qdrant::FieldType;

/// Drill that creates and deletes a collection in parallel.
/// The collection is created and populated with random data if it does not exist.
pub struct CollectionConcurrentLifecycle {
    collection_name: String,
    stopped: Arc<AtomicBool>,
    points_count: usize,
    vec_dim: usize,
    payload_count: usize,
    parallelism: usize,
}

impl CollectionConcurrentLifecycle {
    pub fn new(stopped: Arc<AtomicBool>) -> Self {
        let collection_name = "collection-concurrent-lifecycle-drill".to_string();
        let vec_dim = 512;
        let payload_count = 2;
        let points_count = 50000;
        let parallelism = 20;
        CollectionConcurrentLifecycle {
            collection_name,
            points_count,
            vec_dim,
            payload_count,
            stopped,
            parallelism,
        }
    }
}

#[async_trait]
impl Drill for CollectionConcurrentLifecycle {
    fn name(&self) -> String {
        "collection_concurrent_lifecycle".to_string()
    }

    fn reschedule_after_sec(&self) -> u64 {
        10
    }

    async fn run(&self, client: &Qdrant, args: Arc<Args>) -> Result<(), CoachError> {
        // delete if already exists
        if client.collection_exists(&self.collection_name).await? {
            delete_collection(client, &self.collection_name).await?;
        }

        if self.stopped.load(Ordering::Relaxed) {
            return Err(Cancelled);
        }

        // test concurrent create collection
        let mut creations = FuturesUnordered::new();
        for _ in 0..1 {
            let args = args.clone();
            creations.push(async move {
                create_collection(client, &self.collection_name, self.vec_dim, args).await
            });
        }

        while let Some(result) = creations.next().await {
            if self.stopped.load(Ordering::Relaxed) {
                return Err(Cancelled);
            }
            match result {
                Ok(_) => {} // at least one creation should be successful
                Err(e) => {
                    let msg = format!("{e:?}");
                    if !msg.contains(&format!(
                        "Collection `{}` already exists!",
                        self.collection_name
                    )) {
                        return Err(Invariant(format!(
                            "Creating collection failed for the wrong reason - {msg:?}"
                        )));
                    }
                }
            }
        }

        if self.stopped.load(Ordering::Relaxed) {
            return Err(Cancelled);
        }

        // create field index
        create_field_index(
            client,
            &self.collection_name,
            KEYWORD_PAYLOAD_KEY,
            FieldType::Keyword,
        )
        .await?;

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

        // test concurrent delete collection
        let mut deletions = FuturesUnordered::new();
        for _ in 0..self.parallelism {
            deletions.push(async move { delete_collection(client, &self.collection_name).await });
        }

        while let Some(result) = deletions.next().await {
            if self.stopped.load(Ordering::Relaxed) {
                return Err(Cancelled);
            }
            match result {
                Ok(_) => {}
                Err(e) => {
                    return Err(Invariant(format!(
                        "Deleting collection should not fail - {e:?}"
                    )));
                }
            }
        }

        // test mixed concurrent create and delete (1 vs 1)
        let mut creations = Vec::new();
        for _ in 0..self.parallelism {
            let args = args.clone();
            creations.push(async move {
                create_collection(client, &self.collection_name, self.vec_dim, args).await
            });
        }
        let mut creations_stream = stream::iter(creations).buffer_unordered(1);

        let mut deletions = Vec::new();
        for _ in 0..self.parallelism {
            deletions.push(async move { delete_collection(client, &self.collection_name).await });
        }
        let mut deletions_stream = stream::iter(deletions).buffer_unordered(1);

        loop {
            if self.stopped.load(Ordering::Relaxed) {
                return Err(Cancelled);
            }
            // make sure the collection does not exist (do not care about this result)
            let _clean = delete_collection(client, &self.collection_name).await?;
            // race between 1 creation and 1 deletion
            tokio::select! {
                Some(creation_res) = creations_stream.next() => {
                     match creation_res {
                        Ok(_) => {},
                        Err(e) => {
                            let msg = format!("{e:?}");
                            if !msg.contains(&format!(
                                "Collection `{}` already exists!",
                                self.collection_name
                            )) {
                                return Err(Invariant(format!(
                                    "Creating collection failed for the wrong reason - {msg:?}"
                                )));
                            }
                        }
                    }
                },
                Some(deletion_res) = deletions_stream.next() => {
                    match deletion_res {
                        Ok(_) => {},
                        Err(e) => {
                            return Err(Invariant(format!(
                                "Deleting collection should not fail - {e:?}"
                            )))
                        }
                    }
                },
                else => break,
            }
        }

        Ok(())
    }

    async fn before_all(&self, _client: &Qdrant, _args: Arc<Args>) -> Result<(), CoachError> {
        // no need to explicitly honor args.recreate_collection
        // because we are going to delete the collection anyway
        Ok(())
    }
}
