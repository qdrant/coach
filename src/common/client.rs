use crate::args::Args;
use crate::common::coach_errors::CoachError;
use crate::common::coach_errors::CoachError::Cancelled;
use crate::common::generators::{random_payload, random_vector};
use anyhow::Context;
use qdrant_client::client::QdrantClient;
use qdrant_client::qdrant::point_id::PointIdOptions;
use qdrant_client::qdrant::points_selector::PointsSelectorOneOf;
use qdrant_client::qdrant::vectors_config::Config;
use qdrant_client::qdrant::{
    CollectionStatus, CreateCollection, Distance, OptimizersConfigDiff, PointId, PointStruct,
    PointsIdsList, PointsSelector, VectorParams, VectorsConfig,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

pub async fn get_points_count(
    client: &QdrantClient,
    collection_name: &str,
) -> Result<usize, anyhow::Error> {
    let point_count = client
        .collection_info(collection_name)
        .await
        .context(format!(
            "Failed to fetch points count for {}",
            collection_name
        ))?
        .result
        .unwrap()
        .points_count;
    Ok(point_count as usize)
}

/// delete points (blocking)
pub async fn delete_points(
    client: &QdrantClient,
    collection_name: &str,
    points_count: usize,
) -> Result<(), anyhow::Error> {
    let points_selector = (0..points_count as u64)
        .map(|id| PointId {
            point_id_options: Some(PointIdOptions::Num(id)),
        })
        .collect();

    // delete all points
    client
        .delete_points_blocking(
            collection_name,
            &PointsSelector {
                points_selector_one_of: Some(PointsSelectorOneOf::Points(PointsIdsList {
                    ids: points_selector,
                })),
            },
            None,
        )
        .await
        .context(format!(
            "Failed to delete {} points for {}",
            points_count, collection_name
        ))?;
    Ok(())
}

pub async fn disable_indexing(
    client: &QdrantClient,
    collection_name: &str,
) -> Result<(), anyhow::Error> {
    set_indexing_threshold(client, collection_name, 1000000).await?;
    Ok(())
}

pub async fn enable_indexing(
    client: &QdrantClient,
    collection_name: &str,
) -> Result<(), anyhow::Error> {
    set_indexing_threshold(client, collection_name, 1).await?;
    Ok(())
}

pub async fn set_indexing_threshold(
    client: &QdrantClient,
    collection_name: &str,
    threshold: usize,
) -> Result<(), anyhow::Error> {
    client
        .update_collection(
            collection_name,
            &OptimizersConfigDiff {
                indexing_threshold: Some(threshold as u64),
                ..Default::default()
            },
        )
        .await
        .context(format!(
            "Failed to set indexing threshold for {}",
            collection_name
        ))?;
    Ok(())
}

pub async fn get_collection_status(
    client: &QdrantClient,
    collection_name: &str,
) -> Result<CollectionStatus, anyhow::Error> {
    let status = client
        .collection_info(collection_name)
        .await
        .context(format!(
            "Failed to fetch collection info for {}",
            collection_name
        ))?
        .result
        .unwrap()
        .status;
    Ok(CollectionStatus::from_i32(status).unwrap())
}

pub async fn wait_index(
    client: &QdrantClient,
    collection_name: &str,
    stopped: Arc<AtomicBool>,
) -> Result<f64, CoachError> {
    let start = std::time::Instant::now();
    let mut seen = 0;
    loop {
        if stopped.load(Ordering::Relaxed) {
            return Err(Cancelled);
        }
        sleep(Duration::from_secs(1)).await;
        let collection_status = get_collection_status(client, collection_name).await?;
        if collection_status == CollectionStatus::Green {
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

pub async fn create_collection(
    client: &QdrantClient,
    collection_name: &str,
    args: Arc<Args>,
) -> Result<(), anyhow::Error> {
    client
        .create_collection(&CreateCollection {
            collection_name: collection_name.to_string(),
            vectors_config: Some(VectorsConfig {
                config: Some(Config::Params(VectorParams {
                    size: 128,
                    distance: Distance::Cosine.into(),
                })),
            }),
            replication_factor: Some(args.replication_factor as u32),
            optimizers_config: Some(OptimizersConfigDiff {
                indexing_threshold: args.indexing_threshold.map(|i| i as u64),
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .context(format!("Failed to create collection {}", collection_name))?;
    Ok(())
}

/// delete collection without checking if it exists
pub async fn delete_collection(
    client: &QdrantClient,
    collection_name: &str,
) -> Result<(), anyhow::Error> {
    client
        .delete_collection(&collection_name)
        .await
        .context(format!("Failed to delete collection {}", collection_name))?;
    Ok(())
}

/// delete collection if exists & create new one
pub async fn recreate_collection(
    client: &QdrantClient,
    collection_name: &str,
    args: Arc<Args>,
) -> Result<(), anyhow::Error> {
    if client.has_collection(&collection_name).await? {
        log::info!("recreating existing collection {}", collection_name);
        delete_collection(client, collection_name).await?;
        sleep(Duration::from_secs(2)).await;
    }
    create_collection(client, collection_name, args).await
}

/// insert points into collection (blocking)
pub async fn insert_points(
    client: &QdrantClient,
    collection_name: &str,
    points_count: usize,
    vec_dim: usize,
    payload_count: usize,
    stopped: Arc<AtomicBool>,
) -> Result<(), CoachError> {
    let batch_size = 100;
    let num_batches = points_count / batch_size;

    for batch_id in 0..num_batches {
        let mut points = Vec::new();
        for i in 0..batch_size {
            let idx = batch_id as u64 * batch_size as u64 + i as u64;

            let point_id: PointId = PointId {
                point_id_options: Some(PointIdOptions::Num(idx)),
            };

            points.push(PointStruct::new(
                point_id,
                random_vector(vec_dim),
                random_payload(Some(payload_count)),
            ));
        }
        if stopped.load(Ordering::Relaxed) {
            return Err(Cancelled);
        }

        // push batch blocking
        client
            .upsert_points_blocking(collection_name, points, None)
            .await
            .context(format!(
                "Failed to insert {} points in {}",
                points_count, collection_name
            ))?;
    }
    Ok(())
}
