use crate::args::Args;
use qdrant_client::client::QdrantClient;
use qdrant_client::qdrant::point_id::PointIdOptions;
use qdrant_client::qdrant::points_selector::PointsSelectorOneOf;
use qdrant_client::qdrant::vectors_config::Config;
use qdrant_client::qdrant::{
    CollectionStatus, CreateCollection, Distance, OptimizersConfigDiff, PointId, PointsIdsList,
    PointsSelector, VectorParams, VectorsConfig,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

pub async fn get_points_count(
    client: Arc<QdrantClient>,
    collection_name: &str,
) -> Result<usize, anyhow::Error> {
    let point_count = client
        .collection_info(collection_name)
        .await?
        .result
        .unwrap()
        .points_count;
    Ok(point_count as usize)
}

pub async fn delete_points(
    client: Arc<QdrantClient>,
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
        )
        .await?;
    Ok(())
}

pub async fn wait_index(
    client: Arc<QdrantClient>,
    collection_name: &str,
    stopped: Arc<AtomicBool>,
) -> Result<f64, anyhow::Error> {
    let start = std::time::Instant::now();
    let mut seen = 0;
    loop {
        if stopped.load(Ordering::Relaxed) {
            return Ok(0.0);
        }
        sleep(Duration::from_secs(1)).await;
        let info = client.collection_info(&collection_name).await?;
        if info.result.unwrap().status == CollectionStatus::Green as i32 {
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
    client: Arc<QdrantClient>,
    collection_name: &str,
    args: Arc<Args>,
) -> Result<(), anyhow::Error> {
    match client
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
    {
        Ok(_) => Ok(()),
        Err(e) => Err(anyhow::anyhow!("Failed to create collection: {}", e)),
    }
}

/// delete collection without checking if it exists
pub async fn delete_collection(
    client: Arc<QdrantClient>,
    collection_name: &str,
) -> Result<(), anyhow::Error> {
    match client.delete_collection(&collection_name).await {
        Ok(_) => Ok(()),
        Err(e) => Err(anyhow::anyhow!("Failed to delete collection: {}", e)),
    }
}

/// delete collection if exists & create new one
pub async fn recreate_collection(
    client: Arc<QdrantClient>,
    collection_name: &str,
    args: Arc<Args>,
) -> Result<(), anyhow::Error> {
    if client.has_collection(&collection_name).await? {
        println!("Recreating existing collection {}", collection_name);
        delete_collection(client.clone(), collection_name).await?;
        sleep(Duration::from_secs(1)).await;
    }
    create_collection(client, collection_name, args).await
}
