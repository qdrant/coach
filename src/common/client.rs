use crate::args::Args;
use crate::common::coach_errors::CoachError;
use crate::common::coach_errors::CoachError::{Cancelled, Invariant};
use crate::common::generators::{
    random_filter, random_named_vector, random_payload, random_vector,
};
use anyhow::Context;
use qdrant_client::qdrant::point_id::PointIdOptions;
use qdrant_client::qdrant::quantization_config::Quantization;
use qdrant_client::qdrant::vectors_config::Config;
use qdrant_client::qdrant::{
    CollectionInfo, CollectionStatus, CreateCollectionBuilder, CreateFieldIndexCollectionBuilder,
    CreateSnapshotResponse, DeletePointsBuilder, DeleteSnapshotRequestBuilder, Distance, FieldType,
    GetPointsBuilder, GetResponse, HnswConfigDiff, OptimizersConfigDiff, PointId, PointStruct,
    QuantizationConfig, RetrievedPoint, ScalarQuantization, ScrollPointsBuilder, ScrollResponse,
    SearchPointsBuilder, SearchResponse, SetPayloadPointsBuilder, UpdateCollectionBuilder,
    UpsertPointsBuilder, VectorParams, VectorParamsMap, VectorsConfig, WriteOrdering,
};
use qdrant_client::Qdrant;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

/// Get point by id
pub async fn get_point_by_id(
    client: &Qdrant,
    collection_name: &str,
    point_id: u64,
) -> Result<Option<RetrievedPoint>, anyhow::Error> {
    let point_id_grpc = point_id.into();
    let point = client
        .get_points(GetPointsBuilder::new(collection_name, &[point_id_grpc]))
        .await
        .context(format!(
            "Failed to get point by id {} from {}",
            point_id, collection_name
        ))?;
    Ok(point.result.first().cloned())
}

/// upsert single point into collection (blocking)
pub async fn upsert_point_by_id(
    client: &Qdrant,
    collection_name: &str,
    point_id: u64,
    vec_dim: usize,
    payload_count: usize,
    write_ordering: Option<WriteOrdering>,
) -> Result<(), CoachError> {
    let point_id_grpc: PointId = PointId {
        point_id_options: Some(PointIdOptions::Num(point_id)),
    };

    let point_struct = PointStruct::new(
        point_id_grpc,
        random_named_vector(DEFAULT_VECTOR_NAME.to_string(), vec_dim),
        random_payload(Some(payload_count)),
    );

    let points = vec![point_struct];
    let mut builder = UpsertPointsBuilder::new(collection_name, points).wait(true);
    if let Some(write_ordering) = write_ordering {
        builder = builder.ordering(write_ordering);
    }
    client.upsert_points(builder).await.context(format!(
        "Failed to update point_id:{} in {}",
        point_id, collection_name
    ))?;

    Ok(())
}

/// delete points (blocking)
pub async fn delete_point_by_id(
    client: &Qdrant,
    collection_name: &str,
    point_id: u64,
) -> Result<(), anyhow::Error> {
    let points_selector = vec![PointId {
        point_id_options: Some(PointIdOptions::Num(point_id)),
    }];

    // delete point
    let resp = client
        .delete_points(
            DeletePointsBuilder::new(collection_name)
                .points(points_selector)
                .wait(true),
        )
        .await
        .context(format!(
            "Failed to delete point_id {} for {}",
            point_id, collection_name
        ))?;
    if resp.result.unwrap().status != 2 {
        Err(anyhow::anyhow!(
            "Failed to delete point_id {} for {}",
            point_id,
            collection_name
        ))
    } else {
        Ok(())
    }
}

/// Set payload (blocking)
pub async fn set_payload(
    client: &Qdrant,
    collection_name: &str,
    point_id: u64,
    payload_count: usize,
    write_ordering: Option<WriteOrdering>,
) -> Result<(), anyhow::Error> {
    let payload = random_payload(Some(payload_count));

    let points_id_selector = vec![PointId {
        point_id_options: Some(PointIdOptions::Num(point_id)),
    }];

    let mut builder = SetPayloadPointsBuilder::new(collection_name, payload)
        .points_selector(points_id_selector)
        .wait(true);

    if let Some(write_ordering) = write_ordering {
        builder = builder.ordering(write_ordering);
    }

    let resp = client.set_payload(builder).await.context(format!(
        "Failed to set payload for {} with payload_count {}",
        point_id, payload_count
    ))?;
    if resp.result.unwrap().status != 2 {
        Err(anyhow::anyhow!(
            "Failed to set payload on point_id {} for {}",
            point_id,
            collection_name
        ))
    } else {
        Ok(())
    }
}

/// Search points
pub async fn search_points(
    client: &Qdrant,
    collection_name: &str,
    vec_dim: usize,
    payload_count: usize,
) -> Result<SearchResponse, anyhow::Error> {
    let query_vector = random_vector(vec_dim);
    let query_filter = random_filter(Some(payload_count));

    let mut builder = SearchPointsBuilder::new(collection_name, query_vector, 100)
        .vector_name(DEFAULT_VECTOR_NAME.to_string())
        .with_payload(true);

    if let Some(query_filter) = query_filter {
        builder = builder.filter(query_filter);
    }

    let response = client
        .search_points(builder)
        .await
        .context(format!("Failed to search points on {}", collection_name))?;

    Ok(response)
}

/// Scroll points
#[allow(unused)]
pub async fn scroll_points(
    client: &Qdrant,
    collection_name: &str,
    payload_count: usize,
) -> Result<ScrollResponse, anyhow::Error> {
    let query_filter = random_filter(Some(payload_count));

    let mut builder = ScrollPointsBuilder::new(collection_name)
        .limit(100)
        .with_payload(true);

    if let Some(query_filter) = query_filter {
        builder = builder.filter(query_filter);
    }

    let response = client
        .scroll(builder)
        .await
        .context(format!("Failed to scroll points on {}", collection_name))?;

    Ok(response)
}

/// Retrieve points
pub async fn retrieve_points(
    client: &Qdrant,
    collection_name: &str,
    ids: &[usize],
) -> Result<GetResponse, anyhow::Error> {
    let ids = ids.iter().map(|id| (*id as u64).into()).collect::<Vec<_>>();
    let response = client
        .get_points(GetPointsBuilder::new(collection_name, ids).with_vectors(true))
        .await
        .context(format!("Failed to retrieve points on {}", collection_name))?;

    Ok(response)
}

/// Get points count
pub async fn get_points_count(
    client: &Qdrant,
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
    Ok(point_count.unwrap_or_default() as usize)
}

/// delete points (blocking)
pub async fn delete_points(
    client: &Qdrant,
    collection_name: &str,
    points_count: usize,
) -> Result<(), anyhow::Error> {
    let points_selector: Vec<_> = (0..points_count as u64)
        .map(|id| PointId {
            point_id_options: Some(PointIdOptions::Num(id)),
        })
        .collect();

    // delete all points
    let resp = client
        .delete_points(
            DeletePointsBuilder::new(collection_name)
                .points(points_selector)
                .wait(true),
        )
        .await
        .context(format!(
            "Failed to delete {} points for {}",
            points_count, collection_name
        ))?;
    if resp.result.unwrap().status != 2 {
        Err(anyhow::anyhow!(
            "Failed to delete {} points for {}",
            points_count,
            collection_name
        ))
    } else {
        Ok(())
    }
}

///Disable indexing
pub async fn disable_indexing(client: &Qdrant, collection_name: &str) -> Result<(), anyhow::Error> {
    set_indexing_threshold(client, collection_name, usize::MAX).await?;
    Ok(())
}

/// Enable indexing
pub async fn enable_indexing(client: &Qdrant, collection_name: &str) -> Result<(), anyhow::Error> {
    // 1000 is the min possible value
    set_indexing_threshold(client, collection_name, 1000).await?;
    Ok(())
}

/// Set indexing threshold
pub async fn set_indexing_threshold(
    client: &Qdrant,
    collection_name: &str,
    threshold: usize,
) -> Result<(), anyhow::Error> {
    client
        .update_collection(
            UpdateCollectionBuilder::new(collection_name).optimizers_config(OptimizersConfigDiff {
                indexing_threshold: Some(threshold as u64),
                ..Default::default()
            }),
        )
        .await
        .context(format!(
            "Failed to set indexing threshold to {} for {}",
            threshold, collection_name
        ))?;
    Ok(())
}

/// Set mmap threshold
#[allow(unused)]
pub async fn set_mmap_threshold(
    client: &Qdrant,
    collection_name: &str,
    threshold: usize,
) -> Result<(), anyhow::Error> {
    client
        .update_collection(
            UpdateCollectionBuilder::new(collection_name).optimizers_config(OptimizersConfigDiff {
                memmap_threshold: Some(threshold as u64),
                ..Default::default()
            }),
        )
        .await
        .context(format!(
            "Failed to set mmap threshold to {} for {}",
            threshold, collection_name
        ))?;
    Ok(())
}

/// Set max segment size
#[allow(unused)]
pub async fn set_max_segment_size(
    client: &Qdrant,
    collection_name: &str,
    size: usize,
) -> Result<(), anyhow::Error> {
    client
        .update_collection(
            UpdateCollectionBuilder::new(collection_name).optimizers_config(OptimizersConfigDiff {
                max_segment_size: Some(size as u64),
                ..Default::default()
            }),
        )
        .await
        .context(format!(
            "Failed to set max segment size to {} for {}",
            size, collection_name
        ))?;
    Ok(())
}

/// Get collection info
pub async fn get_collection_info(
    client: &Qdrant,
    collection_name: &str,
) -> Result<Option<CollectionInfo>, anyhow::Error> {
    let collection_info = client
        .collection_info(collection_name)
        .await
        .context(format!(
            "Failed to fetch collection info for {}",
            collection_name
        ))?
        .result;
    Ok(collection_info)
}

/// Get collection status
pub async fn get_collection_status(
    client: &Qdrant,
    collection_name: &str,
) -> Result<CollectionStatus, anyhow::Error> {
    let info = get_collection_info(client, collection_name).await;
    match info {
        Ok(Some(info)) => Ok(CollectionStatus::try_from(info.status)?),
        Ok(None) => Err(anyhow::anyhow!(
            "Failed to get non-empty collection status for {}",
            collection_name
        )),
        Err(e) => Err(anyhow::anyhow!(
            "Failed to get collection for {} with error: {}",
            collection_name,
            e
        )),
    }
}

/// Wait for collection to be indexed
pub async fn wait_index(
    client: &Qdrant,
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

/// Internal vector names
const DEFAULT_VECTOR_NAME: &str = "default_coach_vector";
const UNUSED_VECTOR_NAME: &str = "unused_coach_vector";

/// Create collection
pub async fn create_collection(
    client: &Qdrant,
    collection_name: &str,
    vec_dim: usize,
    args: Arc<Args>,
) -> Result<(), anyhow::Error> {
    let mut builder = CreateCollectionBuilder::new(collection_name)
        .vectors_config(VectorsConfig {
            config: Some(Config::ParamsMap(VectorParamsMap {
                map: vec![
                    (
                        DEFAULT_VECTOR_NAME.to_string(),
                        VectorParams {
                            size: vec_dim as u64,
                            distance: Distance::Cosine.into(),
                            hnsw_config: Some(HnswConfigDiff {
                                m: None,
                                ef_construct: None,
                                full_scan_threshold: None,
                                max_indexing_threads: args.max_indexing_threads.map(|i| i as u64),
                                on_disk: Some(args.hnsw_on_disk),
                                payload_m: None,
                            }),
                            quantization_config: if args.use_scalar_quantization {
                                Some(QuantizationConfig {
                                    quantization: Some(Quantization::Scalar(ScalarQuantization {
                                        r#type: 1, //Int8
                                        quantile: None,
                                        always_ram: Some(true),
                                    })),
                                })
                            } else {
                                None
                            },
                            on_disk: Some(args.vectors_on_disk),
                            datatype: None,
                            multivector_config: None,
                        },
                    ),
                    (
                        UNUSED_VECTOR_NAME.to_string(), // unused vector to generate more complex config
                        VectorParams {
                            size: vec_dim as u64,
                            distance: Distance::Cosine.into(),
                            on_disk: Some(args.vectors_on_disk),
                            ..Default::default()
                        },
                    ),
                ]
                .into_iter()
                .collect::<HashMap<_, _>>(),
            })),
        })
        .replication_factor(args.replication_factor as u32)
        .write_consistency_factor(args.write_consistency_factor as u32)
        .on_disk_payload(args.payload_on_disk)
        .optimizers_config(OptimizersConfigDiff {
            indexing_threshold: args.indexing_threshold.map(|i| i as u64),
            memmap_threshold: args.memmap_threshold.map(|i| i as u64),
            ..Default::default()
        });

    if args.shard_number > 0 {
        builder = builder.shard_number(args.shard_number as u32);
    }

    client
        .create_collection(builder)
        .await
        .context(format!("Failed to create collection {}", collection_name))?;
    Ok(())
}

/// delete collection without checking if it exists
pub async fn delete_collection(
    client: &Qdrant,
    collection_name: &str,
) -> Result<(), anyhow::Error> {
    client
        .delete_collection(collection_name)
        .await
        .context(format!("Failed to delete collection {}", collection_name))?;
    Ok(())
}

/// delete collection if exists & create new one
pub async fn recreate_collection(
    client: &Qdrant,
    collection_name: &str,
    vec_dim: usize,
    args: Arc<Args>,
) -> Result<(), anyhow::Error> {
    if client.collection_exists(collection_name).await? {
        log::info!("recreating existing collection {}", collection_name);
        delete_collection(client, collection_name).await?;
        sleep(Duration::from_secs(2)).await;
    }
    create_collection(client, collection_name, vec_dim, args).await
}

/// insert points into collection (blocking)
/// vec_dim = 0 means no vectors
/// payload_count = 0 means no payloads
pub async fn insert_points_batch(
    client: &Qdrant,
    collection_name: &str,
    points_count: usize,
    vec_dim: usize,
    payload_count: usize,
    write_ordering: Option<WriteOrdering>,
    stopped: Arc<AtomicBool>,
) -> Result<(), CoachError> {
    let cut_off_size = 100;
    // handle less than batch & spill over
    let (batch_size, num_batches, last_batch_size) = if points_count <= cut_off_size {
        (points_count, 1, points_count)
    } else {
        let remainder = points_count % cut_off_size;
        let div = points_count / cut_off_size;
        let num_batches = div + if remainder > 0 { 1 } else { 0 };
        let last_batch_size = if remainder > 0 {
            remainder
        } else {
            cut_off_size
        };
        (cut_off_size, num_batches, last_batch_size)
    };
    for batch_id in 0..num_batches {
        let batch_size = if batch_id == num_batches - 1 {
            last_batch_size
        } else {
            batch_size
        };
        let mut points = Vec::with_capacity(batch_size);
        let batch_base_id = batch_id as u64 * cut_off_size as u64;
        for i in 0..batch_size {
            let idx = batch_base_id + i as u64;
            let point_id: PointId = PointId {
                point_id_options: Some(PointIdOptions::Num(idx)),
            };

            let vectors =
                Some(random_named_vector(DEFAULT_VECTOR_NAME.to_string(), vec_dim).into());
            let payload = random_payload(Some(payload_count)).into();
            let point_struct = PointStruct {
                id: Some(point_id),
                payload,
                vectors,
            };
            points.push(point_struct);
        }

        if stopped.load(Ordering::Relaxed) {
            return Err(Cancelled);
        }

        let mut builder = UpsertPointsBuilder::new(collection_name, points).wait(true);
        if let Some(write_ordering) = write_ordering {
            builder = builder.ordering(write_ordering);
        }
        // push batch blocking
        let resp = client.upsert_points(builder).await.context(format!(
            "Failed to insert {} points (batch {}/{}) into {}",
            batch_size, batch_id, num_batches, collection_name
        ))?;
        if resp.result.unwrap().status != 2 {
            return Err(Invariant(format!(
                "Failed to insert {} points (batch {}/{}) into {} (status {})",
                batch_size,
                batch_id,
                num_batches,
                collection_name,
                resp.result.unwrap().status
            )));
        }
    }
    Ok(())
}

/// Create collection snapshot
pub async fn create_collection_snapshot(
    client: &Qdrant,
    collection_name: &str,
) -> Result<CreateSnapshotResponse, anyhow::Error> {
    client
        .create_snapshot(collection_name)
        .await
        .context(format!(
            "Failed to create collection snapshot for {}",
            collection_name
        ))
}

/// Delete collection snapshot by name
pub async fn delete_collection_snapshot(
    client: &Qdrant,
    collection_name: &str,
    snapshot_name: &str,
) -> Result<(), anyhow::Error> {
    client
        .delete_snapshot(DeleteSnapshotRequestBuilder::new(
            collection_name,
            snapshot_name,
        ))
        .await
        .context(format!(
            "Failed to delete collection snapshot {} for {}",
            snapshot_name, collection_name
        ))?;
    Ok(())
}

/// List collection snapshots
pub async fn list_collection_snapshots(
    client: &Qdrant,
    collection_name: &str,
) -> Result<Vec<String>, anyhow::Error> {
    let snapshots = client
        .list_snapshots(collection_name)
        .await
        .context(format!(
            "Failed to list collection snapshots for {}",
            collection_name
        ))?
        .snapshot_descriptions;
    Ok(snapshots
        .into_iter()
        .map(|s| s.name)
        .collect::<Vec<String>>())
}

/// Count collection snapshots
pub async fn count_collection_snapshots(
    client: &Qdrant,
    collection_name: &str,
) -> Result<usize, anyhow::Error> {
    let snapshots = list_collection_snapshots(client, collection_name).await?;
    Ok(snapshots.len())
}

/// Create field index (non-blocking)
pub async fn create_field_index(
    client: &Qdrant,
    collection_name: &str,
    field_name: &str,
    field_type: FieldType,
) -> Result<(), anyhow::Error> {
    client
        .create_field_index(
            CreateFieldIndexCollectionBuilder::new(collection_name, field_name, field_type)
                .wait(false),
        )
        .await
        .context(format!(
            "Failed to create field index {} for collection {}",
            field_name, collection_name
        ))?;
    Ok(())
}
