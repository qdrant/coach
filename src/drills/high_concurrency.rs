use anyhow::Result;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use crate::args::Args;
use crate::common::client::{
    create_collection, delete_collection, delete_point_by_id, get_point_by_id, get_points_count,
    search_points, set_payload, upsert_point_by_id,
};
use crate::common::coach_errors::CoachError;
use crate::common::coach_errors::CoachError::Cancelled;
use crate::drill_runner::Drill;
use crate::get_config;
use async_trait::async_trait;
use futures::StreamExt;
use qdrant_client::Qdrant;
use qdrant_client::qdrant::WriteOrdering;
use rand::RngExt;
use rand::SeedableRng;
use rand::rngs::SmallRng;

/// Target number of concurrent workers per gRPC connection.
///
/// h2 (>= 0.4.16) enforces a per-connection budget on the framing overhead of small DATA
/// frames: 25600 bytes, charged `256 - payload_len` for every buffered frame that does not
/// carry END_STREAM, and refunded once the body is read. gRPC puts END_STREAM on the
/// trailers, so every unary response is charged while it sits unread, and Qdrant's responses
/// are tiny (~30 bytes, hence ~226 each). When the client falls behind the socket the
/// backlog can exhaust the budget, and h2 tears the connection down with
/// GOAWAY/ENHANCE_YOUR_CALM, which tonic surfaces as `ResourceExhausted`.
///
/// Coach hit exactly that against qdrant:dev in CI (runs 32577563283, 32644303822,
/// 32858211174), starting with the h2 0.4.15 -> 0.4.16 bump that introduced the budget.
/// Spreading the workers over more connections lowers the per-connection backlog without
/// reducing the overall concurrency the drill is meant to apply.
const WORKERS_PER_CONNECTION: usize = 50;

/// Drill that performs operations on a collection with a high level of concurrency (without indexing).
/// Run `concurrency_level` workers which repeatedly call APIs for inserting -> searching -> set payload -> updating -> getting one -> deleting
/// The collection is created and populated with random data if it does not exist.
pub struct HighConcurrency {
    collection_name: String,
    concurrency_level: usize,
    number_iterations: usize,
    vec_dim: usize,
    keyword_variants: usize,
    write_ordering: Option<WriteOrdering>,
    stopped: CancellationToken,
}

impl HighConcurrency {
    pub fn new(stopped: CancellationToken) -> Self {
        let collection_name = "high-concurrency".to_string();
        let concurrency_level = 400;
        let number_iterations = 40000;
        let vec_dim = 128;
        let keyword_variants = 2;
        let write_ordering = None; // default
        HighConcurrency {
            collection_name,
            concurrency_level,
            number_iterations,
            vec_dim,
            keyword_variants,
            write_ordering,
            stopped,
        }
    }

    async fn run_for_point(
        &self,
        client: &Qdrant,
        point_id: u64,
        mut rng: SmallRng,
    ) -> Result<(), CoachError> {
        // insert single point
        upsert_point_by_id(
            client,
            &self.collection_name,
            point_id,
            self.vec_dim,
            self.keyword_variants,
            self.write_ordering,
            &mut rng,
        )
        .await?;

        // search random points
        search_points(
            client,
            &self.collection_name,
            self.vec_dim,
            self.keyword_variants,
            &mut rng,
        )
        .await?;

        // set random payload on point
        set_payload(
            client,
            &self.collection_name,
            point_id,
            self.keyword_variants,
            self.write_ordering,
            &mut rng,
        )
        .await?;

        // updating point by id
        upsert_point_by_id(
            client,
            &self.collection_name,
            point_id,
            self.vec_dim,
            self.keyword_variants,
            self.write_ordering,
            &mut rng,
        )
        .await?;

        // getting point by id
        let retrieved = get_point_by_id(client, &self.collection_name, point_id).await?;
        if retrieved.is_none() {
            return Err(CoachError::Invariant(format!(
                "The point {} was not found after it was inserted in {}",
                point_id, self.collection_name
            )));
        }

        // delete point by id
        delete_point_by_id(client, &self.collection_name, point_id).await?;

        Ok(())
    }

    fn pick_random<'a>(clients: &'a [Qdrant], rng: &mut SmallRng) -> &'a Qdrant {
        let index = rng.random_range(0..clients.len());
        &clients[index]
    }
}

#[async_trait]
impl Drill for HighConcurrency {
    fn name(&self) -> &'static str {
        "high_concurrency"
    }

    fn reschedule_after_sec(&self) -> u64 {
        10
    }

    async fn run(
        &self,
        client: &Qdrant,
        args: Arc<Args>,
        rng: &mut SmallRng,
    ) -> Result<(), CoachError> {
        // delete if already exists
        if client.collection_exists(&self.collection_name).await? {
            delete_collection(client, &self.collection_name).await?;
        }

        // create collection
        create_collection(client, &self.collection_name, self.vec_dim, args.clone()).await?;

        // workers will target random clients for all nodes to stress the cluster
        // each client round-robins over a pool of connections so that no single HTTP/2
        // connection carries all the workers (see `WORKERS_PER_CONNECTION`)
        let pool_size = self.concurrency_level.div_ceil(WORKERS_PER_CONNECTION);
        let mut target_clients = vec![];
        for uri in &args.uris {
            let mut config = get_config(uri, args.grpc_timeout_ms, args.api_key.as_deref());
            config.set_pool_size(pool_size);
            let target_client = Qdrant::new(config)?;
            target_clients.push(target_client);
        }

        // lazy stream of futures - fork a per-future rng from the drill rng so concurrent
        // ops don't share state and stay deterministic per seed
        let query_stream = (0..self.number_iterations).map(|n| {
            let target = Self::pick_random(&target_clients, rng);
            let future_rng = SmallRng::from_rng(rng);
            self.run_for_point(target, n as u64, future_rng)
        });

        // at most self.concurrency_level futures will be running at the same time
        let mut upsert_stream =
            futures::stream::iter(query_stream).buffer_unordered(self.concurrency_level);

        // consume stream
        while let Some(result) = upsert_stream.next().await {
            // stop consuming stream on shutdown
            if self.stopped.is_cancelled() {
                return Err(Cancelled);
            }
            // stop consuming stream on first error
            result?;
        }

        // every iteration inserts and deletes its own point - collection must be empty
        let remaining = get_points_count(client, &self.collection_name).await?;
        if remaining != 0 {
            return Err(CoachError::Invariant(format!(
                "{} has {} leftover points after concurrent insert/delete cycles",
                self.collection_name, remaining
            )));
        }

        // delete collection to not accumulate data over time
        delete_collection(client, &self.collection_name).await?;

        Ok(())
    }

    async fn before_all(
        &self,
        _client: &Qdrant,
        _args: Arc<Args>,
        _rng: &mut SmallRng,
    ) -> Result<(), CoachError> {
        // no need to explicitly honor args.recreate_collection
        // because we are going to delete the collection anyway
        Ok(())
    }
}
