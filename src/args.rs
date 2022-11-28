use clap::Parser;

/// Tool for coaching Qdrant instances
#[derive(Parser, Debug, Clone)]
#[clap(version, about)]
pub struct Args {
    /// Qdrant gRPC service URI
    #[clap(long, default_value = "http://localhost:6334")]
    pub uri: String,
    /// Qdrant HTTP service URI
    #[clap(long, default_value = "http://localhost:6333")]
    pub http_uri: String,
    /// Number of parallel drills to run
    #[clap(short, long, default_value_t = 3)]
    pub parallel_drills: usize,
    /// Replication factor for collections
    #[clap(short, long, default_value_t = 1)]
    pub replication_factor: usize,
    /// Optimizer indexing threshold
    #[clap(short, long)]
    pub indexing_threshold: Option<usize>,
    /// Always create collection before the first run of a drill
    #[clap(long, default_value_t = false)]
    pub recreate_collection: bool,
    /// Stop all drills at the first error encountered
    #[clap(long, default_value_t = false)]
    pub stop_at_first_error: bool,
}
