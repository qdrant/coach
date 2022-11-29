use clap::Parser;

/// Tool for coaching Qdrant instances
#[derive(Parser, Debug, Clone)]
#[command(version, about)]
pub struct Args {
    /// Qdrant gRPC service URIs (can be used several times to specify several URIs)
    #[arg(long, default_value = "http://localhost:6333")]
    pub uris: Vec<String>,
    /// Number of parallel drills to run
    #[arg(short, long, default_value_t = 3)]
    pub parallel_drills: usize,
    /// Replication factor for collections
    #[arg(long, default_value_t = 1)]
    pub replication_factor: usize,
    /// Optimizer indexing threshold
    #[arg(long)]
    pub indexing_threshold: Option<usize>,
    /// Always create collection before the first run of a drill
    #[arg(long, default_value_t = false)]
    pub recreate_collection: bool,
    /// Stop all drills at the first error encountered
    #[arg(long, default_value_t = false)]
    pub stop_at_first_error: bool,
    /// Delay between health checks
    #[arg(long, default_value_t = 100)]
    pub health_check_delay_ms: usize,
}
