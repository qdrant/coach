use clap::Parser;

/// Tool for coaching Qdrant instances
#[derive(Parser, Debug, Clone)]
#[command(version, about)]
pub struct Args {
    /// Qdrant gRPC service URIs (can be used several times to specify several URIs)
    #[arg(long, default_value = "http://localhost:6334")]
    pub uris: Vec<String>,
    /// Number of parallel drills to run
    #[arg(short, long, default_value_t = 2)]
    pub parallel_drills: usize,
    /// Replication factor for collections
    #[arg(long, default_value_t = 1)]
    pub replication_factor: usize,
    /// Writing consistency factor for collections
    #[arg(long, default_value_t = 1)]
    pub write_consistency_factor: usize,
    /// Optimizer indexing threshold
    #[arg(long)]
    pub indexing_threshold: Option<usize>,
    /// Maximum size (in KiloBytes) of vectors to store in-memory per segment.
    #[arg(long)]
    pub memmap_threshold: Option<usize>,
    /// Always create collection before the first run of a drill
    #[arg(long, default_value_t = false)]
    pub recreate_collection: bool,
    /// Stop all drills at the first error encountered
    #[arg(long, default_value_t = false)]
    pub stop_at_first_error: bool,
    /// Only run the healthcheck for the input URI, no drills executed.
    #[arg(long, default_value_t = false)]
    pub only_healthcheck: bool,
    /// Delay between health checks
    #[arg(long, default_value_t = 200)]
    pub health_check_delay_ms: usize,
    /// Name of the drills to ignore
    #[arg(long)]
    pub ignored_drills: Vec<String>,
    /// Name of the drills to run, ignore others
    #[arg(long)]
    pub drills_to_run: Vec<String>,
    /// Timeout of gRPC client
    #[arg(long, default_value_t = 5000)]
    pub grpc_timeout_ms: usize,
    /// Timeout of gRPC health client
    #[arg(long, default_value_t = 50)]
    pub grpc_health_check_timeout_ms: usize,
}
