use clap::Parser;

/// Tool for coaching Qdrant instances
#[derive(Parser, Debug, Clone)]
#[clap(version, about)]
pub struct Args {
    /// Qdrant service URI
    #[clap(long, default_value = "http://localhost:6334")]
    pub uri: String,
    /// Number of parallel drills to run
    #[clap(short, long, default_value_t = 3)]
    pub parallel_drills: usize,
}
