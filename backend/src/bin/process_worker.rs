use anyhow::Result;
use backend::{
    config::Config,
    workers::{
        context::WorkerContext,
        init_tracing,
        process::{ProcessWorkerConfig, run},
    },
};
use clap::Parser;
use std::time::Duration;

#[derive(Parser, Debug)]
#[command(name = "process_worker", about = "Distribution preparation worker")]
struct Args {
    /// Poll interval in seconds
    #[arg(long, default_value = "10")]
    poll_interval: u64,

    /// Maximum number of consecutive empty polls before backing off
    #[arg(long, default_value = "5")]
    max_empty_polls: u32,

    /// Worker identifier (for logging)
    #[arg(long, default_value = "process-worker-1")]
    worker_id: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let args = Args::parse();
    init_tracing("process_worker");

    let config = Config::from_env();
    // This worker only reads from the database; Azure is not required.
    let ctx = WorkerContext::new_db_only(&config).await?;

    tracing::info!(
        "Starting distribution preparation worker: {}",
        args.worker_id
    );

    run(
        ctx,
        ProcessWorkerConfig {
            poll_interval: Duration::from_secs(args.poll_interval),
            max_empty_polls: args.max_empty_polls,
            worker_id: args.worker_id,
        },
    )
    .await
}
