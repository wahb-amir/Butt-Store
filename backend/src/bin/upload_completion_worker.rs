use anyhow::Result;
use backend::{
    config::Config,
    workers::{
        context::WorkerContext,
        init_tracing,
        upload_completion::{UploadCompletionWorkerConfig, run},
    },
};
use clap::Parser;
use std::time::Duration;

#[derive(Parser, Debug)]
#[command(
    name = "upload_completion_worker",
    about = "Commits and finalizes uploaded blobs"
)]
struct Args {
    /// Poll interval in seconds
    #[arg(long, default_value = "10")]
    poll_interval: u64,

    /// Maximum number of consecutive empty polls before backing off
    #[arg(long, default_value = "5")]
    max_empty_polls: u32,

    /// Worker identifier (for logging)
    #[arg(long, default_value = "upload-completion-worker-1")]
    worker_id: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let args = Args::parse();
    init_tracing("upload_completion_worker");

    let config = Config::from_env();
    let ctx = WorkerContext::new_with_azure(&config).await?;

    tracing::info!("Starting upload completion worker: {}", args.worker_id);

    run(
        ctx,
        UploadCompletionWorkerConfig {
            poll_interval: Duration::from_secs(args.poll_interval),
            max_empty_polls: args.max_empty_polls,
            worker_id: args.worker_id,
        },
    )
    .await
}
