use anyhow::Result;
use backend::{
    config::Config,
    utils::runtime::require_command,
    workers::{
        context::WorkerContext,
        init_tracing,
        verify::{VerifyWorkerConfig, run},
    },
};
use clap::Parser;
use std::time::Duration;

#[derive(Parser, Debug)]
#[command(name = "verify_worker", about = "APK verification worker")]
struct Args {
    /// Poll interval in seconds
    #[arg(long, default_value = "10")]
    poll_interval: u64,

    /// Maximum number of consecutive empty polls before backing off
    #[arg(long, default_value = "5")]
    max_empty_polls: u32,

    /// Worker identifier (for logging)
    #[arg(long, default_value = "verify-worker-1")]
    worker_id: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let args = Args::parse();
    init_tracing("verify_worker");

    require_command("aapt2")?;
    require_command("clamdscan")?;

    let config = Config::from_env();
    let ctx = WorkerContext::new_with_azure(&config).await?;

    tracing::info!("Starting verification worker: {}", args.worker_id);

    run(
        ctx,
        VerifyWorkerConfig {
            poll_interval: Duration::from_secs(args.poll_interval),
            max_empty_polls: args.max_empty_polls,
            worker_id: args.worker_id,
        },
    )
    .await
}
