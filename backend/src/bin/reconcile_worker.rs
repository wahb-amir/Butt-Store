use anyhow::Result;
use backend::{
    config::Config,
    workers::{
        context::WorkerContext,
        init_tracing,
        reconcile::{ReconcileWorkerConfig, run, run_once},
    },
};
use clap::Parser;
use std::time::Duration;

#[derive(Parser, Debug)]
#[command(
    name = "reconcile_worker",
    about = "Azure-PostgreSQL reconciliation worker"
)]
struct Args {
    /// Run once and exit (default: run continuously)
    #[arg(long)]
    once: bool,

    /// Poll interval in seconds (for continuous mode)
    #[arg(long, default_value = "300")]
    poll_interval: u64,

    /// Worker identifier (for logging)
    #[arg(long, default_value = "reconcile-worker-1")]
    worker_id: String,

    /// Container name to scan
    #[arg(long, default_value = "uploads")]
    container: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let args = Args::parse();
    init_tracing("reconcile_worker");

    let config = Config::from_env();
    let ctx = WorkerContext::new_with_azure(&config).await?;

    tracing::info!("Starting reconciliation worker: {}", args.worker_id);

    let worker_config = ReconcileWorkerConfig {
        poll_interval: Duration::from_secs(args.poll_interval),
        worker_id: args.worker_id,
        container: args.container,
    };

    if args.once {
        run_once(ctx, worker_config).await
    } else {
        run(ctx, worker_config).await
    }
}
