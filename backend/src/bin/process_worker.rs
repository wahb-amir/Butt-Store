use anyhow::{Context, Result};
use backend::{
    config::Config,
    db::create_pool,
    state::AppState,
    storage::jobs::{claim_job, complete_job, fail_job},
    types::jobs::BackgroundJob,
};
use clap::Parser;
use sqlx::PgPool;
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

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
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,process_worker=debug".into()),
        )
        .init();

    let args = Args::parse();

    // Load configuration
    dotenvy::dotenv().ok();
    let config = Config::from_env();

    // Create database pool
    let pool = create_pool(&config.database_url).await?;
    info!("Database connected");

    let state = Arc::new(AppState { db: pool });

    info!("Starting distribution preparation worker: {}", args.worker_id);

    run_worker(state, args).await?;

    Ok(())
}

async fn run_worker(
    state: Arc<AppState>,
    args: Args,
) -> Result<()> {
    let mut empty_polls = 0u32;
    let mut shutdown = false;

    // Handle shutdown signals
    let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())?;
    let mut sigint = signal::unix::signal(signal::unix::SignalKind::interrupt())?;

    while !shutdown {
        tokio::select! {
            _ = sigterm.recv() => {
                info!("Received SIGTERM, shutting down...");
                shutdown = true;
            }
            _ = sigint.recv() => {
                info!("Received SIGINT, shutting down...");
                shutdown = true;
            }
            _ = tokio::time::sleep(Duration::from_secs(args.poll_interval)) => {
                match claim_job(&state, &args.worker_id).await {
                    Ok(Some(job)) => {
                        // Only process prepare_distribution jobs
                        if job.job_type != "prepare_distribution" {
                            warn!(job_id = %job.id, job_type = %job.job_type, "Skipping non-prepare_distribution job");
                            fail_job(&state, job.id, "Unexpected job type", false).await?;
                            continue;
                        }

                        empty_polls = 0;
                        info!(job_id = %job.id, file_id = %job.file_id, "Claimed distribution preparation job");

                        if let Err(e) = process_preparation_job(&state, &job, &args.worker_id).await {
                            error!(job_id = %job.id, error = %e, "Preparation job failed");
                        }
                    }
                    Ok(None) => {
                        empty_polls += 1;
                        if empty_polls >= args.max_empty_polls {
                            let backoff = Duration::from_secs(
                                (args.poll_interval as u64 * 2_u64.pow(empty_polls.min(6))).min(60)
                            );
                            info!(empty_polls, backoff_secs = backoff.as_secs(), "No jobs available, backing off");
                            tokio::time::sleep(backoff).await;
                        }
                    }
                    Err(e) => {
                        error!(error = %e, "Failed to claim job");
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    }
                }
            }
        }
    }

    info!("Worker stopped");
    Ok(())
}

async fn process_preparation_job(
    state: &AppState,
    job: &BackgroundJob,
    worker_id: &str,
) -> Result<()> {
    let file_id = job.file_id;

    // Get upload record
    let upload = get_upload_record(&state.db, file_id).await?;

    // Verify upload is in VERIFIED state
    if upload.status != "VERIFIED" {
        let err = format!("Upload is not in VERIFIED state (current: {})", upload.status);
        fail_job(state, job.id, &err, false).await?;
        return Ok(());
    }

    info!(job_id = %job.id, file_id = %file_id, blob = %upload.blob_name, "Processing distribution preparation");

    // TODO: Implement actual distribution preparation:
    // 1. Copy blob to public CDN container
    // 2. Extract/generate metadata (package name, version, minSdk, etc.)
    // 3. Create distribution manifest
    // 4. Update CDN/edge configuration
    // 5. Notify downstream services

    // For now, just log and mark as ready
    info!(job_id = %job.id, "Distribution preparation completed (stub)");

    // Update upload status to READY_FOR_DISTRIBUTION
    update_upload_status(&state.db, file_id, "READY_FOR_DISTRIBUTION", None).await?;

    complete_job(state, job.id).await?;
    info!(job_id = %job.id, file_id = %file_id, "Distribution preparation completed successfully");

    Ok(())
}

async fn get_upload_record(
    db: &PgPool,
    file_id: uuid::Uuid,
) -> Result<UploadRecord> {
    let record = sqlx::query_as::<_, UploadRecord>(
        r#"
        SELECT
            id, user_id, blob_name, original_filename, content_type,
            status, expected_size_bytes, expires_at
        FROM uploads
        WHERE id = $1
        "#,
    )
    .bind(file_id)
    .fetch_optional(db)
    .await?
    .ok_or_else(|| anyhow::anyhow!("Upload record not found"))?;

    Ok(record)
}

#[derive(sqlx::FromRow)]
struct UploadRecord {
    id: uuid::Uuid,
    user_id: uuid::Uuid,
    blob_name: String,
    original_filename: String,
    content_type: String,
    status: String,
    expected_size_bytes: i64,
    actual_size_bytes: Option<i64>,
    etag: Option<String>,
    checksum_sha256: Option<Vec<u8>>,
    expires_at: time::OffsetDateTime,
}

async fn update_upload_status(
    db: &PgPool,
    file_id: uuid::Uuid,
    status: &str,
    reason: Option<&str>,
) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE uploads
        SET status = $1, status_reason = $2, updated_at = now()
        WHERE id = $3
        "#,
    )
    .bind(status)
    .bind(reason)
    .bind(file_id)
    .execute(db)
    .await?;

    Ok(())
}