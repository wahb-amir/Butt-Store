use anyhow::Result;
use backend::{
    api::uploads::config::CHUNK_SIZE_BYTES,
    config::Config,
    db::create_pool,
    state::AppState,
    storage::{
        azure::{create_blob_client, create_block_blob_client, init_blob_service, validate_and_commit_upload},
        jobs::{claim_job_type, complete_job, create_job_tx, fail_job},
    },
    types::jobs::{BackgroundJob, JobType},
};
use clap::Parser;
use sqlx::{PgPool, Postgres, Transaction};
use std::sync::Arc;
use std::time::Duration;
use tokio::{signal, time::{sleep, timeout}};
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

const COMPLETION_ATTEMPTS: u32 = 3;
const COMPLETION_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(30);
const COMPLETION_RETRY_BACKOFF: Duration = Duration::from_secs(2);

#[derive(Parser, Debug)]
#[command(name = "upload_completion_worker", about = "Commits and finalizes uploaded blobs")]
struct Args {
    #[arg(long, default_value = "10")]
    poll_interval: u64,
    #[arg(long, default_value = "5")]
    max_empty_polls: u32,
    #[arg(long, default_value = "upload-completion-worker-1")]
    worker_id: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    dotenvy::dotenv().ok();
    let config = Config::from_env();
    let pool = create_pool(&config.database_url).await?;
    let _blob_service = init_blob_service()?;
    run_worker(Arc::new(AppState { db: pool }), Args::parse()).await
}

async fn run_worker(state: Arc<AppState>, args: Args) -> Result<()> {
    let mut empty_polls = 0u32;
    let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())?;
    let mut sigint = signal::unix::signal(signal::unix::SignalKind::interrupt())?;

    loop {
        tokio::select! {
            _ = sigterm.recv() => break,
            _ = sigint.recv() => break,
            _ = tokio::time::sleep(Duration::from_secs(args.poll_interval)) => {
                match claim_job_type(&state, JobType::CompleteUpload, &args.worker_id).await {
                    Ok(Some(job)) => {
                        empty_polls = 0;
                        if let Err(error) = process_completion_job(&state, &job).await {
                            error!(job_id = %job.id, error = %error, "Upload completion job failed");
                            fail_job(&state, job.id, &error.to_string(), true).await?;
                        }
                    }
                    Ok(None) => {
                        empty_polls += 1;
                        if empty_polls >= args.max_empty_polls {
                            let backoff = Duration::from_secs(
                                (args.poll_interval * 2_u64.pow(empty_polls.min(6))).min(60),
                            );
                            tokio::time::sleep(backoff).await;
                        }
                    }
                    Err(error) => {
                        error!(error = %error, "Failed to claim upload completion job");
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    }
                }
            }
        }
    }
    info!("Upload completion worker stopped");
    Ok(())
}

async fn process_completion_job(state: &AppState, job: &BackgroundJob) -> Result<()> {
    let upload = get_upload_record(&state.db, job.file_id).await?;
    if upload.status != "PROCESSING_PENDING" {
        fail_job(state, job.id, "Upload is not in PROCESSING_PENDING state", false).await?;
        return Ok(());
    }

    let block_blob: azure_storage_blob::BlockBlobClient = create_block_blob_client(&upload.blob_name)?;
    let blob = create_blob_client(&upload.blob_name)?;
    let expected_size = u64::try_from(upload.expected_size_bytes)?;
    let (actual_size, etag) = validate_and_commit_with_retry(
        &block_blob,
        &blob,
        expected_size,
        CHUNK_SIZE_BYTES,
    )
    .await?;
    if actual_size != expected_size {
        fail_job(state, job.id, "Committed blob size does not match expected size", false).await?;
        return Ok(());
    }

    let mut tx: Transaction<'_, Postgres> = state.db.begin().await?;
    let updated: sqlx::postgres::PgQueryResult = sqlx::query(
        "UPDATE uploads SET actual_size_bytes = $1, etag = $2, completed_at = now()
         WHERE id = $3 AND status = 'PROCESSING_PENDING'",
    )
    .bind(i64::try_from(actual_size)?)
    .bind(&etag)
    .bind(job.file_id)
    .execute(&mut *tx)
    .await?;
    if updated.rows_affected() != 1 {
        return Err(anyhow::anyhow!("Upload state changed while completing"));
    }
    create_job_tx(&mut tx, job.file_id, JobType::VerifyApk).await?;
    tx.commit().await?;
    complete_job(state, job.id).await?;
    info!(job_id = %job.id, file_id = %job.file_id, etag = %etag, "Upload committed and queued for processing");
    Ok(())
}

async fn validate_and_commit_with_retry(
    block_blob: &azure_storage_blob::BlockBlobClient,
    blob: &azure_storage_blob::clients::BlobClient,
    expected_size: u64,
    chunk_size: u64,
) -> Result<(u64, String)> {
    let mut last_error = None;

    for attempt in 1..=COMPLETION_ATTEMPTS {
        match timeout(
            COMPLETION_ATTEMPT_TIMEOUT,
            validate_and_commit_upload(block_blob, blob, expected_size, chunk_size),
        )
        .await
        {
            Ok(Ok(result)) => return Ok(result),
            Ok(Err(error)) => last_error = Some(error),
            Err(_) => {
                last_error = Some(anyhow::anyhow!(
                    "upload completion attempt timed out after {} seconds",
                    COMPLETION_ATTEMPT_TIMEOUT.as_secs()
                ));
            }
        }

        if attempt < COMPLETION_ATTEMPTS {
            let backoff = COMPLETION_RETRY_BACKOFF * 2u32.pow(attempt - 1);
            sleep(backoff).await;
        }
    }

    Err(last_error.expect("completion attempts always produce an error"))
}

async fn get_upload_record(db: &PgPool, file_id: uuid::Uuid) -> Result<UploadRecord> {
    sqlx::query_as::<_, UploadRecord>(
        "SELECT blob_name, status, expected_size_bytes FROM uploads WHERE id = $1",
    )
    .bind(file_id)
    .fetch_optional(db)
    .await?
    .ok_or_else(|| anyhow::anyhow!("Upload record not found"))
}

#[derive(sqlx::FromRow)]
struct UploadRecord {
    blob_name: String,
    status: String,
    expected_size_bytes: i64,
}
