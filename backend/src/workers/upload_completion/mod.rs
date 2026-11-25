use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use sqlx::{PgPool, Postgres, Transaction};
use tokio::time::{sleep, timeout};
use tracing::{error, info};

use crate::{
    api::uploads::{config::CHUNK_SIZE_BYTES, models::UploadStatus},
    state::AppState,
    storage::{
        azure::{create_blob_client, create_block_blob_client, validate_and_commit_upload},
        jobs::{claim_job_type, complete_job, create_job_tx, fail_job},
    },
    types::jobs::{BackgroundJob, JobType},
    workers::{
        context::WorkerContext,
        lifecycle::{PollConfig, run_poll_loop},
    },
};

const COMPLETION_ATTEMPTS: u32 = 3;
const COMPLETION_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(30);
const COMPLETION_RETRY_BACKOFF: Duration = Duration::from_secs(2);
const COMPLETION_JOB_TIMEOUT: Duration = Duration::from_secs(5 * 60);

/// Configuration for the upload-completion worker.
pub struct UploadCompletionWorkerConfig {
    pub poll_interval: Duration,
    pub max_empty_polls: u32,
    pub worker_id: String,
}

/// Start the upload-completion worker poll loop.
///
/// Claims `complete_upload` jobs, validates that all expected blocks are staged
/// in Azure, commits the block list, records the final blob size and ETag, and
/// enqueues a `verify_apk` job — all within a single database transaction.
pub async fn run(ctx: WorkerContext, config: UploadCompletionWorkerConfig) -> Result<()> {
    let state = Arc::new(ctx.as_app_state());

    let poll_config = PollConfig {
        poll_interval: config.poll_interval,
        max_empty_polls: config.max_empty_polls,
    };
    let worker_id = config.worker_id;

    run_poll_loop(poll_config, move || {
        let state = Arc::clone(&state);
        let worker_id = worker_id.clone();
        async move {
            match claim_job_type(&state, JobType::CompleteUpload, &worker_id).await {
                Ok(Some(job)) => {
                    info!(job_id = %job.id, file_id = %job.file_id, "Claimed upload completion job");
                    let result = tokio::time::timeout(
                        COMPLETION_JOB_TIMEOUT,
                        process_completion_job(&state, &job),
                    )
                    .await
                    .map_err(|_| {
                        anyhow::anyhow!(
                            "upload completion timed out after {} minutes",
                            COMPLETION_JOB_TIMEOUT.as_secs() / 60
                        )
                    })
                    .and_then(|result| result);
                    if let Err(error) = result {
                        error!(job_id = %job.id, error = %error, "Upload completion job failed");
                        fail_job(&state, job.id, &error.to_string(), true).await?;
                    }
                    Ok(true)
                }
                Ok(None) => Ok(false),
                Err(error) => Err(error),
            }
        }
    })
    .await
}

// ---------------------------------------------------------------------------
// Job processing
// ---------------------------------------------------------------------------

async fn process_completion_job(state: &AppState, job: &BackgroundJob) -> Result<()> {
    let upload = get_upload_record(&state.db, job.file_id).await?;
    if upload.status != UploadStatus::ProcessingPending {
        let reason = "Upload is not in PROCESSING_PENDING state";
        fail_job(state, job.id, reason, false).await?;
        update_upload_failure(&state.db, job.file_id, reason).await?;
        return Ok(());
    }

    let block_blob: azure_storage_blob::BlockBlobClient =
        create_block_blob_client(&upload.blob_name)?;
    let blob = create_blob_client(&upload.blob_name)?;
    let expected_size = u64::try_from(upload.expected_size_bytes)?;
    let (actual_size, etag) =
        validate_and_commit_with_retry(&block_blob, &blob, expected_size, CHUNK_SIZE_BYTES).await?;
    if actual_size != expected_size {
        let reason = "Committed blob size does not match expected size";
        fail_job(state, job.id, reason, false).await?;
        update_upload_failure(&state.db, job.file_id, reason).await?;
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

// ---------------------------------------------------------------------------
// Database helpers
// ---------------------------------------------------------------------------

#[derive(sqlx::FromRow)]
struct UploadRecord {
    blob_name: String,
    status: UploadStatus,
    expected_size_bytes: i64,
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

async fn update_upload_failure(db: &PgPool, file_id: uuid::Uuid, reason: &str) -> Result<()> {
    sqlx::query(
        "UPDATE uploads
         SET status = 'FAILED', failure_reason = $1, updated_at = now()
         WHERE id = $2",
    )
    .bind(reason)
    .bind(file_id)
    .execute(db)
    .await?;
    Ok(())
}
