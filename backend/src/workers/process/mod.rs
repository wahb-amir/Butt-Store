use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use sqlx::PgPool;
use tokio::time::timeout;
use tracing::{error, info};

use crate::{
    api::uploads::models::UploadStatus,
    state::AppState,
    storage::jobs::{claim_job_type, complete_job, fail_job},
    types::jobs::{BackgroundJob, JobType},
    workers::{
        context::WorkerContext,
        lifecycle::{PollConfig, run_poll_loop},
    },
};

const PREPARATION_TIMEOUT: Duration = Duration::from_secs(10 * 60);

/// Configuration for the distribution-preparation worker.
pub struct ProcessWorkerConfig {
    pub poll_interval: Duration,
    pub max_empty_polls: u32,
    pub worker_id: String,
}

/// Start the distribution-preparation worker poll loop.
///
/// Claims `prepare_distribution` jobs from the queue and transitions
/// successfully verified uploads to `ReadyForDistribution`.
/// This worker does not require Azure Blob Storage access.
pub async fn run(ctx: WorkerContext, config: ProcessWorkerConfig) -> Result<()> {
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
            match claim_job_type(&state, JobType::PrepareDistribution, &worker_id).await {
                Ok(Some(job)) => {
                    info!(job_id = %job.id, file_id = %job.file_id, "Claimed distribution preparation job");
                    let result = match timeout(
                        PREPARATION_TIMEOUT,
                        process_preparation_job(&state, &job, &worker_id),
                    )
                    .await
                    {
                        Ok(result) => result,
                        Err(_) => Err(anyhow::anyhow!(
                            "distribution preparation timed out after {} minutes",
                            PREPARATION_TIMEOUT.as_secs() / 60
                        )),
                    };
                    if let Err(e) = result {
                        error!(job_id = %job.id, error = %e, "Preparation job failed");
                        fail_job(&state, job.id, &e.to_string(), true).await?;
                    }
                    Ok(true)
                }
                Ok(None) => Ok(false),
                Err(e) => Err(e),
            }
        }
    })
    .await
}

// ---------------------------------------------------------------------------
// Job processing
// ---------------------------------------------------------------------------

async fn process_preparation_job(
    state: &AppState,
    job: &BackgroundJob,
    _worker_id: &str,
) -> Result<()> {
    let file_id = job.file_id;

    // Get upload record
    let upload = get_upload_record(&state.db, file_id).await?;

    // Verify upload is in VERIFIED state
    if upload.status != UploadStatus::Verified {
        let err = format!(
            "Upload is not in VERIFIED state (current: {:?})",
            upload.status
        );
        fail_job(state, job.id, &err, false).await?;
        update_upload_failure(&state.db, file_id, &err).await?;
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
    update_upload_status(&state.db, file_id, UploadStatus::ReadyForDistribution).await?;

    complete_job(state, job.id).await?;
    info!(job_id = %job.id, file_id = %file_id, "Distribution preparation completed successfully");

    Ok(())
}

// ---------------------------------------------------------------------------
// Database helpers
// ---------------------------------------------------------------------------

#[derive(sqlx::FromRow)]
struct UploadRecord {
    #[allow(dead_code)]
    id: uuid::Uuid,
    #[allow(dead_code)]
    user_id: uuid::Uuid,
    blob_name: String,
    #[allow(dead_code)]
    original_filename: String,
    #[allow(dead_code)]
    content_type: String,
    status: UploadStatus,
    #[allow(dead_code)]
    expected_size_bytes: i64,
    #[allow(dead_code)]
    expires_at: time::OffsetDateTime,
}

async fn get_upload_record(db: &PgPool, file_id: uuid::Uuid) -> Result<UploadRecord> {
    sqlx::query_as::<_, UploadRecord>(
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
    .ok_or_else(|| anyhow::anyhow!("Upload record not found"))
}

async fn update_upload_status(
    db: &PgPool,
    file_id: uuid::Uuid,
    status: UploadStatus,
) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE uploads
        SET status = $1, updated_at = now()
        WHERE id = $2
        "#,
    )
    .bind(status)
    .bind(file_id)
    .execute(db)
    .await?;

    Ok(())
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
