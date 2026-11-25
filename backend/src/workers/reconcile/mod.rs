use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use azure_storage_blob::BlobServiceClient;
use sqlx::PgPool;
use tokio::time::{sleep, timeout};
use tracing::{error, info, warn};

use crate::{
    api::uploads::models::UploadStatus,
    state::AppState,
    storage::{
        azure::{delete_blob, list_blobs},
        jobs::create_job,
    },
    types::jobs::JobType,
    workers::{context::WorkerContext, lifecycle::run_interval_loop},
};

const RECONCILIATION_ATTEMPTS: u32 = 3;
const RECONCILIATION_TIMEOUT: Duration = Duration::from_secs(30 * 60);
const RECONCILIATION_RETRY_BACKOFF: Duration = Duration::from_secs(30);

/// Configuration for the Azure-PostgreSQL reconciliation worker.
pub struct ReconcileWorkerConfig {
    pub poll_interval: Duration,
    pub worker_id: String,
    pub container: String,
}

/// Start the reconciliation worker in continuous (interval) mode.
///
/// Runs a full Azure↔PostgreSQL reconciliation scan on every interval.
/// Exits cleanly on SIGTERM / SIGINT.
pub async fn run(ctx: WorkerContext, config: ReconcileWorkerConfig) -> Result<()> {
    let blob_service = ctx.blob_service()?;
    let state = Arc::new(ctx.as_app_state());

    let container = config.container;
    let worker_id = config.worker_id;

    run_interval_loop(config.poll_interval, move || {
        let state = Arc::clone(&state);
        let blob_service = Arc::clone(&blob_service);
        let container = container.clone();
        let worker_id = worker_id.clone();
        async move {
            run_reconciliation_with_retry(&state, &blob_service, &container, &worker_id).await
        }
    })
    .await
}

/// Run a single reconciliation pass and return.
///
/// Used by the `--once` mode in the binary entry point.
pub async fn run_once(ctx: WorkerContext, config: ReconcileWorkerConfig) -> Result<()> {
    let blob_service = ctx.blob_service()?;
    let state = Arc::new(ctx.as_app_state());
    run_reconciliation_with_retry(&state, &blob_service, &config.container, &config.worker_id).await
}

// ---------------------------------------------------------------------------
// Reconciliation logic
// ---------------------------------------------------------------------------

async fn run_reconciliation_with_retry(
    state: &AppState,
    blob_service: &BlobServiceClient,
    container_name: &str,
    worker_id: &str,
) -> Result<()> {
    let mut last_error = None;

    for attempt in 1..=RECONCILIATION_ATTEMPTS {
        match timeout(
            RECONCILIATION_TIMEOUT,
            run_reconciliation_once(state, blob_service, container_name, worker_id),
        )
        .await
        {
            Ok(Ok(())) => return Ok(()),
            Ok(Err(error)) => last_error = Some(error),
            Err(_) => {
                last_error = Some(anyhow::anyhow!(
                    "reconciliation timed out after {} minutes",
                    RECONCILIATION_TIMEOUT.as_secs() / 60
                ));
            }
        }

        if attempt < RECONCILIATION_ATTEMPTS {
            let backoff = RECONCILIATION_RETRY_BACKOFF * attempt;
            warn!(
                attempt,
                backoff_secs = backoff.as_secs(),
                "Retrying reconciliation run"
            );
            sleep(backoff).await;
        }
    }

    Err(last_error.expect("reconciliation attempts always produce an error"))
}

async fn run_reconciliation_once(
    state: &AppState,
    blob_service: &BlobServiceClient,
    container_name: &str,
    worker_id: &str,
) -> Result<()> {
    info!(worker_id, container = %container_name, "Starting reconciliation run");

    // List all blobs in the container
    let blobs = list_blobs(blob_service, container_name).await?;
    info!(count = blobs.len(), "Found blobs in container");

    let mut stats = ReconciliationStats::default();

    for blob_name in blobs {
        // Skip non-APK files
        if !blob_name.ends_with(".apk") && !blob_name.ends_with(".aab") {
            continue;
        }

        match reconcile_blob(state, &blob_name, container_name).await {
            Ok(action) => match action {
                ReconcileAction::CreatedUpload => stats.created_uploads += 1,
                ReconcileAction::CreatedJob => stats.created_jobs += 1,
                ReconcileAction::NoAction => stats.no_action += 1,
            },
            Err(e) => {
                error!(blob = %blob_name, error = %e, "Failed to reconcile blob");
                stats.errors += 1;
            }
        }
    }

    info!(
        created_uploads = stats.created_uploads,
        created_jobs = stats.created_jobs,
        no_action = stats.no_action,
        errors = stats.errors,
        "Reconciliation run completed"
    );

    Ok(())
}

#[derive(Default, Debug)]
struct ReconciliationStats {
    created_uploads: u64,
    created_jobs: u64,
    no_action: u64,
    errors: u64,
}

enum ReconcileAction {
    CreatedUpload,
    CreatedJob,
    NoAction,
}

async fn reconcile_blob(
    state: &AppState,
    blob_name: &str,
    container_name: &str,
) -> Result<ReconcileAction> {
    // Check if upload record exists for this blob
    let existing_upload = sqlx::query_as::<_, UploadRecord>(
        r#"
        SELECT id, status
        FROM uploads
        WHERE blob_name = $1
        "#,
    )
    .bind(blob_name)
    .fetch_optional(&state.db)
    .await?;

    match existing_upload {
        Some(upload) => {
            // Upload record exists - check status
            match upload.status {
                UploadStatus::Pending => {
                    // Upload not yet completed - this is an orphaned blob
                    // Could be a completed upload where DB transaction failed
                    info!(blob = %blob_name, upload_id = %upload.id, "Found orphaned committed blob, checking for verification job");

                    // Check if verification job exists
                    let job_exists = sqlx::query_scalar::<_, bool>(
                        "SELECT EXISTS(SELECT 1 FROM background_jobs WHERE file_id = $1 AND job_type = 'verify_apk')",
                    )
                    .bind(upload.id)
                    .fetch_one(&state.db)
                    .await?;

                    if !job_exists {
                        // Create verification job
                        create_job(state, upload.id, JobType::VerifyApk).await?;
                        info!(blob = %blob_name, upload_id = %upload.id, "Created missing verification job");
                        return Ok(ReconcileAction::CreatedJob);
                    }

                    info!(blob = %blob_name, "Verification job already exists");
                    Ok(ReconcileAction::NoAction)
                }
                UploadStatus::Completed
                | UploadStatus::ProcessingPending
                | UploadStatus::PendingVerification
                | UploadStatus::Verified
                | UploadStatus::ReadyForDistribution
                | UploadStatus::Quarantined => {
                    // Valid completed states - check for jobs
                    let job_exists = sqlx::query_scalar::<_, bool>(
                        "SELECT EXISTS(SELECT 1 FROM background_jobs WHERE file_id = $1 AND job_type = 'verify_apk')",
                    )
                    .bind(upload.id)
                    .fetch_one(&state.db)
                    .await?;

                    if !job_exists && upload.status == UploadStatus::PendingVerification {
                        create_job(state, upload.id, JobType::VerifyApk).await?;
                        info!(blob = %blob_name, upload_id = %upload.id, "Created missing verification job for pending_verification");
                        return Ok(ReconcileAction::CreatedJob);
                    }

                    Ok(ReconcileAction::NoAction)
                }
                UploadStatus::Failed => {
                    delete_blob(blob_name).await?;
                    mark_upload_deleted(
                        &state.db,
                        upload.id,
                        "Deleted failed upload during reconciliation",
                    )
                    .await?;
                    info!(blob = %blob_name, upload_id = %upload.id, "Deleted failed upload blob during reconciliation");
                    Ok(ReconcileAction::NoAction)
                }
                _ => {
                    // Other states (EXPIRED, CANCELLED, DELETED) - no action needed
                    Ok(ReconcileAction::NoAction)
                }
            }
        }
        None => {
            // No upload record for this blob - create one
            info!(blob = %blob_name, "No upload record found, creating orphan record");

            // The blob path is: {user_id}/{upload_id}/{filename}
            // Parse user_id from blob path
            let parts: Vec<&str> = blob_name.split('/').collect();
            if parts.len() < 3 {
                warn!(blob = %blob_name, "Invalid blob path format, skipping");
                return Ok(ReconcileAction::NoAction);
            }

            let user_id_str = parts[0];
            let user_id = user_id_str.parse::<uuid::Uuid>()?;

            // Verify user exists
            let user_exists =
                sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM users WHERE id = $1)")
                    .bind(user_id)
                    .fetch_one(&state.db)
                    .await?;

            if !user_exists {
                warn!(blob = %blob_name, user_id = %user_id, "User not found, skipping");
                return Ok(ReconcileAction::NoAction);
            }

            // Create upload record with minimal info
            let upload_id = uuid::Uuid::new_v4();
            let original_filename = parts[2..].join("/");
            let now = time::OffsetDateTime::now_utc();

            sqlx::query(
                r#"
                INSERT INTO uploads (
                    id, user_id, container_name, blob_name,
                    original_filename, content_type, status,
                    expected_size_bytes, actual_size_bytes,
                    expires_at, completed_at, completion_requested_at
                ) VALUES ($1, $2, $3, $4, $5, $6, 'PENDING_VERIFICATION', $7, $7, $8, $8, $8)
                "#,
            )
            .bind(upload_id)
            .bind(user_id)
            .bind(container_name)
            .bind(blob_name)
            .bind(&original_filename)
            .bind("application/vnd.android.package-archive")
            .bind(0i64) // actual_size_bytes unknown
            .bind(now)
            .execute(&state.db)
            .await?;

            // Create verification job
            create_job(state, upload_id, JobType::VerifyApk).await?;

            info!(blob = %blob_name, upload_id = %upload_id, "Created orphan upload record and verification job");
            Ok(ReconcileAction::CreatedUpload)
        }
    }
}

// ---------------------------------------------------------------------------
// Database helpers
// ---------------------------------------------------------------------------

#[derive(sqlx::FromRow)]
struct UploadRecord {
    id: uuid::Uuid,
    status: UploadStatus,
}

async fn mark_upload_deleted(db: &PgPool, upload_id: uuid::Uuid, reason: &str) -> Result<()> {
    sqlx::query(
        "UPDATE uploads
         SET status = 'DELETED', deletion_reason = $1, deleted_at = now(), updated_at = now()
         WHERE id = $2",
    )
    .bind(reason)
    .bind(upload_id)
    .execute(db)
    .await?;
    Ok(())
}
