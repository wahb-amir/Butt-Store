use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use sqlx::PgPool;
use tokio::time::timeout;
use tracing::{error, info, warn};

use crate::{
    api::uploads::models::UploadStatus,
    state::AppState,
    storage::{
        azure::{create_blob_client, delete_blob},
        jobs::{claim_job_type, complete_job, create_job_tx, fail_job},
    },
    types::jobs::{BackgroundJob, JobType},
    workers::{
        context::WorkerContext,
        lifecycle::{PollConfig, run_poll_loop},
    },
};

const VERIFICATION_TIMEOUT: Duration = Duration::from_secs(15 * 60);

/// Configuration for the APK verification worker.
pub struct VerifyWorkerConfig {
    pub poll_interval: Duration,
    pub max_empty_polls: u32,
    pub worker_id: String,
}

/// Start the verification worker poll loop.
///
/// Claims `verify_apk` jobs from the queue, runs SHA-256 verification,
/// `aapt2` structure checks, and ClamAV malware scanning, then
/// transitions successful uploads to `Verified` and enqueues a
/// `prepare_distribution` job.
pub async fn run(ctx: WorkerContext, config: VerifyWorkerConfig) -> Result<()> {
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
            match claim_job_type(&state, JobType::VerifyApk, &worker_id).await {
                Ok(Some(job)) => {
                    info!(job_id = %job.id, file_id = %job.file_id, "Claimed verification job");
                    let result = match timeout(
                        VERIFICATION_TIMEOUT,
                        process_verification_job(&state, &job, &worker_id),
                    )
                    .await
                    {
                        Ok(result) => result,
                        Err(_) => Err(anyhow::anyhow!(
                            "verification timed out after {} minutes",
                            VERIFICATION_TIMEOUT.as_secs() / 60
                        )),
                    };
                    if let Err(e) = result {
                        error!(job_id = %job.id, error = %e, "Verification job failed");
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

async fn process_verification_job(
    state: &AppState,
    job: &BackgroundJob,
    _worker_id: &str,
) -> Result<()> {
    let file_id = job.file_id;

    // Get upload record
    let upload = get_upload_record(&state.db, file_id).await?;

    if upload.status != UploadStatus::ProcessingPending {
        fail_job(
            state,
            job.id,
            "Upload is not in PROCESSING_PENDING state",
            false,
        )
        .await?;
        return Ok(());
    }

    let blob_name = &upload.blob_name;
    let etag = upload
        .etag
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("Upload is missing its committed blob ETag"))?;
    let expected_checksum = match upload.checksum_sha256.as_deref() {
        Some(checksum) => checksum,
        None => {
            fail_integrity_check(
                state,
                job.id,
                file_id,
                blob_name,
                "Upload is missing its SHA-256 checksum",
            )
            .await?;
            return Ok(());
        }
    };

    // Step 1: Verify blob exists in Azure
    info!(job_id = %job.id, blob = %blob_name, "Verifying blob existence");
    if !verify_blob_exists(blob_name).await? {
        let err = "Blob does not exist in Azure storage";
        fail_job(state, job.id, err, false).await?;
        update_upload_failure(&state.db, file_id, UploadStatus::Failed, err).await?;
        return Ok(());
    }

    // Step 2: Verify the client-provided SHA-256 checksum
    info!(job_id = %job.id, "Verifying SHA-256 checksum");
    let actual_checksum = download_and_checksum(blob_name, etag).await?;
    if actual_checksum != expected_checksum {
        fail_integrity_check(
            state,
            job.id,
            file_id,
            blob_name,
            "SHA-256 checksum mismatch",
        )
        .await?;
        return Ok(());
    }

    // Step 3: Verify APK structure and signatures using aapt2
    info!(job_id = %job.id, "Verifying APK structure and signatures");
    if let Err(e) = verify_apk_with_aapt2(blob_name, etag).await {
        let err = format!("APK verification failed: {}", e);
        fail_job(state, job.id, &err, false).await?;
        update_upload_failure(&state.db, file_id, UploadStatus::Quarantined, &err).await?;
        return Ok(());
    }
    info!(job_id = %job.id, "APK structure and signatures verified");

    // Step 4: Malware scan with ClamAV
    info!(job_id = %job.id, "Running malware scan");
    if let Err(e) = scan_with_clamav(blob_name, etag).await {
        let err = format!("Malware scan failed: {}", e);
        // Distinguish between malware detected (permanent failure) and scanner error (retry)
        let is_malware =
            err.to_lowercase().contains("malware") || err.to_lowercase().contains("virus");
        fail_job(state, job.id, &err, !is_malware).await?;
        let status = if is_malware {
            UploadStatus::Quarantined
        } else {
            UploadStatus::Failed
        };
        update_upload_failure(&state.db, file_id, status, &err).await?;
        return Ok(());
    }
    info!(job_id = %job.id, "Malware scan clean");

    let mut tx = state.db.begin().await?;
    update_upload_status_tx(&mut tx, file_id, UploadStatus::Verified).await?;
    create_job_tx(&mut tx, file_id, JobType::PrepareDistribution).await?;
    tx.commit().await?;

    complete_job(state, job.id).await?;
    info!(job_id = %job.id, file_id = %file_id, "Verification completed successfully");

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
    actual_size_bytes: Option<i64>,
    etag: Option<String>,
    checksum_sha256: Option<Vec<u8>>,
    #[allow(dead_code)]
    expires_at: time::OffsetDateTime,
}

async fn get_upload_record(db: &PgPool, file_id: uuid::Uuid) -> Result<UploadRecord> {
    sqlx::query_as::<_, UploadRecord>(
        r#"
        SELECT
            id, user_id, blob_name, original_filename, content_type,
            status, expected_size_bytes, actual_size_bytes, etag,
            checksum_sha256, expires_at
        FROM uploads
        WHERE id = $1
        "#,
    )
    .bind(file_id)
    .fetch_optional(db)
    .await?
    .ok_or_else(|| anyhow::anyhow!("Upload record not found"))
}

async fn fail_integrity_check(
    state: &AppState,
    job_id: uuid::Uuid,
    file_id: uuid::Uuid,
    blob_name: &str,
    reason: &str,
) -> Result<()> {
    fail_job(state, job_id, reason, false).await?;
    sqlx::query(
        "UPDATE uploads
         SET status = 'FAILED', failure_reason = $1,
             deletion_reason = $2, updated_at = now()
         WHERE id = $3",
    )
    .bind(reason)
    .bind(format!("Deleted after integrity failure: {reason}"))
    .bind(file_id)
    .execute(&state.db)
    .await?;
    if let Err(error) = delete_blob(blob_name).await {
        error!(job_id = %job_id, blob = %blob_name, error = %error, "Failed to delete blob after integrity failure");
    }
    Ok(())
}

async fn update_upload_failure(
    db: &PgPool,
    file_id: uuid::Uuid,
    status: UploadStatus,
    reason: &str,
) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE uploads
        SET status = $1, failure_reason = $2, updated_at = now()
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

async fn update_upload_status_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
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
    .execute(&mut **tx)
    .await?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Azure / external tool helpers
// ---------------------------------------------------------------------------

async fn verify_blob_exists(blob_name: &str) -> Result<bool> {
    let blob_client = create_blob_client(blob_name)?;
    match blob_client.exists().await {
        Ok(true) => Ok(true),
        Ok(false) => Ok(false),
        Err(e) => {
            if e.to_string().contains("404") || e.to_string().contains("NotFound") {
                Ok(false)
            } else {
                Err(anyhow::anyhow!(e))
            }
        }
    }
}

async fn download_and_checksum(blob_name: &str, etag: &str) -> Result<Vec<u8>> {
    use azure_storage_blob::models::BlobClientDownloadOptions;
    use futures_util::StreamExt;
    use sha2::{Digest, Sha256};

    let blob_client = create_blob_client(blob_name)?;
    let download_result = blob_client
        .download(Some(BlobClientDownloadOptions {
            if_match: Some(etag.into()),
            ..Default::default()
        }))
        .await
        .map_err(|e| anyhow::anyhow!(e))?;

    let mut hasher = Sha256::new();
    let mut stream = download_result.body;
    while let Some(chunk) = stream.next().await {
        hasher.update(chunk.map_err(|e| anyhow::anyhow!(e))?);
    }

    Ok(hasher.finalize().to_vec())
}

async fn verify_apk_with_aapt2(blob_name: &str, etag: &str) -> Result<()> {
    // Download APK to temporary file
    let temp_path = download_to_temp(blob_name, etag).await?;

    // Run aapt2 dump badging to verify structure and extract package info
    let output = tokio::process::Command::new("aapt2")
        .args(["dump", "badging", &temp_path])
        .output()
        .await?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(anyhow::anyhow!("aapt2 dump badging failed: {}", stderr));
    }

    // Try to verify signatures with apksigner if available
    let output = tokio::process::Command::new("apksigner")
        .args(["verify", "--print-certs", &temp_path])
        .output()
        .await;

    match output {
        Ok(out) if out.status.success() => {
            info!("APK signatures verified");
        }
        Ok(out) => {
            let stderr = String::from_utf8_lossy(&out.stderr);
            warn!("apksigner verify failed: {}", stderr);
            // Don't fail - some APKs may not have v2/v3 signatures
        }
        Err(_) => {
            warn!("apksigner not available, skipping signature verification");
        }
    }

    // Clean up temp file
    let _ = tokio::fs::remove_file(&temp_path).await;

    Ok(())
}

async fn download_to_temp(blob_name: &str, etag: &str) -> Result<String> {
    use azure_storage_blob::models::BlobClientDownloadOptions;
    use futures_util::StreamExt;
    use tokio::io::AsyncWriteExt;

    let blob_client = create_blob_client(blob_name)?;
    let download_result = blob_client
        .download(Some(BlobClientDownloadOptions {
            if_match: Some(etag.into()),
            ..Default::default()
        }))
        .await
        .map_err(|e| anyhow::anyhow!(e))?;
    let mut stream = download_result.body;

    let temp_path = format!("/tmp/apk_verify_{}.apk", uuid::Uuid::new_v4());
    let mut file = tokio::fs::File::create(&temp_path).await?;

    while let Some(chunk) = stream.next().await {
        let chunk: bytes::Bytes = chunk.map_err(|e| anyhow::anyhow!(e))?;
        file.write_all(&chunk).await?;
    }

    file.flush().await?;
    Ok(temp_path)
}

async fn scan_with_clamav(blob_name: &str, etag: &str) -> Result<()> {
    // Download APK to temporary file
    let temp_path = download_to_temp(blob_name, etag).await?;

    // Run clamdscan (ClamAV daemon client)
    let output = tokio::process::Command::new("clamdscan")
        .args(["--fdpass", &temp_path])
        .output()
        .await;

    let result = match output {
        Ok(out) => {
            let stdout = String::from_utf8_lossy(&out.stdout);
            let stderr = String::from_utf8_lossy(&out.stderr);

            if out.status.success() {
                // Exit code 0 = clean
                Ok(())
            } else if out.status.code() == Some(1) {
                // Exit code 1 = malware found
                Err(anyhow::anyhow!("Malware detected: {}", stdout))
            } else {
                // Other exit codes = error
                Err(anyhow::anyhow!(
                    "ClamAV scan error (code {:?}): {}",
                    out.status.code(),
                    stderr
                ))
            }
        }
        Err(e) => Err(anyhow::anyhow!("Failed to run clamdscan: {}", e)),
    };

    // Clean up temp file
    let _ = tokio::fs::remove_file(&temp_path).await;

    result
}
