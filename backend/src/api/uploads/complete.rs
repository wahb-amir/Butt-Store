use crate::{
    errors::{AppError, AppErrorWithMessage},
    state::AppState,
    storage::azure::{commit_upload, create_block_blob_client, create_blob_client},
    types::jobs::JobType,
};
use azure_storage_blob::models::{BlobClientGetPropertiesResultHeaders, BlobClientGetPropertiesResult};
use axum::{
    extract::{Json, Path, State},
    http::{HeaderMap, StatusCode},
};
use serde::{Deserialize, Serialize};
use sqlx::{query, query_as, PgPool, Transaction, Postgres, Row};
use time::{Duration, OffsetDateTime};
use uuid::Uuid;

use super::{
    auth::authenticate,
    config::{CHUNK_SIZE_BYTES, MAX_FILE_SIZE_BYTES},
    models::UploadRecord,
    utils::{db_err, format_timestamp},
};

#[derive(Debug, Deserialize)]
pub(crate) struct CompleteRequest {
    /// Optional idempotency key for safe retry.
    /// If provided, must match the original upload's idempotency_key.
    #[serde(rename = "idempotencyKey")]
    pub idempotency_key: Option<Uuid>,
}

#[derive(Debug, Serialize)]
pub struct CompleteResponse {
    #[serde(rename = "jobId")]
    pub job_id: Uuid,
    pub status: String,
}

/// POST /uploads/{id}/complete
/// Commits the uploaded blocks to Azure, finalizes the upload record,
/// and enqueues a verification job.
pub async fn complete_upload(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(upload_id): Path<Uuid>,
    Json(req): Json<CompleteRequest>,
) -> Result<(StatusCode, Json<CompleteResponse>), AppErrorWithMessage> {
    let user = authenticate(&state, &headers).await?;

    // Find and validate the upload session
    let mut upload = find_upload_by_id(&state.db, user.id, upload_id)
        .await?
        .ok_or_else(|| AppError::NotFound.with_message("Upload not found".to_string()))?;

    // Validate idempotency key if provided
    if let Some(provided_key) = req.idempotency_key {
        // We need to fetch the idempotency_key from the database
        let stored_key: Option<Uuid> = query(
            "SELECT idempotency_key FROM uploads WHERE id = $1",
        )
        .bind(upload_id)
        .fetch_optional(&state.db)
        .await
        .map_err(db_err)?
        .and_then(|row| row.try_get("idempotency_key").ok());

        if stored_key != Some(provided_key) {
            return Err(AppError::Conflict.with_message(
                "Idempotency key does not match".to_string(),
            ));
        }
    }

    // Validate upload is in UPLOADING state
    validate_upload_session(&upload)?;

    // Check if already completed (idempotent completion)
    if matches!(upload.status.as_str(), "COMPLETED" | "PENDING_VERIFICATION" | "VERIFIED" | "READY_FOR_DISTRIBUTION") {
        // Already completed, return existing job if any
        let existing_job: Option<Uuid> = query(
            "SELECT id FROM background_jobs WHERE file_id = $1 AND job_type = 'verify_apk' ORDER BY created_at DESC LIMIT 1",
        )
        .bind(upload_id)
        .fetch_optional(&state.db)
        .await
        .map_err(db_err)?
        .and_then(|row| row.try_get("id").ok());

        if let Some(job_id) = existing_job {
            return Ok((
                StatusCode::OK,
                Json(CompleteResponse {
                    job_id,
                    status: upload.status.clone(),
                }),
            ));
        }
    }

    // Ensure status is UPLOADING
    if upload.status != "UPLOADING" {
        return Err(AppError::Conflict.with_message(
            "Upload is not in UPLOADING state".to_string(),
        ));
    }

    // Create Azure block blob client for committing blocks
    let block_blob_client = create_block_blob_client(&upload.blob_name)
        .map_err(|e| {
            eprintln!("Failed to create block blob client: {e:?}");
            AppError::Internal.with_message("Unable to access blob storage")
        })?;

    // Commit blocks in Azure
    let file_size = upload.expected_size_bytes as u64;
    commit_upload(&block_blob_client, file_size, CHUNK_SIZE_BYTES)
        .await
        .map_err(|e| {
            eprintln!("Azure commit failed for upload {upload_id}: {e:?}");
            AppError::Internal.with_message("Failed to commit upload to storage")
        })?;

    // Create base blob client for properties and download
    let blob_client = create_blob_client(&upload.blob_name)
        .map_err(|e| {
            eprintln!("Failed to create blob client: {e:?}");
            AppError::Internal.with_message("Unable to access blob storage")
        })?;

    // Get blob properties (etag, size) after commit
    let blob_properties = blob_client.get_properties(None).await.map_err(|e| {
        eprintln!("Failed to get blob properties: {e:?}");
        AppError::Internal.with_message("Failed to verify committed blob")
    })?;

    let actual_size = blob_properties.content_length().map_err(|e| {
        eprintln!("Failed to get content length: {e:?}");
        AppError::Internal.with_message("Failed to get blob size")
    })?.unwrap_or(file_size as u64) as i64;
    let etag = blob_properties.etag().map_err(|e| {
        eprintln!("Failed to get etag: {e:?}");
        AppError::Internal.with_message("Failed to get blob etag")
    })?.map(|e| e.to_string());

    // Download blob to compute SHA256 checksum
    let checksum = download_and_checksum(&blob_client, actual_size).await?;

    // Run database transaction: update upload + create verification job
    let job_id = run_completion_transaction(
        &state.db,
        upload_id,
        actual_size,
        etag,
        checksum,
    )
    .await?;

    // Update local upload record for response
    upload.status = "PENDING_VERIFICATION".to_string();
    upload.actual_size_bytes = Some(actual_size);

    Ok((
        StatusCode::OK,
        Json(CompleteResponse {
            job_id,
            status: "PENDING_VERIFICATION".to_string(),
        }),
    ))
}

/// Download blob and compute SHA256 checksum.
async fn download_and_checksum(
    blob_client: &azure_storage_blob::clients::BlobClient,
    size: i64,
) -> Result<Vec<u8>, AppErrorWithMessage> {
    use futures_util::StreamExt;
    use sha2::{Digest, Sha256};

    let download_result = blob_client.download(None).await.map_err(|e| {
        eprintln!("Failed to start blob download: {e:?}");
        AppError::Internal.with_message("Failed to download blob for checksum")
    })?;

    let mut hasher = Sha256::new();
    let mut stream = download_result.body;
    let mut bytes_read = 0i64;

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| {
            eprintln!("Failed to read blob chunk: {e:?}");
            AppError::Internal.with_message("Failed to read blob data")
        })?;
        bytes_read += chunk.len() as i64;
        hasher.update(&chunk);
    }

    if bytes_read != size {
        eprintln!(
            "Checksum size mismatch: expected {}, got {}",
            size, bytes_read
        );
        return Err(AppError::Internal.with_message("Blob size mismatch during checksum"));
    }

    Ok(hasher.finalize().to_vec())
}

/// Run the completion transaction: update upload record and create verification job.
async fn run_completion_transaction(
    db: &PgPool,
    upload_id: Uuid,
    actual_size: i64,
    etag: Option<String>,
    checksum: Vec<u8>,
) -> Result<Uuid, AppErrorWithMessage> {
    let mut tx: Transaction<'_, Postgres> = db.begin().await.map_err(db_err)?;

    // Update upload record
    query(
        r#"
        UPDATE uploads
        SET
            status = 'PENDING_VERIFICATION',
            actual_size_bytes = $1,
            etag = $2,
            checksum_sha256 = $3,
            completed_at = now(),
            completion_requested_at = now()
        WHERE id = $4
        "#,
    )
    .bind(actual_size)
    .bind(etag)
    .bind(checksum)
    .bind(upload_id)
    .execute(&mut *tx)
    .await
    .map_err(db_err)?;

    // Create verification job
    let job_id = Uuid::new_v4();
    query(
        r#"
        INSERT INTO background_jobs (id, file_id, job_type)
        VALUES ($1, $2, 'verify_apk')
        "#,
    )
    .bind(job_id)
    .bind(upload_id)
    .execute(&mut *tx)
    .await
    .map_err(db_err)?;

    tx.commit().await.map_err(db_err)?;

    Ok(job_id)
}

/// Find upload by ID (reused from upload.rs)
async fn find_upload_by_id(
    db: &PgPool,
    user_id: Uuid,
    upload_id: Uuid,
) -> Result<Option<UploadRecord>, AppErrorWithMessage> {
    query_as::<_, UploadRecord>(
        r#"
        SELECT
            id,
            user_id,
            blob_name,
            original_filename,
            content_type,
            status,
            expected_size_bytes,
            expires_at
        FROM uploads
        WHERE id = $1
          AND user_id = $2
        "#,
    )
    .bind(upload_id)
    .bind(user_id)
    .fetch_optional(db)
    .await
    .map_err(db_err)
}

/// Validate upload session (reused from upload.rs)
fn validate_upload_session(record: &UploadRecord) -> Result<(), AppErrorWithMessage> {
    if !matches!(record.status.as_str(), "PENDING" | "UPLOADING") {
        return Err(
            AppError::Conflict.with_message("Upload session is no longer eligible".to_string())
        );
    }

    if record.expires_at <= OffsetDateTime::now_utc() {
        return Err(AppError::Conflict.with_message("Upload session has expired".to_string()));
    }

    Ok(())
}