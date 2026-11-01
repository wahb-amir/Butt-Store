use crate::{
    errors::{AppError, AppErrorWithMessage},
    state::AppState,
};
use axum::{
    extract::{Json, Path, State},
    http::{HeaderMap, StatusCode},
};
use serde::{Deserialize, Serialize};
use sqlx::{query, query_as, PgPool, Postgres, Row, Transaction};
use time::OffsetDateTime;
use uuid::Uuid;

use super::{
    auth::authenticate,
    models::UploadRecord,
    utils::db_err,
};

#[derive(Debug, Deserialize)]
pub(crate) struct CompleteRequest {
    #[serde(rename = "idempotencyKey")]
    idempotency_key: Option<Uuid>,
    #[serde(rename = "checksumSha256")]
    checksum_sha256: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct CompleteResponse {
    #[serde(rename = "jobId")]
    job_id: Uuid,
    status: String,
}

pub(crate) async fn complete_upload(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(upload_id): Path<Uuid>,
    Json(request): Json<CompleteRequest>,
) -> Result<(StatusCode, Json<CompleteResponse>), AppErrorWithMessage> {
    let user = authenticate(&state, &headers).await?;
    let checksum_sha256 = parse_sha256_hex(&request.checksum_sha256)?;
    let mut upload = find_upload_by_id(&state.db, user.id, upload_id)
        .await?
        .ok_or_else(|| AppError::NotFound.with_message("Upload not found"))?;

    if let Some(provided_key) = request.idempotency_key {
        let stored_key: Option<Uuid> = query("SELECT idempotency_key FROM uploads WHERE id = $1")
            .bind(upload_id)
            .fetch_optional(&state.db)
            .await
            .map_err(db_err)?
            .and_then(|row| row.try_get("idempotency_key").ok());

        if stored_key != Some(provided_key) {
            return Err(AppError::Conflict.with_message("Idempotency key does not match"));
        }
    }

    if matches!(upload.status.as_str(), "PROCESSING_PENDING" | "PENDING_VERIFICATION" | "QUARANTINED" | "VERIFIED" | "READY_FOR_DISTRIBUTION") {
        if let Some(job_id) = find_completion_job(&state.db, upload_id).await? {
            return Ok((
                StatusCode::OK,
                Json(CompleteResponse {
                    job_id,
                    status: upload.status,
                }),
            ));
        }
    }

    if upload.status != "PENDING" {
        return Err(AppError::Conflict.with_message("Upload is not in PENDING state"));
    }
    validate_upload_session(&upload)?;

    let job_id = enqueue_completion_job(&state.db, upload_id, checksum_sha256).await?;
    upload.status = "PROCESSING_PENDING".to_string();
    Ok((
        StatusCode::OK,
        Json(CompleteResponse {
            job_id,
            status: upload.status,
        }),
    ))
}

async fn find_completion_job(
    db: &PgPool,
    upload_id: Uuid,
) -> Result<Option<Uuid>, AppErrorWithMessage> {
    query(
        "SELECT id FROM background_jobs
         WHERE file_id = $1 AND job_type = 'complete_upload'
         ORDER BY created_at DESC LIMIT 1",
    )
    .bind(upload_id)
    .fetch_optional(db)
    .await
    .map_err(db_err)?
    .map(|row| row.try_get("id"))
    .transpose()
    .map_err(db_err)
}

async fn enqueue_completion_job(
    db: &PgPool,
    upload_id: Uuid,
    checksum_sha256: Vec<u8>,
) -> Result<Uuid, AppErrorWithMessage> {
    let mut tx: Transaction<'_, Postgres> = db.begin().await.map_err(db_err)?;
    let job_id = Uuid::new_v4();
    let updated = query(
        "UPDATE uploads SET status = 'PROCESSING_PENDING', checksum_sha256 = $2,
            completion_requested_at = now()
         WHERE id = $1 AND status = 'PENDING'",
    )
    .bind(upload_id)
        .bind(checksum_sha256)
    .execute(&mut *tx)
    .await
    .map_err(db_err)?;
    if updated.rows_affected() != 1 {
        return Err(AppError::Conflict.with_message("Upload is no longer in PENDING state"));
    }
    query(
        "INSERT INTO background_jobs (id, file_id, job_type)
         VALUES ($1, $2, 'complete_upload')",
    )
    .bind(job_id)
    .bind(upload_id)
    .execute(&mut *tx)
    .await
    .map_err(db_err)?;
    tx.commit().await.map_err(db_err)?;
    Ok(job_id)
}

fn parse_sha256_hex(value: &str) -> Result<Vec<u8>, AppErrorWithMessage> {
    if value.len() != 64 {
        return Err(AppError::BadRequest.with_message(
            "checksumSha256 must be a 64-character hexadecimal SHA-256 hash",
        ));
    }

    (0..value.len())
        .step_by(2)
        .map(|index| {
            u8::from_str_radix(&value[index..index + 2], 16).map_err(|_| {
                AppError::BadRequest.with_message(
                    "checksumSha256 must be a 64-character hexadecimal SHA-256 hash",
                )
            })
        })
        .collect()
}

async fn find_upload_by_id(
    db: &PgPool,
    user_id: Uuid,
    upload_id: Uuid,
) -> Result<Option<UploadRecord>, AppErrorWithMessage> {
    query_as(
        "SELECT id, user_id, blob_name, original_filename, content_type, status,
                expected_size_bytes, expires_at
         FROM uploads WHERE id = $1 AND user_id = $2",
    )
    .bind(upload_id)
    .bind(user_id)
    .fetch_optional(db)
    .await
    .map_err(db_err)
}

fn validate_upload_session(record: &UploadRecord) -> Result<(), AppErrorWithMessage> {
    if record.expires_at <= OffsetDateTime::now_utc() {
        return Err(AppError::Conflict.with_message("Upload session has expired"));
    }
    Ok(())
}
