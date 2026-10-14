use crate::utils::jwt;
use crate::utils::validate::{sanitize_filename, validate_file};
use crate::{
    errors::{AppError, AppErrorWithMessage},
    state::AppState,
    storage::azure::generate_upload_sas_url,
    types::users::UserStatus,
};
use axum::{
    extract::{Json, State},
    http::{HeaderMap, StatusCode},
};
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, PgPool, query_as};
use uuid::Uuid;

const MAX_FILE_SIZE_BYTES: u64 = 200 * 1024 *1024;

#[derive(Debug, FromRow)]
struct UserLookup {
    id: Uuid,
    status: UserStatus,
}

/// Row stored in `uploads`. Always the source of truth for a given idempotency key.
#[derive(Debug, FromRow)]
struct UploadRecord {
    id: Uuid,
    blob_name: String,
    original_filename: String,
    content_type: String,
    status: String,
    expected_size_bytes: i64,
    expires_at: String,
    is_expired: bool,
}

#[derive(Debug, Deserialize)]
pub(crate) struct Request {
    filename: String,
    mime: String,
    file_size: u64,
    idempotency_key: Uuid,
}

#[derive(Debug, Serialize)]
pub(crate) struct Response {
    upload_id: Uuid,
    upload_url: String,
    expires_at: String,
    chunk_size_bytes: u64,
    original_filename:String,
    sanitized_filename:String,
    #[cfg(debug_assertions)]
    debug: DebugResponse,
}

#[cfg(debug_assertions)]
#[derive(Debug, Serialize)]
struct DebugResponse {
    user_id: Uuid,
    upload_id: Uuid,
    filename: String,
    mime: String,
    file_size: u64,
    blob_name: String,
    status: String,
    idempotency_key: Uuid,
}

fn db_err(e: sqlx::Error) -> AppErrorWithMessage {
    eprintln!("Database error: {:?}", e);
    AppError::Internal.with_message("Database error")
}

async fn authenticate(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<UserLookup, AppErrorWithMessage> {
    let value = headers.get("authorization").ok_or_else(|| {
        AppError::Unauthorized.with_message("Authorization header is required".to_string())
    })?;

    let value = value.to_str().map_err(|_| {
        AppError::Unauthorized.with_message("Invalid authorization header".to_string())
    })?;

    let token = value.strip_prefix("Bearer ").ok_or_else(|| {
        AppError::Unauthorized.with_message("Invalid authorization scheme".to_string())
    })?;

    let payload: jwt::Payload = jwt::verify_token(token.to_string()).map_err(|_| {
        AppError::Unauthorized.with_message("Unable to verify the token".to_string())
    })?;

    let user = query_as::<_, UserLookup>("SELECT id, status FROM users WHERE id = $1")
        .bind(&payload.sub)
        .fetch_optional(&state.db)
        .await
        .map_err(db_err)?
        .ok_or_else(|| AppError::Unauthorized.with_message("Invalid user".to_string()))?;

    if !matches!(&user.status, UserStatus::Verified) {
        return Err(AppError::FORBIDDEN.with_message("User is not verified".to_string()));
    }

    Ok(user)
}

async fn find_upload(
    db: &PgPool,
    user_id: Uuid,
    idempotency_key: Uuid,
) -> Result<Option<UploadRecord>, AppErrorWithMessage> {
    query_as::<_, UploadRecord>(
        r#"
        SELECT
            id,
            blob_name,
            original_filename,
            content_type,
            status,
            expected_size_bytes,
            to_char(expires_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS expires_at,
            (expires_at <= now()) AS is_expired
        FROM uploads
        WHERE user_id = $1 AND idempotency_key = $2
        "#,
    )
    .bind(user_id)
    .bind(idempotency_key)
    .fetch_optional(db)
    .await
    .map_err(db_err)
}

async fn extend_expiry(db: &PgPool, upload_id: Uuid) -> Result<UploadRecord, AppErrorWithMessage> {
    query_as::<_, UploadRecord>(
        r#"
        UPDATE uploads
        SET expires_at = now() + interval '1 hour'
        WHERE id = $1 AND status = 'PENDING'
        RETURNING
            id,
            blob_name,
            original_filename,
            content_type,
            status,
            expected_size_bytes,
            to_char(expires_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS expires_at,
            (expires_at <= now()) AS is_expired
        "#,
    )
    .bind(upload_id)
    .fetch_optional(db)
    .await
    .map_err(db_err)?
    .ok_or_else(|| AppError::Conflict.with_message("Upload is no longer pending".to_string()))
}

pub async fn upload(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<Request>,
) -> Result<(StatusCode, Json<Response>), AppErrorWithMessage> {
    let user = authenticate(&state, &headers).await?;

    validate_file(&req.filename, &req.mime)?;

    if req.file_size == 0 || req.file_size > MAX_FILE_SIZE_BYTES {
        return Err(AppError::BadRequest.with_message("Invalid file size".to_string()));
    }
    let expected_size = i64::try_from(req.file_size)
        .map_err(|_| AppError::BadRequest.with_message("Invalid file size".to_string()))?;

    let sanitized = sanitize_filename(&req.filename);
    if sanitized.is_empty() {
        return Err(AppError::BadRequest.with_message("Invalid filename".to_string()));
    }

    // ---- Idempotent create-or-fetch ----
    let (mut record, created) = match find_upload(&state.db, user.id, req.idempotency_key).await? {
        Some(existing) => (existing, false),
        None => {
            let new_id = Uuid::new_v4();
            let (_, new_blob_name) =
                generate_upload_sas_url(&user.id.to_string(), &new_id.to_string(), &sanitized)
                    .await
                    .map_err(|error| {
                        eprintln!("Azure storage error: {error}");
                        AppError::Internal.with_message("Unable to prepare upload")
                    })?;

            let inserted = query_as::<_, UploadRecord>(
                r#"
                INSERT INTO uploads (
                    id, user_id, container_name, blob_name, original_filename,
                    content_type, status, expected_size_bytes, expires_at, idempotency_key
                )
                VALUES ($1, $2, $3, $4, $5, $6, 'PENDING', $7, now() + interval '1 hour', $8)
                ON CONFLICT (user_id, idempotency_key) DO NOTHING
                RETURNING
                    id,
                    blob_name,
                    original_filename,
                    content_type,
                    status,
                    expected_size_bytes,
                    to_char(expires_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS expires_at,
                    (expires_at <= now()) AS is_expired
                "#,
            )
            .bind(new_id)
            .bind(user.id)
            .bind("uploads")
            .bind(new_blob_name.to_string())
            .bind(&req.filename)
            .bind(&req.mime)
            .bind(expected_size)
            .bind(req.idempotency_key)
            .fetch_optional(&state.db)
            .await
            .map_err(db_err)?;

            match inserted {
                Some(row) => (row, true),
                // Lost a race with a concurrent request using the same key.
                None => {
                    let row = find_upload(&state.db, user.id, req.idempotency_key)
                        .await?
                        .ok_or_else(|| AppError::Internal.with_message("Database error"))?;
                    (row, false)
                }
            }
        }
    };

    // Replay checks
    if record.original_filename != req.filename
        || record.content_type != req.mime
        || record.expected_size_bytes != expected_size
    {
        return Err(AppError::Conflict.with_message(
            "Idempotency key was already used with different parameters".to_string(),
        ));
    }

    if record.status != "PENDING" {
        return Err(AppError::Conflict
            .with_message("Upload already completed or cancelled".to_string()));
    }

    if record.is_expired {
        record = extend_expiry(&state.db, record.id).await?;
    }

    let (upload_url, blob_name) = generate_upload_sas_url(
        &user.id.to_string(),
        &record.id.to_string(),
        &sanitize_filename(&record.original_filename),
    )
    .await
    .map_err(|error| {
        eprintln!("Azure storage error: {error}");
        AppError::Internal.with_message("Unable to prepare upload")
    })?;

    if blob_name.to_string() != record.blob_name {
        eprintln!(
            "Blob name mismatch for upload {}: stored={} generated={}",
            record.id, record.blob_name, blob_name
        );
        return Err(AppError::Internal.with_message("Unable to prepare upload"));
    }

    let status = if created { StatusCode::CREATED } else { StatusCode::OK };

    Ok((
        status,
        Json(Response {
            upload_id: record.id,
            upload_url: upload_url.to_string(),
            expires_at: record.expires_at.clone(),
            chunk_size_bytes: req.file_size,
            original_filename: record.original_filename.clone(),
            sanitized_filename: sanitize_filename(&record.original_filename),
            #[cfg(debug_assertions)]
            debug: DebugResponse {
                user_id: user.id,
                upload_id: record.id,
                filename: record.original_filename,
                mime: record.content_type,
                file_size: req.file_size,
                blob_name: record.blob_name,
                status: record.status,
                idempotency_key: req.idempotency_key,
            },
        }),
    ))
}