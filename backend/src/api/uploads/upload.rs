use crate::utils::jwt;
use crate::utils::validate::{sanitize_filename, validate_file};
use crate::{
    errors::{AppError, AppErrorWithMessage},
    state::AppState,
    storage::azure::{generate_upload_sas_url, generate_upload_sas_url_for_blob},
    types::users::UserStatus,
};

use axum::{
    extract::{Json, Path, State},
    http::{HeaderMap, StatusCode},
};

use serde::{Deserialize, Serialize};
use sqlx::{query_as, FromRow, PgPool};
use time::{
    format_description::well_known::Rfc3339,
    Duration,
    OffsetDateTime,
};
use uuid::Uuid;

// -----------------------------------------------------------------------------
// Configuration
// -----------------------------------------------------------------------------

const MAX_FILE_SIZE_BYTES: u64 = 200 * 1024 * 1024;

// Actual upload chunk size, not the total file size.
// Make sure the frontend uses the same server-provided value.
const CHUNK_SIZE_BYTES: u64 = 4 * 1024 * 1024;

// The code below also rejects a generated SAS whose expiry exceeds the session.
const SAS_MAX_LIFETIME_SECONDS: i64 = 10 * 60;
const SAS_EXPIRY_SAFETY_MARGIN_SECONDS: i64 = 30;

// -----------------------------------------------------------------------------
// Database models
// -----------------------------------------------------------------------------

#[derive(Debug, FromRow)]
struct UserLookup {
    id: Uuid,
    status: UserStatus,
}

/// upload session.
#[derive(Debug, FromRow)]
struct UploadRecord {
    id: Uuid,
    user_id: Uuid,
    blob_name: String,
    original_filename: String,
    content_type: String,
    status: String,
    expected_size_bytes: i64,
    expires_at: OffsetDateTime,
}

// -----------------------------------------------------------------------------
// Request and response models
// -----------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub(crate) struct Request {
    filename: String,
    mime: String,
    file_size: u64,
    idempotency_key: Uuid,
}

#[derive(Debug, Serialize)]
pub struct SasResponse {
    #[serde(rename = "uploadId")]
    upload_id: Uuid,

    #[serde(rename = "uploadUrl")]
    upload_url: String,

    #[serde(rename = "sasExpiresAt")]
    sas_expires_at: String,
    #[serde(rename = "sessionExpiresAt")]
    session_expires_at: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct Response {
    upload_id: Uuid,

    upload_url: String,

    sas_expires_at: String,
    session_expires_at: String,

    chunk_size_bytes: u64,
    file_size_bytes: u64,

    original_filename: String,

    sanitized_filename: String,

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

// Error helpers

fn db_err(error: sqlx::Error) -> AppErrorWithMessage {
    eprintln!("Database error: {:?}", error);

    AppError::Internal.with_message("Database error")
}

fn format_timestamp(
    timestamp: OffsetDateTime,
) -> Result<String, AppErrorWithMessage> {
    timestamp.format(&Rfc3339).map_err(|error| {
        eprintln!("Timestamp formatting error: {error}");

        AppError::Internal.with_message("Unable to prepare upload")
    })
}

// Authentication

async fn authenticate(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<UserLookup, AppErrorWithMessage> {
    let value = headers.get("authorization").ok_or_else(|| {
        AppError::Unauthorized.with_message(
            "Authorization header is required".to_string(),
        )
    })?;

    let value = value.to_str().map_err(|_| {
        AppError::Unauthorized.with_message(
            "Invalid authorization header".to_string(),
        )
    })?;

    // Keep Bearer scheme handling explicit.
    let token = value.strip_prefix("Bearer ").ok_or_else(|| {
        AppError::Unauthorized.with_message(
            "Invalid authorization scheme".to_string(),
        )
    })?;

    let payload: jwt::Payload =
        jwt::verify_token(token.to_string()).map_err(|_| {
            AppError::Unauthorized.with_message(
                "Unable to verify the token".to_string(),
            )
        })?;

    let user = query_as::<_, UserLookup>(
        r#"
        SELECT id, status
        FROM users
        WHERE id = $1
        "#,
    )
    .bind(&payload.sub)
    .fetch_optional(&state.db)
    .await
    .map_err(db_err)?
    .ok_or_else(|| {
        AppError::Unauthorized.with_message("Invalid user".to_string())
    })?;

    if !matches!(&user.status, UserStatus::Verified) {
        return Err(AppError::FORBIDDEN.with_message(
            "User is not verified".to_string(),
        ));
    }

    Ok(user)
}

// Upload queries

async fn find_upload(
    db: &PgPool,
    user_id: Uuid,
    idempotency_key: Uuid,
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
        WHERE user_id = $1
          AND idempotency_key = $2
        "#,
    )
    .bind(user_id)
    .bind(idempotency_key)
    .fetch_optional(db)
    .await
    .map_err(db_err)
}

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

// Expiration and state validation

async fn expire_upload_if_due(
    db: &PgPool,
    upload_id: Uuid,
) -> Result<(), AppErrorWithMessage> {
    sqlx::query(
        r#"
        UPDATE uploads
        SET
            status = 'EXPIRED',
            status_reason = 'Upload session deadline exceeded'
        WHERE id = $1
          AND status IN ('PENDING', 'UPLOADING')
          AND expires_at <= now()
        "#,
    )
    .bind(upload_id)
    .execute(db)
    .await
    .map_err(db_err)?;

    Ok(())
}

fn validate_upload_session(
    record: &UploadRecord,
) -> Result<(), AppErrorWithMessage> {
    if !matches!(record.status.as_str(), "PENDING" | "UPLOADING") {
        return Err(AppError::Conflict.with_message(
            "Upload session is no longer eligible".to_string(),
        ));
    }

    // Additional application-level check.
    // SQL remains responsible for persisting the EXPIRED transition.
    if record.expires_at <= OffsetDateTime::now_utc() {
        return Err(AppError::Conflict.with_message(
            "Upload session has expired".to_string(),
        ));
    }

    Ok(())
}

// SAS issuance and rotation
async fn issue_sas_for_session(
    record: &UploadRecord,
) -> Result<(String, OffsetDateTime), AppErrorWithMessage> {
    let now = OffsetDateTime::now_utc();

    let minimum_deadline = now
        + Duration::seconds(
            SAS_MAX_LIFETIME_SECONDS + SAS_EXPIRY_SAFETY_MARGIN_SECONDS,
        );

    if record.expires_at <= minimum_deadline {
        return Err(AppError::Conflict.with_message(
            "Upload session is too close to expiry to renew its SAS URL"
                .to_string(),
        ));
    }

    let (upload_url, sas_expires_at) =
        generate_upload_sas_url_for_blob(&record.blob_name)
            .await
            .map_err(|error| {
                eprintln!("Azure SAS generation error: {error}");

                AppError::Internal.with_message("Unable to prepare upload")
            })?;
    if sas_expires_at > record.expires_at {
        eprintln!(
            "SAS expiry exceeds session expiry for upload {}",
            record.id
        );

        return Err(AppError::Internal.with_message(
            "SAS expiration exceeds upload-session expiration",
        ));
    }

    Ok((upload_url.to_string(), sas_expires_at))
}

// Create or fetch an upload session
pub async fn upload(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<Request>,
) -> Result<(StatusCode, Json<Response>), AppErrorWithMessage> {
    let user = authenticate(&state, &headers).await?;

    validate_file(&req.filename, &req.mime)?;

    if req.file_size == 0 || req.file_size > MAX_FILE_SIZE_BYTES {
        return Err(AppError::BadRequest.with_message(
            "Invalid file size".to_string(),
        ));
    }

    let expected_size = i64::try_from(req.file_size).map_err(|_| {
        AppError::BadRequest.with_message("Invalid file size".to_string())
    })?;

    let sanitized_filename = sanitize_filename(&req.filename);

    if sanitized_filename.is_empty() {
        return Err(AppError::BadRequest.with_message(
            "Invalid filename".to_string(),
        ));
    }
    let (record, created) =
        match find_upload(&state.db, user.id, req.idempotency_key).await? {
            Some(existing) => {
                (existing, false)
            }

            None => {
                let new_id = Uuid::new_v4();
                let (_discarded_sas_url, new_blob_name) =
                    generate_upload_sas_url(
                        &user.id.to_string(),
                        &new_id.to_string(),
                        &sanitized_filename,
                    )
                    .await
                    .map_err(|error| {
                        eprintln!("Azure storage error: {error}");

                        AppError::Internal
                            .with_message("Unable to prepare upload")
                    })?;


                let inserted = query_as::<_, UploadRecord>(
                    r#"
                    INSERT INTO uploads (
                        id,
                        user_id,
                        container_name,
                        blob_name,
                        original_filename,
                        content_type,
                        status,
                        expected_size_bytes,
                        expires_at,
                        idempotency_key
                    )
                    VALUES (
                        $1,
                        $2,
                        $3,
                        $4,
                        $5,
                        $6,
                        'PENDING',
                        $7,
                        now() + interval '24 hours',
                        $8
                    )
                    ON CONFLICT (user_id, idempotency_key)
                    DO NOTHING
                    RETURNING
                        id,
                        user_id,
                        blob_name,
                        original_filename,
                        content_type,
                        status,
                        expected_size_bytes,
                        expires_at
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
                    Some(inserted_record) => (inserted_record, true),
                    None => {
                        let existing = find_upload(
                            &state.db,
                            user.id,
                            req.idempotency_key,
                        )
                        .await?
                        .ok_or_else(|| {
                            AppError::Internal
                                .with_message("Unable to retrieve upload session")
                        })?;

                        (existing, false)
                    }
                }
            }
        };


    if record.original_filename != req.filename
        || record.content_type != req.mime
        || record.expected_size_bytes != expected_size
    {
        return Err(AppError::Conflict.with_message(
            "Idempotency key was already used with different parameters"
                .to_string(),
        ));
    }

    expire_upload_if_due(&state.db, record.id).await?;

    let record = find_upload_by_id(&state.db, user.id, record.id)
        .await?
        .ok_or_else(|| {
            AppError::NotFound.with_message("Upload not found".to_string())
        })?;

    validate_upload_session(&record)?;

    let (upload_url, sas_expires_at) = issue_sas_for_session(&record).await?;


    let session_expires_at = format_timestamp(record.expires_at)?;
    let sas_expires_at = format_timestamp(sas_expires_at)?;

    let status = if created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };

    #[cfg(debug_assertions)]
    let debug = DebugResponse {
        user_id: user.id,
        upload_id: record.id,
        filename: record.original_filename.clone(),
        mime: record.content_type.clone(),
        file_size: req.file_size,
        blob_name: record.blob_name.clone(),
        status: record.status.clone(),
        idempotency_key: req.idempotency_key,
    };

    Ok((
        status,
        Json(Response {
            upload_id: record.id,
            upload_url,
            sas_expires_at,
            session_expires_at,
            chunk_size_bytes: CHUNK_SIZE_BYTES,
            file_size_bytes: req.file_size,
            original_filename: record.original_filename.clone(),
            sanitized_filename: sanitize_filename(&record.original_filename),

            #[cfg(debug_assertions)]
            debug,
        }),
    ))
}

// Rotate / refresh a SAS URL for an existing upload

pub async fn upload_sas(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(upload_id): Path<Uuid>,
) -> Result<Json<SasResponse>, AppErrorWithMessage> {
    let user = authenticate(&state, &headers).await?;

    expire_upload_if_due(&state.db, upload_id).await?;

    let session = find_upload_by_id(&state.db, user.id, upload_id)
        .await?
        .ok_or_else(|| {
            AppError::NotFound.with_message("Upload not found".to_string())
        })?;

    validate_upload_session(&session)?;

    let (upload_url, sas_expires_at) =
        issue_sas_for_session(&session).await?;

    Ok(Json(SasResponse {
        upload_id: session.id,
        upload_url,
        sas_expires_at: format_timestamp(sas_expires_at)?,
        session_expires_at: format_timestamp(session.expires_at)?,
    }))
}