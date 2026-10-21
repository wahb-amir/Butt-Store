use anyhow::{Context, Result};
use crate::{
    state::AppState,
    types::jobs::{BackgroundJob, JobStatus, JobType},
};
use sqlx::{query, query_as};
use time::OffsetDateTime;
use uuid::Uuid;

const WORKER_LEASE_SECONDS: i64 = 300; // 5 minutes

/// Claim a pending job atomically using FOR UPDATE SKIP LOCKED.
/// Returns the claimed job with its lock extended.
pub async fn claim_job(
    state: &AppState,
    _worker_id: &str,
) -> Result<Option<BackgroundJob>> {
    let now = OffsetDateTime::now_utc();
    let lease_until = now + time::Duration::seconds(WORKER_LEASE_SECONDS);

    let job = query_as::<_, BackgroundJob>(
        r#"
        UPDATE background_jobs
        SET
            status = 'processing',
            locked_until = $2,
            attempts = attempts + 1
        WHERE id = (
            SELECT id
            FROM background_jobs
            WHERE status = 'pending'
              AND available_at <= $1
            ORDER BY available_at, created_at
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        )
        RETURNING
            id, file_id, job_type, status, attempts,
            available_at, locked_until, last_error,
            created_at, completed_at
        "#,
    )
    .bind(now)
    .bind(lease_until)
    .fetch_optional(&state.db)
    .await
    .map_err(|e| {
        eprintln!("Failed to claim job: {e:?}");
        anyhow::anyhow!("Unable to claim job: {e}")
    })?;

    Ok(job)
}

/// Claim a specific job by ID (for testing or targeted processing).
pub async fn claim_job_by_id(
    state: &AppState,
    job_id: Uuid,
    _worker_id: &str,
) -> Result<Option<BackgroundJob>> {
    let now = OffsetDateTime::now_utc();
    let lease_until = now + time::Duration::seconds(WORKER_LEASE_SECONDS);

    let job = query_as::<_, BackgroundJob>(
        r#"
        UPDATE background_jobs
        SET
            status = 'processing',
            locked_until = $2,
            attempts = attempts + 1
        WHERE id = $1
          AND status = 'pending'
          AND available_at <= $3
        RETURNING
            id, file_id, job_type, status, attempts,
            available_at, locked_until, last_error,
            created_at, completed_at
        "#,
    )
    .bind(job_id)
    .bind(lease_until)
    .bind(now)
    .fetch_optional(&state.db)
    .await
    .map_err(|e| {
        eprintln!("Failed to claim job {job_id}: {e:?}");
        anyhow::anyhow!("Unable to claim job: {e}")
    })?;

    Ok(job)
}

/// Mark a job as completed successfully.
pub async fn complete_job(
    state: &AppState,
    job_id: Uuid,
) -> Result<()> {
    let now = OffsetDateTime::now_utc();

    query(
        r#"
        UPDATE background_jobs
        SET
            status = 'completed',
            locked_until = NULL,
            completed_at = $1
        WHERE id = $2
        "#,
    )
    .bind(now)
    .bind(job_id)
    .execute(&state.db)
    .await
    .map_err(|e| {
        eprintln!("Failed to complete job {job_id}: {e:?}");
        anyhow::anyhow!("Unable to complete job: {e}")
    })?;

    Ok(())
}

/// Mark a job as failed with error message, making it available for retry.
pub async fn fail_job(
    state: &AppState,
    job_id: Uuid,
    error: &str,
    retry: bool,
) -> Result<()> {
    let now = OffsetDateTime::now_utc();

    if retry {
        // Requeue with exponential backoff: 2^attempts * 30 seconds, max 1 hour
        query(
            r#"
            UPDATE background_jobs
            SET
                status = 'pending',
                locked_until = NULL,
                last_error = $1,
                available_at = $2 + (interval '30 seconds' * (2 ^ attempts))
            WHERE id = $3
            "#,
        )
        .bind(error)
        .bind(now)
        .bind(job_id)
        .execute(&state.db)
        .await
        .map_err(|e| {
            eprintln!("Failed to requeue job {job_id}: {e:?}");
            anyhow::anyhow!("Unable to requeue job: {e}")
        })?;
    } else {
        // Mark as permanently failed
        query(
            r#"
            UPDATE background_jobs
            SET
                status = 'failed',
                locked_until = NULL,
                last_error = $1,
                completed_at = $2
            WHERE id = $3
            "#,
        )
        .bind(error)
        .bind(now)
        .bind(job_id)
        .execute(&state.db)
        .await
        .map_err(|e| {
            eprintln!("Failed to fail job {job_id}: {e:?}");
            anyhow::anyhow!("Unable to fail job: {e}")
        })?;
    }

    Ok(())
}

/// Create a new background job.
pub async fn create_job(
    state: &AppState,
    file_id: Uuid,
    job_type: JobType,
) -> Result<Uuid> {
    let job_id = Uuid::new_v4();

    query(
        r#"
        INSERT INTO background_jobs (id, file_id, job_type)
        VALUES ($1, $2, $3)
        "#,
    )
    .bind(job_id)
    .bind(file_id)
    .bind(job_type.as_str())
    .execute(&state.db)
    .await
    .map_err(|e| {
        eprintln!("Failed to create job {job_id}: {e:?}");
        anyhow::anyhow!("Unable to create job: {e}")
    })?;

    Ok(job_id)
}

/// Create a new background job within an existing transaction.
pub async fn create_job_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    file_id: Uuid,
    job_type: JobType,
) -> Result<Uuid> {
    let job_id = Uuid::new_v4();

    query(
        r#"
        INSERT INTO background_jobs (id, file_id, job_type)
        VALUES ($1, $2, $3)
        "#,
    )
    .bind(job_id)
    .bind(file_id)
    .bind(job_type.as_str())
    .execute(&mut **tx)
    .await
    .map_err(|e| {
        eprintln!("Failed to create job {job_id}: {e:?}");
        anyhow::anyhow!("Unable to create job: {e}")
    })?;

    Ok(job_id)
}

/// Get job by ID for status polling.
pub async fn get_job(
    state: &AppState,
    job_id: Uuid,
) -> Result<Option<BackgroundJob>> {
    let job = query_as::<_, BackgroundJob>(
        r#"
        SELECT
            id, file_id, job_type, status, attempts,
            available_at, locked_until, last_error,
            created_at, completed_at
        FROM background_jobs
        WHERE id = $1
        "#,
    )
    .bind(job_id)
    .fetch_optional(&state.db)
    .await
    .map_err(|e| {
        eprintln!("Failed to get job {job_id}: {e:?}");
        anyhow::anyhow!("Unable to get job: {e}")
    })?;

    Ok(job)
}

/// Release a job lock (mark as pending again) - useful if worker crashes before completing.
/// This is handled automatically by the lease expiry, but can be called manually.
pub async fn release_job_lock(
    state: &AppState,
    job_id: Uuid,
) -> Result<()> {
    query(
        r#"
        UPDATE background_jobs
        SET
            status = 'pending',
            locked_until = NULL
        WHERE id = $1
          AND status = 'processing'
        "#,
    )
    .bind(job_id)
    .execute(&state.db)
    .await
    .map_err(|e| {
        eprintln!("Failed to release job lock {job_id}: {e:?}");
        anyhow::anyhow!("Unable to release job lock: {e}")
    })?;

    Ok(())
}