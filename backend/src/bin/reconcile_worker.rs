use anyhow::{Context, Result};
use backend::{
    config::Config,
    db::create_pool,
    state::AppState,
    storage::azure::{init_blob_service, list_blobs},
    storage::jobs::create_job,
    types::jobs::JobType,
};
use clap::Parser;
use sqlx::PgPool;
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(name = "reconcile_worker", about = "Azure-PostgreSQL reconciliation worker")]
struct Args {
    /// Run once and exit (default: run continuously)
    #[arg(long)]
    once: bool,

    /// Poll interval in seconds (for continuous mode)
    #[arg(long, default_value = "300")]
    poll_interval: u64,

    /// Worker identifier (for logging)
    #[arg(long, default_value = "reconcile-worker-1")]
    worker_id: String,

    /// Container name to scan
    #[arg(long, default_value = "uploads")]
    container: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,reconcile_worker=debug".into()),
        )
        .init();

    let args = Args::parse();

    // Load configuration
    dotenvy::dotenv().ok();
    let config = Config::from_env();

    // Create database pool
    let pool = create_pool(&config.database_url).await?;
    info!("Database connected");

    // Initialize Azure Blob Service
    let blob_service = init_blob_service()?;
    info!("Azure Blob Service connected");

    let state = Arc::new(AppState { db: pool });

    info!("Starting reconciliation worker: {}", args.worker_id);

    if args.once {
        run_reconciliation_once(&state, &*blob_service, &args.container, &args.worker_id).await?;
    } else {
        run_reconciliation_loop(state, blob_service, args).await?;
    }

    Ok(())
}

async fn run_reconciliation_loop(
    state: Arc<AppState>,
    blob_service: Arc<azure_storage_blob::BlobServiceClient>,
    args: Args,
) -> Result<()> {
    let mut shutdown = false;

    let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())?;
    let mut sigint = signal::unix::signal(signal::unix::SignalKind::interrupt())?;

    while !shutdown {
        tokio::select! {
            _ = sigterm.recv() => {
                info!("Received SIGTERM, shutting down...");
                shutdown = true;
            }
            _ = sigint.recv() => {
                info!("Received SIGINT, shutting down...");
                shutdown = true;
            }
            _ = tokio::time::sleep(Duration::from_secs(args.poll_interval)) => {
                if let Err(e) = run_reconciliation_once(&state, &*blob_service, &args.container, &args.worker_id).await {
                    error!(error = %e, "Reconciliation run failed");
                }
            }
        }
    }

    info!("Reconciliation worker stopped");
    Ok(())
}

async fn run_reconciliation_once(
    state: &AppState,
    blob_service: &azure_storage_blob::BlobServiceClient,
    container_name: &str,
    worker_id: &str,
) -> Result<()> {
    info!(worker_id, container = %container_name, "Starting reconciliation run");

    // List all blobs in the container
    let blobs = list_blobs(blob_service, container_name).await?;
    info!(count = blobs.len(), "Found blobs in container");

    let mut stats = ReconciliationStats::default();

    for blob_name in blobs {
        // Skip non-APK files (optional filtering)
        if !blob_name.ends_with(".apk") && !blob_name.ends_with(".aab") {
            continue;
        }

        match reconcile_blob(state, &blob_name, container_name).await {
            Ok(action) => {
                match action {
                    ReconcileAction::CreatedUpload => stats.created_uploads += 1,
                    ReconcileAction::CreatedJob => stats.created_jobs += 1,
                    ReconcileAction::NoAction => stats.no_action += 1,
                }
            }
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
        SELECT id, user_id, blob_name, original_filename, content_type,
               status, expected_size_bytes, expires_at
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
            match upload.status.as_str() {
                "PENDING" | "UPLOADING" => {
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
                "COMPLETED" | "PENDING_VERIFICATION" | "VERIFIED" | "READY_FOR_DISTRIBUTION" | "QUARANTINED" => {
                    // Valid completed states - check for jobs
                    let job_exists = sqlx::query_scalar::<_, bool>(
                        "SELECT EXISTS(SELECT 1 FROM background_jobs WHERE file_id = $1 AND job_type = 'verify_apk')",
                    )
                    .bind(upload.id)
                    .fetch_one(&state.db)
                    .await?;

                    if !job_exists && matches!(upload.status.as_str(), "PENDING_VERIFICATION") {
                        create_job(state, upload.id, JobType::VerifyApk).await?;
                        info!(blob = %blob_name, upload_id = %upload.id, "Created missing verification job for pending_verification");
                        return Ok(ReconcileAction::CreatedJob);
                    }

                    Ok(ReconcileAction::NoAction)
                }
                _ => {
                    // Other states (EXPIRED, CANCELLED, FAILED, DELETED) - no action needed
                    Ok(ReconcileAction::NoAction)
                }
            }
        }
        None => {
            // No upload record for this blob - create one
            info!(blob = %blob_name, "No upload record found, creating orphan record");

            // We can't determine the original user from blob name alone in current schema
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
            let user_exists = sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM users WHERE id = $1)",
            )
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

#[derive(sqlx::FromRow)]
struct UploadRecord {
    id: uuid::Uuid,
    user_id: uuid::Uuid,
    blob_name: String,
    original_filename: String,
    content_type: String,
    status: String,
    expected_size_bytes: i64,
    actual_size_bytes: Option<i64>,
    etag: Option<String>,
    checksum_sha256: Option<Vec<u8>>,
    expires_at: time::OffsetDateTime,
}