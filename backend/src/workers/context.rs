use std::sync::Arc;

use anyhow::Result;
use azure_storage_blob::BlobServiceClient;
use sqlx::PgPool;
use tracing::info;

use crate::{config::Config, db::create_pool, state::AppState, storage::azure::init_blob_service};

/// Shared infrastructure context available to all worker processes.
///
/// Built once at startup; workers that do not need Azure call [`new_db_only`],
/// workers that read or write blobs call [`new_with_azure`]. Neither path forces
/// a worker to hold a dependency it does not use.
pub struct WorkerContext {
    /// Shared PostgreSQL connection pool.
    pub db: PgPool,
    /// Azure Blob Storage service client. `None` for workers that skip Azure init.
    blob_service: Option<Arc<BlobServiceClient>>,
}

impl WorkerContext {
    /// Initialise with a database connection only.
    ///
    /// Use this for workers that do not access Azure Blob Storage directly
    /// (e.g. the distribution-preparation worker).
    pub async fn new_db_only(config: &Config) -> Result<Self> {
        let db = create_pool(&config.database_url).await?;
        info!("Database connected");
        Ok(Self {
            db,
            blob_service: None,
        })
    }

    /// Initialise with a database connection and an Azure Blob Storage client.
    ///
    /// Use this for workers that read, write, or delete blobs
    /// (verify, upload-completion, reconcile workers).
    pub async fn new_with_azure(config: &Config) -> Result<Self> {
        let db = create_pool(&config.database_url).await?;
        info!("Database connected");

        let blob_service = init_blob_service()?;
        info!("Azure Blob Storage connected");

        Ok(Self {
            db,
            blob_service: Some(blob_service),
        })
    }

    /// Return the Azure Blob Storage client, or an error if Azure was not
    /// initialised for this context (i.e. built via [`new_db_only`]).
    pub fn blob_service(&self) -> Result<Arc<BlobServiceClient>> {
        self.blob_service.clone().ok_or_else(|| {
            anyhow::anyhow!("Azure Blob Storage was not initialised for this worker")
        })
    }

    /// Convert to the [`AppState`] type expected by [`crate::storage::jobs`] functions.
    ///
    /// This is a cheap clone of the underlying pool handle.
    pub fn as_app_state(&self) -> AppState {
        AppState {
            db: self.db.clone(),
        }
    }
}
