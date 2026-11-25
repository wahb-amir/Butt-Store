pub mod context;
pub mod lifecycle;

pub mod process;
pub mod reconcile;
pub mod upload_completion;
pub mod verify;

use tracing_subscriber::EnvFilter;

/// Initialise the global `tracing` subscriber for a worker binary.
///
/// Reads `RUST_LOG` from the environment and falls back to
/// `info,{worker_name}=debug` when the variable is absent or invalid.
pub fn init_tracing(worker_name: &str) {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| format!("info,{worker_name}=debug").into()),
        )
        .init();
}
