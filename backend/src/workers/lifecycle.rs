use std::future::Future;
use std::time::Duration;

use anyhow::Result;
use tokio::signal;
use tracing::{error, info};

/// Configuration for the standard polling worker loop.
pub struct PollConfig {
    /// How long to wait between job-claim attempts under normal conditions.
    pub poll_interval: Duration,
    /// Number of consecutive empty polls that trigger exponential backoff.
    pub max_empty_polls: u32,
}

/// Run a polling worker loop with exponential backoff and graceful shutdown.
///
/// `tick` is called after every `poll_interval` sleep. It should return:
/// - `Ok(true)` — a job was claimed and processed.
/// - `Ok(false)` — the queue was empty.
/// - `Err(…)` — a transient error (logged; the loop pauses 5 s before retrying).
///
/// Backoff: after `max_empty_polls` consecutive empty polls, the loop sleeps for
/// `poll_interval * 2^empty_polls` (capped at 60 s) before the next attempt.
/// The backoff counter resets whenever a job is successfully claimed.
///
/// The loop exits cleanly on `SIGTERM` or `SIGINT`.
pub async fn run_poll_loop<F, Fut>(config: PollConfig, tick: F) -> Result<()>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<bool>>,
{
    let mut empty_polls: u32 = 0;
    let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())?;
    let mut sigint = signal::unix::signal(signal::unix::SignalKind::interrupt())?;

    loop {
        tokio::select! {
            _ = sigterm.recv() => {
                info!("Received SIGTERM, shutting down...");
                break;
            }
            _ = sigint.recv() => {
                info!("Received SIGINT, shutting down...");
                break;
            }
            _ = tokio::time::sleep(config.poll_interval) => {
                match tick().await {
                    Ok(true) => {
                        empty_polls = 0;
                    }
                    Ok(false) => {
                        empty_polls += 1;
                        if empty_polls >= config.max_empty_polls {
                            let backoff = Duration::from_secs(
                                (config.poll_interval.as_secs()
                                    * 2_u64.pow(empty_polls.min(6)))
                                .min(60),
                            );
                            info!(
                                empty_polls,
                                backoff_secs = backoff.as_secs(),
                                "No jobs available, backing off"
                            );
                            tokio::time::sleep(backoff).await;
                        }
                    }
                    Err(e) => {
                        error!(error = %e, "Failed to claim job");
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    }
                }
            }
        }
    }

    info!("Worker stopped");
    Ok(())
}

/// Run an interval-based worker loop that fires a task on a fixed schedule.
///
/// Unlike [`run_poll_loop`], this does not track empty polls or apply backoff.
/// It is intended for periodic maintenance tasks (e.g. the reconcile worker)
/// that scan the full dataset rather than claiming individual jobs.
///
/// The loop exits cleanly on `SIGTERM` or `SIGINT`.
pub async fn run_interval_loop<F, Fut>(interval: Duration, task: F) -> Result<()>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())?;
    let mut sigint = signal::unix::signal(signal::unix::SignalKind::interrupt())?;

    loop {
        tokio::select! {
            _ = sigterm.recv() => {
                info!("Received SIGTERM, shutting down...");
                break;
            }
            _ = sigint.recv() => {
                info!("Received SIGINT, shutting down...");
                break;
            }
            _ = tokio::time::sleep(interval) => {
                if let Err(e) = task().await {
                    error!(error = %e, "Reconciliation run failed");
                }
            }
        }
    }

    info!("Reconciliation worker stopped");
    Ok(())
}
