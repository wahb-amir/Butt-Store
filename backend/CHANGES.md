# APK Upload Workflow Implementation - Changes Summary

## Overview
Implementation of a complete APK upload workflow with background job processing, verification workers, and Azure-PostgreSQL reconciliation.

---

## Architecture Refactor — Module Reorganisation

### Dead code removed
- **`src/lib/azure.rs`** — Deleted. Was a parallel, stale Azure client implementation superseded
  by `src/storage/azure.rs`. It was never referenced via `lib.rs` and had a different error type
  (`Box<dyn Error>` vs `anyhow::Result`) and an outdated SAS signing path.
- **`src/api/uploads/complete.rs`** — Deleted. Unreferenced draft of the upload-completion
  endpoint; the active implementation is `upload_complete.rs`.
- **`src/types/outbox.rs`** — Deleted. `OutboxEventPayload` was defined but never read or written
  anywhere in the codebase.

### Renames
- **`src/api/uploads/uploadComplete.rs`** → **`upload_complete.rs`** — Corrected to follow Rust
  snake_case naming convention. The router reference and `#[allow(non_snake_case)]` annotation
  were updated accordingly.

### New: `src/workers/` module

Centralises the shared infrastructure that was previously copy-pasted across all four worker
binaries:

#### `src/workers/context.rs` — `WorkerContext`
- `WorkerContext::new_db_only(config)` — connects to PostgreSQL only (used by `process_worker`,
  which does not access Azure).
- `WorkerContext::new_with_azure(config)` — connects to PostgreSQL + Azure Blob Storage (used by
  `verify_worker`, `upload_completion_worker`, `reconcile_worker`).
- `WorkerContext::as_app_state()` — cheap conversion to `AppState` for `storage::jobs` functions.
- Each worker receives only the dependencies it actually uses.

#### `src/workers/lifecycle.rs` — Shared poll loops
- `run_poll_loop(PollConfig, tick)` — standard job-queue poll loop with exponential backoff on
  empty queues (identical to the per-worker loops that existed before) and graceful SIGTERM/SIGINT
  shutdown.
- `run_interval_loop(interval, task)` — fixed-interval loop for maintenance tasks (reconcile
  worker). Same signal handling, no backoff.

#### `src/workers/mod.rs`
- `init_tracing(worker_name)` — initialises the global `tracing` subscriber. Previously
  copy-pasted into every `main()`.

#### `src/workers/{verify,process,reconcile,upload_completion}/mod.rs`
- All job-processing logic moved from `src/bin/` into these modules. Each module exposes a `run()`
  function (and `run_once()` for the reconcile worker's `--once` mode).
- Job state transitions, SQL queries, lease constants, retry semantics, and Azure calls are
  preserved verbatim.

### Thinned binary entry points (`src/bin/*.rs`)
Each binary is now ~30 lines:
1. `dotenvy::dotenv().ok()`
2. Parse `Args` with `clap`
3. `init_tracing()`
4. `Config::from_env()`
5. `WorkerContext::new_*()`
6. `workers::*/run(ctx, config).await`

### `src/config.rs` — Minor cleanup
- Removed the internal `dotenvy::dotenv().ok()` call from `Config::from_env()`. All callers
  (all five `main()` functions) already call `dotenvy::dotenv().ok()` before constructing
  `Config`, so the internal call was redundant.

---


## Database Migrations

### `migrations/20261021080000_background_jobs.sql`
- New `background_jobs` table with:
  - Job types: `verify_apk`, `prepare_distribution`
  - Status states: `pending`, `processing`, `completed`, `failed`
  - Lease-based worker recovery via `locked_until`
  - Exponential backoff retry logic
  - Index on pending jobs for efficient polling

### `migrations/20261021080001_upload_statuses.sql`
- Extended upload status CHECK constraint with new states:
  - `PENDING_VERIFICATION` - Upload committed, awaiting verification
  - `VERIFIED` - APK passed all validation checks
  - `READY_FOR_DISTRIBUTION` - Prepared for CDN distribution
- Added consistency checks between upload status and job existence

---

## Core Types

### `src/types/jobs.rs`
- `JobType` enum: `VerifyApk`, `PrepareDistribution`, `CompleteUpload`
- `JobStatus` enum: `Pending`, `Processing`, `Completed`, `Failed`
- `BackgroundJob` struct mapping to database row
- `JobResponse` for API serialization

---

## Job Queue Infrastructure

### `src/storage/jobs.rs`
- `claim_job()` - Atomic claim using `FOR UPDATE SKIP LOCKED`
- `claim_job_by_id()` - Targeted job claiming
- `complete_job()` - Mark job as completed
- `fail_job()` - Mark failed with retry/backoff logic
- `create_job()` / `create_job_tx()` - Job creation (transactional)
- `get_job()` - Status polling
- `release_job_lock()` - Manual lock release

---

## API Endpoints

### `src/api/uploads/upload_complete.rs` (renamed from `uploadComplete.rs`)
- `POST /uploads/{id}/complete` — accept client-provided SHA-256, enqueue `complete_upload` job
- Idempotent completion with optional idempotency key
- Returns job ID and upload status
- Transactional: sets `PROCESSING_PENDING`, inserts `complete_upload` job atomically


### `src/api/uploads/models.rs`
- Extended `UploadRecord` with:
  - `actual_size_bytes`, `etag`, `checksum_sha256`

---

## Worker Binaries

### `src/bin/verify_worker.rs`
- Polls `verify_apk` jobs with lease-based claiming
- **Verification steps:**
  1. Blob existence check (Azure HEAD)
  2. SHA256 checksum verification (streaming download)
  3. APK structure validation (`aapt2 dump badging`)
  4. Signature verification (`apksigner verify`)
  5. Malware scan (`clamdscan` via ClamAV daemon)
- On success: Updates upload to `VERIFIED`, enqueues `prepare_distribution` job (transactional)
- On failure: Distinguishes permanent (malware/invalid APK) vs transient failures

### `src/bin/process_worker.rs`
- Polls `prepare_distribution` jobs
- Stub implementation (logs, updates status to `READY_FOR_DISTRIBUTION`)
- Extensible for: CDN upload, metadata extraction, manifest generation

### `src/bin/reconcile_worker.rs`
- Periodic reconciliation (configurable interval, default 5 min)
- Lists all blobs in Azure container
- For orphaned committed blobs (no DB record):
  - Parses user_id from blob path
  - Creates upload record + verification job
- For existing uploads in `PENDING`/`UPLOADING` with committed blobs:
  - Creates missing verification job
- Handles Azure-PostgreSQL consistency gaps

---

## Storage Layer

### `src/storage/azure.rs`
- Refactored error handling to use `anyhow::Result`
- `init_blob_service()` - Singleton blob service client
- `create_block_blob_client()` / `create_blob_client()` - Client factories
- `commit_upload()` - Commits block list with ordered block IDs
- `list_blobs()` - Paginated container listing
- `generate_upload_sas_url()` - User delegation SAS for client uploads

---

## Configuration

### `Cargo.toml`
- Added binaries: `verify_worker`, `process_worker`, `reconcile_worker`
- New dependencies:
  - `anyhow` - Error handling
  - `clap` - CLI argument parsing
  - `tokio-stream` - Stream utilities
  - `tracing-subscriber` with `env-filter` feature

---

## Architectural Decisions

1. **Separate binaries for workers** - Independent scaling, separate processes
2. **Background jobs (not outbox)** - Jobs = "do something", Events = "something happened"
3. **Lease-based recovery** - `locked_until` prevents stuck jobs on worker crash
4. **Idempotent completion** - Safe retry via idempotency key
5. **Reconciliation process** - Handles Azure/PostgreSQL consistency gaps
6. **anyhow for binaries** - Ergonomic error handling in worker processes

---

## Running the Workers

```bash
# Verification worker (runs continuously)
cargo run --bin verify_worker -- --poll-interval 10 --worker-id verify-1

# Distribution preparation worker
cargo run --bin process_worker -- --poll-interval 10 --worker-id process-1

# Reconciliation worker (runs every 5 minutes by default)
cargo run --bin reconcile_worker -- --once  # single run
cargo run --bin reconcile_worker --        # continuous

# Upload completion worker
cargo run --bin upload_completion_worker -- --poll-interval 10 --worker-id completion-1
```

## External Dependencies Required

- **aapt2** - Android Asset Packaging Tool (from Android SDK build-tools)
- **apksigner** - APK signature verification (from Android SDK build-tools)
- **clamdscan** - ClamAV daemon client (clamav-daemon package)