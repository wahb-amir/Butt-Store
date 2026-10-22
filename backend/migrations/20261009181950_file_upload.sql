CREATE TYPE upload_status AS ENUM ('PENDING', 'COMPLETED', 'FAILED',
        'EXPIRED', 'CANCELLED', 'QUARANTINED', 'DELETED');
CREATE TABLE uploads (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    user_id UUID NOT NULL
        REFERENCES users (id) ON DELETE RESTRICT,
    idempotency_key UUID NOT NULL,

    container_name TEXT NOT NULL,
    blob_name VARCHAR(330) NOT NULL,

    original_filename VARCHAR(255) NOT NULL,
    content_type VARCHAR(255) NOT NULL,
    expected_size_bytes BIGINT NOT NULL,

    -- Filled in when the upload is verified against storage
    actual_size_bytes BIGINT,
    checksum_sha256 BYTEA,

    status upload_status NOT NULL DEFAULT 'PENDING',

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL,
    last_activity_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- For retries and duplicate completion requests
    completion_requested_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    deleted_at TIMESTAMPTZ,
    deletion_reason TEXT,
    cleanup_at TIMESTAMPTZ,

    -- Idempotency: required for ON CONFLICT (user_id, idempotency_key)
    CONSTRAINT uploads_user_idempotency_uq UNIQUE (user_id, idempotency_key),
    CONSTRAINT uploads_container_blob_uq UNIQUE (container_name, blob_name),

    -- Keep in sync with MAX_FILE_SIZE_BYTES in the Rust handler (200 MiB)
    CONSTRAINT uploads_expected_size_chk
        CHECK (expected_size_bytes > 0 AND expected_size_bytes <= 209715200),
    CONSTRAINT uploads_actual_size_chk
        CHECK (actual_size_bytes IS NULL OR actual_size_bytes >= 0),

    CONSTRAINT uploads_checksum_len_chk
        CHECK (checksum_sha256 IS NULL OR octet_length(checksum_sha256) = 32),

    CONSTRAINT uploads_checksum_required_chk
        CHECK (status NOT IN ('COMPLETED', 'QUARANTINED')
               OR checksum_sha256 IS NOT NULL),

    CONSTRAINT uploads_expiry_after_create_chk CHECK (expires_at > created_at),

    -- State / timestamp consistency
    CONSTRAINT uploads_completed_consistency_chk
        CHECK (status <> 'COMPLETED'
               OR (completed_at IS NOT NULL AND actual_size_bytes IS NOT NULL)),
    CONSTRAINT uploads_deleted_consistency_chk
        CHECK (status <> 'DELETED' OR deleted_at IS NOT NULL)
);

-- "My uploads" listings, filtered by status, newest first
CREATE INDEX uploads_user_status_created_idx
    ON uploads (user_id, status, created_at DESC);

-- Expiry sweeper: only rows that can still expire
CREATE INDEX uploads_expiry_idx
    ON uploads (expires_at)
    WHERE status IN ('PENDING');

-- Stale-upload detection
CREATE INDEX uploads_stale_uploading_idx
    ON uploads (last_activity_at)
    WHERE status = 'PENDING';

-- Blob cleanup worker
CREATE INDEX uploads_cleanup_idx
    ON uploads (cleanup_at)
    WHERE cleanup_at IS NOT NULL AND status <> 'DELETED';

CREATE OR REPLACE FUNCTION set_updated_at() RETURNS trigger AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER uploads_set_updated_at
    BEFORE UPDATE ON uploads
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();