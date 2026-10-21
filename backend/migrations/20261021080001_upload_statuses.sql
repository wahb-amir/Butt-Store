-- Add migration script here
-- Add new statuses to uploads table: pending_verification, verified, ready_for_distribution

-- Drop the existing status check constraint
ALTER TABLE uploads DROP CONSTRAINT uploads_status_chk;

-- Add new constraint with additional statuses
ALTER TABLE uploads ADD CONSTRAINT uploads_status_chk CHECK (status IN (
    'PENDING', 'UPLOADING', 'COMPLETED', 'FAILED',
    'EXPIRED', 'CANCELLED', 'QUARANTINED', 'DELETED',
    'PENDING_VERIFICATION', 'VERIFIED', 'READY_FOR_DISTRIBUTION'
));

-- Add consistency check for VERIFIED status
ALTER TABLE uploads ADD CONSTRAINT uploads_verified_consistency_chk
    CHECK (status <> 'VERIFIED' OR (completed_at IS NOT NULL AND actual_size_bytes IS NOT NULL));

-- Add consistency check for READY_FOR_DISTRIBUTION status
ALTER TABLE uploads ADD CONSTRAINT uploads_ready_distribution_consistency_chk
    CHECK (status <> 'READY_FOR_DISTRIBUTION' OR (completed_at IS NOT NULL AND actual_size_bytes IS NOT NULL));

-- Add consistency check for PENDING_VERIFICATION status
ALTER TABLE uploads ADD CONSTRAINT uploads_pending_verification_consistency_chk
    CHECK (status <> 'PENDING_VERIFICATION' OR (completed_at IS NOT NULL AND actual_size_bytes IS NOT NULL));