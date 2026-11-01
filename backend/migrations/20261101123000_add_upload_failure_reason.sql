ALTER TABLE uploads
    ADD COLUMN IF NOT EXISTS failure_reason TEXT;