ALTER TABLE background_jobs
    DROP CONSTRAINT background_jobs_job_type_check;

ALTER TABLE background_jobs
    ADD CONSTRAINT background_jobs_job_type_check
    CHECK (job_type IN ('complete_upload', 'verify_apk', 'prepare_distribution'));
