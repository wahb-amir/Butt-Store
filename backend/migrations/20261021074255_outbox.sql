-- Add migration script here

CREATE TABLE outbox_events (
    id            UUID PRIMARY KEY,
    aggregate_type TEXT NOT NULL,
    aggregate_id   TEXT NOT NULL,
    event_type    TEXT NOT NULL,
    payload       JSONB NOT NULL,

    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at  TIMESTAMPTZ,

    attempts      INTEGER NOT NULL DEFAULT 0,
    available_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_until  TIMESTAMPTZ,
    last_error    TEXT
);

CREATE INDEX idx_outbox_pending
    ON outbox_events (available_at, created_at)
    WHERE published_at IS NULL;