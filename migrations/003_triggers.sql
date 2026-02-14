-- Phase: Trigger support schema

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'trigger_target_type') THEN
        CREATE TYPE trigger_target_type AS ENUM ('webhook', 'grpc', 'local');
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS triggers (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    queue_id UUID NOT NULL REFERENCES queues(id),
    enabled BOOLEAN NOT NULL DEFAULT true,
    target_type trigger_target_type NOT NULL,
    target_url TEXT NOT NULL,
    batch_size INTEGER NOT NULL DEFAULT 10,
    batch_window_seconds INTEGER NOT NULL DEFAULT 0,
    max_concurrency INTEGER NOT NULL DEFAULT 1,
    visibility_timeout_override INTEGER,
    max_concurrency_per_group INTEGER NOT NULL DEFAULT 1,
    auto_scale BOOLEAN NOT NULL DEFAULT false,
    min_pollers INTEGER NOT NULL DEFAULT 1,
    max_pollers INTEGER NOT NULL DEFAULT 1,
    failure_threshold INTEGER NOT NULL DEFAULT 5,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at TIMESTAMPTZ,
    CONSTRAINT triggers_batch_size_chk CHECK (batch_size BETWEEN 1 AND 10),
    CONSTRAINT triggers_batch_window_chk CHECK (batch_window_seconds BETWEEN 0 AND 300),
    CONSTRAINT triggers_max_concurrency_chk CHECK (max_concurrency >= 1),
    CONSTRAINT triggers_visibility_override_chk CHECK (visibility_timeout_override IS NULL OR (visibility_timeout_override BETWEEN 0 AND 43200)),
    CONSTRAINT triggers_group_concurrency_chk CHECK (max_concurrency_per_group >= 1),
    CONSTRAINT triggers_pollers_chk CHECK (min_pollers >= 1 AND max_pollers >= 1 AND min_pollers <= max_pollers),
    CONSTRAINT triggers_failure_threshold_chk CHECK (failure_threshold >= 1)
);

CREATE INDEX IF NOT EXISTS idx_triggers_queue_active
    ON triggers (queue_id, enabled, created_at)
    WHERE deleted_at IS NULL;

CREATE TABLE IF NOT EXISTS trigger_metrics (
    trigger_id UUID PRIMARY KEY REFERENCES triggers(id),
    invocations_total BIGINT NOT NULL DEFAULT 0,
    invocations_success BIGINT NOT NULL DEFAULT 0,
    invocations_failed BIGINT NOT NULL DEFAULT 0,
    messages_processed_total BIGINT NOT NULL DEFAULT 0,
    messages_failed_total BIGINT NOT NULL DEFAULT 0,
    average_invocation_duration_ms BIGINT NOT NULL DEFAULT 0,
    iterator_age_ms BIGINT NOT NULL DEFAULT 0,
    last_invocation_at TIMESTAMPTZ,
    last_success_at TIMESTAMPTZ,
    last_failure_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
