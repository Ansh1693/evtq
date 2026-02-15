-- Phase 1: Database Schema

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- queues table: stores queue configuration
CREATE TABLE queues (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,                                     -- uniqueness enforced by partial index below
    visibility_timeout INTEGER NOT NULL DEFAULT 30,        -- seconds
    message_retention  INTEGER NOT NULL DEFAULT 345600,     -- 4 days in seconds
    delay_seconds      INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at TIMESTAMPTZ                                  -- NULL = active, set = soft-deleted
);

-- messages table: the workhorse
CREATE TABLE messages (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    queue_id       UUID NOT NULL REFERENCES queues(id),     -- no CASCADE; soft-delete handles cleanup
    body           TEXT NOT NULL,
    attributes     JSONB DEFAULT '{}',
    receipt_handle UUID,                                    -- NULL = available
    receive_count  INTEGER NOT NULL DEFAULT 0,
    visible_at     TIMESTAMPTZ NOT NULL,
    expires_at     TIMESTAMPTZ NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at     TIMESTAMPTZ                              -- NULL = active, set = soft-deleted
);

-- Hot path index: finding visible, non-deleted messages for a queue
CREATE INDEX idx_messages_queue_visible
    ON messages (queue_id, visible_at)
    WHERE deleted_at IS NULL;

-- Cleanup job index: finding expired non-deleted messages to soft-delete
CREATE INDEX idx_messages_expires
    ON messages (expires_at)
    WHERE deleted_at IS NULL;

-- Receipt handle lookups for delete/visibility changes (non-deleted only)
CREATE INDEX idx_messages_receipt_handle
    ON messages (queue_id, receipt_handle)
    WHERE receipt_handle IS NOT NULL AND deleted_at IS NULL;

-- Hard-delete cleanup: find soft-deleted rows efficiently
CREATE INDEX idx_messages_deleted ON messages (deleted_at) WHERE deleted_at IS NOT NULL;
CREATE INDEX idx_queues_deleted ON queues (deleted_at) WHERE deleted_at IS NOT NULL;

-- Queue name lookups should only find active queues
CREATE UNIQUE INDEX idx_queues_name_active ON queues (name) WHERE deleted_at IS NULL;
