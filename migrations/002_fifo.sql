-- Phase: FIFO support schema changes

-- Queue type support and FIFO options.
ALTER TABLE queues
    ADD COLUMN queue_type TEXT NOT NULL DEFAULT 'STANDARD',
    ADD COLUMN content_based_dedup BOOLEAN NOT NULL DEFAULT false;

-- Message fields for FIFO ordering and deduplication.
ALTER TABLE messages
    ADD COLUMN message_group_id TEXT,
    ADD COLUMN message_dedup_id TEXT,
    ADD COLUMN sequence_number BIGINT;

-- Global sequence for FIFO sequence numbers.
CREATE SEQUENCE IF NOT EXISTS messages_sequence_number_seq;

-- Backfill sequence numbers for existing rows to keep NOT NULL safe.
UPDATE messages
SET sequence_number = nextval('messages_sequence_number_seq')
WHERE sequence_number IS NULL;

ALTER TABLE messages
    ALTER COLUMN sequence_number SET NOT NULL;

-- Dedup uniqueness among active rows while allowing dedup slot reuse when nullified.
CREATE UNIQUE INDEX idx_messages_queue_dedup_active
    ON messages (queue_id, message_dedup_id)
    WHERE message_dedup_id IS NOT NULL AND deleted_at IS NULL;

-- FIFO hot path: oldest message per group lookup.
CREATE INDEX idx_messages_queue_group_sequence_active
    ON messages (queue_id, message_group_id, sequence_number)
    WHERE deleted_at IS NULL;

-- Blocked-groups check support (cannot use now() in partial index predicate).
CREATE INDEX idx_messages_queue_group_receipt_active
    ON messages (queue_id, message_group_id, receipt_handle)
    WHERE deleted_at IS NULL;
