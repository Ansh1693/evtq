package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Ansh1693/evtq/internal"
)

var (
	ErrQueueNotFound    = errors.New("queue not found")
	ErrMessageNotFound  = errors.New("message not found or receipt handle is stale")
	ErrTriggerNotFound  = errors.New("trigger not found")
	ErrFunctionNotFound = errors.New("function not found")
)

// Store holds a connection pool and implements raw database operations.
type Store struct {
	pool *pgxpool.Pool
}

// New creates a new Store with the given connection pool.
func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// ---------- Queue operations ----------

// CreateQueue inserts a new queue and returns it.
func (s *Store) CreateQueue(ctx context.Context, q *internal.Queue) (*internal.Queue, error) {
	row := s.pool.QueryRow(ctx, `
		INSERT INTO queues (name, queue_type, content_based_dedup, visibility_timeout, message_retention, delay_seconds)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING id, name, queue_type, content_based_dedup, visibility_timeout, message_retention, delay_seconds, created_at, updated_at
	`, q.Name, q.QueueType, q.ContentBasedDedup, q.VisibilityTimeout, q.MessageRetention, q.DelaySeconds)

	var out internal.Queue
	err := row.Scan(
		&out.ID, &out.Name, &out.QueueType, &out.ContentBasedDedup, &out.VisibilityTimeout,
		&out.MessageRetention, &out.DelaySeconds,
		&out.CreatedAt, &out.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("create queue: %w", err)
	}
	return &out, nil
}

// GetQueueByName fetches an active (non-deleted) queue by its unique name.
func (s *Store) GetQueueByName(ctx context.Context, name string) (*internal.Queue, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, name, queue_type, content_based_dedup, visibility_timeout, message_retention, delay_seconds, created_at, updated_at
		FROM queues WHERE name = $1 AND deleted_at IS NULL
	`, name)

	var q internal.Queue
	err := row.Scan(
		&q.ID, &q.Name, &q.QueueType, &q.ContentBasedDedup, &q.VisibilityTimeout,
		&q.MessageRetention, &q.DelaySeconds,
		&q.CreatedAt, &q.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrQueueNotFound
		}
		return nil, fmt.Errorf("get queue: %w", err)
	}
	return &q, nil
}

// GetQueueByID fetches an active queue by ID.
func (s *Store) GetQueueByID(ctx context.Context, queueID uuid.UUID) (*internal.Queue, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, name, queue_type, content_based_dedup, visibility_timeout, message_retention, delay_seconds, created_at, updated_at
		FROM queues WHERE id = $1 AND deleted_at IS NULL
	`, queueID)

	var q internal.Queue
	err := row.Scan(
		&q.ID, &q.Name, &q.QueueType, &q.ContentBasedDedup, &q.VisibilityTimeout,
		&q.MessageRetention, &q.DelaySeconds,
		&q.CreatedAt, &q.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrQueueNotFound
		}
		return nil, fmt.Errorf("get queue by id: %w", err)
	}
	return &q, nil
}

// DeleteQueue soft-deletes a queue by name and soft-deletes all its messages.
func (s *Store) DeleteQueue(ctx context.Context, name string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("delete queue begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	// Soft-delete the queue and capture its ID.
	var queueID uuid.UUID
	err = tx.QueryRow(ctx, `
		UPDATE queues SET deleted_at = now(), updated_at = now()
		WHERE name = $1 AND deleted_at IS NULL
		RETURNING id
	`, name).Scan(&queueID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrQueueNotFound
		}
		return fmt.Errorf("delete queue: %w", err)
	}

	// Soft-delete all messages belonging to this queue.
	_, err = tx.Exec(ctx, `
		UPDATE messages SET deleted_at = now()
		WHERE queue_id = $1 AND deleted_at IS NULL
	`, queueID)
	if err != nil {
		return fmt.Errorf("delete queue messages: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("delete queue commit: %w", err)
	}
	return nil
}

// ---------- Message operations ----------

// SendMessage inserts a message into the given queue and fires a NOTIFY
// so long-polling consumers wake up immediately.
func (s *Store) SendMessage(ctx context.Context, msg *internal.Message) (*internal.Message, error) {
	attrs, err := json.Marshal(msg.Attributes)
	if err != nil {
		return nil, fmt.Errorf("marshal attributes: %w", err)
	}

	row := s.pool.QueryRow(ctx, `
		INSERT INTO messages (queue_id, message_group_id, message_dedup_id, body, attributes, visible_at, expires_at, sequence_number)
		VALUES ($1, $2, $3, $4, $5, $6, $7, nextval('messages_sequence_number_seq'))
		RETURNING id, queue_id, message_group_id, message_dedup_id, sequence_number, body, attributes, receipt_handle, receive_count, visible_at, expires_at, created_at
	`, msg.QueueID, msg.MessageGroupID, msg.MessageDedupID, msg.Body, attrs, msg.VisibleAt, msg.ExpiresAt)

	out, err := scanMessage(row)
	if err != nil {
		return nil, err
	}

	// Best-effort NOTIFY — don't fail the send if notification fails.
	_ = s.notifyQueue(ctx, msg.QueueID)

	return out, nil
}

// SendMessageFIFO performs SQS-like silent dedup in a single transaction.
// If the dedup ID was seen in the past 5 minutes for this queue, it returns
// the original message as a success and skips insert.
func (s *Store) SendMessageFIFO(ctx context.Context, msg *internal.Message) (*internal.Message, error) {
	if msg.MessageDedupID == nil {
		return nil, fmt.Errorf("send fifo: message_dedup_id is required")
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("send fifo begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	// 5-minute dedup window.
	dupRow := tx.QueryRow(ctx, `
		SELECT id, queue_id, message_group_id, message_dedup_id, sequence_number, body, attributes, receipt_handle, receive_count, visible_at, expires_at, created_at
		FROM messages
		WHERE queue_id = $1
		  AND message_dedup_id = $2
		  AND deleted_at IS NULL
		  AND created_at >= now() - interval '5 minutes'
		ORDER BY created_at ASC
		LIMIT 1
	`, msg.QueueID, *msg.MessageDedupID)

	if existing, err := scanMessage(dupRow); err == nil {
		if commitErr := tx.Commit(ctx); commitErr != nil {
			return nil, fmt.Errorf("send fifo dedup commit: %w", commitErr)
		}
		return existing, nil
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("send fifo dedup check: %w", err)
	}

	attrs, err := json.Marshal(msg.Attributes)
	if err != nil {
		return nil, fmt.Errorf("marshal attributes: %w", err)
	}

	row := tx.QueryRow(ctx, `
		INSERT INTO messages (queue_id, message_group_id, message_dedup_id, body, attributes, visible_at, expires_at, sequence_number)
		VALUES ($1, $2, $3, $4, $5, $6, $7, nextval('messages_sequence_number_seq'))
		RETURNING id, queue_id, message_group_id, message_dedup_id, sequence_number, body, attributes, receipt_handle, receive_count, visible_at, expires_at, created_at
	`, msg.QueueID, msg.MessageGroupID, msg.MessageDedupID, msg.Body, attrs, msg.VisibleAt, msg.ExpiresAt)

	out, err := scanMessage(row)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("send fifo commit: %w", err)
	}

	_ = s.notifyQueue(ctx, msg.QueueID)
	return out, nil
}

// channelName returns the Postgres NOTIFY channel for a queue.
func channelName(queueID uuid.UUID) string {
	return "queue_" + strings.ReplaceAll(queueID.String(), "-", "")
}

// notifyQueue sends a Postgres NOTIFY on the queue's channel.
func (s *Store) notifyQueue(ctx context.Context, queueID uuid.UUID) error {
	_, err := s.pool.Exec(ctx, "SELECT pg_notify($1, '')", channelName(queueID))
	return err
}

// WaitForNotification blocks until a NOTIFY arrives on the queue's channel
// or the context is cancelled. It acquires a dedicated connection from the pool
// for the duration of the wait.
func (s *Store) WaitForNotification(ctx context.Context, queueID uuid.UUID) error {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire conn for listen: %w", err)
	}
	defer conn.Release()

	channel := channelName(queueID)

	_, err = conn.Exec(ctx, "LISTEN "+channel)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}

	// Ensure we UNLISTEN on the way out so the connection is clean when returned to the pool.
	defer func() {
		// Use a background context since ctx may already be cancelled.
		_, _ = conn.Exec(context.Background(), "UNLISTEN "+channel)
	}()

	// Block until notification or context cancellation.
	_, err = conn.Conn().WaitForNotification(ctx)
	if err != nil {
		// Context cancellation is expected (deadline/timeout), not a real error.
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("wait for notification: %w", err)
	}
	return nil
}

// ReceiveMessages atomically claims up to maxMessages visible, non-deleted messages.
// Uses FOR UPDATE SKIP LOCKED to allow concurrent consumers without blocking.
func (s *Store) ReceiveMessages(ctx context.Context, queueID uuid.UUID, visibilityTimeout int, maxMessages int) ([]internal.Message, error) {
	rows, err := s.pool.Query(ctx, `
		UPDATE messages
		SET receipt_handle = gen_random_uuid(),
		    visible_at     = now() + interval '1 second' * $2,
		    receive_count  = receive_count + 1
		WHERE id IN (
			SELECT id FROM messages
			WHERE queue_id = $1
			  AND visible_at <= now()
			  AND expires_at > now()
			  AND deleted_at IS NULL
			ORDER BY created_at
			LIMIT $3
			FOR UPDATE SKIP LOCKED
		)
		RETURNING id, queue_id, message_group_id, message_dedup_id, sequence_number, body, attributes, receipt_handle, receive_count, visible_at, expires_at, created_at
	`, queueID, visibilityTimeout, maxMessages)
	if err != nil {
		return nil, fmt.Errorf("receive messages: %w", err)
	}
	defer rows.Close()

	var messages []internal.Message
	for rows.Next() {
		msg, err := scanMessageFromRows(rows)
		if err != nil {
			return nil, err
		}
		messages = append(messages, *msg)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("receive messages rows: %w", err)
	}
	return messages, nil
}

// ReceiveMessagesFIFO returns at most one available message per message group.
// It also enforces at-most-one in-flight message per group using advisory locks.
func (s *Store) ReceiveMessagesFIFO(ctx context.Context, queueID uuid.UUID, visibilityTimeout int, maxMessages int) ([]internal.Message, error) {
	rows, err := s.pool.Query(ctx, `
		WITH blocked_groups AS (
			SELECT DISTINCT message_group_id
			FROM messages
			WHERE queue_id = $1
			  AND message_group_id IS NOT NULL
			  AND receipt_handle IS NOT NULL
			  AND visible_at > now()
			  AND deleted_at IS NULL
		),
		free_groups AS (
			SELECT DISTINCT message_group_id
			FROM messages
			WHERE queue_id = $1
			  AND message_group_id IS NOT NULL
			  AND visible_at <= now()
			  AND expires_at > now()
			  AND deleted_at IS NULL
			  AND message_group_id NOT IN (SELECT message_group_id FROM blocked_groups)
		),
		candidates AS (
			SELECT first_per_group.id
			FROM free_groups fg
			JOIN LATERAL (
				SELECT m.id
				FROM messages m
				WHERE m.queue_id = $1
				  AND m.message_group_id = fg.message_group_id
				  AND m.visible_at <= now()
				  AND m.expires_at > now()
				  AND m.deleted_at IS NULL
				ORDER BY m.sequence_number
				LIMIT 1
				FOR UPDATE SKIP LOCKED
			) AS first_per_group ON TRUE
			ORDER BY fg.message_group_id
			LIMIT $3
		)
		UPDATE messages m
		SET receipt_handle = gen_random_uuid(),
		    visible_at = now() + interval '1 second' * $2,
		    receive_count = receive_count + 1
		FROM candidates c
		WHERE m.id = c.id
		  AND pg_try_advisory_xact_lock(hashtext(m.message_group_id))
		RETURNING m.id, m.queue_id, m.message_group_id, m.message_dedup_id, m.sequence_number, m.body, m.attributes, m.receipt_handle, m.receive_count, m.visible_at, m.expires_at, m.created_at
	`, queueID, visibilityTimeout, maxMessages)
	if err != nil {
		return nil, fmt.Errorf("receive fifo messages: %w", err)
	}
	defer rows.Close()

	var messages []internal.Message
	for rows.Next() {
		msg, scanErr := scanMessageFromRows(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		messages = append(messages, *msg)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("receive fifo rows: %w", err)
	}
	return messages, nil
}

// DeleteMessage soft-deletes a message by queue_id and receipt_handle.
// If the receipt handle is stale, returns ErrMessageNotFound.
func (s *Store) DeleteMessage(ctx context.Context, queueID uuid.UUID, receiptHandle uuid.UUID) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE messages SET deleted_at = now()
		WHERE queue_id = $1 AND receipt_handle = $2 AND deleted_at IS NULL
	`, queueID, receiptHandle)
	if err != nil {
		return fmt.Errorf("delete message: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrMessageNotFound
	}
	// Delete can unblock the next FIFO message in-group.
	_ = s.notifyQueue(ctx, queueID)
	return nil
}

// ChangeMessageVisibility updates the visibility timeout for an in-flight, non-deleted message.
func (s *Store) ChangeMessageVisibility(ctx context.Context, queueID uuid.UUID, receiptHandle uuid.UUID, newTimeout int) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE messages
		SET visible_at = now() + interval '1 second' * $3
		WHERE queue_id = $1 AND receipt_handle = $2 AND deleted_at IS NULL
	`, queueID, receiptHandle, newTimeout)
	if err != nil {
		return fmt.Errorf("change visibility: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrMessageNotFound
	}
	return nil
}

// PurgeQueue soft-deletes all non-deleted messages from a queue.
func (s *Store) PurgeQueue(ctx context.Context, queueID uuid.UUID) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE messages SET deleted_at = now()
		WHERE queue_id = $1 AND deleted_at IS NULL
	`, queueID)
	if err != nil {
		return 0, fmt.Errorf("purge queue: %w", err)
	}
	return tag.RowsAffected(), nil
}

// GetQueueStats returns message counts for active (non-deleted) messages:
// available, in-flight, and delayed.
func (s *Store) GetQueueStats(ctx context.Context, queueID uuid.UUID) (*internal.QueueStats, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT
			COUNT(*) FILTER (WHERE visible_at <= now() AND expires_at > now()) AS available,
			COUNT(*) FILTER (WHERE visible_at > now() AND receipt_handle IS NOT NULL AND expires_at > now()) AS in_flight,
			COUNT(*) FILTER (WHERE visible_at > now() AND receipt_handle IS NULL AND expires_at > now()) AS delayed
		FROM messages
		WHERE queue_id = $1 AND deleted_at IS NULL
	`, queueID)

	var stats internal.QueueStats
	if err := row.Scan(&stats.Available, &stats.InFlight, &stats.Delayed); err != nil {
		return nil, fmt.Errorf("get queue stats: %w", err)
	}
	return &stats, nil
}

// ---------- Batch operations ----------

// SendMessageBatch inserts multiple messages in a single transaction.
// Returns the successfully inserted messages and any per-index errors.
// The transaction is committed even if individual inserts fail.
func (s *Store) SendMessageBatch(ctx context.Context, msgs []*internal.Message) ([]*internal.Message, map[int]error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		// If we can't even start the tx, return error for every entry.
		errs := make(map[int]error, len(msgs))
		for i := range msgs {
			errs[i] = fmt.Errorf("begin tx: %w", err)
		}
		return nil, errs
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	results := make([]*internal.Message, len(msgs))
	errs := make(map[int]error)

	for i, msg := range msgs {
		if msg.MessageDedupID != nil {
			dupRow := tx.QueryRow(ctx, `
				SELECT id, queue_id, message_group_id, message_dedup_id, sequence_number, body, attributes, receipt_handle, receive_count, visible_at, expires_at, created_at
				FROM messages
				WHERE queue_id = $1
				  AND message_dedup_id = $2
				  AND deleted_at IS NULL
				  AND created_at >= now() - interval '5 minutes'
				ORDER BY created_at ASC
				LIMIT 1
			`, msg.QueueID, *msg.MessageDedupID)

			if existing, dupErr := scanMessage(dupRow); dupErr == nil {
				results[i] = existing
				continue
			} else if !errors.Is(dupErr, pgx.ErrNoRows) {
				errs[i] = fmt.Errorf("batch dedup check: %w", dupErr)
				continue
			}
		}

		attrs, err := json.Marshal(msg.Attributes)
		if err != nil {
			errs[i] = fmt.Errorf("marshal attributes: %w", err)
			continue
		}

		row := tx.QueryRow(ctx, `
			INSERT INTO messages (queue_id, message_group_id, message_dedup_id, body, attributes, visible_at, expires_at, sequence_number)
			VALUES ($1, $2, $3, $4, $5, $6, $7, nextval('messages_sequence_number_seq'))
			RETURNING id, queue_id, message_group_id, message_dedup_id, sequence_number, body, attributes, receipt_handle, receive_count, visible_at, expires_at, created_at
		`, msg.QueueID, msg.MessageGroupID, msg.MessageDedupID, msg.Body, attrs, msg.VisibleAt, msg.ExpiresAt)

		out, err := scanMessage(row)
		if err != nil {
			errs[i] = err
			continue
		}
		results[i] = out
	}

	if err := tx.Commit(ctx); err != nil {
		// Commit failure means nothing was persisted.
		allErrs := make(map[int]error, len(msgs))
		for i := range msgs {
			allErrs[i] = fmt.Errorf("commit tx: %w", err)
		}
		return nil, allErrs
	}

	// Fire a single NOTIFY for the queue (all messages share the same queue).
	if len(msgs) > 0 {
		_ = s.notifyQueue(ctx, msgs[0].QueueID)
	}

	return results, errs
}

// DeleteMessageBatch soft-deletes multiple messages in a single transaction.
// Returns per-index errors. An entry succeeds if its receipt handle matched.
func (s *Store) DeleteMessageBatch(ctx context.Context, queueID uuid.UUID, receiptHandles []uuid.UUID) map[int]error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		errs := make(map[int]error, len(receiptHandles))
		for i := range receiptHandles {
			errs[i] = fmt.Errorf("begin tx: %w", err)
		}
		return errs
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	errs := make(map[int]error)

	for i, rh := range receiptHandles {
		tag, err := tx.Exec(ctx, `
			UPDATE messages SET deleted_at = now()
			WHERE queue_id = $1 AND receipt_handle = $2 AND deleted_at IS NULL
		`, queueID, rh)
		if err != nil {
			errs[i] = fmt.Errorf("delete message: %w", err)
			continue
		}
		if tag.RowsAffected() == 0 {
			errs[i] = ErrMessageNotFound
		}
	}

	if err := tx.Commit(ctx); err != nil {
		allErrs := make(map[int]error, len(receiptHandles))
		for i := range receiptHandles {
			allErrs[i] = fmt.Errorf("commit tx: %w", err)
		}
		return allErrs
	}

	return errs
}

// SoftDeleteExpiredMessages marks expired messages as deleted (soft-delete).
// Returns the number of affected messages.
func (s *Store) SoftDeleteExpiredMessages(ctx context.Context) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE messages SET deleted_at = now()
		WHERE expires_at <= now() AND deleted_at IS NULL
	`)
	if err != nil {
		return 0, fmt.Errorf("soft delete expired: %w", err)
	}
	return tag.RowsAffected(), nil
}

// ExpireDedupIDs reopens FIFO dedup slots after the 5-minute window.
func (s *Store) ExpireDedupIDs(ctx context.Context) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE messages
		SET message_dedup_id = NULL
		WHERE message_dedup_id IS NOT NULL
		  AND created_at < now() - interval '5 minutes'
		  AND deleted_at IS NULL
	`)
	if err != nil {
		return 0, fmt.Errorf("expire dedup ids: %w", err)
	}
	return tag.RowsAffected(), nil
}

// HardDeleteMessages permanently removes soft-deleted messages.
// Returns the number of rows removed.
func (s *Store) HardDeleteMessages(ctx context.Context) (int64, error) {
	tag, err := s.pool.Exec(ctx, `DELETE FROM messages WHERE deleted_at IS NOT NULL`)
	if err != nil {
		return 0, fmt.Errorf("hard delete messages: %w", err)
	}
	return tag.RowsAffected(), nil
}

// HardDeleteQueues permanently removes soft-deleted queues.
// Should be called after HardDeleteMessages to avoid FK violations.
// Returns the number of rows removed.
func (s *Store) HardDeleteQueues(ctx context.Context) (int64, error) {
	tag, err := s.pool.Exec(ctx, `DELETE FROM queues WHERE deleted_at IS NOT NULL`)
	if err != nil {
		return 0, fmt.Errorf("hard delete queues: %w", err)
	}
	return tag.RowsAffected(), nil
}

// ---------- Function operations ----------

func (s *Store) CreateFunction(ctx context.Context, fn *internal.Function) (*internal.Function, error) {
	env, err := json.Marshal(fn.Environment)
	if err != nil {
		return nil, fmt.Errorf("marshal function environment: %w", err)
	}
	row := s.pool.QueryRow(ctx, `
		INSERT INTO functions (
			name, runtime, handler, timeout_seconds, memory_mb, environment, code_path,
			container_strategy, warm_pool_size, image_name
		)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
		RETURNING id, name, runtime, handler, timeout_seconds, memory_mb, environment, code_path,
			container_strategy, warm_pool_size, image_name, created_at, updated_at, deleted_at
	`, fn.Name, fn.Runtime, fn.Handler, fn.TimeoutSeconds, fn.MemoryMB, env, fn.CodePath,
		fn.ContainerStrategy, fn.WarmPoolSize, fn.ImageName)
	out, err := scanFunction(row)
	if err != nil {
		return nil, fmt.Errorf("create function: %w", err)
	}
	return out, nil
}

func (s *Store) ListFunctions(ctx context.Context) ([]internal.Function, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, name, runtime, handler, timeout_seconds, memory_mb, environment, code_path,
			container_strategy, warm_pool_size, image_name, created_at, updated_at, deleted_at
		FROM functions
		WHERE deleted_at IS NULL
		ORDER BY created_at ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("list functions: %w", err)
	}
	defer rows.Close()
	out := make([]internal.Function, 0)
	for rows.Next() {
		fn, scanErr := scanFunctionFromRows(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		out = append(out, *fn)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list functions rows: %w", err)
	}
	return out, nil
}

func (s *Store) GetFunctionByName(ctx context.Context, name string) (*internal.Function, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, name, runtime, handler, timeout_seconds, memory_mb, environment, code_path,
			container_strategy, warm_pool_size, image_name, created_at, updated_at, deleted_at
		FROM functions
		WHERE name = $1 AND deleted_at IS NULL
	`, name)
	out, err := scanFunction(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) || strings.Contains(err.Error(), pgx.ErrNoRows.Error()) {
			return nil, ErrFunctionNotFound
		}
		return nil, fmt.Errorf("get function: %w", err)
	}
	return out, nil
}

func (s *Store) UpdateFunction(ctx context.Context, fn *internal.Function) (*internal.Function, error) {
	env, err := json.Marshal(fn.Environment)
	if err != nil {
		return nil, fmt.Errorf("marshal function environment: %w", err)
	}
	row := s.pool.QueryRow(ctx, `
		UPDATE functions
		SET runtime = $2,
		    handler = $3,
		    timeout_seconds = $4,
		    memory_mb = $5,
		    environment = $6,
		    code_path = $7,
		    container_strategy = $8,
		    warm_pool_size = $9,
		    image_name = $10,
		    updated_at = now()
		WHERE name = $1 AND deleted_at IS NULL
		RETURNING id, name, runtime, handler, timeout_seconds, memory_mb, environment, code_path,
			container_strategy, warm_pool_size, image_name, created_at, updated_at, deleted_at
	`, fn.Name, fn.Runtime, fn.Handler, fn.TimeoutSeconds, fn.MemoryMB, env, fn.CodePath, fn.ContainerStrategy, fn.WarmPoolSize, fn.ImageName)
	out, err := scanFunction(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) || strings.Contains(err.Error(), pgx.ErrNoRows.Error()) {
			return nil, ErrFunctionNotFound
		}
		return nil, fmt.Errorf("update function: %w", err)
	}
	return out, nil
}

func (s *Store) DeleteFunction(ctx context.Context, name string) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE functions SET deleted_at = now(), updated_at = now()
		WHERE name = $1 AND deleted_at IS NULL
	`, name)
	if err != nil {
		return fmt.Errorf("delete function: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrFunctionNotFound
	}
	return nil
}

// ---------- Trigger operations ----------

// CreateTrigger inserts a trigger for a queue.
func (s *Store) CreateTrigger(ctx context.Context, t *internal.Trigger) (*internal.Trigger, error) {
	row := s.pool.QueryRow(ctx, `
		INSERT INTO triggers (
			queue_id, enabled, target_type, target_url, batch_size, batch_window_seconds,
			max_concurrency, visibility_timeout_override, max_concurrency_per_group,
			auto_scale, min_pollers, max_pollers, failure_threshold
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		RETURNING id, queue_id, enabled, target_type, target_url, batch_size, batch_window_seconds,
			max_concurrency, visibility_timeout_override, max_concurrency_per_group,
			auto_scale, min_pollers, max_pollers, failure_threshold, created_at, updated_at, deleted_at
	`, t.QueueID, t.Enabled, t.TargetType, t.TargetURL, t.BatchSize, t.BatchWindowSeconds,
		t.MaxConcurrency, t.VisibilityTimeoutOverride, t.MaxConcurrencyPerGroup,
		t.AutoScale, t.MinPollers, t.MaxPollers, t.FailureThreshold)

	out, err := scanTrigger(row)
	if err != nil {
		return nil, fmt.Errorf("create trigger: %w", err)
	}
	return out, nil
}

// ListTriggersByQueue returns all active triggers for the queue.
func (s *Store) ListTriggersByQueue(ctx context.Context, queueID uuid.UUID) ([]internal.Trigger, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, queue_id, enabled, target_type, target_url, batch_size, batch_window_seconds,
			max_concurrency, visibility_timeout_override, max_concurrency_per_group,
			auto_scale, min_pollers, max_pollers, failure_threshold, created_at, updated_at, deleted_at
		FROM triggers
		WHERE queue_id = $1 AND deleted_at IS NULL
		ORDER BY created_at ASC
	`, queueID)
	if err != nil {
		return nil, fmt.Errorf("list triggers: %w", err)
	}
	defer rows.Close()

	triggers := make([]internal.Trigger, 0)
	for rows.Next() {
		t, scanErr := scanTriggerFromRows(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		triggers = append(triggers, *t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list triggers rows: %w", err)
	}
	return triggers, nil
}

// GetTriggerByIDAndQueue fetches one active trigger by ID and queue.
func (s *Store) GetTriggerByIDAndQueue(ctx context.Context, queueID, triggerID uuid.UUID) (*internal.Trigger, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, queue_id, enabled, target_type, target_url, batch_size, batch_window_seconds,
			max_concurrency, visibility_timeout_override, max_concurrency_per_group,
			auto_scale, min_pollers, max_pollers, failure_threshold, created_at, updated_at, deleted_at
		FROM triggers
		WHERE queue_id = $1 AND id = $2 AND deleted_at IS NULL
	`, queueID, triggerID)

	t, err := scanTrigger(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) || strings.Contains(err.Error(), pgx.ErrNoRows.Error()) {
			return nil, ErrTriggerNotFound
		}
		return nil, fmt.Errorf("get trigger: %w", err)
	}
	return t, nil
}

// GetTriggerByID fetches one active trigger by ID, including queue fields.
func (s *Store) GetTriggerByID(ctx context.Context, triggerID uuid.UUID) (*internal.TriggerWithQueue, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT t.id, t.queue_id, t.enabled, t.target_type, t.target_url, t.batch_size, t.batch_window_seconds,
			t.max_concurrency, t.visibility_timeout_override, t.max_concurrency_per_group,
			t.auto_scale, t.min_pollers, t.max_pollers, t.failure_threshold, t.created_at, t.updated_at, t.deleted_at,
			q.name, q.queue_type, q.visibility_timeout
		FROM triggers t
		JOIN queues q ON q.id = t.queue_id
		WHERE t.id = $1 AND t.deleted_at IS NULL AND q.deleted_at IS NULL
	`, triggerID)

	out, err := scanTriggerWithQueue(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) || strings.Contains(err.Error(), pgx.ErrNoRows.Error()) {
			return nil, ErrTriggerNotFound
		}
		return nil, fmt.Errorf("get trigger by id: %w", err)
	}
	return out, nil
}

// ListEnabledTriggersWithQueue returns active+enabled triggers with queue info.
func (s *Store) ListEnabledTriggersWithQueue(ctx context.Context) ([]internal.TriggerWithQueue, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT t.id, t.queue_id, t.enabled, t.target_type, t.target_url, t.batch_size, t.batch_window_seconds,
			t.max_concurrency, t.visibility_timeout_override, t.max_concurrency_per_group,
			t.auto_scale, t.min_pollers, t.max_pollers, t.failure_threshold, t.created_at, t.updated_at, t.deleted_at,
			q.name, q.queue_type, q.visibility_timeout
		FROM triggers t
		JOIN queues q ON q.id = t.queue_id
		WHERE t.enabled = true AND t.deleted_at IS NULL AND q.deleted_at IS NULL
		ORDER BY t.created_at ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("list enabled triggers: %w", err)
	}
	defer rows.Close()

	out := make([]internal.TriggerWithQueue, 0)
	for rows.Next() {
		t, scanErr := scanTriggerWithQueueFromRows(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		out = append(out, *t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list enabled triggers rows: %w", err)
	}
	return out, nil
}

// UpdateTrigger updates mutable trigger fields.
func (s *Store) UpdateTrigger(ctx context.Context, t *internal.Trigger) (*internal.Trigger, error) {
	row := s.pool.QueryRow(ctx, `
		UPDATE triggers
		SET enabled = $3,
		    target_type = $4,
		    target_url = $5,
		    batch_size = $6,
		    batch_window_seconds = $7,
		    max_concurrency = $8,
		    visibility_timeout_override = $9,
		    max_concurrency_per_group = $10,
		    auto_scale = $11,
		    min_pollers = $12,
		    max_pollers = $13,
		    failure_threshold = $14,
		    updated_at = now()
		WHERE queue_id = $1 AND id = $2 AND deleted_at IS NULL
		RETURNING id, queue_id, enabled, target_type, target_url, batch_size, batch_window_seconds,
			max_concurrency, visibility_timeout_override, max_concurrency_per_group,
			auto_scale, min_pollers, max_pollers, failure_threshold, created_at, updated_at, deleted_at
	`, t.QueueID, t.ID, t.Enabled, t.TargetType, t.TargetURL, t.BatchSize, t.BatchWindowSeconds,
		t.MaxConcurrency, t.VisibilityTimeoutOverride, t.MaxConcurrencyPerGroup,
		t.AutoScale, t.MinPollers, t.MaxPollers, t.FailureThreshold)

	out, err := scanTrigger(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) || strings.Contains(err.Error(), pgx.ErrNoRows.Error()) {
			return nil, ErrTriggerNotFound
		}
		return nil, fmt.Errorf("update trigger: %w", err)
	}
	return out, nil
}

// DeleteTrigger soft-deletes a trigger.
func (s *Store) DeleteTrigger(ctx context.Context, queueID, triggerID uuid.UUID) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE triggers SET deleted_at = now(), updated_at = now()
		WHERE queue_id = $1 AND id = $2 AND deleted_at IS NULL
	`, queueID, triggerID)
	if err != nil {
		return fmt.Errorf("delete trigger: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrTriggerNotFound
	}
	return nil
}

// SetTriggerEnabled toggles trigger enabled state.
func (s *Store) SetTriggerEnabled(ctx context.Context, queueID, triggerID uuid.UUID, enabled bool) (*internal.Trigger, error) {
	row := s.pool.QueryRow(ctx, `
		UPDATE triggers
		SET enabled = $3, updated_at = now()
		WHERE queue_id = $1 AND id = $2 AND deleted_at IS NULL
		RETURNING id, queue_id, enabled, target_type, target_url, batch_size, batch_window_seconds,
			max_concurrency, visibility_timeout_override, max_concurrency_per_group,
			auto_scale, min_pollers, max_pollers, failure_threshold, created_at, updated_at, deleted_at
	`, queueID, triggerID, enabled)
	out, err := scanTrigger(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) || strings.Contains(err.Error(), pgx.ErrNoRows.Error()) {
			return nil, ErrTriggerNotFound
		}
		return nil, fmt.Errorf("set trigger enabled: %w", err)
	}
	return out, nil
}

// RecordTriggerInvocation updates rolling trigger metrics from one invocation.
func (s *Store) RecordTriggerInvocation(
	ctx context.Context,
	triggerID uuid.UUID,
	invocationDurationMS int64,
	iteratorAgeMS int64,
	messagesProcessed int64,
	messagesFailed int64,
	success bool,
) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO trigger_metrics (
			trigger_id, invocations_total, invocations_success, invocations_failed,
			messages_processed_total, messages_failed_total, average_invocation_duration_ms,
			iterator_age_ms, last_invocation_at, last_success_at, last_failure_at
		)
		VALUES (
			$1,
			1,
			CASE WHEN $6 THEN 1 ELSE 0 END,
			CASE WHEN $6 THEN 0 ELSE 1 END,
			$4,
			$5,
			$2,
			$3,
			now(),
			CASE WHEN $6 THEN now() ELSE NULL END,
			CASE WHEN $6 THEN NULL ELSE now() END
		)
		ON CONFLICT (trigger_id) DO UPDATE
		SET
			invocations_total = trigger_metrics.invocations_total + 1,
			invocations_success = trigger_metrics.invocations_success + CASE WHEN $6 THEN 1 ELSE 0 END,
			invocations_failed = trigger_metrics.invocations_failed + CASE WHEN $6 THEN 0 ELSE 1 END,
			messages_processed_total = trigger_metrics.messages_processed_total + $4,
			messages_failed_total = trigger_metrics.messages_failed_total + $5,
			average_invocation_duration_ms =
				((trigger_metrics.average_invocation_duration_ms * trigger_metrics.invocations_total) + $2) /
				(trigger_metrics.invocations_total + 1),
			iterator_age_ms = $3,
			last_invocation_at = now(),
			last_success_at = CASE WHEN $6 THEN now() ELSE trigger_metrics.last_success_at END,
			last_failure_at = CASE WHEN $6 THEN trigger_metrics.last_failure_at ELSE now() END,
			updated_at = now()
	`, triggerID, invocationDurationMS, iteratorAgeMS, messagesProcessed, messagesFailed, success)
	if err != nil {
		return fmt.Errorf("record trigger invocation: %w", err)
	}
	return nil
}

// GetTriggerMetrics returns current metrics for one trigger.
func (s *Store) GetTriggerMetrics(ctx context.Context, triggerID uuid.UUID) (*internal.TriggerMetrics, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT trigger_id, invocations_total, invocations_success, invocations_failed,
			messages_processed_total, messages_failed_total, average_invocation_duration_ms,
			iterator_age_ms, last_invocation_at, last_success_at, last_failure_at, created_at, updated_at
		FROM trigger_metrics
		WHERE trigger_id = $1
	`, triggerID)

	var m internal.TriggerMetrics
	err := row.Scan(
		&m.TriggerID, &m.InvocationsTotal, &m.InvocationsSuccess, &m.InvocationsFailed,
		&m.MessagesProcessedTotal, &m.MessagesFailedTotal, &m.AverageInvocationDurationMS,
		&m.IteratorAgeMS, &m.LastInvocationAt, &m.LastSuccessAt, &m.LastFailureAt, &m.CreatedAt, &m.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			empty := &internal.TriggerMetrics{TriggerID: triggerID}
			return empty, nil
		}
		return nil, fmt.Errorf("get trigger metrics: %w", err)
	}
	return &m, nil
}

// ---------- Row scanners ----------

// scanMessage scans a single message row from QueryRow.
func scanMessage(row pgx.Row) (*internal.Message, error) {
	var msg internal.Message
	var attrsJSON []byte
	err := row.Scan(
		&msg.ID, &msg.QueueID, &msg.MessageGroupID, &msg.MessageDedupID, &msg.SequenceNumber, &msg.Body, &attrsJSON,
		&msg.ReceiptHandle, &msg.ReceiveCount,
		&msg.VisibleAt, &msg.ExpiresAt, &msg.CreatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("scan message: %w", err)
	}
	if attrsJSON != nil {
		if err := json.Unmarshal(attrsJSON, &msg.Attributes); err != nil {
			return nil, fmt.Errorf("unmarshal attributes: %w", err)
		}
	}
	return &msg, nil
}

// scanMessageFromRows scans a single message from an open Rows cursor.
func scanMessageFromRows(rows pgx.Rows) (*internal.Message, error) {
	var msg internal.Message
	var attrsJSON []byte
	err := rows.Scan(
		&msg.ID, &msg.QueueID, &msg.MessageGroupID, &msg.MessageDedupID, &msg.SequenceNumber, &msg.Body, &attrsJSON,
		&msg.ReceiptHandle, &msg.ReceiveCount,
		&msg.VisibleAt, &msg.ExpiresAt, &msg.CreatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("scan message row: %w", err)
	}
	if attrsJSON != nil {
		if err := json.Unmarshal(attrsJSON, &msg.Attributes); err != nil {
			return nil, fmt.Errorf("unmarshal attributes: %w", err)
		}
	}
	return &msg, nil
}

func scanTrigger(row pgx.Row) (*internal.Trigger, error) {
	var t internal.Trigger
	err := row.Scan(
		&t.ID, &t.QueueID, &t.Enabled, &t.TargetType, &t.TargetURL, &t.BatchSize, &t.BatchWindowSeconds,
		&t.MaxConcurrency, &t.VisibilityTimeoutOverride, &t.MaxConcurrencyPerGroup,
		&t.AutoScale, &t.MinPollers, &t.MaxPollers, &t.FailureThreshold, &t.CreatedAt, &t.UpdatedAt, &t.DeletedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("scan trigger: %w", err)
	}
	return &t, nil
}

func scanTriggerFromRows(rows pgx.Rows) (*internal.Trigger, error) {
	var t internal.Trigger
	err := rows.Scan(
		&t.ID, &t.QueueID, &t.Enabled, &t.TargetType, &t.TargetURL, &t.BatchSize, &t.BatchWindowSeconds,
		&t.MaxConcurrency, &t.VisibilityTimeoutOverride, &t.MaxConcurrencyPerGroup,
		&t.AutoScale, &t.MinPollers, &t.MaxPollers, &t.FailureThreshold, &t.CreatedAt, &t.UpdatedAt, &t.DeletedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("scan trigger row: %w", err)
	}
	return &t, nil
}

func scanTriggerWithQueue(row pgx.Row) (*internal.TriggerWithQueue, error) {
	var t internal.TriggerWithQueue
	err := row.Scan(
		&t.ID, &t.QueueID, &t.Enabled, &t.TargetType, &t.TargetURL, &t.BatchSize, &t.BatchWindowSeconds,
		&t.MaxConcurrency, &t.VisibilityTimeoutOverride, &t.MaxConcurrencyPerGroup,
		&t.AutoScale, &t.MinPollers, &t.MaxPollers, &t.FailureThreshold, &t.CreatedAt, &t.UpdatedAt, &t.DeletedAt,
		&t.QueueName, &t.QueueType, &t.QueueVisibilityTimeout,
	)
	if err != nil {
		return nil, fmt.Errorf("scan trigger with queue: %w", err)
	}
	return &t, nil
}

func scanTriggerWithQueueFromRows(rows pgx.Rows) (*internal.TriggerWithQueue, error) {
	var t internal.TriggerWithQueue
	err := rows.Scan(
		&t.ID, &t.QueueID, &t.Enabled, &t.TargetType, &t.TargetURL, &t.BatchSize, &t.BatchWindowSeconds,
		&t.MaxConcurrency, &t.VisibilityTimeoutOverride, &t.MaxConcurrencyPerGroup,
		&t.AutoScale, &t.MinPollers, &t.MaxPollers, &t.FailureThreshold, &t.CreatedAt, &t.UpdatedAt, &t.DeletedAt,
		&t.QueueName, &t.QueueType, &t.QueueVisibilityTimeout,
	)
	if err != nil {
		return nil, fmt.Errorf("scan trigger with queue row: %w", err)
	}
	return &t, nil
}

func scanFunction(row pgx.Row) (*internal.Function, error) {
	var fn internal.Function
	var envJSON []byte
	err := row.Scan(
		&fn.ID, &fn.Name, &fn.Runtime, &fn.Handler, &fn.TimeoutSeconds, &fn.MemoryMB, &envJSON, &fn.CodePath,
		&fn.ContainerStrategy, &fn.WarmPoolSize, &fn.ImageName, &fn.CreatedAt, &fn.UpdatedAt, &fn.DeletedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("scan function: %w", err)
	}
	fn.Environment = map[string]string{}
	if len(envJSON) > 0 {
		if err := json.Unmarshal(envJSON, &fn.Environment); err != nil {
			return nil, fmt.Errorf("unmarshal function environment: %w", err)
		}
	}
	return &fn, nil
}

func scanFunctionFromRows(rows pgx.Rows) (*internal.Function, error) {
	var fn internal.Function
	var envJSON []byte
	err := rows.Scan(
		&fn.ID, &fn.Name, &fn.Runtime, &fn.Handler, &fn.TimeoutSeconds, &fn.MemoryMB, &envJSON, &fn.CodePath,
		&fn.ContainerStrategy, &fn.WarmPoolSize, &fn.ImageName, &fn.CreatedAt, &fn.UpdatedAt, &fn.DeletedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("scan function row: %w", err)
	}
	fn.Environment = map[string]string{}
	if len(envJSON) > 0 {
		if err := json.Unmarshal(envJSON, &fn.Environment); err != nil {
			return nil, fmt.Errorf("unmarshal function environment: %w", err)
		}
	}
	return &fn, nil
}
