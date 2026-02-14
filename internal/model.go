package internal

import (
	"time"

	"github.com/google/uuid"
)

// Queue represents a message queue and its configuration.
type Queue struct {
	ID                uuid.UUID  `json:"id"`
	Name              string     `json:"name"`
	QueueType         string     `json:"queue_type"`
	ContentBasedDedup bool       `json:"content_based_dedup"`
	VisibilityTimeout int        `json:"visibility_timeout"` // seconds
	MessageRetention  int        `json:"message_retention"`  // seconds
	DelaySeconds      int        `json:"delay_seconds"`
	CreatedAt         time.Time  `json:"created_at"`
	UpdatedAt         time.Time  `json:"updated_at"`
	DeletedAt         *time.Time `json:"deleted_at,omitempty"`
}

// Message represents a single message in a queue.
type Message struct {
	ID             uuid.UUID              `json:"id"`
	QueueID        uuid.UUID              `json:"queue_id"`
	MessageGroupID *string                `json:"message_group_id,omitempty"`
	MessageDedupID *string                `json:"message_dedup_id,omitempty"`
	SequenceNumber *int64                 `json:"sequence_number,omitempty"`
	Body           string                 `json:"body"`
	Attributes     map[string]interface{} `json:"attributes,omitempty"`
	ReceiptHandle  *uuid.UUID             `json:"receipt_handle,omitempty"`
	ReceiveCount   int                    `json:"receive_count"`
	VisibleAt      time.Time              `json:"visible_at"`
	ExpiresAt      time.Time              `json:"expires_at"`
	CreatedAt      time.Time              `json:"created_at"`
	DeletedAt      *time.Time             `json:"deleted_at,omitempty"`
}

// QueueStats holds the counts for queue observability.
type QueueStats struct {
	Available int `json:"available"`
	InFlight  int `json:"in_flight"`
	Delayed   int `json:"delayed"`
}

// BatchResultEntry holds the outcome of a single entry in a batch operation.
type BatchResultEntry struct {
	Index   int        `json:"index"`
	ID      *uuid.UUID `json:"id,omitempty"`    // set on success for send batch
	Error   string     `json:"error,omitempty"` // set on failure
	Success bool       `json:"success"`
}

const (
	TriggerTargetTypeWebhook = "webhook"
	TriggerTargetTypeGRPC    = "grpc"
	TriggerTargetTypeLocal   = "local"
	TriggerTargetTypeLambda  = "lambda"
)

// Trigger defines lambda-style queue trigger configuration.
type Trigger struct {
	ID                        uuid.UUID  `json:"id"`
	QueueID                   uuid.UUID  `json:"queue_id"`
	Enabled                   bool       `json:"enabled"`
	TargetType                string     `json:"target_type"`
	TargetURL                 string     `json:"target_url"`
	BatchSize                 int        `json:"batch_size"`
	BatchWindowSeconds        int        `json:"batch_window_seconds"`
	MaxConcurrency            int        `json:"max_concurrency"`
	VisibilityTimeoutOverride *int       `json:"visibility_timeout_override,omitempty"`
	MaxConcurrencyPerGroup    int        `json:"max_concurrency_per_group"`
	AutoScale                 bool       `json:"auto_scale"`
	MinPollers                int        `json:"min_pollers"`
	MaxPollers                int        `json:"max_pollers"`
	FailureThreshold          int        `json:"failure_threshold"`
	CreatedAt                 time.Time  `json:"created_at"`
	UpdatedAt                 time.Time  `json:"updated_at"`
	DeletedAt                 *time.Time `json:"deleted_at,omitempty"`
}

// TriggerWithQueue includes trigger config and queue metadata used by pollers.
type TriggerWithQueue struct {
	Trigger
	QueueName              string `json:"queue_name"`
	QueueType              string `json:"queue_type"`
	QueueVisibilityTimeout int    `json:"queue_visibility_timeout"`
}

// TriggerRecord is one SQS-like record delivered to trigger targets.
type TriggerRecord struct {
	MessageID               uuid.UUID              `json:"message_id"`
	ReceiptHandle           uuid.UUID              `json:"receipt_handle"`
	Body                    string                 `json:"body"`
	Attributes              map[string]interface{} `json:"attributes"`
	MessageGroupID          *string                `json:"message_group_id,omitempty"`
	ApproximateReceiveCount int                    `json:"approximate_receive_count"`
}

// TriggerPayload mirrors Lambda SQS event payload shape.
type TriggerPayload struct {
	TriggerID uuid.UUID       `json:"trigger_id"`
	QueueID   uuid.UUID       `json:"queue_id"`
	QueueName string          `json:"queue_name"`
	QueueType string          `json:"queue_type"`
	Target    string          `json:"target"`
	Records   []TriggerRecord `json:"records"`
}

// BatchItemFailure identifies a failed message for partial batch response.
type BatchItemFailure struct {
	ItemIdentifier uuid.UUID `json:"item_identifier"`
}

// TriggerInvocationResponse supports partial batch failure semantics.
type TriggerInvocationResponse struct {
	BatchItemFailures []BatchItemFailure `json:"batch_item_failures,omitempty"`
}

// TriggerMetrics captures per-trigger invocation and processing stats.
type TriggerMetrics struct {
	TriggerID                   uuid.UUID  `json:"trigger_id"`
	InvocationsTotal            int64      `json:"invocations_total"`
	InvocationsSuccess          int64      `json:"invocations_success"`
	InvocationsFailed           int64      `json:"invocations_failed"`
	MessagesProcessedTotal      int64      `json:"messages_processed_total"`
	MessagesFailedTotal         int64      `json:"messages_failed_total"`
	AverageInvocationDurationMS int64      `json:"average_invocation_duration_ms"`
	IteratorAgeMS               int64      `json:"iterator_age_ms"`
	LastInvocationAt            *time.Time `json:"last_invocation_at,omitempty"`
	LastSuccessAt               *time.Time `json:"last_success_at,omitempty"`
	LastFailureAt               *time.Time `json:"last_failure_at,omitempty"`
	CreatedAt                   time.Time  `json:"created_at"`
	UpdatedAt                   time.Time  `json:"updated_at"`
}

// Function represents a lambda-like function configuration.
type Function struct {
	ID                uuid.UUID         `json:"id"`
	Name              string            `json:"name"`
	Runtime           string            `json:"runtime"`
	Handler           string            `json:"handler"`
	TimeoutSeconds    int               `json:"timeout_seconds"`
	MemoryMB          int               `json:"memory_mb"`
	Environment       map[string]string `json:"environment"`
	CodePath          string            `json:"code_path"`
	ContainerStrategy string            `json:"container_strategy"`
	WarmPoolSize      int               `json:"warm_pool_size"`
	ImageName         string            `json:"image_name"`
	CreatedAt         time.Time         `json:"created_at"`
	UpdatedAt         time.Time         `json:"updated_at"`
	DeletedAt         *time.Time        `json:"deleted_at,omitempty"`
}
