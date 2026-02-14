package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/Ansh1693/evtq/internal"
	lambdaruntime "github.com/Ansh1693/evtq/internal/lambda"
	"github.com/Ansh1693/evtq/internal/store"
)

const (
	maxBodySize          = 256 * 1024 // 256 KB
	maxDelaySeconds      = 900        // 15 minutes
	maxVisibilityTimeout = 43200      // 12 hours
	maxReceiveMessages   = 10
	maxWaitTimeSeconds   = 20
	maxBatchSize         = 10
	defaultRetention     = 345600 // 4 days in seconds
	defaultVisibility    = 30     // seconds
	maxBatchWindow       = 300
	defaultBatchSize     = 10
	defaultConcurrency   = 1
	defaultFailureThresh = 5

	queueTypeStandard = "STANDARD"
	queueTypeFIFO     = "FIFO"
)

var (
	ErrBodyTooLarge                  = errors.New("message body exceeds 256KB limit")
	ErrInvalidDelay                  = errors.New("delay_seconds must be between 0 and 900")
	ErrInvalidVisibility             = errors.New("visibility_timeout must be between 0 and 43200")
	ErrInvalidMaxMessages            = errors.New("max_messages must be between 1 and 10")
	ErrInvalidWaitTime               = errors.New("wait_time_seconds must be between 0 and 20")
	ErrInvalidRetention              = errors.New("message_retention must be between 60 and 1209600")
	ErrQueueNameRequired             = errors.New("queue name is required")
	ErrMessageBodyRequired           = errors.New("message body is required")
	ErrBatchTooLarge                 = fmt.Errorf("batch size exceeds maximum of %d", maxBatchSize)
	ErrBatchEmpty                    = errors.New("batch must contain at least 1 entry")
	ErrGroupIDRequired               = errors.New("message_group_id is required for FIFO queues")
	ErrDedupIDRequired               = errors.New("message_dedup_id is required unless content-based deduplication is enabled")
	ErrQueueNameNotFIFO              = errors.New("FIFO queue names must end with .fifo")
	ErrInvalidQueueType              = errors.New("queue_type must be STANDARD or FIFO")
	ErrContentBasedDedupRequiresFIFO = errors.New("content_based_dedup is only valid for FIFO queues")
	ErrInvalidTriggerTargetType      = errors.New("target_type must be webhook, grpc, local, or lambda")
	ErrInvalidTargetURL              = errors.New("target_url is required")
	ErrInvalidTriggerBatchSize       = errors.New("batch_size must be between 1 and 10")
	ErrInvalidTriggerBatchWindow     = errors.New("batch_window_seconds must be between 0 and 300")
	ErrInvalidTriggerConcurrency     = errors.New("max_concurrency must be at least 1")
	ErrInvalidVisibilityOverride     = errors.New("visibility_timeout_override must be between 0 and 43200")
	ErrInvalidFIFOGroupConcurrency   = errors.New("max_concurrency_per_group must be 1 for FIFO queues")
	ErrInvalidPollerRange            = errors.New("min_pollers and max_pollers must be at least 1 and min_pollers <= max_pollers")
	ErrInvalidFailureThreshold       = errors.New("failure_threshold must be at least 1")
	ErrInvalidFunctionName           = errors.New("function name is required")
	ErrInvalidFunctionRuntime        = errors.New("runtime must be nodejs22 or go122")
	ErrInvalidFunctionHandler        = errors.New("handler is required")
	ErrInvalidFunctionTimeout        = errors.New("timeout_seconds must be between 1 and 900")
	ErrInvalidFunctionMemory         = errors.New("memory_mb must be at least 64")
	ErrInvalidFunctionCodePath       = errors.New("code_path must be an absolute path")
	ErrInvalidFunctionStrategy       = errors.New("container_strategy must be cold or warm")
	ErrInvalidWarmPoolSize           = errors.New("warm_pool_size must be at least 1")
)

// Service implements business logic on top of the Store.
type Service struct {
	store        *store.Store
	lambdaRunner lambdaruntime.Invoker
}

// New creates a new Service.
func New(s *store.Store, lambdaRunner lambdaruntime.Invoker) *Service {
	return &Service{store: s, lambdaRunner: lambdaRunner}
}

// ---------- Queue operations ----------

type CreateQueueInput struct {
	Name              string
	QueueType         *string
	ContentBasedDedup *bool
	VisibilityTimeout *int
	MessageRetention  *int
	DelaySeconds      *int
}

func (s *Service) CreateQueue(ctx context.Context, in CreateQueueInput) (*internal.Queue, error) {
	if in.Name == "" {
		return nil, ErrQueueNameRequired
	}

	q := &internal.Queue{
		Name:              in.Name,
		QueueType:         queueTypeStandard,
		ContentBasedDedup: false,
		VisibilityTimeout: defaultVisibility,
		MessageRetention:  defaultRetention,
		DelaySeconds:      0,
	}

	if in.QueueType != nil && *in.QueueType != "" {
		queueType := strings.ToUpper(*in.QueueType)
		if queueType != queueTypeStandard && queueType != queueTypeFIFO {
			return nil, ErrInvalidQueueType
		}
		q.QueueType = queueType
	}

	if in.ContentBasedDedup != nil {
		q.ContentBasedDedup = *in.ContentBasedDedup
	}

	if q.QueueType == queueTypeFIFO && !strings.HasSuffix(in.Name, ".fifo") {
		return nil, ErrQueueNameNotFIFO
	}
	if q.QueueType != queueTypeFIFO && q.ContentBasedDedup {
		return nil, ErrContentBasedDedupRequiresFIFO
	}

	if in.VisibilityTimeout != nil {
		if *in.VisibilityTimeout < 0 || *in.VisibilityTimeout > maxVisibilityTimeout {
			return nil, ErrInvalidVisibility
		}
		q.VisibilityTimeout = *in.VisibilityTimeout
	}
	if in.MessageRetention != nil {
		if *in.MessageRetention < 60 || *in.MessageRetention > 1209600 {
			return nil, ErrInvalidRetention
		}
		q.MessageRetention = *in.MessageRetention
	}
	if in.DelaySeconds != nil {
		if *in.DelaySeconds < 0 || *in.DelaySeconds > maxDelaySeconds {
			return nil, ErrInvalidDelay
		}
		q.DelaySeconds = *in.DelaySeconds
	}

	return s.store.CreateQueue(ctx, q)
}

func (s *Service) GetQueue(ctx context.Context, name string) (*internal.Queue, error) {
	return s.store.GetQueueByName(ctx, name)
}

func (s *Service) DeleteQueue(ctx context.Context, name string) error {
	return s.store.DeleteQueue(ctx, name)
}

// ---------- Single message operations ----------

type SendMessageInput struct {
	QueueName      string
	Body           string
	Attributes     map[string]interface{}
	DelaySeconds   *int
	MessageGroupID *string
	MessageDedupID *string
}

func (s *Service) SendMessage(ctx context.Context, in SendMessageInput) (*internal.Message, error) {
	if in.Body == "" {
		return nil, ErrMessageBodyRequired
	}
	if len(in.Body) > maxBodySize {
		return nil, ErrBodyTooLarge
	}

	perMessageDelay := 0
	if in.DelaySeconds != nil {
		if *in.DelaySeconds < 0 || *in.DelaySeconds > maxDelaySeconds {
			return nil, ErrInvalidDelay
		}
		perMessageDelay = *in.DelaySeconds
	}

	queue, err := s.store.GetQueueByName(ctx, in.QueueName)
	if err != nil {
		return nil, fmt.Errorf("send message: %w", err)
	}

	now := time.Now()
	totalDelay := time.Duration(queue.DelaySeconds+perMessageDelay) * time.Second
	msg := &internal.Message{
		QueueID:    queue.ID,
		Body:       in.Body,
		Attributes: in.Attributes,
		VisibleAt:  now.Add(totalDelay),
		ExpiresAt:  now.Add(time.Duration(queue.MessageRetention) * time.Second),
	}
	if msg.Attributes == nil {
		msg.Attributes = map[string]interface{}{}
	}

	if queue.QueueType == queueTypeFIFO {
		groupID, dedupID, err := resolveFIFOFields(queue, in.Body, in.MessageGroupID, in.MessageDedupID)
		if err != nil {
			return nil, err
		}
		msg.MessageGroupID = &groupID
		msg.MessageDedupID = &dedupID
		return s.store.SendMessageFIFO(ctx, msg)
	}

	return s.store.SendMessage(ctx, msg)
}

type ReceiveMessagesInput struct {
	QueueName         string
	MaxMessages       int
	WaitTimeSeconds   int
	VisibilityTimeout *int
}

func (s *Service) ReceiveMessages(ctx context.Context, in ReceiveMessagesInput) ([]internal.Message, error) {
	if in.MaxMessages < 1 || in.MaxMessages > maxReceiveMessages {
		return nil, ErrInvalidMaxMessages
	}
	if in.WaitTimeSeconds < 0 || in.WaitTimeSeconds > maxWaitTimeSeconds {
		return nil, ErrInvalidWaitTime
	}
	if in.VisibilityTimeout != nil {
		if *in.VisibilityTimeout < 0 || *in.VisibilityTimeout > maxVisibilityTimeout {
			return nil, ErrInvalidVisibility
		}
	}

	queue, err := s.store.GetQueueByName(ctx, in.QueueName)
	if err != nil {
		return nil, fmt.Errorf("receive messages: %w", err)
	}

	visTimeout := queue.VisibilityTimeout
	if in.VisibilityTimeout != nil {
		visTimeout = *in.VisibilityTimeout
	}

	receive := s.store.ReceiveMessages
	if queue.QueueType == queueTypeFIFO {
		receive = s.store.ReceiveMessagesFIFO
	}

	if in.WaitTimeSeconds == 0 {
		return receive(ctx, queue.ID, visTimeout, in.MaxMessages)
	}

	deadline := time.Now().Add(time.Duration(in.WaitTimeSeconds) * time.Second)
	pollCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	msgs, err := receive(pollCtx, queue.ID, visTimeout, in.MaxMessages)
	if err != nil {
		return nil, err
	}
	if len(msgs) > 0 {
		return msgs, nil
	}

	for {
		err := s.store.WaitForNotification(pollCtx, queue.ID)
		if err != nil {
			if pollCtx.Err() != nil {
				return []internal.Message{}, nil
			}
			return nil, fmt.Errorf("wait for notification: %w", err)
		}

		msgs, err := receive(pollCtx, queue.ID, visTimeout, in.MaxMessages)
		if err != nil {
			return nil, err
		}
		if len(msgs) > 0 {
			return msgs, nil
		}
	}
}

func (s *Service) DeleteMessage(ctx context.Context, queueName string, receiptHandle uuid.UUID) error {
	queue, err := s.store.GetQueueByName(ctx, queueName)
	if err != nil {
		return fmt.Errorf("delete message: %w", err)
	}
	return s.store.DeleteMessage(ctx, queue.ID, receiptHandle)
}

func (s *Service) ChangeMessageVisibility(ctx context.Context, queueName string, receiptHandle uuid.UUID, newTimeout int) error {
	if newTimeout < 0 || newTimeout > maxVisibilityTimeout {
		return ErrInvalidVisibility
	}

	queue, err := s.store.GetQueueByName(ctx, queueName)
	if err != nil {
		return fmt.Errorf("change visibility: %w", err)
	}
	return s.store.ChangeMessageVisibility(ctx, queue.ID, receiptHandle, newTimeout)
}

func (s *Service) PurgeQueue(ctx context.Context, queueName string) (int64, error) {
	queue, err := s.store.GetQueueByName(ctx, queueName)
	if err != nil {
		return 0, fmt.Errorf("purge queue: %w", err)
	}
	return s.store.PurgeQueue(ctx, queue.ID)
}

func (s *Service) GetQueueStats(ctx context.Context, queueName string) (*internal.Queue, *internal.QueueStats, error) {
	queue, err := s.store.GetQueueByName(ctx, queueName)
	if err != nil {
		return nil, nil, fmt.Errorf("get stats: %w", err)
	}
	stats, err := s.store.GetQueueStats(ctx, queue.ID)
	if err != nil {
		return nil, nil, err
	}
	return queue, stats, nil
}

// ---------- Batch operations ----------

type SendMessageBatchEntry struct {
	Body           string
	Attributes     map[string]interface{}
	DelaySeconds   *int
	MessageGroupID *string
	MessageDedupID *string
}

func (s *Service) SendMessageBatch(ctx context.Context, queueName string, entries []SendMessageBatchEntry) []internal.BatchResultEntry {
	results := make([]internal.BatchResultEntry, len(entries))

	if len(entries) == 0 {
		return []internal.BatchResultEntry{{Index: 0, Error: ErrBatchEmpty.Error()}}
	}
	if len(entries) > maxBatchSize {
		return []internal.BatchResultEntry{{Index: 0, Error: ErrBatchTooLarge.Error()}}
	}

	queue, err := s.store.GetQueueByName(ctx, queueName)
	if err != nil {
		for i := range entries {
			results[i] = internal.BatchResultEntry{Index: i, Error: fmt.Sprintf("queue lookup: %v", err)}
		}
		return results
	}

	now := time.Now()
	msgs := make([]*internal.Message, len(entries))
	validIndexes := make([]int, 0, len(entries))

	for i, e := range entries {
		results[i].Index = i

		if e.Body == "" {
			results[i].Error = ErrMessageBodyRequired.Error()
			continue
		}
		if len(e.Body) > maxBodySize {
			results[i].Error = ErrBodyTooLarge.Error()
			continue
		}

		perMessageDelay := 0
		if e.DelaySeconds != nil {
			if *e.DelaySeconds < 0 || *e.DelaySeconds > maxDelaySeconds {
				results[i].Error = ErrInvalidDelay.Error()
				continue
			}
			perMessageDelay = *e.DelaySeconds
		}

		totalDelay := time.Duration(queue.DelaySeconds+perMessageDelay) * time.Second
		attrs := e.Attributes
		if attrs == nil {
			attrs = map[string]interface{}{}
		}

		msg := &internal.Message{
			QueueID:    queue.ID,
			Body:       e.Body,
			Attributes: attrs,
			VisibleAt:  now.Add(totalDelay),
			ExpiresAt:  now.Add(time.Duration(queue.MessageRetention) * time.Second),
		}

		if queue.QueueType == queueTypeFIFO {
			groupID, dedupID, ferr := resolveFIFOFields(queue, e.Body, e.MessageGroupID, e.MessageDedupID)
			if ferr != nil {
				results[i].Error = ferr.Error()
				continue
			}
			msg.MessageGroupID = &groupID
			msg.MessageDedupID = &dedupID
		}

		msgs[i] = msg
		validIndexes = append(validIndexes, i)
	}

	if len(validIndexes) == 0 {
		return results
	}

	validMsgs := make([]*internal.Message, len(validIndexes))
	for j, idx := range validIndexes {
		validMsgs[j] = msgs[idx]
	}

	inserted, storeErrs := s.store.SendMessageBatch(ctx, validMsgs)
	for j, idx := range validIndexes {
		if storeErr, ok := storeErrs[j]; ok {
			results[idx].Error = storeErr.Error()
		} else if inserted[j] != nil {
			id := inserted[j].ID
			results[idx].ID = &id
			results[idx].Success = true
		}
	}

	return results
}

type DeleteMessageBatchEntry struct {
	ReceiptHandle uuid.UUID
}

func (s *Service) DeleteMessageBatch(ctx context.Context, queueName string, entries []DeleteMessageBatchEntry) []internal.BatchResultEntry {
	results := make([]internal.BatchResultEntry, len(entries))

	if len(entries) == 0 {
		return []internal.BatchResultEntry{{Index: 0, Error: ErrBatchEmpty.Error()}}
	}
	if len(entries) > maxBatchSize {
		return []internal.BatchResultEntry{{Index: 0, Error: ErrBatchTooLarge.Error()}}
	}

	queue, err := s.store.GetQueueByName(ctx, queueName)
	if err != nil {
		for i := range entries {
			results[i] = internal.BatchResultEntry{Index: i, Error: fmt.Sprintf("queue lookup: %v", err)}
		}
		return results
	}

	handles := make([]uuid.UUID, len(entries))
	for i, e := range entries {
		results[i].Index = i
		handles[i] = e.ReceiptHandle
	}

	storeErrs := s.store.DeleteMessageBatch(ctx, queue.ID, handles)
	for i := range entries {
		if storeErr, ok := storeErrs[i]; ok {
			results[i].Error = storeErr.Error()
		} else {
			results[i].Success = true
		}
	}

	return results
}

func resolveFIFOFields(queue *internal.Queue, body string, groupID, dedupID *string) (string, string, error) {
	if groupID == nil || strings.TrimSpace(*groupID) == "" {
		return "", "", ErrGroupIDRequired
	}
	resolvedGroup := strings.TrimSpace(*groupID)

	if dedupID != nil && strings.TrimSpace(*dedupID) != "" {
		return resolvedGroup, strings.TrimSpace(*dedupID), nil
	}

	if queue.ContentBasedDedup {
		hash := sha256.Sum256([]byte(body))
		return resolvedGroup, hex.EncodeToString(hash[:]), nil
	}

	return "", "", ErrDedupIDRequired
}

// ---------- Trigger operations ----------

type CreateTriggerInput struct {
	TargetType                string
	TargetURL                 string
	Enabled                   *bool
	BatchSize                 *int
	BatchWindowSeconds        *int
	MaxConcurrency            *int
	VisibilityTimeoutOverride *int
	MaxConcurrencyPerGroup    *int
	AutoScale                 *bool
	MinPollers                *int
	MaxPollers                *int
	FailureThreshold          *int
}

type UpdateTriggerInput struct {
	TargetType                *string
	TargetURL                 *string
	Enabled                   *bool
	BatchSize                 *int
	BatchWindowSeconds        *int
	MaxConcurrency            *int
	VisibilityTimeoutOverride **int
	MaxConcurrencyPerGroup    *int
	AutoScale                 *bool
	MinPollers                *int
	MaxPollers                *int
	FailureThreshold          *int
}

func (s *Service) CreateTrigger(ctx context.Context, queueName string, in CreateTriggerInput) (*internal.Trigger, error) {
	queue, err := s.store.GetQueueByName(ctx, queueName)
	if err != nil {
		return nil, fmt.Errorf("create trigger queue lookup: %w", err)
	}

	t := &internal.Trigger{
		QueueID:                queue.ID,
		Enabled:                true,
		TargetType:             in.TargetType,
		TargetURL:              strings.TrimSpace(in.TargetURL),
		BatchSize:              defaultBatchSize,
		BatchWindowSeconds:     0,
		MaxConcurrency:         defaultConcurrency,
		MaxConcurrencyPerGroup: 1,
		AutoScale:              false,
		MinPollers:             1,
		MaxPollers:             1,
		FailureThreshold:       defaultFailureThresh,
	}
	if in.Enabled != nil {
		t.Enabled = *in.Enabled
	}
	if in.BatchSize != nil {
		t.BatchSize = *in.BatchSize
	}
	if in.BatchWindowSeconds != nil {
		t.BatchWindowSeconds = *in.BatchWindowSeconds
	}
	if in.MaxConcurrency != nil {
		t.MaxConcurrency = *in.MaxConcurrency
	}
	if in.VisibilityTimeoutOverride != nil {
		override := *in.VisibilityTimeoutOverride
		t.VisibilityTimeoutOverride = &override
	}
	if in.MaxConcurrencyPerGroup != nil {
		t.MaxConcurrencyPerGroup = *in.MaxConcurrencyPerGroup
	}
	if in.AutoScale != nil {
		t.AutoScale = *in.AutoScale
	}
	if in.MinPollers != nil {
		t.MinPollers = *in.MinPollers
	}
	if in.MaxPollers != nil {
		t.MaxPollers = *in.MaxPollers
	}
	if in.FailureThreshold != nil {
		t.FailureThreshold = *in.FailureThreshold
	}

	if err := validateTrigger(queue, t); err != nil {
		return nil, err
	}
	return s.store.CreateTrigger(ctx, t)
}

func (s *Service) ListTriggers(ctx context.Context, queueName string) ([]internal.Trigger, error) {
	queue, err := s.store.GetQueueByName(ctx, queueName)
	if err != nil {
		return nil, fmt.Errorf("list triggers queue lookup: %w", err)
	}
	return s.store.ListTriggersByQueue(ctx, queue.ID)
}

func (s *Service) GetTrigger(ctx context.Context, queueName string, triggerID uuid.UUID) (*internal.Trigger, error) {
	queue, err := s.store.GetQueueByName(ctx, queueName)
	if err != nil {
		return nil, fmt.Errorf("get trigger queue lookup: %w", err)
	}
	return s.store.GetTriggerByIDAndQueue(ctx, queue.ID, triggerID)
}

func (s *Service) UpdateTrigger(ctx context.Context, queueName string, triggerID uuid.UUID, in UpdateTriggerInput) (*internal.Trigger, error) {
	queue, err := s.store.GetQueueByName(ctx, queueName)
	if err != nil {
		return nil, fmt.Errorf("update trigger queue lookup: %w", err)
	}

	current, err := s.store.GetTriggerByIDAndQueue(ctx, queue.ID, triggerID)
	if err != nil {
		return nil, err
	}
	next := *current

	if in.TargetType != nil {
		next.TargetType = *in.TargetType
	}
	if in.TargetURL != nil {
		next.TargetURL = strings.TrimSpace(*in.TargetURL)
	}
	if in.Enabled != nil {
		next.Enabled = *in.Enabled
	}
	if in.BatchSize != nil {
		next.BatchSize = *in.BatchSize
	}
	if in.BatchWindowSeconds != nil {
		next.BatchWindowSeconds = *in.BatchWindowSeconds
	}
	if in.MaxConcurrency != nil {
		next.MaxConcurrency = *in.MaxConcurrency
	}
	if in.VisibilityTimeoutOverride != nil {
		next.VisibilityTimeoutOverride = *in.VisibilityTimeoutOverride
	}
	if in.MaxConcurrencyPerGroup != nil {
		next.MaxConcurrencyPerGroup = *in.MaxConcurrencyPerGroup
	}
	if in.AutoScale != nil {
		next.AutoScale = *in.AutoScale
	}
	if in.MinPollers != nil {
		next.MinPollers = *in.MinPollers
	}
	if in.MaxPollers != nil {
		next.MaxPollers = *in.MaxPollers
	}
	if in.FailureThreshold != nil {
		next.FailureThreshold = *in.FailureThreshold
	}

	if err := validateTrigger(queue, &next); err != nil {
		return nil, err
	}
	return s.store.UpdateTrigger(ctx, &next)
}

func (s *Service) DeleteTrigger(ctx context.Context, queueName string, triggerID uuid.UUID) error {
	queue, err := s.store.GetQueueByName(ctx, queueName)
	if err != nil {
		return fmt.Errorf("delete trigger queue lookup: %w", err)
	}
	return s.store.DeleteTrigger(ctx, queue.ID, triggerID)
}

func (s *Service) SetTriggerEnabled(ctx context.Context, queueName string, triggerID uuid.UUID, enabled bool) (*internal.Trigger, error) {
	queue, err := s.store.GetQueueByName(ctx, queueName)
	if err != nil {
		return nil, fmt.Errorf("set trigger enabled queue lookup: %w", err)
	}
	return s.store.SetTriggerEnabled(ctx, queue.ID, triggerID, enabled)
}

func (s *Service) GetTriggerMetrics(ctx context.Context, queueName string, triggerID uuid.UUID) (*internal.TriggerMetrics, error) {
	queue, err := s.store.GetQueueByName(ctx, queueName)
	if err != nil {
		return nil, fmt.Errorf("get trigger metrics queue lookup: %w", err)
	}
	if _, err := s.store.GetTriggerByIDAndQueue(ctx, queue.ID, triggerID); err != nil {
		return nil, err
	}
	return s.store.GetTriggerMetrics(ctx, triggerID)
}

// ---------- Function operations ----------

type CreateFunctionInput struct {
	Name              string
	Runtime           string
	Handler           string
	TimeoutSeconds    *int
	MemoryMB          *int
	Environment       map[string]string
	CodePath          string
	ContainerStrategy *string
	WarmPoolSize      *int
}

type UpdateFunctionInput struct {
	Runtime           *string
	Handler           *string
	TimeoutSeconds    *int
	MemoryMB          *int
	Environment       map[string]string
	CodePath          *string
	ContainerStrategy *string
	WarmPoolSize      *int
}

func (s *Service) CreateFunction(ctx context.Context, in CreateFunctionInput) (*internal.Function, error) {
	fn := internal.Function{
		Name:              strings.TrimSpace(in.Name),
		Runtime:           strings.TrimSpace(in.Runtime),
		Handler:           strings.TrimSpace(in.Handler),
		TimeoutSeconds:    30,
		MemoryMB:          128,
		Environment:       map[string]string{},
		CodePath:          strings.TrimSpace(in.CodePath),
		ContainerStrategy: "warm",
		WarmPoolSize:      1,
	}
	if in.TimeoutSeconds != nil {
		fn.TimeoutSeconds = *in.TimeoutSeconds
	}
	if in.MemoryMB != nil {
		fn.MemoryMB = *in.MemoryMB
	}
	if in.Environment != nil {
		fn.Environment = in.Environment
	}
	if in.ContainerStrategy != nil {
		fn.ContainerStrategy = strings.TrimSpace(*in.ContainerStrategy)
	}
	if in.WarmPoolSize != nil {
		fn.WarmPoolSize = *in.WarmPoolSize
	}

	if err := validateFunction(&fn); err != nil {
		return nil, err
	}
	if s.lambdaRunner == nil {
		return nil, errors.New("lambda runtime is not configured")
	}
	image, err := s.lambdaRunner.BuildImage(ctx, fn)
	if err != nil {
		return nil, err
	}
	fn.ImageName = image
	return s.store.CreateFunction(ctx, &fn)
}

func (s *Service) ListFunctions(ctx context.Context) ([]internal.Function, error) {
	return s.store.ListFunctions(ctx)
}

func (s *Service) GetFunction(ctx context.Context, name string) (*internal.Function, error) {
	return s.store.GetFunctionByName(ctx, name)
}

func (s *Service) UpdateFunction(ctx context.Context, name string, in UpdateFunctionInput) (*internal.Function, error) {
	current, err := s.store.GetFunctionByName(ctx, name)
	if err != nil {
		return nil, err
	}
	next := *current
	if in.Runtime != nil {
		next.Runtime = strings.TrimSpace(*in.Runtime)
	}
	if in.Handler != nil {
		next.Handler = strings.TrimSpace(*in.Handler)
	}
	if in.TimeoutSeconds != nil {
		next.TimeoutSeconds = *in.TimeoutSeconds
	}
	if in.MemoryMB != nil {
		next.MemoryMB = *in.MemoryMB
	}
	if in.Environment != nil {
		next.Environment = in.Environment
	}
	if in.CodePath != nil {
		next.CodePath = strings.TrimSpace(*in.CodePath)
	}
	if in.ContainerStrategy != nil {
		next.ContainerStrategy = strings.TrimSpace(*in.ContainerStrategy)
	}
	if in.WarmPoolSize != nil {
		next.WarmPoolSize = *in.WarmPoolSize
	}

	if err := validateFunction(&next); err != nil {
		return nil, err
	}
	if s.lambdaRunner == nil {
		return nil, errors.New("lambda runtime is not configured")
	}
	image, err := s.lambdaRunner.BuildImage(ctx, next)
	if err != nil {
		return nil, err
	}
	next.ImageName = image
	return s.store.UpdateFunction(ctx, &next)
}

func (s *Service) DeleteFunction(ctx context.Context, name string) error {
	return s.store.DeleteFunction(ctx, name)
}

func (s *Service) InvokeFunction(ctx context.Context, name string, payload internal.TriggerPayload) (*internal.TriggerInvocationResponse, error) {
	if s.lambdaRunner == nil {
		return nil, errors.New("lambda runtime is not configured")
	}
	fn, err := s.store.GetFunctionByName(ctx, name)
	if err != nil {
		return nil, err
	}
	return s.lambdaRunner.Invoke(ctx, *fn, payload)
}

func validateTrigger(queue *internal.Queue, t *internal.Trigger) error {
	targetType := strings.ToLower(strings.TrimSpace(t.TargetType))
	if targetType != internal.TriggerTargetTypeWebhook &&
		targetType != internal.TriggerTargetTypeGRPC &&
		targetType != internal.TriggerTargetTypeLocal &&
		targetType != internal.TriggerTargetTypeLambda {
		return ErrInvalidTriggerTargetType
	}
	t.TargetType = targetType

	if strings.TrimSpace(t.TargetURL) == "" {
		return ErrInvalidTargetURL
	}
	if t.BatchSize < 1 || t.BatchSize > maxBatchSize {
		return ErrInvalidTriggerBatchSize
	}
	if t.BatchWindowSeconds < 0 || t.BatchWindowSeconds > maxBatchWindow {
		return ErrInvalidTriggerBatchWindow
	}
	if t.MaxConcurrency < 1 {
		return ErrInvalidTriggerConcurrency
	}
	if t.VisibilityTimeoutOverride != nil {
		if *t.VisibilityTimeoutOverride < 0 || *t.VisibilityTimeoutOverride > maxVisibilityTimeout {
			return ErrInvalidVisibilityOverride
		}
	}
	if t.FailureThreshold < 1 {
		return ErrInvalidFailureThreshold
	}

	if t.AutoScale {
		if t.MinPollers < 1 || t.MaxPollers < 1 || t.MinPollers > t.MaxPollers {
			return ErrInvalidPollerRange
		}
	} else {
		t.MinPollers = 1
		t.MaxPollers = 1
	}

	if queue.QueueType == queueTypeFIFO {
		if t.MaxConcurrencyPerGroup != 1 {
			return ErrInvalidFIFOGroupConcurrency
		}
	} else if t.MaxConcurrencyPerGroup < 1 {
		t.MaxConcurrencyPerGroup = 1
	}

	return nil
}

func validateFunction(fn *internal.Function) error {
	if strings.TrimSpace(fn.Name) == "" {
		return ErrInvalidFunctionName
	}
	rt := strings.ToLower(strings.TrimSpace(fn.Runtime))
	if rt != "nodejs22" && rt != "go122" {
		return ErrInvalidFunctionRuntime
	}
	fn.Runtime = rt
	if strings.TrimSpace(fn.Handler) == "" {
		return ErrInvalidFunctionHandler
	}
	if fn.TimeoutSeconds < 1 || fn.TimeoutSeconds > 900 {
		return ErrInvalidFunctionTimeout
	}
	if fn.MemoryMB < 64 {
		return ErrInvalidFunctionMemory
	}
	if !filepath.IsAbs(fn.CodePath) {
		return ErrInvalidFunctionCodePath
	}
	strategy := strings.ToLower(strings.TrimSpace(fn.ContainerStrategy))
	if strategy == "" {
		strategy = "warm"
	}
	if strategy != "cold" && strategy != "warm" {
		return ErrInvalidFunctionStrategy
	}
	fn.ContainerStrategy = strategy
	if fn.WarmPoolSize < 1 {
		return ErrInvalidWarmPoolSize
	}
	if fn.Environment == nil {
		fn.Environment = map[string]string{}
	}
	return nil
}
