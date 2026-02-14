package trigger

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/Ansh1693/evtq/internal"
	"github.com/Ansh1693/evtq/internal/store"
)

const (
	maxBackoffDelay = 5 * time.Minute
)

type pollerRuntime struct {
	cancel context.CancelFunc
}

type triggerRuntime struct {
	cfg               internal.TriggerWithQueue
	cancel            context.CancelFunc
	pollers           map[int]pollerRuntime
	nextPollerID      int
	consecutiveFailed atomic.Int64
	emptyReceives     atomic.Int64
}

// Manager reconciles trigger config and runs pollers.
type Manager struct {
	store           *store.Store
	logger          *slog.Logger
	refreshInterval time.Duration
	webhookTimeout  time.Duration
	localDispatcher *localDispatcher
	dispatchers     map[string]Dispatcher
	mu              sync.Mutex
	activeByTrigger map[uuid.UUID]*triggerRuntime
}

func NewManager(st *store.Store, logger *slog.Logger, refreshInterval time.Duration) *Manager {
	local := NewLocalDispatcher()
	faasBaseURL := os.Getenv("FAAS_BASE_URL")
	m := &Manager{
		store:           st,
		logger:          logger,
		refreshInterval: refreshInterval,
		webhookTimeout:  30 * time.Second,
		localDispatcher: local,
		dispatchers: map[string]Dispatcher{
			internal.TriggerTargetTypeWebhook: NewWebhookDispatcher(30 * time.Second),
			internal.TriggerTargetTypeGRPC:    NewGRPCDispatcher(),
			internal.TriggerTargetTypeLocal:   local,
			internal.TriggerTargetTypeLambda:  NewLambdaDispatcher(faasBaseURL, 30*time.Second),
		},
		activeByTrigger: make(map[uuid.UUID]*triggerRuntime),
	}
	return m
}

// RegisterLocalHandler registers a local trigger function.
func (m *Manager) RegisterLocalHandler(name string, fn LocalHandler) {
	m.localDispatcher.Register(name, fn)
}

// Run starts long-lived trigger reconciliation until context cancellation.
func (m *Manager) Run(ctx context.Context) {
	m.reconcile(ctx)
	ticker := time.NewTicker(m.refreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			m.stopAll()
			return
		case <-ticker.C:
			m.reconcile(ctx)
		}
	}
}

func (m *Manager) stopAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, rt := range m.activeByTrigger {
		rt.cancel()
		delete(m.activeByTrigger, id)
	}
}

func (m *Manager) reconcile(ctx context.Context) {
	enabled, err := m.store.ListEnabledTriggersWithQueue(ctx)
	if err != nil {
		m.logger.Error("trigger manager reconcile failed", "error", err)
		return
	}

	desired := make(map[uuid.UUID]internal.TriggerWithQueue, len(enabled))
	for _, t := range enabled {
		desired[t.ID] = t
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	for id, cfg := range desired {
		rt, exists := m.activeByTrigger[id]
		if !exists {
			m.activeByTrigger[id] = m.startTriggerLocked(ctx, cfg)
			continue
		}
		if triggerConfigChanged(rt.cfg, cfg) {
			rt.cancel()
			delete(m.activeByTrigger, id)
			m.activeByTrigger[id] = m.startTriggerLocked(ctx, cfg)
			continue
		}
		m.adjustPollersLocked(ctx, rt)
	}

	for id, rt := range m.activeByTrigger {
		if _, ok := desired[id]; !ok {
			rt.cancel()
			delete(m.activeByTrigger, id)
		}
	}
}

func (m *Manager) startTriggerLocked(ctx context.Context, cfg internal.TriggerWithQueue) *triggerRuntime {
	tctx, cancel := context.WithCancel(ctx)
	rt := &triggerRuntime{
		cfg:     cfg,
		cancel:  cancel,
		pollers: make(map[int]pollerRuntime),
	}
	target := cfg.MinPollers
	if !cfg.AutoScale {
		target = 1
	}
	if target < 1 {
		target = 1
	}
	for i := 0; i < target; i++ {
		m.addPollerLocked(tctx, rt)
	}
	m.logger.Info("trigger pollers started", "trigger_id", cfg.ID, "queue", cfg.QueueName, "pollers", target)
	return rt
}

func (m *Manager) addPollerLocked(parent context.Context, rt *triggerRuntime) {
	pctx, cancel := context.WithCancel(parent)
	id := rt.nextPollerID
	rt.nextPollerID++
	rt.pollers[id] = pollerRuntime{cancel: cancel}
	go m.pollerLoop(pctx, rt, id)
}

func (m *Manager) removePollerLocked(rt *triggerRuntime) {
	if len(rt.pollers) == 0 {
		return
	}
	for id, pr := range rt.pollers {
		pr.cancel()
		delete(rt.pollers, id)
		return
	}
}

func (m *Manager) adjustPollersLocked(ctx context.Context, rt *triggerRuntime) {
	if !rt.cfg.AutoScale {
		return
	}
	stats, err := m.store.GetQueueStats(ctx, rt.cfg.QueueID)
	if err != nil {
		m.logger.Error("failed to fetch queue stats for trigger scaling", "trigger_id", rt.cfg.ID, "error", err)
		return
	}
	current := len(rt.pollers)
	target := current
	scaleUpThreshold := rt.cfg.BatchSize * current * 5
	if stats.Available > scaleUpThreshold && current < rt.cfg.MaxPollers {
		target++
	}
	if rt.emptyReceives.Load() >= int64(current*3) && current > rt.cfg.MinPollers {
		target--
		rt.emptyReceives.Store(0)
	}
	for len(rt.pollers) < target {
		m.addPollerLocked(ctx, rt)
	}
	for len(rt.pollers) > target {
		m.removePollerLocked(rt)
	}
}

func (m *Manager) pollerLoop(ctx context.Context, rt *triggerRuntime, pollerID int) {
	sem := make(chan struct{}, rt.cfg.MaxConcurrency)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		backoff := backoffForFailures(rt.cfg.FailureThreshold, rt.consecutiveFailed.Load())
		if backoff > 0 {
			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}

		batch, err := m.collectBatch(ctx, rt.cfg)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			m.logger.Error("trigger receive failed", "trigger_id", rt.cfg.ID, "queue", rt.cfg.QueueName, "poller_id", pollerID, "error", err)
			continue
		}
		if len(batch) == 0 {
			rt.emptyReceives.Add(1)
			continue
		}
		rt.emptyReceives.Store(0)

		chunks := splitForDispatch(rt.cfg.QueueType, batch)
		for _, chunk := range chunks {
			select {
			case <-ctx.Done():
				return
			case sem <- struct{}{}:
			}
			go func(msgs []internal.Message) {
				defer func() { <-sem }()
				success := m.invokeAndHandle(ctx, rt.cfg, msgs)
				if success {
					rt.consecutiveFailed.Store(0)
				} else {
					rt.consecutiveFailed.Add(1)
				}
			}(chunk)
		}
	}
}

func (m *Manager) collectBatch(ctx context.Context, cfg internal.TriggerWithQueue) ([]internal.Message, error) {
	visibilityTimeout := cfg.QueueVisibilityTimeout
	if cfg.VisibilityTimeoutOverride != nil {
		visibilityTimeout = *cfg.VisibilityTimeoutOverride
	}

	msgs, err := m.receiveLongPoll(ctx, cfg, visibilityTimeout, cfg.BatchSize)
	if err != nil {
		return nil, err
	}
	if len(msgs) == 0 || cfg.BatchWindowSeconds == 0 || len(msgs) >= cfg.BatchSize {
		return msgs, nil
	}

	windowDeadline := time.Now().Add(time.Duration(cfg.BatchWindowSeconds) * time.Second)
	for len(msgs) < cfg.BatchSize {
		remaining := time.Until(windowDeadline)
		if remaining <= 0 {
			break
		}
		need := cfg.BatchSize - len(msgs)
		if need < 1 {
			break
		}
		waitCtx, cancel := context.WithTimeout(ctx, remaining)
		next, recvErr := m.receiveLongPoll(waitCtx, cfg, visibilityTimeout, need)
		cancel()
		if recvErr != nil {
			return nil, recvErr
		}
		if len(next) == 0 {
			break
		}
		msgs = append(msgs, next...)
	}
	return msgs, nil
}

func (m *Manager) receiveLongPoll(ctx context.Context, cfg internal.TriggerWithQueue, visibilityTimeout, max int) ([]internal.Message, error) {
	receive := m.store.ReceiveMessages
	if cfg.QueueType == "FIFO" {
		receive = m.store.ReceiveMessagesFIFO
	}

	msgs, err := receive(ctx, cfg.QueueID, visibilityTimeout, max)
	if err != nil {
		return nil, err
	}
	if len(msgs) > 0 {
		return msgs, nil
	}

	waitCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	if err := m.store.WaitForNotification(waitCtx, cfg.QueueID); err != nil {
		if waitCtx.Err() != nil {
			return []internal.Message{}, nil
		}
		return nil, err
	}
	return receive(ctx, cfg.QueueID, visibilityTimeout, max)
}

func (m *Manager) invokeAndHandle(ctx context.Context, cfg internal.TriggerWithQueue, msgs []internal.Message) bool {
	dispatcher, ok := m.dispatchers[cfg.TargetType]
	if !ok {
		m.logger.Error("trigger dispatcher missing", "trigger_id", cfg.ID, "target_type", cfg.TargetType)
		return false
	}

	records := make([]internal.TriggerRecord, 0, len(msgs))
	successHandles := make([]uuid.UUID, 0, len(msgs))
	for _, msg := range msgs {
		if msg.ReceiptHandle == nil {
			continue
		}
		records = append(records, internal.TriggerRecord{
			MessageID:               msg.ID,
			ReceiptHandle:           *msg.ReceiptHandle,
			Body:                    msg.Body,
			Attributes:              msg.Attributes,
			MessageGroupID:          msg.MessageGroupID,
			ApproximateReceiveCount: msg.ReceiveCount,
		})
		successHandles = append(successHandles, *msg.ReceiptHandle)
	}
	if len(records) == 0 {
		return true
	}

	oldest := msgs[0].CreatedAt
	for _, msg := range msgs[1:] {
		if msg.CreatedAt.Before(oldest) {
			oldest = msg.CreatedAt
		}
	}
	iteratorAgeMS := time.Since(oldest).Milliseconds()

	timeout := m.webhookTimeout
	if cfg.VisibilityTimeoutOverride != nil {
		vt := time.Duration(*cfg.VisibilityTimeoutOverride) * time.Second
		if vt > 5*time.Second && vt-2*time.Second < timeout {
			timeout = vt - 2*time.Second
		}
	}
	invokeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	start := time.Now()
	err := dispatcher.Invoke(invokeCtx, internal.TriggerPayload{
		TriggerID: cfg.ID,
		QueueID:   cfg.QueueID,
		QueueName: cfg.QueueName,
		QueueType: cfg.QueueType,
		Target:    cfg.TargetURL,
		Records:   records,
	})
	durationMS := time.Since(start).Milliseconds()

	success := true
	messagesFailed := int64(0)
	messagesProcessed := int64(0)

	if err != nil {
		var partial *BatchFailureError
		if errors.As(err, &partial) {
			failed := partial.FailedIDs()
			filtered := successHandles[:0]
			for i, rec := range records {
				if _, bad := failed[rec.MessageID]; bad {
					messagesFailed++
					continue
				}
				filtered = append(filtered, successHandles[i])
			}
			successHandles = filtered
			messagesProcessed = int64(len(successHandles))
			success = messagesFailed == 0
		} else {
			messagesFailed = int64(len(records))
			success = false
			m.logger.Error("trigger invocation failed", "trigger_id", cfg.ID, "queue", cfg.QueueName, "batch_size", len(records), "error", err)
		}
	} else {
		messagesProcessed = int64(len(records))
	}

	if len(successHandles) > 0 {
		deleteErrs := m.store.DeleteMessageBatch(ctx, cfg.QueueID, successHandles)
		if len(deleteErrs) > 0 {
			success = false
			failedDelete := int64(len(deleteErrs))
			messagesFailed += failedDelete
			messagesProcessed -= failedDelete
			m.logger.Error("trigger message delete partial failure", "trigger_id", cfg.ID, "queue", cfg.QueueName, "failures", len(deleteErrs))
		}
	}

	if err := m.store.RecordTriggerInvocation(
		ctx,
		cfg.ID,
		durationMS,
		iteratorAgeMS,
		messagesProcessed,
		messagesFailed,
		success,
	); err != nil {
		m.logger.Error("failed to persist trigger metrics", "trigger_id", cfg.ID, "error", err)
	}

	return success
}

func splitForDispatch(queueType string, msgs []internal.Message) [][]internal.Message {
	if queueType != "FIFO" {
		return [][]internal.Message{msgs}
	}
	grouped := make(map[string][]internal.Message)
	for _, msg := range msgs {
		group := ""
		if msg.MessageGroupID != nil {
			group = *msg.MessageGroupID
		}
		grouped[group] = append(grouped[group], msg)
	}
	out := make([][]internal.Message, 0, len(grouped))
	for _, chunk := range grouped {
		out = append(out, chunk)
	}
	return out
}

func backoffForFailures(threshold int, consecutive int64) time.Duration {
	if threshold <= 0 || consecutive < int64(threshold) {
		return 0
	}
	steps := consecutive - int64(threshold) + 1
	delay := time.Second
	for i := int64(1); i < steps; i++ {
		delay *= 2
		if delay >= maxBackoffDelay {
			return maxBackoffDelay
		}
	}
	if delay > maxBackoffDelay {
		return maxBackoffDelay
	}
	return delay
}

func triggerConfigChanged(a, b internal.TriggerWithQueue) bool {
	if a.ID != b.ID ||
		a.QueueID != b.QueueID ||
		a.Enabled != b.Enabled ||
		a.TargetType != b.TargetType ||
		a.TargetURL != b.TargetURL ||
		a.BatchSize != b.BatchSize ||
		a.BatchWindowSeconds != b.BatchWindowSeconds ||
		a.MaxConcurrency != b.MaxConcurrency ||
		a.MaxConcurrencyPerGroup != b.MaxConcurrencyPerGroup ||
		a.AutoScale != b.AutoScale ||
		a.MinPollers != b.MinPollers ||
		a.MaxPollers != b.MaxPollers ||
		a.FailureThreshold != b.FailureThreshold ||
		a.QueueType != b.QueueType ||
		a.QueueVisibilityTimeout != b.QueueVisibilityTimeout {
		return true
	}
	if (a.VisibilityTimeoutOverride == nil) != (b.VisibilityTimeoutOverride == nil) {
		return true
	}
	if a.VisibilityTimeoutOverride != nil && b.VisibilityTimeoutOverride != nil && *a.VisibilityTimeoutOverride != *b.VisibilityTimeoutOverride {
		return true
	}
	return false
}
