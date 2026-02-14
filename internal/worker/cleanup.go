package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/Ansh1693/evtq/internal/store"
)

// CleanupWorker runs two periodic jobs:
//  1. Soft-delete expired messages (frequent, e.g. every 60s)
//  2. Hard-delete soft-deleted rows from both tables (infrequent, e.g. once a day)
type CleanupWorker struct {
	store               *store.Store
	expiryInterval      time.Duration // how often to soft-delete expired messages
	dedupExpiryInterval time.Duration // how often to clear expired dedup IDs
	hardDeleteInterval  time.Duration // how often to permanently remove soft-deleted rows
	logger              *slog.Logger
}

// NewCleanupWorker creates a worker with two configurable intervals.
func NewCleanupWorker(s *store.Store, expiryInterval, dedupExpiryInterval, hardDeleteInterval time.Duration, logger *slog.Logger) *CleanupWorker {
	return &CleanupWorker{
		store:               s,
		expiryInterval:      expiryInterval,
		dedupExpiryInterval: dedupExpiryInterval,
		hardDeleteInterval:  hardDeleteInterval,
		logger:              logger,
	}
}

// Run starts both cleanup loops. It blocks until the context is cancelled.
func (w *CleanupWorker) Run(ctx context.Context) {
	w.logger.Info("cleanup worker started",
		"expiry_interval", w.expiryInterval,
		"dedup_expiry_interval", w.dedupExpiryInterval,
		"hard_delete_interval", w.hardDeleteInterval,
	)

	expiryTicker := time.NewTicker(w.expiryInterval)
	dedupExpiryTicker := time.NewTicker(w.dedupExpiryInterval)
	hardDeleteTicker := time.NewTicker(w.hardDeleteInterval)
	defer expiryTicker.Stop()
	defer dedupExpiryTicker.Stop()
	defer hardDeleteTicker.Stop()

	// Run expiry check once immediately on startup.
	w.softDeleteExpired(ctx)
	w.expireDedupIDs(ctx)

	for {
		select {
		case <-ctx.Done():
			w.logger.Info("cleanup worker stopped")
			return
		case <-expiryTicker.C:
			w.softDeleteExpired(ctx)
		case <-dedupExpiryTicker.C:
			w.expireDedupIDs(ctx)
		case <-hardDeleteTicker.C:
			w.hardDeleteAll(ctx)
		}
	}
}

// softDeleteExpired marks expired messages as soft-deleted.
func (w *CleanupWorker) softDeleteExpired(ctx context.Context) {
	count, err := w.store.SoftDeleteExpiredMessages(ctx)
	if err != nil {
		w.logger.Error("soft-delete expired messages failed", "error", err)
		return
	}
	if count > 0 {
		w.logger.Info("soft-deleted expired messages", "count", count)
	}
}

func (w *CleanupWorker) expireDedupIDs(ctx context.Context) {
	count, err := w.store.ExpireDedupIDs(ctx)
	if err != nil {
		w.logger.Error("expire dedup ids failed", "error", err)
		return
	}
	if count > 0 {
		w.logger.Info("expired dedup ids", "count", count)
	}
}

// hardDeleteAll permanently removes all soft-deleted messages and queues.
// Messages are deleted first to avoid FK constraint violations.
func (w *CleanupWorker) hardDeleteAll(ctx context.Context) {
	msgCount, err := w.store.HardDeleteMessages(ctx)
	if err != nil {
		w.logger.Error("hard-delete messages failed", "error", err)
		return
	}

	queueCount, err := w.store.HardDeleteQueues(ctx)
	if err != nil {
		w.logger.Error("hard-delete queues failed", "error", err)
		return
	}

	if msgCount > 0 || queueCount > 0 {
		w.logger.Info("hard-deleted soft-deleted rows",
			"messages", msgCount,
			"queues", queueCount,
		)
	}
}
