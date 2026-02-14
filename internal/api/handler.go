package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"

	"github.com/Ansh1693/evtq/internal"
	"github.com/Ansh1693/evtq/internal/queue"
	"github.com/Ansh1693/evtq/internal/store"
)

// Handler holds the HTTP handler dependencies.
type Handler struct {
	svc    *queue.Service
	logger *slog.Logger
}

// NewRouter builds the chi router with all SQS-clone endpoints.
func NewRouter(svc *queue.Service, logger *slog.Logger) http.Handler {
	h := &Handler{svc: svc, logger: logger}

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Heartbeat("/health"))

	// Queue routes
	r.Post("/queues", h.CreateQueue)
	r.Delete("/queues/{name}", h.DeleteQueue)
	r.Get("/queues/{name}", h.GetQueueAttributes)

	// Message routes
	r.Post("/queues/{name}/messages", h.SendMessage)
	r.Post("/queues/{name}/messages/receive", h.ReceiveMessages)
	r.Delete("/queues/{name}/messages", h.PurgeQueue)
	r.Delete("/queues/{name}/messages/{receiptHandle}", h.DeleteMessage)
	r.Put("/queues/{name}/messages/{receiptHandle}/visibility", h.ChangeMessageVisibility)

	// Batch routes
	r.Post("/queues/{name}/messages/batch", h.SendMessageBatch)
	r.Post("/queues/{name}/messages/batch/delete", h.DeleteMessageBatch)

	// Trigger routes
	r.Post("/queues/{name}/triggers", h.CreateTrigger)
	r.Get("/queues/{name}/triggers", h.ListTriggers)
	r.Get("/queues/{name}/triggers/{id}", h.GetTrigger)
	r.Put("/queues/{name}/triggers/{id}", h.UpdateTrigger)
	r.Delete("/queues/{name}/triggers/{id}", h.DeleteTrigger)
	r.Put("/queues/{name}/triggers/{id}/enable", h.SetTriggerEnabled)
	r.Get("/queues/{name}/triggers/{id}/metrics", h.GetTriggerMetrics)

	return r
}

// ---------- Queue handlers ----------

type createQueueRequest struct {
	Name              string  `json:"name"`
	QueueType         *string `json:"queue_type,omitempty"`
	ContentBasedDedup *bool   `json:"content_based_dedup,omitempty"`
	VisibilityTimeout *int    `json:"visibility_timeout,omitempty"`
	MessageRetention  *int    `json:"message_retention,omitempty"`
	DelaySeconds      *int    `json:"delay_seconds,omitempty"`
}

func (h *Handler) CreateQueue(w http.ResponseWriter, r *http.Request) {
	var req createQueueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	queue, err := h.svc.CreateQueue(r.Context(), queue.CreateQueueInput{
		Name:              req.Name,
		QueueType:         req.QueueType,
		ContentBasedDedup: req.ContentBasedDedup,
		VisibilityTimeout: req.VisibilityTimeout,
		MessageRetention:  req.MessageRetention,
		DelaySeconds:      req.DelaySeconds,
	})
	if err != nil {
		h.handleServiceError(w, err)
		return
	}

	writeJSON(w, http.StatusCreated, queue)
}

func (h *Handler) DeleteQueue(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	if err := h.svc.DeleteQueue(r.Context(), name); err != nil {
		h.handleServiceError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) GetQueueAttributes(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	queue, stats, err := h.svc.GetQueueStats(r.Context(), name)
	if err != nil {
		h.handleServiceError(w, err)
		return
	}

	resp := map[string]interface{}{
		"queue": queue,
		"stats": stats,
	}
	writeJSON(w, http.StatusOK, resp)
}

// ---------- Message handlers ----------

type sendMessageRequest struct {
	Body           string                 `json:"body"`
	Attributes     map[string]interface{} `json:"attributes,omitempty"`
	DelaySeconds   *int                   `json:"delay_seconds,omitempty"`
	MessageGroupID *string                `json:"message_group_id,omitempty"`
	MessageDedupID *string                `json:"message_dedup_id,omitempty"`
}

func (h *Handler) SendMessage(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")

	var req sendMessageRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	msg, err := h.svc.SendMessage(r.Context(), queue.SendMessageInput{
		QueueName:      name,
		Body:           req.Body,
		Attributes:     req.Attributes,
		DelaySeconds:   req.DelaySeconds,
		MessageGroupID: req.MessageGroupID,
		MessageDedupID: req.MessageDedupID,
	})
	if err != nil {
		h.handleServiceError(w, err)
		return
	}

	writeJSON(w, http.StatusCreated, msg)
}

func (h *Handler) ReceiveMessages(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")

	maxMessages := 1
	if v := r.URL.Query().Get("max_messages"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "max_messages must be an integer")
			return
		}
		maxMessages = n
	}

	waitTime := 0
	if v := r.URL.Query().Get("wait_time_seconds"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "wait_time_seconds must be an integer")
			return
		}
		waitTime = n
	}

	var visTimeout *int
	if v := r.URL.Query().Get("visibility_timeout"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "visibility_timeout must be an integer")
			return
		}
		visTimeout = &n
	}

	msgs, err := h.svc.ReceiveMessages(r.Context(), queue.ReceiveMessagesInput{
		QueueName:         name,
		MaxMessages:       maxMessages,
		WaitTimeSeconds:   waitTime,
		VisibilityTimeout: visTimeout,
	})
	if err != nil {
		h.handleServiceError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"messages": msgs,
	})
}

func (h *Handler) DeleteMessage(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	handleStr := chi.URLParam(r, "receiptHandle")

	receiptHandle, err := uuid.Parse(handleStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid receipt handle format")
		return
	}

	if err := h.svc.DeleteMessage(r.Context(), name, receiptHandle); err != nil {
		h.handleServiceError(w, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

type changeVisibilityRequest struct {
	VisibilityTimeout int `json:"visibility_timeout"`
}

func (h *Handler) ChangeMessageVisibility(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	handleStr := chi.URLParam(r, "receiptHandle")

	receiptHandle, err := uuid.Parse(handleStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid receipt handle format")
		return
	}

	var req changeVisibilityRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	if err := h.svc.ChangeMessageVisibility(r.Context(), name, receiptHandle, req.VisibilityTimeout); err != nil {
		h.handleServiceError(w, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) PurgeQueue(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")

	count, err := h.svc.PurgeQueue(r.Context(), name)
	if err != nil {
		h.handleServiceError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"deleted": count,
	})
}

// ---------- Batch handlers ----------

type sendMessageBatchRequest struct {
	Entries []sendMessageRequest `json:"entries"`
}

func (h *Handler) SendMessageBatch(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")

	var req sendMessageBatchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	if len(req.Entries) == 0 {
		writeError(w, http.StatusBadRequest, "entries must contain at least 1 entry")
		return
	}
	if len(req.Entries) > 10 {
		writeError(w, http.StatusBadRequest, "entries must contain at most 10 entries")
		return
	}

	entries := make([]queue.SendMessageBatchEntry, len(req.Entries))
	for i, e := range req.Entries {
		entries[i] = queue.SendMessageBatchEntry{
			Body:           e.Body,
			Attributes:     e.Attributes,
			DelaySeconds:   e.DelaySeconds,
			MessageGroupID: e.MessageGroupID,
			MessageDedupID: e.MessageDedupID,
		}
	}

	results := h.svc.SendMessageBatch(r.Context(), name, entries)

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"results": results,
	})
}

type deleteMessageBatchRequest struct {
	Entries []deleteMessageBatchEntry `json:"entries"`
}

type deleteMessageBatchEntry struct {
	ReceiptHandle string `json:"receipt_handle"`
}

func (h *Handler) DeleteMessageBatch(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")

	var req deleteMessageBatchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	if len(req.Entries) == 0 {
		writeError(w, http.StatusBadRequest, "entries must contain at least 1 entry")
		return
	}
	if len(req.Entries) > 10 {
		writeError(w, http.StatusBadRequest, "entries must contain at most 10 entries")
		return
	}

	entries := make([]queue.DeleteMessageBatchEntry, len(req.Entries))
	for i, e := range req.Entries {
		rh, err := uuid.Parse(e.ReceiptHandle)
		if err != nil {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("entry %d: invalid receipt handle format", i))
			return
		}
		entries[i] = queue.DeleteMessageBatchEntry{ReceiptHandle: rh}
	}

	results := h.svc.DeleteMessageBatch(r.Context(), name, entries)

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"results": results,
	})
}

// ---------- Trigger handlers ----------

type triggerRequest struct {
	Enabled                   *bool   `json:"enabled,omitempty"`
	TargetType                string  `json:"target_type"`
	TargetURL                 string  `json:"target_url"`
	FunctionName              *string `json:"function_name,omitempty"`
	BatchSize                 *int    `json:"batch_size,omitempty"`
	BatchWindowSeconds        *int    `json:"batch_window_seconds,omitempty"`
	MaxConcurrency            *int    `json:"max_concurrency,omitempty"`
	VisibilityTimeoutOverride *int    `json:"visibility_timeout_override,omitempty"`
	MaxConcurrencyPerGroup    *int    `json:"max_concurrency_per_group,omitempty"`
	AutoScale                 *bool   `json:"auto_scale,omitempty"`
	MinPollers                *int    `json:"min_pollers,omitempty"`
	MaxPollers                *int    `json:"max_pollers,omitempty"`
	FailureThreshold          *int    `json:"failure_threshold,omitempty"`
}

type triggerUpdateRequest struct {
	Enabled                   *bool    `json:"enabled,omitempty"`
	TargetType                *string  `json:"target_type,omitempty"`
	TargetURL                 *string  `json:"target_url,omitempty"`
	FunctionName              **string `json:"function_name,omitempty"`
	BatchSize                 *int     `json:"batch_size,omitempty"`
	BatchWindowSeconds        *int     `json:"batch_window_seconds,omitempty"`
	MaxConcurrency            *int     `json:"max_concurrency,omitempty"`
	VisibilityTimeoutOverride **int    `json:"visibility_timeout_override,omitempty"`
	MaxConcurrencyPerGroup    *int     `json:"max_concurrency_per_group,omitempty"`
	AutoScale                 *bool    `json:"auto_scale,omitempty"`
	MinPollers                *int     `json:"min_pollers,omitempty"`
	MaxPollers                *int     `json:"max_pollers,omitempty"`
	FailureThreshold          *int     `json:"failure_threshold,omitempty"`
}

type triggerEnableRequest struct {
	Enabled bool `json:"enabled"`
}

func (h *Handler) CreateTrigger(w http.ResponseWriter, r *http.Request) {
	queueName := chi.URLParam(r, "name")
	var req triggerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	t, err := h.svc.CreateTrigger(r.Context(), queueName, queue.CreateTriggerInput{
		TargetType:                req.TargetType,
		TargetURL:                 req.TargetURL,
		FunctionName:              req.FunctionName,
		Enabled:                   req.Enabled,
		BatchSize:                 req.BatchSize,
		BatchWindowSeconds:        req.BatchWindowSeconds,
		MaxConcurrency:            req.MaxConcurrency,
		VisibilityTimeoutOverride: req.VisibilityTimeoutOverride,
		MaxConcurrencyPerGroup:    req.MaxConcurrencyPerGroup,
		AutoScale:                 req.AutoScale,
		MinPollers:                req.MinPollers,
		MaxPollers:                req.MaxPollers,
		FailureThreshold:          req.FailureThreshold,
	})
	if err != nil {
		h.handleServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, t)
}

func (h *Handler) ListTriggers(w http.ResponseWriter, r *http.Request) {
	queueName := chi.URLParam(r, "name")
	triggers, err := h.svc.ListTriggers(r.Context(), queueName)
	if err != nil {
		h.handleServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"triggers": triggers})
}

func (h *Handler) GetTrigger(w http.ResponseWriter, r *http.Request) {
	queueName := chi.URLParam(r, "name")
	triggerID, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid trigger id format")
		return
	}

	t, err := h.svc.GetTrigger(r.Context(), queueName, triggerID)
	if err != nil {
		h.handleServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, t)
}

func (h *Handler) UpdateTrigger(w http.ResponseWriter, r *http.Request) {
	queueName := chi.URLParam(r, "name")
	triggerID, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid trigger id format")
		return
	}

	var req triggerUpdateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	t, err := h.svc.UpdateTrigger(r.Context(), queueName, triggerID, queue.UpdateTriggerInput{
		Enabled:                   req.Enabled,
		TargetType:                req.TargetType,
		TargetURL:                 req.TargetURL,
		FunctionName:              req.FunctionName,
		BatchSize:                 req.BatchSize,
		BatchWindowSeconds:        req.BatchWindowSeconds,
		MaxConcurrency:            req.MaxConcurrency,
		VisibilityTimeoutOverride: req.VisibilityTimeoutOverride,
		MaxConcurrencyPerGroup:    req.MaxConcurrencyPerGroup,
		AutoScale:                 req.AutoScale,
		MinPollers:                req.MinPollers,
		MaxPollers:                req.MaxPollers,
		FailureThreshold:          req.FailureThreshold,
	})
	if err != nil {
		h.handleServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, t)
}

func (h *Handler) DeleteTrigger(w http.ResponseWriter, r *http.Request) {
	queueName := chi.URLParam(r, "name")
	triggerID, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid trigger id format")
		return
	}
	if err := h.svc.DeleteTrigger(r.Context(), queueName, triggerID); err != nil {
		h.handleServiceError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) SetTriggerEnabled(w http.ResponseWriter, r *http.Request) {
	queueName := chi.URLParam(r, "name")
	triggerID, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid trigger id format")
		return
	}
	var req triggerEnableRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	t, err := h.svc.SetTriggerEnabled(r.Context(), queueName, triggerID, req.Enabled)
	if err != nil {
		h.handleServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, t)
}

func (h *Handler) GetTriggerMetrics(w http.ResponseWriter, r *http.Request) {
	queueName := chi.URLParam(r, "name")
	triggerID, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid trigger id format")
		return
	}
	metrics, err := h.svc.GetTriggerMetrics(r.Context(), queueName, triggerID)
	if err != nil {
		h.handleServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, metrics)
}

// ---------- Function handlers ----------

type functionRequest struct {
	Name              string            `json:"name"`
	Runtime           string            `json:"runtime"`
	Handler           string            `json:"handler"`
	TimeoutSeconds    *int              `json:"timeout_seconds,omitempty"`
	MemoryMB          *int              `json:"memory_mb,omitempty"`
	Environment       map[string]string `json:"environment,omitempty"`
	CodePath          string            `json:"code_path"`
	ContainerStrategy *string           `json:"container_strategy,omitempty"`
	WarmPoolSize      *int              `json:"warm_pool_size,omitempty"`
}

type functionUpdateRequest struct {
	Runtime           *string           `json:"runtime,omitempty"`
	Handler           *string           `json:"handler,omitempty"`
	TimeoutSeconds    *int              `json:"timeout_seconds,omitempty"`
	MemoryMB          *int              `json:"memory_mb,omitempty"`
	Environment       map[string]string `json:"environment,omitempty"`
	CodePath          *string           `json:"code_path,omitempty"`
	ContainerStrategy *string           `json:"container_strategy,omitempty"`
	WarmPoolSize      *int              `json:"warm_pool_size,omitempty"`
}

func (h *Handler) CreateFunction(w http.ResponseWriter, r *http.Request) {
	var req functionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	fn, err := h.svc.CreateFunction(r.Context(), queue.CreateFunctionInput{
		Name:              req.Name,
		Runtime:           req.Runtime,
		Handler:           req.Handler,
		TimeoutSeconds:    req.TimeoutSeconds,
		MemoryMB:          req.MemoryMB,
		Environment:       req.Environment,
		CodePath:          req.CodePath,
		ContainerStrategy: req.ContainerStrategy,
		WarmPoolSize:      req.WarmPoolSize,
	})
	if err != nil {
		h.handleServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, fn)
}

func (h *Handler) ListFunctions(w http.ResponseWriter, r *http.Request) {
	functions, err := h.svc.ListFunctions(r.Context())
	if err != nil {
		h.handleServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"functions": functions})
}

func (h *Handler) GetFunction(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	fn, err := h.svc.GetFunction(r.Context(), name)
	if err != nil {
		h.handleServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, fn)
}

func (h *Handler) UpdateFunction(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	var req functionUpdateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	fn, err := h.svc.UpdateFunction(r.Context(), name, queue.UpdateFunctionInput{
		Runtime:           req.Runtime,
		Handler:           req.Handler,
		TimeoutSeconds:    req.TimeoutSeconds,
		MemoryMB:          req.MemoryMB,
		Environment:       req.Environment,
		CodePath:          req.CodePath,
		ContainerStrategy: req.ContainerStrategy,
		WarmPoolSize:      req.WarmPoolSize,
	})
	if err != nil {
		h.handleServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, fn)
}

func (h *Handler) DeleteFunction(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	if err := h.svc.DeleteFunction(r.Context(), name); err != nil {
		h.handleServiceError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) InvokeFunction(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	var payload internal.TriggerPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	resp, err := h.svc.InvokeFunction(r.Context(), name, payload)
	if err != nil {
		h.handleServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

// ---------- Helpers ----------

func (h *Handler) handleServiceError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, store.ErrQueueNotFound):
		writeError(w, http.StatusNotFound, "queue not found")
	case errors.Is(err, store.ErrMessageNotFound):
		writeError(w, http.StatusNotFound, "message not found or receipt handle is stale")
	case errors.Is(err, store.ErrTriggerNotFound):
		writeError(w, http.StatusNotFound, "trigger not found")
	case errors.Is(err, store.ErrFunctionNotFound):
		writeError(w, http.StatusNotFound, "function not found")
	case errors.Is(err, queue.ErrBodyTooLarge),
		errors.Is(err, queue.ErrInvalidDelay),
		errors.Is(err, queue.ErrInvalidVisibility),
		errors.Is(err, queue.ErrInvalidMaxMessages),
		errors.Is(err, queue.ErrInvalidWaitTime),
		errors.Is(err, queue.ErrInvalidRetention),
		errors.Is(err, queue.ErrInvalidQueueType),
		errors.Is(err, queue.ErrQueueNameRequired),
		errors.Is(err, queue.ErrQueueNameNotFIFO),
		errors.Is(err, queue.ErrMessageBodyRequired),
		errors.Is(err, queue.ErrGroupIDRequired),
		errors.Is(err, queue.ErrDedupIDRequired),
		errors.Is(err, queue.ErrContentBasedDedupRequiresFIFO),
		errors.Is(err, queue.ErrBatchTooLarge),
		errors.Is(err, queue.ErrBatchEmpty),
		errors.Is(err, queue.ErrInvalidTriggerTargetType),
		errors.Is(err, queue.ErrInvalidTargetURL),
		errors.Is(err, queue.ErrInvalidLambdaFunctionName),
		errors.Is(err, queue.ErrInvalidTriggerBatchSize),
		errors.Is(err, queue.ErrInvalidTriggerBatchWindow),
		errors.Is(err, queue.ErrInvalidTriggerConcurrency),
		errors.Is(err, queue.ErrInvalidVisibilityOverride),
		errors.Is(err, queue.ErrInvalidFIFOGroupConcurrency),
		errors.Is(err, queue.ErrInvalidPollerRange),
		errors.Is(err, queue.ErrInvalidFailureThreshold),
		errors.Is(err, queue.ErrInvalidFunctionName),
		errors.Is(err, queue.ErrInvalidFunctionRuntime),
		errors.Is(err, queue.ErrInvalidFunctionHandler),
		errors.Is(err, queue.ErrInvalidFunctionTimeout),
		errors.Is(err, queue.ErrInvalidFunctionMemory),
		errors.Is(err, queue.ErrInvalidFunctionCodePath),
		errors.Is(err, queue.ErrInvalidFunctionStrategy),
		errors.Is(err, queue.ErrInvalidWarmPoolSize):
		writeError(w, http.StatusBadRequest, err.Error())
	default:
		h.logger.Error("internal error", "error", err)
		writeError(w, http.StatusInternalServerError, "internal server error")
	}
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("failed to encode response", "error", err)
	}
}

func writeError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": message}) //nolint:errcheck
}
