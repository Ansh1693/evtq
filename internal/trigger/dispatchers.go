package trigger

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding"

	"github.com/Ansh1693/evtq/internal"
)

// Dispatcher invokes a trigger target with a batch payload.
type Dispatcher interface {
	Invoke(ctx context.Context, payload internal.TriggerPayload) error
}

// BatchFailureError captures partial batch failures.
type BatchFailureError struct {
	Failed map[uuid.UUID]struct{}
}

func (e *BatchFailureError) Error() string {
	return "partial batch failure"
}

func (e *BatchFailureError) FailedIDs() map[uuid.UUID]struct{} {
	if e == nil {
		return map[uuid.UUID]struct{}{}
	}
	return e.Failed
}

type webhookDispatcher struct {
	client *http.Client
}

// NewWebhookDispatcher creates an HTTP webhook dispatcher.
func NewWebhookDispatcher(timeout time.Duration) Dispatcher {
	return &webhookDispatcher{
		client: &http.Client{Timeout: timeout},
	}
}

func (d *webhookDispatcher) Invoke(ctx context.Context, payload internal.TriggerPayload) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, payload.Target, bytes.NewReader(raw))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := d.client.Do(req)
	if err != nil {
		return fmt.Errorf("webhook call: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4*1024))
		return fmt.Errorf("webhook status=%d body=%s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	if err != nil {
		return fmt.Errorf("read webhook response: %w", err)
	}
	if len(bytes.TrimSpace(body)) == 0 {
		return nil
	}

	var invocationResp internal.TriggerInvocationResponse
	if err := json.Unmarshal(body, &invocationResp); err != nil {
		return fmt.Errorf("decode webhook response: %w", err)
	}
	if len(invocationResp.BatchItemFailures) == 0 {
		return nil
	}

	failed := make(map[uuid.UUID]struct{}, len(invocationResp.BatchItemFailures))
	for _, entry := range invocationResp.BatchItemFailures {
		failed[entry.ItemIdentifier] = struct{}{}
	}
	return &BatchFailureError{Failed: failed}
}

type lambdaDispatcher struct {
	client  *http.Client
	baseURL string
}

func NewLambdaDispatcher(baseURL string, timeout time.Duration) Dispatcher {
	return &lambdaDispatcher{
		client:  &http.Client{Timeout: timeout},
		baseURL: strings.TrimRight(strings.TrimSpace(baseURL), "/"),
	}
}

func (d *lambdaDispatcher) Invoke(ctx context.Context, payload internal.TriggerPayload) error {
	if d.baseURL == "" {
		return errors.New("faas base url is not configured")
	}
	if strings.TrimSpace(payload.Target) == "" {
		return errors.New("lambda function name is required")
	}

	raw, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	url := d.baseURL + "/internal/invoke/" + payload.Target
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(raw))
	if err != nil {
		return fmt.Errorf("build lambda request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := d.client.Do(req)
	if err != nil {
		return fmt.Errorf("lambda invoke call: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	if err != nil {
		return fmt.Errorf("read lambda invoke response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("lambda status=%d body=%s", resp.StatusCode, string(body))
	}
	if len(bytes.TrimSpace(body)) == 0 {
		return nil
	}

	var invocationResp internal.TriggerInvocationResponse
	if err := json.Unmarshal(body, &invocationResp); err != nil {
		return fmt.Errorf("decode lambda response: %w", err)
	}
	if len(invocationResp.BatchItemFailures) == 0 {
		return nil
	}
	failed := make(map[uuid.UUID]struct{}, len(invocationResp.BatchItemFailures))
	for _, entry := range invocationResp.BatchItemFailures {
		failed[entry.ItemIdentifier] = struct{}{}
	}
	return &BatchFailureError{Failed: failed}
}

type grpcDispatcher struct{}

type grpcJSONCodec struct{}

func (c grpcJSONCodec) Name() string {
	return "json"
}

func (c grpcJSONCodec) Marshal(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func (c grpcJSONCodec) Unmarshal(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

type grpcInvokeRequest struct {
	Payload internal.TriggerPayload `json:"payload"`
}

type grpcInvokeResponse struct {
	BatchItemFailures []internal.BatchItemFailure `json:"batch_item_failures,omitempty"`
}

// NewGRPCDispatcher returns a gRPC dispatcher implementation.
func NewGRPCDispatcher() Dispatcher {
	encoding.RegisterCodec(grpcJSONCodec{})
	return &grpcDispatcher{}
}

func (d *grpcDispatcher) Invoke(ctx context.Context, payload internal.TriggerPayload) error {
	address, method, err := parseGRPCTarget(payload.Target)
	if err != nil {
		return err
	}

	conn, err := grpc.DialContext(
		ctx,
		address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(grpcJSONCodec{})),
	)
	if err != nil {
		return fmt.Errorf("grpc dial: %w", err)
	}
	defer conn.Close()

	req := grpcInvokeRequest{Payload: payload}
	var resp grpcInvokeResponse
	if err := conn.Invoke(ctx, method, &req, &resp); err != nil {
		return fmt.Errorf("grpc invoke: %w", err)
	}

	if len(resp.BatchItemFailures) == 0 {
		return nil
	}
	failed := make(map[uuid.UUID]struct{}, len(resp.BatchItemFailures))
	for _, item := range resp.BatchItemFailures {
		failed[item.ItemIdentifier] = struct{}{}
	}
	return &BatchFailureError{Failed: failed}
}

// LocalHandler is a Go function target for local triggers.
type LocalHandler func(payload internal.TriggerPayload) error

type localDispatcher struct {
	mu       sync.RWMutex
	handlers map[string]LocalHandler
}

// NewLocalDispatcher creates a local function dispatcher.
func NewLocalDispatcher() *localDispatcher {
	return &localDispatcher{
		handlers: make(map[string]LocalHandler),
	}
}

func (d *localDispatcher) Register(name string, fn LocalHandler) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.handlers[name] = fn
}

func (d *localDispatcher) Invoke(_ context.Context, payload internal.TriggerPayload) error {
	d.mu.RLock()
	fn, ok := d.handlers[payload.Target]
	d.mu.RUnlock()
	if !ok {
		return fmt.Errorf("local handler not found: %s", payload.Target)
	}
	return fn(payload)
}

func parseGRPCTarget(target string) (address string, method string, err error) {
	trimmed := strings.TrimSpace(target)
	if trimmed == "" {
		return "", "", errors.New("grpc target is required")
	}

	defaultMethod := "/evtq.trigger.v1.TriggerService/Invoke"
	if !strings.Contains(trimmed, "/") {
		return trimmed, defaultMethod, nil
	}

	idx := strings.Index(trimmed, "/")
	address = strings.TrimSpace(trimmed[:idx])
	method = strings.TrimSpace(trimmed[idx:])
	if address == "" {
		return "", "", errors.New("grpc target address is required")
	}
	if method == "" {
		method = defaultMethod
	}
	if !strings.HasPrefix(method, "/") {
		method = "/" + method
	}
	return address, method, nil
}
