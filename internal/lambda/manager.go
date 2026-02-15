package lambda

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Ansh1693/evtq/internal"
)

const (
	defaultHealthTimeout = 10 * time.Second
	defaultInvokeTimeout = 30 * time.Second
	defaultMaxContainers = 20
	defaultRecycleEvery  = 50
)

var (
	ErrRuntimeUnsupported = errors.New("runtime is not supported yet")
)

// Invoker defines build/invoke behavior for function runtimes.
type Invoker interface {
	BuildImage(ctx context.Context, fn internal.Function) (string, error)
	Invoke(ctx context.Context, fn internal.Function, payload internal.TriggerPayload) (*internal.TriggerInvocationResponse, error)
}

type Manager struct {
	httpClient     *http.Client
	maxContainers  int
	recycleEvery   int
	containerSlots chan struct{}
	poolsMu        sync.Mutex
	pools          map[string]*warmPool
}

type warmContainer struct {
	id          string
	hostPort    string
	invocations int
	lastUsedAt  time.Time
}

type warmPool struct {
	name       string
	fn         internal.Function
	containers chan *warmContainer
	mu         sync.Mutex
	active     int
}

func NewManager() *Manager {
	maxContainers := envIntOrDefault("MAX_CONCURRENT_CONTAINERS", defaultMaxContainers)
	recycleEvery := envIntOrDefault("WARM_CONTAINER_RECYCLE_INVOCATIONS", defaultRecycleEvery)
	if maxContainers < 1 {
		maxContainers = defaultMaxContainers
	}
	if recycleEvery < 1 {
		recycleEvery = defaultRecycleEvery
	}
	return &Manager{
		httpClient:     &http.Client{},
		maxContainers:  maxContainers,
		recycleEvery:   recycleEvery,
		containerSlots: make(chan struct{}, maxContainers),
		pools:          make(map[string]*warmPool),
	}
}

func (m *Manager) BuildImage(ctx context.Context, fn internal.Function) (string, error) {
	switch strings.ToLower(strings.TrimSpace(fn.Runtime)) {
	case "nodejs22":
		return m.buildNodeImage(ctx, fn)
	default:
		return "", ErrRuntimeUnsupported
	}
}

func (m *Manager) Invoke(ctx context.Context, fn internal.Function, payload internal.TriggerPayload) (*internal.TriggerInvocationResponse, error) {
	if strings.ToLower(strings.TrimSpace(fn.ContainerStrategy)) == "warm" {
		return m.invokeWarm(ctx, fn, payload)
	}
	return m.invokeCold(ctx, fn, payload)
}

func (m *Manager) invokeCold(ctx context.Context, fn internal.Function, payload internal.TriggerPayload) (*internal.TriggerInvocationResponse, error) {
	if err := m.acquireSlot(ctx); err != nil {
		return nil, err
	}
	containerID, hostPort, err := m.startContainer(ctx, fn)
	if err != nil {
		m.releaseSlot()
		return nil, err
	}
	defer func() {
		m.forceRemoveContainer(context.Background(), containerID)
		m.releaseSlot()
	}()
	if err := m.waitForHealth(ctx, hostPort); err != nil {
		return nil, err
	}
	return m.invokeHTTP(ctx, fn, hostPort, payload)
}

func (m *Manager) invokeWarm(ctx context.Context, fn internal.Function, payload internal.TriggerPayload) (*internal.TriggerInvocationResponse, error) {
	pool := m.ensurePool(fn)
	container, err := m.acquireWarmContainer(ctx, pool)
	if err != nil {
		return nil, err
	}

	resp, invokeErr := m.invokeHTTP(ctx, fn, container.hostPort, payload)
	container.lastUsedAt = time.Now()
	container.invocations++

	if invokeErr != nil {
		m.removeWarmContainer(context.Background(), pool, container)
		m.spawnWarmContainerAsync(pool)
		return nil, invokeErr
	}

	if container.invocations >= m.recycleEvery {
		m.removeWarmContainer(context.Background(), pool, container)
		m.spawnWarmContainerAsync(pool)
	} else {
		select {
		case pool.containers <- container:
		case <-ctx.Done():
			m.removeWarmContainer(context.Background(), pool, container)
		}
	}
	return resp, nil
}

func (m *Manager) ensurePool(fn internal.Function) *warmPool {
	key := fn.Name
	m.poolsMu.Lock()
	defer m.poolsMu.Unlock()

	if existing, ok := m.pools[key]; ok {
		existing.mu.Lock()
		existing.fn = fn
		existing.mu.Unlock()
		return existing
	}

	size := fn.WarmPoolSize
	if size < 1 {
		size = 1
	}
	pool := &warmPool{
		name:       key,
		fn:         fn,
		containers: make(chan *warmContainer, size),
	}
	m.pools[key] = pool
	for i := 0; i < size; i++ {
		m.spawnWarmContainerAsync(pool)
	}
	return pool
}

func (m *Manager) spawnWarmContainerAsync(pool *warmPool) {
	go func() {
		pool.mu.Lock()
		if pool.active >= cap(pool.containers) {
			pool.mu.Unlock()
			return
		}
		pool.mu.Unlock()

		if err := m.acquireSlot(context.Background()); err != nil {
			return
		}
		pool.mu.Lock()
		fn := pool.fn
		pool.mu.Unlock()

		containerID, hostPort, err := m.startContainer(context.Background(), fn)
		if err != nil {
			m.releaseSlot()
			return
		}
		if err := m.waitForHealth(context.Background(), hostPort); err != nil {
			m.forceRemoveContainer(context.Background(), containerID)
			m.releaseSlot()
			return
		}
		w := &warmContainer{
			id:         containerID,
			hostPort:   hostPort,
			lastUsedAt: time.Now(),
		}

		pool.mu.Lock()
		pool.active++
		pool.mu.Unlock()

		select {
		case pool.containers <- w:
		default:
			m.removeWarmContainer(context.Background(), pool, w)
		}
	}()
}

func (m *Manager) acquireWarmContainer(ctx context.Context, pool *warmPool) (*warmContainer, error) {
	select {
	case c := <-pool.containers:
		return c, nil
	default:
	}

	pool.mu.Lock()
	active := pool.active
	capacity := cap(pool.containers)
	pool.mu.Unlock()

	if active < capacity {
		if err := m.acquireSlot(ctx); err != nil {
			return nil, err
		}
		pool.mu.Lock()
		fn := pool.fn
		pool.mu.Unlock()
		containerID, hostPort, err := m.startContainer(ctx, fn)
		if err != nil {
			m.releaseSlot()
			return nil, err
		}
		if err := m.waitForHealth(ctx, hostPort); err != nil {
			m.forceRemoveContainer(context.Background(), containerID)
			m.releaseSlot()
			return nil, err
		}
		pool.mu.Lock()
		pool.active++
		pool.mu.Unlock()
		return &warmContainer{id: containerID, hostPort: hostPort, lastUsedAt: time.Now()}, nil
	}

	select {
	case c := <-pool.containers:
		return c, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (m *Manager) removeWarmContainer(ctx context.Context, pool *warmPool, c *warmContainer) {
	m.forceRemoveContainer(ctx, c.id)
	m.releaseSlot()
	pool.mu.Lock()
	if pool.active > 0 {
		pool.active--
	}
	pool.mu.Unlock()
}

func (m *Manager) invokeHTTP(ctx context.Context, fn internal.Function, hostPort string, payload internal.TriggerPayload) (*internal.TriggerInvocationResponse, error) {
	invokeTimeout := time.Duration(fn.TimeoutSeconds) * time.Second
	if invokeTimeout <= 0 {
		invokeTimeout = defaultInvokeTimeout
	}
	invokeCtx, cancel := context.WithTimeout(ctx, invokeTimeout)
	defer cancel()

	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal invocation payload: %w", err)
	}
	req, err := http.NewRequestWithContext(invokeCtx, http.MethodPost, "http://127.0.0.1:"+hostPort+"/invoke", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build invoke request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := m.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("invoke function: %w", err)
	}
	defer resp.Body.Close()

	respBody := bytes.Buffer{}
	if _, err := respBody.ReadFrom(resp.Body); err != nil {
		return nil, fmt.Errorf("read invoke response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("function returned status=%d body=%s", resp.StatusCode, respBody.String())
	}
	if strings.TrimSpace(respBody.String()) == "" {
		return &internal.TriggerInvocationResponse{}, nil
	}

	var out internal.TriggerInvocationResponse
	if err := json.Unmarshal(respBody.Bytes(), &out); err != nil {
		return nil, fmt.Errorf("decode invocation response: %w", err)
	}
	return &out, nil
}

func (m *Manager) acquireSlot(ctx context.Context) error {
	select {
	case m.containerSlots <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *Manager) releaseSlot() {
	select {
	case <-m.containerSlots:
	default:
	}
}

func (m *Manager) buildNodeImage(ctx context.Context, fn internal.Function) (string, error) {
	if strings.TrimSpace(fn.CodePath) == "" {
		return "", errors.New("code_path is required")
	}
	imageName := strings.TrimSpace(fn.ImageName)
	if imageName == "" {
		imageName = "lambda-fn-" + sanitizeTag(fn.Name) + ":latest"
	}

	bootstrapPath := filepath.Join(fn.CodePath, ".evtq_bootstrap.js")
	dockerfilePath := filepath.Join(fn.CodePath, ".evtq.Dockerfile")

	if err := os.WriteFile(bootstrapPath, []byte(nodeBootstrapJS), 0o644); err != nil {
		return "", fmt.Errorf("write bootstrap: %w", err)
	}
	defer os.Remove(bootstrapPath) //nolint:errcheck

	df := `FROM node:22-alpine
WORKDIR /var/task
COPY . /var/task
RUN if [ -f package.json ]; then npm install --omit=dev; fi
EXPOSE 8080
CMD ["node", "/var/task/.evtq_bootstrap.js"]
`
	if err := os.WriteFile(dockerfilePath, []byte(df), 0o644); err != nil {
		return "", fmt.Errorf("write dockerfile: %w", err)
	}
	defer os.Remove(dockerfilePath) //nolint:errcheck

	cmd := exec.CommandContext(ctx, "docker", "build", "-f", dockerfilePath, "-t", imageName, fn.CodePath)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("docker build failed: %w output=%s", err, string(out))
	}
	return imageName, nil
}

func (m *Manager) startContainer(ctx context.Context, fn internal.Function) (containerID string, hostPort string, err error) {
	mem := fn.MemoryMB
	if mem <= 0 {
		mem = 128
	}
	timeoutMS := strconv.Itoa(fn.TimeoutSeconds * 1000)
	if fn.TimeoutSeconds <= 0 {
		timeoutMS = "30000"
	}

	args := []string{"run", "-d", "--rm", "-p", "127.0.0.1::8080", "-m", fmt.Sprintf("%dm", mem)}
	args = append(args, "-e", "HANDLER="+fn.Handler)
	args = append(args, "-e", "FUNCTION_NAME="+fn.Name)
	args = append(args, "-e", "FUNCTION_TIMEOUT_MS="+timeoutMS)
	for k, v := range fn.Environment {
		args = append(args, "-e", k+"="+v)
	}
	args = append(args, fn.ImageName)

	run := exec.CommandContext(ctx, "docker", args...)
	out, err := run.CombinedOutput()
	if err != nil {
		return "", "", fmt.Errorf("docker run failed: %w output=%s", err, string(out))
	}
	id := strings.TrimSpace(string(out))

	portCmd := exec.CommandContext(ctx, "docker", "port", id, "8080/tcp")
	portOut, err := portCmd.CombinedOutput()
	if err != nil {
		return "", "", fmt.Errorf("docker port failed: %w output=%s", err, string(portOut))
	}
	// format: 127.0.0.1:49153
	parts := strings.Split(strings.TrimSpace(string(portOut)), ":")
	if len(parts) < 2 {
		return "", "", fmt.Errorf("unexpected docker port output: %s", strings.TrimSpace(string(portOut)))
	}
	return id, strings.TrimSpace(parts[len(parts)-1]), nil
}

func (m *Manager) waitForHealth(ctx context.Context, port string) error {
	deadline := time.Now().Add(defaultHealthTimeout)
	for time.Now().Before(deadline) {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://127.0.0.1:"+port+"/health", nil)
		resp, err := m.httpClient.Do(req)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
	return errors.New("container health check timeout")
}

func (m *Manager) forceRemoveContainer(ctx context.Context, containerID string) {
	_ = exec.CommandContext(ctx, "docker", "rm", "-f", containerID).Run()
}

func sanitizeTag(name string) string {
	replacer := strings.NewReplacer(" ", "-", "/", "-", ":", "-", "@", "-", "\\", "-")
	return strings.ToLower(replacer.Replace(name))
}

func envIntOrDefault(name string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return n
}

const nodeBootstrapJS = `
const http = require("http");

function resolveHandler(handlerSpec) {
  const spec = handlerSpec || "index.handler";
  const split = spec.lastIndexOf(".");
  if (split <= 0 || split >= spec.length - 1) {
    throw new Error("invalid HANDLER format, expected file.export (e.g. index.handler)");
  }
  const modulePath = spec.slice(0, split);
  const exportName = spec.slice(split + 1);
  const mod = require("/var/task/" + modulePath);
  const fn = mod[exportName];
  if (typeof fn !== "function") {
    throw new Error("handler export is not a function: " + spec);
  }
  return fn;
}

const handler = resolveHandler(process.env.HANDLER);
const timeoutMs = Number(process.env.FUNCTION_TIMEOUT_MS || "30000");
const functionName = process.env.FUNCTION_NAME || "unknown";

function parseBody(req) {
  return new Promise((resolve, reject) => {
    let data = "";
    req.on("data", chunk => data += chunk);
    req.on("end", () => {
      if (!data) return resolve({});
      try {
        resolve(JSON.parse(data));
      } catch (err) {
        reject(err);
      }
    });
    req.on("error", reject);
  });
}

const server = http.createServer(async (req, res) => {
  if (req.method === "GET" && req.url === "/health") {
    res.writeHead(200, {"Content-Type": "application/json"});
    res.end(JSON.stringify({status: "ok"}));
    return;
  }

  if (req.method !== "POST" || req.url !== "/invoke") {
    res.writeHead(404, {"Content-Type": "application/json"});
    res.end(JSON.stringify({error: "not found"}));
    return;
  }

  let event;
  try {
    event = await parseBody(req);
  } catch (err) {
    res.writeHead(400, {"Content-Type": "application/json"});
    res.end(JSON.stringify({error: "invalid JSON", detail: String(err)}));
    return;
  }

  const invocationId = event && event.invocation_id ? event.invocation_id : "unknown";
  const context = { invocation_id: invocationId, timeout_ms: timeoutMs, function_name: functionName };

  try {
    const result = await Promise.resolve(handler(event, context));
    res.writeHead(200, {"Content-Type": "application/json"});
    if (typeof result === "undefined") {
      res.end("{}");
      return;
    }
    res.end(JSON.stringify(result));
  } catch (err) {
    res.writeHead(500, {"Content-Type": "application/json"});
    res.end(JSON.stringify({error: "handler failed", detail: String(err && err.stack ? err.stack : err)}));
  }
});

server.listen(8080, "0.0.0.0");
`
