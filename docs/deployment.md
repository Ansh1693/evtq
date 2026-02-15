# EvtQ Deployment

## Table of contents

- [Docker Compose (development)](#docker-compose-development)
- [Docker standalone](#docker-standalone)
- [Binary deployment](#binary-deployment)
- [Production considerations](#production-considerations)
- [Health checks](#health-checks)

## Docker Compose (development)

`docker-compose.yml`:

```yaml
version: "3.9"

services:
  postgres:
    image: postgres:16
    container_name: evtq-postgres
    environment:
      POSTGRES_USER: evtq
      POSTGRES_PASSWORD: evtq
      POSTGRES_DB: evtq
    ports:
      - "5432:5432"
    volumes:
      - evtq_pgdata:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U evtq"]
      interval: 5s
      timeout: 3s
      retries: 20

  evtq:
    build:
      context: .
      dockerfile: Dockerfile
    container_name: evtq
    depends_on:
      postgres:
        condition: service_healthy
    environment:
      DATABASE_URL: postgres://evtq:evtq@postgres:5432/evtq?sslmode=disable
      LISTEN_ADDR: ":8080"
      PGX_MAX_CONNS: "20"
    ports:
      - "8080:8080"
    restart: unless-stopped

volumes:
  evtq_pgdata:
```

Run:

```bash
docker compose up -d
```

## Docker standalone

Build image:

```bash
docker build -t evtq:latest .
```

Run PostgreSQL:

```bash
docker run -d --name evtq-postgres \
  -e POSTGRES_USER=evtq \
  -e POSTGRES_PASSWORD=evtq \
  -e POSTGRES_DB=evtq \
  -p 5432:5432 postgres:16
```

Run EvtQ:

```bash
docker run -d --name evtq \
  -e DATABASE_URL="postgres://evtq:evtq@host.docker.internal:5432/evtq?sslmode=disable" \
  -e LISTEN_ADDR=":8080" \
  -p 8080:8080 evtq:latest
```

## Binary deployment

Build:

```bash
go build -o bin/evtq ./cmd/server
```

Run:

```bash
DATABASE_URL="postgres://localhost:5432/evtq?sslmode=disable" \
LISTEN_ADDR=":8080" \
./bin/evtq
```

## Production considerations

- **DB sizing**: keep PostgreSQL CPU and IOPS headroom for claim/delete bursts.
- **Connection pool**: set `PGX_MAX_CONNS` based on API + trigger concurrency.
- **Autovacuum**: tune for high churn in `messages` table.
- **Indexes**: ensure queue and visibility-related indexes are healthy.
- **Timeouts**: set HTTP and trigger timeouts to bounded values.
- **Observability**:
  - API latency
  - receive/delete rates
  - in-flight/available backlog
  - trigger error rates/backoff state
- **Backup**: use regular PostgreSQL base backup + WAL archiving for recovery.

## Health checks

Endpoint:

```bash
curl -sS http://localhost:8080/health
```

Expected: HTTP `200`.

For readiness, include a DB ping on startup and fail fast if migration/schema is missing.

Related:

- [Configuration](./configuration.md)
- [Architecture](./architecture.md)
