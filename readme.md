# Stock Price System

## Overview

Based on recent interview, just testing the logics, vibe coded most of the items.
Status #still testing

Key flows:
- **Generator**: creates synthetic ticks (~50 ticks/sec), exposes a WebSocket feed.
- **Aggregator**: consumes the WebSocket ticks, publishes raw ticks to Kafka (`ticks` topic), builds OHLCV buckets and emits aggregated JSON into Kafka (`ohlcv` topic).
- **Stock-detail**: consumes raw ticks to update last-price per ticker in Redis; exposes HTTP lookup.
- **Price-graph**: consumes OHLCV messages and upserts into TimescaleDB; exposes recent bucketed OHLCV for a ticker.
- **Portfolio**: fetches price detail to compute portfolio value.
- **Observability stack**: OpenTelemetry Collector, Jaeger (traces), Grafana (dashboards).

## Architecture Diagram

```
[Generator WS] -> [Aggregator]
      |                |
      |                +--> Kafka topic: ticks --> [Stock-detail] -> Redis (last price)
      |                |
      |                +--> Kafka topic: ohlcv --> [Price-graph] -> TimescaleDB
      |                                         |
      |                                         +--> /ohlcv/{ticker} HTTP API
      +--> WebSocket feed (for aggregator)      |
                                               +--> Portfolio queries stock-detail /price/{ticker}
```

All services emit OpenTelemetry spans to the collector; Grafana + Jaeger pick these up.

## Components & Endpoints

### Generator
- **Purpose**: Synthetic tick generation (~50 ticks/sec), WebSocket feed.
- **Endpoint**: `ws://generator:8000/feed`
- **Health**: `http://generator:8000/health`

### Aggregator
- **Purpose**: Ingest WebSocket ticks, publish to Kafka (`ticks`), compute 5-minute OHLCV buckets, publish aggregated JSON to Kafka (`ohlcv`).
- **Env**:
  - `FEED_URL` (e.g., `ws://generator:8000/feed`)
  - `KAFKA_BROKERS` (e.g., `redpanda:9092`)
- **Topics Produced**: `ticks`, `ohlcv`

### Stock-detail
- **Purpose**: Consume raw ticks from `ticks` topic, update Redis with last price per ticker, expose price detail.
- **Endpoints**:
  - `GET /health`
  - `GET /price/{ticker}` returns JSON `{ ticker, last_price, updated_at }`
- **Env**:
  - `KAFKA_BROKERS`
  - `REDIS_ADDR` (e.g., `redis:6379`)

### Price-graph
- **Purpose**: Consume OHLCV JSON, upsert into TimescaleDB, expose recent OHLCV for tickers.
- **Endpoints**:
  - `GET /health`
  - `GET /ohlcv/{ticker}` returns latest bucketed OHLCV array.
- **Env**:
  - `KAFKA_BROKERS`
  - `PG_USER`, `PG_PASSWORD`, `PG_HOST`, `PG_DB`

### Portfolio
- **Purpose**: Static portfolio, fetches last prices from stock-detail, computes total value.
- **Endpoints**:
  - `GET /health`
  - `GET /portfolio` returns breakdown and total value.
- **Env**:
  - `STOCK_DETAIL_URL` (e.g., `http://stock-detail:8080`)

### Observability
- **OpenTelemetry Collector**: Receives OTLP from services.
  - OTLP gRPC: `4317`
  - Prometheus scrape: `8888`
- **Jaeger UI**: Traces visualization: `http://localhost:16686`
- **Grafana**: Metrics/dashboard: `http://localhost:3000` (default admin/admin)
  - Provisioned dashboards can show:
    - Service latency (from traces/metrics)
    - Tick throughput
    - OHLCV ingestion rate
    - Portfolio value over time

## Setup & Run

### Requirements
- Docker & Docker Compose (v2)
- (Optional) Access to `localhost` ports: 9092, 8080–8085, 3000, 16686

### Environment
Copy or adapt `.env` if used. At minimum, ensure the following are set in the compose file or environment:

```env
KAFKA_BROKERS=redpanda:9092
REDIS_ADDR=redis:6379
PG_USER=user
PG_PASSWORD=example
PG_HOST=timescaledb:5432
PG_DB=prices
STOCK_DETAIL_URL=http://stock-detail:8080
FEED_URL=ws://generator:8000/feed
OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4317
```

### Start the stack

```bash
docker compose up --build
```

### Initial Verification

- Redpanda topics:
  ```bash
  docker compose exec redpanda rpk topic list
  docker compose exec redpanda rpk topic describe ticks
  docker compose exec redpanda rpk topic describe ohlcv
  ```
- Health endpoints:
  ```
  curl http://localhost:8000/health         # generator
  curl http://localhost:8080/health         # stock-detail
  curl http://localhost:8081/health         # price-graph
  curl http://localhost:8082/health         # portfolio
  ```
- Fetch portfolio:
  ```
  curl http://localhost:8082/portfolio
  ```

## Visualization & Dashboarding

### Grafana
- URL: `http://localhost:3000`
- Login: `admin` / `admin` (customize via env)
- Data sources:
  - Prometheus (scraping metrics from collector)
  - Jaeger (for tracing integration if using a plugin)
- Suggested panels:
  - Ticks per second (via custom metrics)
  - OHLCV publish latency
  - Redis updates (last-price writes)
  - Portfolio total value trend (pull from portfolio endpoint)
  - Trace waterfall per request (via Jaeger)

### Jaeger
- URL: `http://localhost:16686`
- Look for services: `generator`, `aggregator`, `stock-detail`, `price-graph`, `portfolio`.
- Inspect spans like `process-tick`, `compute-portfolio`, `price-lookup`, etc.

## Data Model

### Tick (raw)
Published to Kafka `ticks` topic (JSON):
```json
{
  "ticker": "ABCD",
  "price": 123.45,
  "timestamp": "2025-08-01T06:50:00Z"
}
```

### OHLCV
Published to Kafka `ohlcv` topic (JSON):
```json
{
  "ticker": "ABCD",
  "bucket_start": "2025-08-01T06:45:00Z",
  "open": 120.0,
  "high": 125.0,
  "low": 119.0,
  "close": 123.45,
  "volume": 42.0,
  "count": 10
}
```

## Troubleshooting

- **Redpanda topic resolution errors (e.g., `failed to resolve 'redpanda:9092'`)**
  - Ensure services inside compose use the service name `redpanda:9092`.
  - From host use `localhost:9092`.
  - Ensure DNS in Docker is working; `docker compose exec aggregator ping redpanda`.

- **Kafka consumers see no data**
  - Inspect that aggregator is publishing to the topics: check its logs for `published tick to kafka` and `published OHLCV`.
  - Use `rpk topic consume ticks --from-beginning` inside Redpanda container or `kcat` pointed correctly.

- **TimescaleDB empty**
  - Verify `price-graph` is successfully consuming `ohlcv` and upserting; check its logs for errors.
  - Connect into DB: 
    ```bash
    docker compose exec timescaledb psql -U user -d prices
    SELECT * FROM ohlcv LIMIT 10;
    ```

- **Tracing not appearing**
  - Ensure `OTEL_EXPORTER_OTLP_ENDPOINT` is set for each service.
  - Collector config must accept OTLP and forward to Jaeger/Prometheus.
  - Check collector logs for pipeline errors.

## Cleaning/Rebuilding

To force rebuild all services:

```bash
docker compose build --no-cache
docker compose up
```

## Extending

- Replace synthetic generator with real market feed.
- Add authentication to HTTP endpoints.
- Persist portfolio positions externally instead of hardcoded.
- Add alerting rules in Grafana (e.g., if OHLCV lag grows).
