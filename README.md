# API Intelligence Platform — System Architecture, MVP Documentation & Production Roadmap

---

## 1. Executive Summary & What Was Built

The **API Intelligence Platform** is an end-to-end, event-driven observability and telemetry platform designed to monitor, analyze, and diagnose HTTP APIs in real-time. It acts as an intelligent traffic plane sitting in front of upstream services, extracting detailed metrics without adding noticeable latency to client requests, streaming raw telemetry through a distributed message broker, aggregating time-windowed metrics, detecting operational anomalies, and visualizing health metrics on a reactive dashboard.

### Core Capabilities Built in the MVP:
1. **Zero-Overhead Asynchronous Reverse Proxy Gateway**: Intercepts HTTP traffic, proxies requests to upstream destinations, logs microsecond timings and response codes, and streams telemetry events out-of-band to Apache Kafka with in-memory fallback buffers.
2. **Distributed Telemetry Pipeline**: Built on Apache Kafka (KRaft mode) using strongly typed event schemas (`TelemetryEvent`, `SecurityEvent`, `DeploymentEvent`, `Anomaly`).
3. **Stream Analytics & Rollup Engine**: Kafka consumer calculating real-time sliding-window statistics (P50, P95, P99 latency percentiles, error rates, throughput) and persisting time-bucketed aggregates into PostgreSQL.
4. **Real-Time Hot State & WebSockets**: Redis-backed cache for instantaneous route lookups and a Redis Pub/Sub layer broadcasting live telemetry ticks over WebSockets to client applications.
5. **Rule-Based Anomaly Detection**: Automated detectors identifying latency spikes and error rate breaches, emitting structured anomaly alerts into Kafka and Postgres.
6. **Fault-Injectable Mock Service & Traffic Simulator**: A full-featured test API capable of dynamically injecting latency spikes, 5xx server errors, slow unindexed SQL queries, and schema-breaking payload changes, paired with a weighted synthetic traffic generator.
7. **Reactive Modern Dashboard**: A dark-mode SPA built with React 19, Vite, Tailwind CSS, TanStack React Query, and WebSockets providing live metric updates, route breakdowns, and system status scorecards.
8. **Automated Monorepo & Build Pipeline**: Unified TypeScript monorepo with npm workspaces and cross-project compilation (`composite: true`).

---

## 2. Monorepo Architecture & Package Breakdown

The repository is structured as a TypeScript monorepo using npm workspaces:

```
api_monitor/
├── docker-compose.yml              # Local container infrastructure (Postgres, Redis, Kafka)
├── infra/
│   └── postgres/init.sql           # Database schema, indices, and mock tables
├── packages/
│   ├── shared/                     # Shared Zod schemas, TypeScript types, and contracts
│   ├── gateway/                    # Reverse proxy & Kafka telemetry emitter
│   ├── analytics/                  # Stream aggregation, anomaly detection, REST/WS server
│   ├── dashboard/                  # React 19 + Vite + Tailwind CSS frontend
│   ├── test-api/                   # Fault-injectable upstream mock API
│   └── traffic-generator/          # Configurable synthetic traffic generator
├── api-intelligence-platform-execution-plan.md
├── DOCUMENTATION.md                # System documentation and production roadmap
└── package.json
```

### Detailed Component Roles

#### 1. `packages/shared`
* **Purpose**: Single source of truth for event schemas and data contracts across all services.
* **Key Components**:
  - `TelemetryEventSchema`: Zod schema validating incoming request logs (`request_id`, `timestamp`, `method`, `path`, `route_template`, `status_code`, `latency_ms`, `upstream_latency_ms`, `request_size_bytes`, `response_size_bytes`, `client_ip`).
  - `SecurityEventSchema`: Schema for suspicious behavior (e.g., authentication failures, abnormal client requests).
  - `DeploymentEventSchema`: Schema for CI/CD deployment markers used for change correlation.
  - `AnomalySchema`: Schema for detected operational issues (`latency_spike`, `error_spike`, `traffic_spike`, `contract_violation`).

#### 2. `packages/gateway` (Port `3000`)
* **Purpose**: Frontline edge proxy sitting on the hot path between API clients and upstream servers.
* **Key Components**:
  - Built with Node.js `http` and `http-proxy`.
  - **Fire-and-forget telemetry**: Telemetry emission does not block or delay HTTP responses.
  - **Local Degradation Ring Buffer**: If Kafka is unreachable or suffers network degradation, the gateway buffers up to 10,000 events in memory and periodically flushes them in batches once the broker recovers, preventing gateway crashes or request drops.
  - **Security Heuristics**: Automatically flags repeated 401s on sensitive endpoints (such as `/login`) and emits `SecurityEvent` messages to Kafka topic `telemetry.security`.
  - **Automatic Route Parameter Normalization**: Converts parameterized dynamic paths (e.g. `/orders/123`, `/users/456`) into route templates (`/orders/:id`, `/users/:id`) to prevent metric cardinality explosion.

#### 3. `packages/analytics` (Port `3002`)
* **Purpose**: Stream processing engine, metric aggregation pipeline, anomaly detector, and API server.
* **Key Components**:
  - **Kafka Consumer Group (`analytics-group`)**: Ingests raw telemetry events from Kafka topic `telemetry.raw`.
  - **In-Memory Sliding Window**: Aggregates requests over 5-second windows per route template, computing request counts, status distributions (2xx, 4xx, 5xx), and exact P50/P95/P99 latency percentiles.
  - **Durable Batch Flusher**: Every 5 seconds, writes aggregated rollups into PostgreSQL table `metrics_rollup` and flushes raw events into table `requests`.
  - **Hot State & Pub/Sub**: Caches latest route statistics into Redis keys (`route:<template>:latest`) and publishes rollup events to the Redis `live_metrics` channel.
  - **REST API & WebSocket Server**:
    - `GET /api/metrics`: Fetches the last 100 historical metric rollups from Postgres.
    - `GET /api/anomalies`: Fetches detected anomalies.
    - `ws://localhost:3002`: Broadcasts live rollup ticks from Redis directly to connected frontend clients.
  - **Anomaly Detection Module (`detectors.ts`)**: Evaluates incoming 5-second rollups against operational baselines (e.g., P95 > 150ms triggers `latency_spike`; 5xx errors > 5 triggers `error_spike`), writing detections to Postgres and emitting them to Kafka topic `anomalies.detected`.

#### 4. `packages/test-api` (Port `3001`)
* **Purpose**: Upstream simulated service exposing business endpoints and a programmatic chaos/fault-injection control plane.
* **Endpoints**:
  - `POST /login`: Mock authentication endpoint (returns 200 on valid credentials, 401 on failure).
  - `GET /orders`: Fetches mock orders from Postgres table `test_api.orders`.
  - `GET /orders/:id`: Fetches a single order.
  - `GET /users/:id`: Fetches user profile data, with a deliberately unindexed query path.
  - `GET /products`: Returns product listings.
* **Fault Control Plane (`POST /_fault/:type`)**:
  - `latency_spike`: Injects synthetic 3000ms delay into targeted routes.
  - `error_spike`: Injects 500 Internal Server Errors at configurable failure rates (e.g., 50% or 100%).
  - `slow_query`: Executes an unindexed, unoptimized SQL query joining 10,000 orders against users, causing massive database lock contention and latency.
  - `schema_violation`: Returns malformed response payloads violating expected contracts (e.g., string instead of number).

#### 5. `packages/traffic-generator`
* **Purpose**: Generates realistic, continuous synthetic client traffic against the gateway.
* **Key Components**:
  - Configurable RPS (Requests Per Second, default 10 RPS).
  - Weighted route distributions:
    - 40% `/orders`
    - 30% `/products`
    - 20% `/users/1`
    - 10% `/orders/1`
  - Asynchronous dispatch with non-blocking error handling to simulate real-world client traffic patterns under failures.

#### 6. `packages/dashboard` (Port `5173`)
* **Purpose**: Operator observability console for engineering and DevOps teams.
* **Key Components**:
  - Built with React 19, TypeScript, Vite, and Tailwind CSS.
  - TanStack React Query for initial REST data fetching and automatic cache management.
  - Persistent WebSocket connection to `ws://localhost:3002` that merges live metric events into React Query state in real-time.
  - Live route table with request counts, P95 latency indicators, and 5xx error badges.
  - Navigation shell for Overview, Endpoints, Incidents, Security, and Deployments.

#### 7. Infrastructure (`docker-compose.yml` & `infra/postgres/init.sql`)
* **PostgreSQL 15**:
  - Tables: `requests`, `metrics_rollup`, `anomalies`, `deployments`, `incidents`, `health_scores`.
  - Mock Schema: `test_api.users`, `test_api.orders` populated with 10,000 seeded rows.
* **Redis 7**: High-speed hot state cache and Pub/Sub broker for live dashboard updates.
* **Apache Kafka 3.6 (KRaft)**: Distributed message log operating without Zookeeper dependencies.

---

## 3. How the Project Works (End-to-End Lifecycle)

```
 [Client / Traffic Generator]
              │
              ▼  (HTTP Request)
      ┌───────────────┐
      │    Gateway    │ (Port 3000)
      │  (http-proxy) │
      └───────┬───────┘
              │  1. Proxies request to Upstream
              ├─────────────────────────────────────────┐
              ▼                                         ▼
      ┌───────────────┐                         ┌───────────────┐
      │   Test API    │ (Port 3001)             │  Kafka Broker │ (Port 9092)
      │  (Upstream)   │                         │(telemetry.raw)│
      └───────┬───────┘                         └───────┬───────┘
              │ 2. Returns response                     │
              │                                         ▼
              │ 3. Emits TelemetryEvent         ┌───────────────┐
              │    (Async fire-and-forget)      │   Analytics   │ (Port 3002)
              │                                 │    Engine     │
              │                                 └───────┬───────┘
              │                                         │
              │                                ┌────────┴────────┐
              ▼                                ▼                 ▼
        [HTTP Client]                   ┌─────────────┐   ┌─────────────┐
                                        │  Postgres   │   │    Redis    │
                                        │ (Rollups &  │   │  (Hot Cache │
                                        │  Anomalies) │   │  & Pub/Sub) │
                                        └─────────────┘   └──────┬──────┘
                                                                 │
                                                       (WebSocket Push)
                                                                 ▼
                                                          ┌─────────────┐
                                                          │  Dashboard  │ (Port 5173)
                                                          │ (React SPA) │
                                                          └─────────────┘
```

### Step-by-Step Request Lifecycle:
1. **Traffic Arrival**: The client (e.g. browser, mobile app, or `traffic-generator`) sends an HTTP request to the Gateway at `http://localhost:3000/orders`.
2. **Context Enrichment**: The Gateway assigns a unique UUID `request_id`, captures the high-resolution start timestamp, and measures the request payload byte size.
3. **Upstream Forwarding**: The Gateway proxies the request to the upstream target (e.g. `http://localhost:3001/orders`).
4. **Response Interception**: When the upstream returns a response, the Gateway calculates round-trip latency (`latency_ms`), extracts response size, normalizes the URL to `/orders`, and delivers the response immediately to the client.
5. **Asynchronous Telemetry Stream**: The Gateway formats a `TelemetryEvent` and fires it into the Kafka topic `telemetry.raw`. If Kafka is unreachable, the event is retained in a local 10,000-item memory buffer and retried once the connection recovers.
6. **Windowed Stream Processing**: The Analytics service consumes messages from `telemetry.raw`. It pushes events into a 5-second tumbling window grouped by route template.
7. **Metric Calculation**: At the end of every 5-second interval:
   - Latencies are sorted to compute exact 50th, 95th, and 99th percentiles.
   - Status codes are categorized into 2xx, 4xx, and 5xx counters.
   - `detectAnomalies()` evaluates whether P95 or error counts exceed operational thresholds.
8. **Storage & Live Broadcast**:
   - The aggregated 5-second rollup is inserted into PostgreSQL (`metrics_rollup`).
   - The raw events are batch-inserted into PostgreSQL (`requests`).
   - The latest route state is cached in Redis (`route:/orders:latest`).
   - The rollup is published to the Redis channel `live_metrics`.
9. **Dashboard Push**: The Analytics WebSocket server receives the Redis publication and pushes the JSON payload to all active WebSocket clients. The React dashboard updates its state without polling.

---

## 4. How to Run the Project (Step-by-Step Guide)

### Prerequisites
- **Node.js**: v18+ or v20+
- **npm**: v9+
- **Docker & Docker Compose**: Installed and running

---

### Step 1: Start Supporting Infrastructure
Run the database, cache, and message broker in the background:
```bash
docker compose up -d
```
Verify that all 3 containers are healthy:
```bash
docker compose ps
```
You should see:
- `api_monitor-postgres-1` on port `5432`
- `api_monitor-redis-1` on port `6379`
- `api_monitor-kafka-1` on port `9092`

---

### Step 2: Install Dependencies & Build Packages
From the root workspace directory (`e:\Programming\api_monitor`):
```bash
# Install all dependencies across all packages
npm install

# Build all TypeScript projects
npm run build
```

---

### Step 3: Run the Services
Open separate terminal windows (or tabs) for each service to view their logs:

#### Terminal 1 — Start the Upstream Test API:
```bash
cd packages/test-api
npm run dev
# Starts on http://localhost:3001
```

#### Terminal 2 — Start the Gateway:
```bash
cd packages/gateway
npm run dev
# Starts on http://localhost:3000 (proxies to http://localhost:3001)
```

#### Terminal 3 — Start the Analytics Service:
```bash
cd packages/analytics
npm run dev
# Starts REST & WebSocket server on http://localhost:3002
```

#### Terminal 4 — Start the Dashboard Frontend:
```bash
cd packages/dashboard
npm run dev
# Starts Vite frontend on http://localhost:5173
```

#### Terminal 5 — Start the Traffic Generator:
```bash
cd packages/traffic-generator
npm run dev
# Begins generating steady-state traffic (~10 RPS) through the gateway
```

---

### Step 4: Access the System
- **Dashboard UI**: Open [http://localhost:5173](http://localhost:5173) in your browser.
- You will see live request counts, P95 latency indicators, and 5xx error metrics streaming across the table in real-time.

---

### Step 5: Test Fault Injection & Anomaly Detection

You can inject real-time faults into the upstream service to verify that the telemetry pipeline and anomaly detectors catch them:

#### Scenario A: Trigger a Latency Spike (e.g., on `/orders`)
```bash
curl -X POST http://localhost:3001/_fault/latency_spike \
  -H "Content-Type: application/json" \
  -d '{"action": "enable", "targetRoute": "/orders"}'
```
*Effect*: The test API will inject a 3-second delay on `/orders`. Within 5 seconds, the dashboard's P95 column will jump to `>3000ms`, and the analytics engine will insert a `latency_spike` record into the `anomalies` table.

#### Scenario B: Trigger a 50% Error Spike
```bash
curl -X POST http://localhost:3001/_fault/error_spike \
  -H "Content-Type: application/json" \
  -d '{"action": "enable", "rate": 0.5}'
```
*Effect*: 50% of all requests will fail with HTTP 500. The dashboard's `Errors (5xx)` count will turn red, and a critical `error_spike` anomaly will trigger.

#### Scenario C: Disable Faults and Return to Normal
```bash
curl -X POST http://localhost:3001/_fault/latency_spike \
  -H "Content-Type: application/json" \
  -d '{"action": "disable"}'

curl -X POST http://localhost:3001/_fault/error_spike \
  -H "Content-Type: application/json" \
  -d '{"action": "disable"}'
```

---

## 5. Current Limitations of the MVP

While the MVP demonstrates a complete event-driven pipeline, it was architected for proof-of-concept validation. The primary architectural limitations are:

| Area | Current MVP Implementation | Limitation / Vulnerability |
|---|---|---|
| **Multi-Tenancy** | Single-tenant database and single Kafka topic for all traffic. | Cannot host multiple distinct client organizations; no API key authentication or tenant isolation. |
| **Integration Pattern** | Reverse proxy only (`http-proxy`). | Requires clients to route all traffic through our reverse proxy server. Inflexible for clients with existing API gateways (Kong, AWS API Gateway, Cloudflare). |
| **Route Normalization** | Handcrafted prefix rules in `gateway/src/index.ts`. | Cannot automatically detect routes with complex REST hierarchies (e.g. `/api/v2/orgs/:orgId/projects/:pId/items/:itemId`). |
| **Anomaly Detection** | Hardcoded static thresholds (P95 > 150ms, errors > 5). | Produces false alarms during high-traffic sales events and misses subtle degradations on low-traffic endpoints. |
| **Data Storage Scalability** | Raw events written directly to standard PostgreSQL `requests` table. | At 10,000+ RPS, PostgreSQL write I/O and index maintenance will collapse under sustained write load. |
| **Analytics Processing** | Single Node.js in-memory ring buffer. | Cannot scale analytics horizontally across multiple worker nodes without losing window state or duplicating counts. |
| **AI Root Cause Analysis** | Architecture defined in specification, but automated autonomous tool-calling loop is not yet hooked up to live LLM APIs. | Incident narratives currently require manual query inspection. |
| **Security & Privacy** | Plaintext payload inspection with no redaction. | Sensitive client data (PII, credit card numbers, authorization headers, passwords) is captured without masking. |

---

## 6. Enterprise Production Roadmap: Going from MVP to Product

To transform this MVP into a commercial, enterprise-ready API Observability and Intelligence SaaS platform that any client can seamlessly integrate, the platform must undergo architectural evolution across six core pillars:

```
┌───────────────────────────────────────────────────────────────────────────────────────┐
│                           ENTERPRISE PRODUCT BLUEPRINT                                 │
├───────────────────┬───────────────────┬───────────────────┬───────────────────────────┤
│ 1. INGESTION      │ 2. PIPELINE       │ 3. ANALYTICS & ML │ 4. AI AGENT & ACTIONS     │
│ - Drop-in SDKs    │ - Multi-Tenant    │ - ClickHouse OLAP │ - LLM Tool-Calling Loop   │
│ - eBPF DaemonSet  │   Kafka Partition │ - Adaptive Baselines│ - Automated Rollbacks   │
│ - Cloudflare/Kong │ - Schema Registry │ - Contract Drift  │ - Slack/PagerDuty Alerts  │
└───────────────────┴───────────────────┴───────────────────┴───────────────────────────┘
```

---

### Pillar 1: Multiple Frictionless Client Ingestion Modes

Enterprise clients rarely want to redirect their production DNS through a third-party reverse proxy. We must offer **three integration modes**:

1. **Lightweight Drop-in Language SDKs (Zero Infrastructure Change)**:
   - Provide native middleware packages for Node.js (`@api-intel/express`, `@api-intel/fastify`), Python (`api-intel-django`, `api-intel-fastapi`), Go (`apiintel-gin`), and Java (`api-intel-spring-boot-starter`).
   - SDK hooks into request start and finish, batches telemetry in a background daemon thread, and pushes compressed protobuf batches to our Ingestion API via HTTP/2 or gRPC.
2. **API Gateway Plugins & Edge Proxies**:
   - Pre-built plugins for **Kong**, **Envoy**, **AWS API Gateway (CloudWatch/Kinesis integration)**, and **Cloudflare Workers**.
3. **eBPF DaemonSet (Zero-Code Kubernetes Ingestion)**:
   - A lightweight eBPF agent (deployed via Helm chart) running on client Kubernetes nodes that snoops HTTP/gRPC socket calls at the Linux kernel level without requiring code changes, language runtimes, or proxy hops.

---

### Pillar 2: Multi-Tenancy, Authentication & RBAC

1. **Tenant Identification & API Key Management**:
   - Every client organization receives an API Key and Tenant ID (`org_xxxxxxxx`).
   - Telemetry ingestion endpoints validate API keys in memory using high-speed Redis caches or Envoy external authorization.
2. **Data Partitioning & Isolation**:
   - **Kafka**: Message keys prefixed with `tenant_id:route_template`. Kafka partitions are allocated by tenant tier.
   - **Database**: Logical separation using PostgreSQL Row-Level Security (RLS) or dedicated tenant schemas, with tenant-isolated database roles.
3. **Role-Based Access Control (RBAC)**:
   - User roles: `Admin`, `DevOps/SRE`, `Developer`, `Read-Only Viewer`.
   - Single Sign-On (SSO) integration with SAML 2.0 and OIDC (Okta, Azure AD, Google Workspace).

---

### Pillar 3: High-Throughput Analytics & Specialized OLAP Storage

At production scale (100,000+ requests per second across hundreds of customers):
1. **Replace PostgreSQL for Raw Telemetry with ClickHouse**:
   - **ClickHouse** is a columnar analytical database capable of ingesting millions of rows per second and executing sub-second aggregations across billions of historical requests.
   - Use vectorized compression (ZSTD/Gorilla), partitioning by `toYYYYMM(timestamp)` and primary keys sorted by `(tenant_id, route_template, timestamp)`.
2. **Distributed Stream Processing (Apache Flink or Rust Workers)**:
   - Transition from the single-instance Node.js analytics worker to **Apache Flink** or a compiled Rust consumer pool.
   - Enables partitioned tumbling and sliding event-time windows with state stored in RocksDB checkpoints, providing fault tolerance and horizontal scale-out.

---

### Pillar 4: Statistical Machine Learning & Dynamic Anomaly Detection

Replace hardcoded thresholds with adaptive, statistical baselines:
1. **Dynamic Baselines (Seasonal EWMA & Z-Score)**:
   - Track rolling means and standard deviations over 1-hour, 24-hour, and 7-day cyclical windows.
   - An anomaly triggers when metric deviation exceeds $3\sigma$ ($Z > 3.0$), accounting for expected weekly traffic patterns (e.g. Monday morning surges).
2. **Automated Route Discovery & OpenAPI Drift Detection**:
   - Automatically infer path parameter templates using clustering algorithms (e.g. identifying that `/users/1`, `/users/2`, `/users/999` match `/users/:id`).
   - Ingest customer OpenAPI / Swagger specifications. Detect schema drift when upstream APIs return undeclared fields, wrong types, or missing required properties.

---

### Pillar 5: Production-Grade Autonomous AI Root-Cause Analysis (RCA)

Turn the platform from an alert sender into an **automated triage engineer**:
1. **Autonomous Tool-Calling Loop**:
   - When an incident is declared, an LLM agent (powered by Groq for sub-second inference, with fallback to Anthropic Claude 3.5 Sonnet / OpenAI GPT-4o) enters an investigative loop.
   - The agent is supplied with deterministic tools:
     - `getRouteMetrics(route, timerange)`: Queries ClickHouse for latency percentiles and error breakdown.
     - `getRecentDeployments(service, timerange)`: Queries GitHub/GitLab CI/CD webhooks for recent commits, PR descriptions, and deploy times.
     - `getSlowQueries(timerange)`: Inspects database query logs and pg_stat_statements.
     - `getSecurityEvents(timerange)`: Checks for credential stuffing, token enumeration, or DDoS attacks.
2. **Automated Post-Mortem Generation**:
   - The agent produces a structured markdown incident report with:
     - Root Cause Hypothesis with confidence score (e.g., *"Commit `7a8f9c` introduced an unindexed join on `orders.user_id` at 14:32 UTC"*).
     - Timeline of events.
     - Recommended remediation steps.

---

### Pillar 6: Security, Compliance & SaaS Onboarding

1. **PII Masking & Data Sanitization**:
   - In-memory regex and token scanners run inside the client SDK/Gateway before data ever leaves client premises.
   - Automatic masking of Authorization headers, API keys, passwords, Credit Cards (PCI-DSS), Social Security Numbers, and emails.
2. **Compliance**:
   - SOC 2 Type II certification, GDPR compliance (right to erasure / data retention controls), and HIPAA Business Associate Agreements (BAA).
3. **Outbound Notification Integrations**:
   - PagerDuty, Opsgenie, VictorOps for incident escalation.
   - Slack and Microsoft Teams interactive notification bots with "Acknowledge" and "Run AI Root Cause Analysis" buttons.
   - OpenTelemetry (OTLP) exporter allowing customers to feed metrics into Datadog, Dynatrace, New Relic, or Prometheus.
4. **Self-Service Onboarding Flow**:
   - A single-command onboarding script:
     ```bash
     npm install @api-intel/node
     ```
     ```typescript
     import { apiIntel } from '@api-intel/node';
     app.use(apiIntel({ apiKey: process.env.API_INTEL_KEY }));
     ```
   - Instantly validates connection and displays live requests on the customer's dashboard in under 60 seconds.
