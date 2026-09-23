# API Intelligence Platform

An **event-driven API observability and intelligence platform** that acts as an intelligent proxy in front of any HTTP API — capturing telemetry, detecting anomalies, correlating incidents with deployments, and performing AI-powered root-cause analysis.

```
 Your API Clients
       │
       ▼  HTTP (all traffic routed here)
 ┌─────────────┐   async telemetry    ┌──────────┐
 │   Gateway   │────────────────────▶│  Kafka   │
 │  Port 3000  │  (fire-and-forget)   │  Broker  │
 └──────┬──────┘                      └────┬─────┘
        │  proxies to                      │
        ▼                                  ▼
 ┌─────────────┐               ┌───────────────────┐
 │  Your API   │               │  Analytics Engine │
 │  Port 3001  │               │  Port 3002        │
 └─────────────┘               │  · Rollup metrics │
                                │  · Anomaly detect │
                                │  · Health scoring │
                                └────┬──────────────┘
                                     │
                          ┌──────────┴──────────┐
                          ▼                     ▼
                     ┌─────────┐         ┌─────────────┐
                     │Postgres │         │    Redis    │
                     │(history)│         │ (hot state) │
                     └─────────┘         └──────┬──────┘
                                                │ WebSocket push
                                                ▼
                                     ┌──────────────────┐
                                     │   Dashboard SPA  │
                                     │   Port 5173      │
                                     └──────────────────┘
                                                │
                                     ┌──────────▼──────────┐
                                     │    AI RCA Agent     │
                                     │    Port 3003        │
                                     │  (Groq / Ollama)    │
                                     └─────────────────────┘
```

---

## Quick Start (One Command)

```bash
# 1. Clone and enter
git clone <repo-url>
cd api_monitor

# 2. Configure (optional — AI RCA requires a Groq key)
cp .env.example .env
# Edit .env and set GROQ_API_KEY=gsk_... (free at console.groq.com)

# 3. Install dependencies (needed for scenario scripts on host)
npm install

# 4. Start the platform
docker compose up --build

# 5. Open the dashboard
open http://localhost:5173

# 6. Start generating traffic (in a new terminal)
docker compose --profile traffic up traffic-generator
```

> **Kafka takes ~30 seconds to initialize.** If services fail on first start, run `docker compose restart gateway analytics` after ~30s.

---

## Services & Ports

| Service | Port | Description |
|---|---|---|
| **Gateway** | `3000` | Reverse proxy — point your clients here |
| **Test API** | `3001` | Built-in fault-injectable mock upstream |
| **Analytics** | `3002` | REST + WebSocket API, Kafka consumer |
| **RCA Agent** | `3003` | AI root-cause analysis service |
| **Dashboard** | `5173` | React SPA (dev) / `80` (Docker) |
| **PostgreSQL** | `5432` | Persistent metrics, incidents, anomalies |
| **Redis** | `6379` | Hot state cache + pub/sub |
| **Kafka** | `9092` | Event streaming backbone |

---

## Integrating Your Own API

The platform is designed to monitor **any HTTP API**. There are two ways to connect:

### Option A: Point the Gateway at your API (Recommended)

Set `UPSTREAM_URL` to your API's address:

```bash
# In docker-compose.yml, change the gateway environment:
UPSTREAM_URL: http://your-api-host:your-api-port

# Or override at startup:
UPSTREAM_URL=http://my-api:8080 docker compose up gateway
```

All traffic sent to `http://localhost:3000` will be transparently proxied to your API. The gateway adds zero measurable latency (telemetry is async/fire-and-forget).

**Client configuration change:** update your clients to call `http://localhost:3000` instead of your API directly.

### Option B: Dev Mode (run everything locally)

```bash
# Terminal 1 — Infrastructure only
docker compose up postgres redis kafka

# Terminal 2 — Test API (or replace with your own API on port 3001)
cd packages/test-api && npm run dev

# Terminal 3 — Gateway
UPSTREAM_URL=http://localhost:3001 cd packages/gateway && npm run dev

# Terminal 4 — Analytics
cd packages/analytics && npm run dev

# Terminal 5 — RCA Agent
GROQ_API_KEY=your_key cd packages/rca-agent && npm run dev

# Terminal 6 — Dashboard
cd packages/dashboard && npm run dev

# Terminal 7 — Traffic (optional)
cd packages/traffic-generator && npm run dev
```

### Option C: Route normalize your API paths

If your API uses parametric routes (e.g. `/api/v1/users/123/orders/456`), update the `getRouteTemplate` function in [`packages/gateway/src/index.ts`](packages/gateway/src/index.ts) to match your URL structure. Without normalization, every unique ID creates a separate metric timeseries.

---

## Environment Variables

Copy `.env.example` to `.env` in the project root:

```bash
cp .env.example .env
```

| Variable | Default | Description |
|---|---|---|
| `GROQ_API_KEY` | *(empty)* | Free key from [console.groq.com](https://console.groq.com) — enables AI RCA |
| `LLM_PROVIDER` | `none` | `groq` \| `ollama` \| `none` |
| `OLLAMA_BASE_URL` | `http://host.docker.internal:11434/v1` | Ollama API URL (for local LLM) |
| `OLLAMA_MODEL` | `llama3.1` | Ollama model name |
| `GROQ_MODEL` | *(Auto-detected)* | Groq model name (auto-detects available model if unset) |
| `UPSTREAM_URL` | `http://test-api:3001` | Your upstream API address (gateway) |

### Configuring the AI RCA Agent

**Option 1: Groq (Recommended — free, fast)**
1. Create a free account at [console.groq.com](https://console.groq.com)
2. Generate an API key
3. Set `GROQ_API_KEY=gsk_...` and `LLM_PROVIDER=groq` in `.env`

**Option 2: Ollama (fully local, no API key)**
1. Install [Ollama](https://ollama.ai)
2. Pull a model: `ollama pull llama3.1`
3. Set `LLM_PROVIDER=ollama` in `.env`
4. On Windows/Mac with Docker: Ollama runs on the host, Docker accesses it via `host.docker.internal`

**Option 3: Disabled (no AI)**
Leave `LLM_PROVIDER=none` (default). The platform works fully — incidents open, anomalies detect, deployments correlate — just without AI narrative summaries.

---

## Fault Injection Scenarios

The platform ships with 9 automated test scenarios that inject faults and assert detection.

### Running Individual Scenarios

Prerequisites: all services running (docker compose up or dev mode).

```bash
cd packages/traffic-generator

# Run one scenario
npm run scenario:01   # Latency Spike
npm run scenario:02   # Error Spike (500s)
npm run scenario:03   # Traffic Flood
npm run scenario:04   # Login Flood (brute force)
npm run scenario:05   # Endpoint Enumeration (scanner)
npm run scenario:05b  # Schema Violation (OpenAPI contract breach)
npm run scenario:06   # Deployment Correlation (deploy → regression)
npm run scenario:07   # Slow DB Query
npm run scenario:08   # Kafka Failure (gateway resilience)
npm run scenario:09   # Duplicate Events (deduplication)
```

### Running All Scenarios (Full Validation)

```bash
cd packages/traffic-generator
npm run validate
```

This runs all 9 scenarios sequentially and prints a summary table:

```
═══════════════════════════════════════════════════════════════
                   END-TO-END VALIDATION RESULTS
═══════════════════════════════════════════════════════════════

Scenario                       Status   Duration
───────────────────────────────────────────────────────────
01: Latency Spike              PASS     48.2s
02: Error Spike                PASS     52.1s
03: Traffic Flood              PASS     55.8s
04: Login Flood                PASS     28.4s
05: Endpoint Enumeration       PASS     35.1s
05b: Schema Violation          PASS     44.6s
06: Deployment Correlation     PASS     58.3s
07: Slow Query                 PASS     52.9s
08: Kafka Failure              PASS     40.2s
09: Duplicate Events           PASS     18.5s
───────────────────────────────────────────────────────────

Total: 10 scenarios | 10 PASS | 0 FAIL

✓ All scenarios passed — platform validated!
```

### Manual Fault Injection via curl

You can also trigger faults directly against the Test API:

```bash
# Enable a 3-second latency delay on /orders
curl -X POST http://localhost:3001/_fault/latency_spike \
  -H "Content-Type: application/json" \
  -d '{"action": "enable", "targetRoute": "/orders"}'

# Enable 80% 500-error rate globally
curl -X POST http://localhost:3001/_fault/error_spike \
  -H "Content-Type: application/json" \
  -d '{"action": "enable", "rate": 0.8}'

# Enable slow unindexed DB query on /users/:id
curl -X POST http://localhost:3001/_fault/slow_query \
  -H "Content-Type: application/json" \
  -d '{"action": "enable"}'

# Trigger schema violation on /orders/:id responses
curl -X POST http://localhost:3001/_fault/schema_violation \
  -H "Content-Type: application/json" \
  -d '{"action": "enable"}'

# Disable any fault
curl -X POST http://localhost:3001/_fault/latency_spike \
  -H "Content-Type: application/json" \
  -d '{"action": "disable"}'
```

### Simulate a Deployment

Record a deployment marker from the dashboard (**Deployments → Simulate Deploy**) or via API:

```bash
curl -X POST http://localhost:3002/api/deployments \
  -H "Content-Type: application/json" \
  -d '{
    "version": "v2.1.0",
    "commit_sha": "abc1234",
    "description": "Optimized user query — removed covering index"
  }'
```

If you then trigger a `slow_query` fault, the resulting incident will automatically link to this deployment record.

---

## Analytics REST API Reference

All endpoints are served by the Analytics service at `http://localhost:3002`.

### Metrics

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/metrics` | Aggregated route metrics (last 30 min) |
| `GET` | `/api/metrics?route=/orders/:id` | Filter by route |
| `GET` | `/api/metrics/history/:route?minutes=60` | Historical rollups for a route |
| `GET` | `/api/requests/samples?route=...&limit=20` | Raw request samples |

### Anomalies

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/anomalies` | List anomalies (last 24h) |
| `GET` | `/api/anomalies?type=latency_spike&severity=high` | Filter anomalies |
| `PATCH` | `/api/anomalies/:id/resolve` | Mark anomaly as resolved |

### Incidents

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/incidents` | List all incidents |
| `GET` | `/api/incidents/:id` | Get incident details |
| `POST` | `/api/incidents/:id/rca` | Trigger/re-run AI RCA for an incident |
| `PATCH` | `/api/incidents/:id/close` | Close an incident |

### Deployments

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/deployments` | List deployments with incident counts |
| `POST` | `/api/deployments` | Record a new deployment |

### Health & Security

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/health` | Latest health score (updates every 60s) |
| `GET` | `/api/security` | Security anomaly events |
| `GET` | `/api/status` | Platform liveness check |

### WebSocket (Live Updates)

Connect to `ws://localhost:3002` to receive real-time push events:

```javascript
const ws = new WebSocket('ws://localhost:3002');
ws.onmessage = (event) => {
  const msg = JSON.parse(event.data);
  // msg.type: 'metrics' | 'health_score' | 'incident_opened'
  // msg.data: the payload
};
```

---

## Monorepo Structure

```
api_monitor/
├── docker-compose.yml              # Full-stack container setup
├── .env.example                    # Environment variable template
├── infra/
│   └── postgres/init.sql           # DB schema + seed data
└── packages/
    ├── shared/                     # Shared Zod schemas & TypeScript types
    │   └── src/index.ts            # TelemetryEvent, Anomaly, SecurityEvent, DeploymentEvent
    ├── gateway/                    # Reverse proxy + Kafka telemetry emitter (Port 3000)
    │   └── src/index.ts
    ├── analytics/                  # Stream aggregation, REST/WS API, anomaly detection (Port 3002)
    │   └── src/
    │       ├── index.ts            # Main service + all REST endpoints
    │       ├── detectors.ts        # Anomaly detectors (latency, error, traffic, security)
    │       └── contract-monitor.ts # OpenAPI response contract validation
    ├── rca-agent/                  # AI root-cause analysis service (Port 3003)
    │   └── src/
    │       ├── index.ts            # Express server
    │       ├── agent.ts            # Multi-turn LLM investigation loop
    │       ├── tools.ts            # 5 read-only data tools
    │       └── providers.ts        # Groq / Ollama / no-op provider abstraction
    ├── dashboard/                  # React 19 + Vite SPA (Port 5173 dev, 80 Docker)
    │   └── src/App.tsx             # Overview, Endpoints, Incidents, Security, Deployments
    ├── test-api/                   # Fault-injectable upstream mock API (Port 3001)
    │   └── src/index.ts
    └── traffic-generator/          # Steady-state load + 9 fault-injection scenarios
        └── src/
            ├── index.ts            # Steady-state traffic generator
            ├── run-all-scenarios.ts # E2E validation orchestrator
            └── scenarios/
                ├── utils.ts
                ├── 01-latency-spike.ts
                ├── 02-error-spike.ts
                ├── 03-traffic-flood.ts
                ├── 04-login-flood.ts
                ├── 05-endpoint-enumeration.ts
                ├── 05b-schema-violation.ts
                ├── 06-deployment-correlation.ts
                ├── 07-slow-query.ts
                ├── 08-kafka-failure.ts
                └── 09-duplicate-events.ts
```

---

## How Anomaly Detection Works

Each 5-second telemetry window triggers the detector pipeline:

| Detector | Signal | Threshold |
|---|---|---|
| **Latency Spike** | Current P95 vs. trailing 10-window median | >2.5× the baseline, with ≥5 samples |
| **Error Spike** | Error rate vs. trailing baseline | +10pp above baseline AND >15% error rate |
| **Traffic Flood** | Request count Z-score vs. trailing 10 windows | Z-score >2.5 with ≥5 baseline windows |
| **Unusual Endpoint** | New route not seen in previous windows | Fires once on first-ever observation |
| **Login Flood** | Failed login count per client | 10+ failures in 60 seconds |
| **Endpoint Enumeration** | Distinct unknown paths per client | 15+ distinct probed paths |
| **Contract Violation** | Response validates against embedded OpenAPI schema | Undeclared status code or undersized response body |

All anomalies with `high` or `critical` severity **automatically open an incident** and trigger the AI RCA agent (if configured).

---

## Dashboard Pages

| Page | URL | What you see |
|---|---|---|
| **Overview** | `/` | Health scores, live route table, recent anomalies feed |
| **Endpoints** | `/endpoints` | P50/P95/P99 latency, error %, status code breakdown per route |
| **Incidents** | `/incidents` | Open/closed incident list, AI RCA summary, evidence trace, re-run button |
| **Security** | `/security` | Login flood, enumeration, and security anomaly events |
| **Deployments** | `/deployments` | Deployment log with incident correlation + "Simulate Deploy" form |

---

## Acceptance Criteria (Platform Validated When)

- [ ] `docker compose up` starts all services with no manual steps
- [ ] Traffic through the gateway appears on the dashboard within ~5 seconds
- [ ] Scenario 01: `latency_spike` anomaly fires within 30s of enabling fault
- [ ] Scenario 02: `error_spike` anomaly fires within 30s of enabling fault
- [ ] Scenario 06: incident correctly links to the deployment marker
- [ ] Scenario 08: gateway continues serving requests with Kafka stopped
- [ ] Scenario 09: replayed events don't double-count in `metrics_rollup`
- [ ] AI RCA summary populates on the incident detail page (with `GROQ_API_KEY`)

---

## Tech Stack

| Layer | Technology | Rationale |
|---|---|---|
| Gateway | Node.js `http` + `http-proxy` | Zero-latency proxy, native streaming, fire-and-forget telemetry |
| Streaming | Apache Kafka (KRaft, no Zookeeper) | Durable, replayable, partitioned by route for ordering guarantees |
| Analytics | Node.js Kafka consumer + in-memory windows | Simple, debuggable, keeps backend in one language |
| Database | PostgreSQL 15 | Relational (incidents ↔ deployments ↔ anomalies), JSONB for blobs |
| Hot Cache | Redis | Sub-ms rolling counters + pub/sub for live dashboard push |
| Dashboard | React 19 + Vite + Tailwind + TanStack Query | Fast SPA dev loop, WebSocket-native live updates |
| AI Agent | Groq (free tier, Llama 3.3 70B) / Ollama | Tool-calling loop, OpenAI-compatible, zero-cost options |
| Shared Types | TypeScript + Zod | Runtime-validated contract across all services |
| Infrastructure | Docker Compose | Single-command reproducible local environment |
