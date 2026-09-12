# API Intelligence Platform — Execution Plan

A build-ready specification for an event-driven API observability and intelligence platform, designed to be executed end-to-end (MVP → advanced features → AI RCA → fault-injection testing) as a single connected effort.

---

## 1. Goals and Non-Goals

**Goal:** Build a reverse-proxy gateway that sits in front of any HTTP API, streams telemetry through Kafka, computes real-time metrics and anomalies, stores them in PostgreSQL, renders them on a live dashboard, and lets an LLM agent perform root-cause analysis using telemetry + deployment + log data as tools.

**Non-goals for v1:**
- Multi-tenant SaaS billing/auth-for-customers (single-tenant, local-first is fine)
- Full OpenTelemetry/Prometheus integration (stubbed as "later," per the brief)
- Horizontal auto-scaling — this is a portfolio-grade single-node system, built to be architecturally correct, not to survive Black Friday traffic.

**Success definition:** The fault-injection traffic generator triggers 8 distinct incident classes; the platform detects each one, correlates it with the right cause, and the AI agent produces a root-cause narrative matching (or reasonably close to) the injected fault, all visible on the dashboard within a few seconds of injection.

---

## 2. Architecture Overview

```
                     ┌─────────────────────┐
 Client / Test       │                     │
 Traffic Generator ─▶│  API Intelligence   │──▶ Customer API (upstream)
                     │  Gateway (proxy)    │      (your faulty test API)
                     │                     │
                     └─────────┬───────────┘
                               │ async telemetry events
                               ▼
                          ┌─────────┐
                          │  Kafka  │  (topics: raw-events, security-events,
                          │         │   deploy-events, anomalies)
                          └────┬────┘
                               ▼
                     ┌───────────────────┐
                     │ Analytics Service  │  (Kafka consumer group)
                     │ - windowed metrics │
                     │ - anomaly detectors│
                     │ - contract checker │
                     │ - health scorer    │
                     └────────┬───────────┘
                     ▼        │        ▼
                ┌────────┐    │   ┌─────────┐
                │ Redis  │◀───┴──▶│Postgres │
                │(hot,   │        │(durable,│
                │ rolling│        │ history,│
                │ stats) │        │ agg     │
                └────────┘        └────┬────┘
                                        ▼
                              ┌───────────────────┐
                              │  Dashboard (React) │◀── WebSocket/SSE (live)
                              │  + REST API layer   │
                              └────────┬────────────┘
                                       ▼
                              ┌───────────────────┐
                              │  AI RCA Agent       │
                              │  (tool-calling LLM) │
                              │  tools: query        │
                              │  metrics, logs,       │
                              │  deploys, DB stats     │
                              └───────────────────┘
```

Everything runs via **Docker Compose** as one system: gateway, analytics service, Postgres, Redis, Kafka (+ Zookeeper or KRaft), dashboard, test API, traffic generator, and the RCA agent service.

---

## 3. Tech Stack and Justification

| Layer | Choice | Why |
|---|---|---|
| **Gateway** | Node.js + TypeScript, built on `http-proxy` / `undici` primitives (not a framework like Express for the proxy path itself) | The gateway is on the hot path for every request, so it needs low per-request overhead and native streaming support for request/response bodies. Node's async I/O model is a natural fit for a proxy that mostly waits on network I/O. TypeScript gives you compile-time safety for the telemetry event schema, which matters because that schema is a contract shared with Kafka consumers and the DB layer. |
| **Streaming** | Apache Kafka (KRaft mode, no Zookeeper) | Telemetry is a firehose of independent events that must be durable, replayable, and consumable by multiple independent downstream systems (analytics, security detector, contract checker) without coupling them to the gateway. Kafka's partitioned log gives you replay (rebuild aggregates after a bug fix), backpressure isolation (analytics being slow doesn't slow the proxy), and is the direct mechanism for demonstrating "duplicate events" and "Kafka failure" fault scenarios called out in the testing strategy — those are hard to simulate meaningfully with a simple queue. KRaft mode removes the Zookeeper dependency, simplifying the Docker Compose setup with no functional loss for this scale. |
| **Analytics/Stream processing** | A plain Node.js/TypeScript Kafka consumer service (not Kafka Streams/Flink) | At this scale, a hand-rolled consumer with in-memory sliding windows (per-endpoint ring buffers for P50/P95/P99, error-rate counters) plus periodic flush to Postgres/Redis is far easier to reason about, debug, and extend with custom anomaly logic than standing up a JVM-based stream processing framework. It also keeps the whole backend in one language, which matters when an agent (human or AI) needs to reason across the gateway's event schema and the analytics service's consumption of it. |
| **Database** | PostgreSQL | Metrics, incidents, deployments, and health scores are naturally relational (foreign keys between incidents ↔ deployments ↔ endpoints), and you need real transactional guarantees for incident state changes. Postgres also supports `TimescaleDB`-style time-bucketing via native `date_trunc`/window functions without needing a separate time-series DB — good enough at this scale and one less moving part. JSONB columns handle variable-shaped payloads (e.g., raw anomaly detail blobs) without needing a schema migration for every new anomaly type. |
| **Caching / hot state** | Redis | Two distinct uses: (1) rolling-window counters and sketches (e.g., last-60-seconds request counts per endpoint, per-client rate tracking for security detection) that need sub-millisecond read/write and don't need durability, and (2) pub/sub to push live updates to the dashboard's WebSocket layer without hammering Postgres on every tick. |
| **Frontend/Dashboard** | React.js (Vite) + TypeScript + Tailwind + shadcn/ui | A pure client-side SPA is sufficient here — the dashboard has no SEO surface and no need for server-rendered pages, so Vite + React gives a much faster dev loop (instant HMR, no server-component/build-output complexity) than a full-stack framework would. It talks to the analytics service's REST API for initial data and a WebSocket connection for live ticks, keeping the dashboard a thin, stateless client. shadcn/ui (works the same way on plain React/Vite as on Next.js) still gives production-looking charts/tables/cards fast without hand-rolling a design system, which matters for a portfolio piece where visual polish sells the "product" framing. React Router handles the small set of routes (`/`, `/endpoints`, `/incidents`, `/security`, `/deployments`). |
| **Real-time transport** | WebSocket (via a small `ws` server co-located with the analytics service's REST API, fed by Redis pub/sub) | Dashboards need push, not poll, for a "live" feel. SSE would also work and is simpler, but WebSocket is chosen because it's bidirectional (useful later for dashboard-triggered actions like "re-run RCA" or "ack incident"). Since the dashboard is now a plain SPA with no backend of its own, the WS server lives in the analytics/API service rather than inside the frontend project. |
| **AI agent** | LLM with tool-calling via **Groq** (hosted, free-tier, OpenAI-compatible function-calling API — e.g. Llama 3.x / Qwen models) with **Ollama** (local, e.g. Llama 3.1 or Qwen2.5, which support tool-calling) as a fully offline fallback, orchestrated by a small Node/TypeScript agent service | Root cause analysis is fundamentally an investigative loop: look at metrics, decide what's suspicious, pull deployment history, pull DB query stats, form a hypothesis, verify against another data source, conclude. That loop maps directly onto tool-calling — give the model read-only tools (`getMetrics`, `getDeployments`, `getDbStats`, `getLogs`, `getAnomalies`) and let it decide which to call and in what order, rather than hand-coding a fixed diagnostic script. Groq is chosen over a paid API because it's free-tier and extremely low-latency (custom LPU inference), which matters for a multi-turn tool-calling loop where several round-trips happen per investigation; its API is OpenAI-compatible, so the same request/response and tool-schema shape works with minimal adapter code. Ollama is kept as a fallback/offline mode so the whole platform — including the AI layer — can run with zero external dependencies and zero API keys, which is valuable both for demos without internet and for keeping the project fully self-hostable. A thin provider-abstraction interface (`LlmProvider.chat(messages, tools)`) is used so the agent service isn't hard-wired to either backend. |
| **Infrastructure** | Docker Compose | Single-command, reproducible local environment covering 8+ services. No orchestration complexity (Kubernetes) is justified for a demo-scale system, and Compose is trivially readable by anyone reviewing the project. |
| **Test API + traffic generator** | Node.js/TypeScript, same language as the rest of the stack | Deliberately faulty by design (see §9). Kept in-stack so fault injection can be toggled via a simple control API instead of external chaos tooling — cheaper to build and easier to demo live. |

**Rejected alternatives worth naming (for the justification narrative):**
- *Kafka Streams/Flink* — rejected: JVM dependency and steep operational overhead not justified at this event volume; a plain consumer is more legible.
- *Prometheus + Grafana in place of custom dashboard* — rejected for v1 because the brief wants a bespoke dashboard demonstrating full-stack skill and tight coupling with the AI agent's UI (e.g., "explain this anomaly" button); Prometheus-compatible metrics are explicitly deferred to "later."
- *MongoDB for telemetry* — rejected: relational structure (endpoint → metrics → incidents → deployments) benefits from joins and constraints; document flexibility isn't actually needed once the event schema is fixed at the gateway.
- *GraphQL API layer* — rejected: dashboard's read patterns are simple and mostly time-series/tabular; REST + typed client is less overhead.
- *Next.js for the dashboard* — rejected: no SSR/SEO requirement exists for an internal-style observability dashboard, and a plain React/Vite SPA is simpler to build, deploy, and reason about when all data comes from the analytics service's own REST/WebSocket API anyway.
- *Paid hosted LLM APIs (e.g. Anthropic/OpenAI) as the only option* — rejected as a hard dependency: the brief calls for a "something free" AI layer, so Groq (free-tier, fast, tool-calling) is the primary path and Ollama (fully local, no API key, no cost) is kept as a drop-in fallback, keeping the entire platform runnable at zero cost.

---

## 4. Monorepo Structure

```
api-intel-platform/
├── docker-compose.yml
├── packages/
│   ├── gateway/              # reverse proxy + telemetry emitter
│   ├── analytics/            # Kafka consumer, aggregation, anomaly detection
│   ├── contract-monitor/     # OpenAPI diff engine (can live inside analytics)
│   ├── rca-agent/            # LLM tool-calling service
│   ├── dashboard/            # React (Vite) SPA
│   ├── test-api/             # deliberately faulty upstream service
│   ├── traffic-generator/    # load + fault injector
│   └── shared/                # shared TS types: TelemetryEvent, Anomaly, DeploymentEvent, DB schema types
├── infra/
│   ├── postgres/init.sql
│   └── kafka/topics.sh
└── README.md
```

`shared` is important: the telemetry event schema is the contract every service depends on, so it should be a single versioned TypeScript package imported everywhere (gateway producer, analytics consumer, RCA agent tool definitions), not duplicated.

---

## 5. Data Model (PostgreSQL)

Core tables (illustrative DDL, not exhaustive):

```sql
-- raw-ish, but pre-validated telemetry (bulk-inserted from analytics, batched)
CREATE TABLE requests (
  id BIGSERIAL PRIMARY KEY,
  request_id UUID NOT NULL,
  timestamp TIMESTAMPTZ NOT NULL,
  method TEXT NOT NULL,
  path TEXT NOT NULL,
  route_template TEXT,             -- normalized, e.g. /orders/:id
  status_code INT NOT NULL,
  latency_ms DOUBLE PRECISION NOT NULL,
  request_size_bytes INT,
  response_size_bytes INT,
  client_ip TEXT,
  client_id TEXT,                  -- API key / auth subject if available
  upstream_latency_ms DOUBLE PRECISION,
  db_latency_ms DOUBLE PRECISION
);
CREATE INDEX idx_requests_ts ON requests (timestamp);
CREATE INDEX idx_requests_route_ts ON requests (route_template, timestamp);

-- pre-aggregated rollups (written every N seconds by analytics service)
CREATE TABLE metrics_rollup (
  id BIGSERIAL PRIMARY KEY,
  bucket_start TIMESTAMPTZ NOT NULL,
  bucket_size_seconds INT NOT NULL,
  route_template TEXT NOT NULL,
  request_count INT NOT NULL,
  error_count INT NOT NULL,
  p50_latency_ms DOUBLE PRECISION,
  p95_latency_ms DOUBLE PRECISION,
  p99_latency_ms DOUBLE PRECISION,
  status_2xx INT, status_4xx INT, status_5xx INT
);

CREATE TABLE anomalies (
  id BIGSERIAL PRIMARY KEY,
  detected_at TIMESTAMPTZ NOT NULL,
  type TEXT NOT NULL,              -- latency_spike | error_spike | traffic_spike | unusual_endpoint | security | contract_violation
  severity TEXT NOT NULL,          -- low | medium | high | critical
  route_template TEXT,
  details JSONB NOT NULL,
  resolved_at TIMESTAMPTZ
);

CREATE TABLE deployments (
  id BIGSERIAL PRIMARY KEY,
  version TEXT NOT NULL,
  deployed_at TIMESTAMPTZ NOT NULL,
  commit_sha TEXT,
  description TEXT
);

CREATE TABLE incidents (
  id BIGSERIAL PRIMARY KEY,
  opened_at TIMESTAMPTZ NOT NULL,
  closed_at TIMESTAMPTZ,
  title TEXT NOT NULL,
  linked_anomaly_ids BIGINT[],
  linked_deployment_id BIGINT REFERENCES deployments(id),
  rca_summary TEXT,                -- filled in by the AI agent
  rca_raw JSONB                    -- full tool-call trace for auditability
);

CREATE TABLE health_scores (
  id BIGSERIAL PRIMARY KEY,
  computed_at TIMESTAMPTZ NOT NULL,
  performance_score NUMERIC(5,2),
  reliability_score NUMERIC(5,2),
  security_score NUMERIC(5,2),
  api_quality_score NUMERIC(5,2),
  overall_score NUMERIC(5,2)
);
```

Design rationale: raw `requests` rows are kept (bounded by a retention job, e.g. 24–72h) to support drill-down and the RCA agent's need for granular evidence; `metrics_rollup` is what the dashboard actually queries for charts, keeping dashboard queries cheap regardless of raw volume.

---

## 6. Kafka Topic Design

| Topic | Key | Purpose | Partitions (dev) |
|---|---|---|---|
| `telemetry.raw` | `route_template` | Every gateway request/response event | 3 |
| `telemetry.security` | `client_id`/`client_ip` | Auth events, failed logins, suspicious client behavior, emitted by gateway | 3 |
| `deployments.events` | `service` | Deployment markers (version, timestamp, commit) | 1 |
| `anomalies.detected` | `type` | Output of the analytics service, consumed by dashboard-push and RCA-trigger | 1 |

Keying by `route_template` keeps all events for one endpoint in the same partition, which matters because the analytics service computes per-route sliding-window percentiles — you want ordering guarantees within a route without needing global ordering across the whole topic.

**Idempotency / duplicate events:** each event carries a `request_id` (UUID generated at the gateway). The analytics consumer uses an upsert-by-`request_id` pattern (or a short-lived dedup set in Redis) before writing to Postgres, which is precisely what lets you test the "duplicate events" fault scenario meaningfully — you inject duplicates and confirm rollups don't double-count.

---

## 7. Gateway Design

Responsibilities:
1. Receive client request → generate `request_id`, capture start time, request size, headers of interest (auth subject if present).
2. Forward to upstream (customer/test API), streaming body through without full buffering where possible.
3. Capture response: status code, size, upstream latency (time to first byte + total).
4. Emit a `TelemetryEvent` to Kafka **asynchronously** (fire-and-forget with a bounded local buffer + retry) — the proxy response must never wait on Kafka. This is the central architectural rule: telemetry capture must be zero-blocking on the request path.
5. Also emit security-relevant sub-events (e.g., `POST /login` with 401, repeated across a client) to `telemetry.security`.
6. If Kafka is unreachable, buffer in memory (bounded ring buffer) and drop with a counter once full, rather than backpressuring the client — this is also your "Kafka failure" fault scenario hook: you can kill the Kafka container and verify the gateway keeps serving traffic while a `dropped_events` metric climbs.

Why fire-and-forget + local buffer instead of synchronous produce: a gateway that blocks client requests on a downstream analytics dependency defeats the purpose of a "transparent" intelligence layer — it must add negligible latency and must degrade gracefully.

---

## 8. Analytics Service Design

Runs as a Kafka consumer group with (at least) two logical workers, both readable as one Node process for v1:

**8.1 Metrics aggregator**
- Maintains an in-memory sliding window (e.g., t-digest or a simple reservoir sampler) per `route_template` for latency percentiles, refreshed every few seconds.
- Every N seconds: flush current window state as a `metrics_rollup` row, push the same payload to Redis (`route:{template}:latest`) and publish to a Redis pub/sub channel the dashboard's WS server subscribes to.

**8.2 Anomaly detectors** (each a small, independently testable function fed the same event stream / rollups)

| Detector | Approach | Justification |
|---|---|---|
| Latency spike | Compare current P95 in window vs. trailing baseline (e.g., median of last 30 windows); flag if ratio > threshold (e.g., 3×) and sustained for ≥2 consecutive windows | Simple statistical baseline avoids false positives from single noisy requests; sustained-window requirement is standard practice to reduce flapping. |
| Error spike | Error rate (5xx / total) per window vs. baseline; absolute floor (e.g., ignore if <5 req/window, too few samples) | Same baseline-deviation approach; floor avoids noise on low-traffic endpoints. |
| Traffic spike/flood | Request-count z-score vs. trailing window average | Z-score is a cheap, explainable anomaly signal appropriate for demo-scale data. |
| Unusual endpoint usage | Track known route set; flag calls to routes not seen in the OpenAPI spec or not seen in the last N days ("endpoint enumeration" overlaps here) | Doubles as an early signal for the security detector. |
| Login flood / brute force | Redis-based sliding counter per `client_ip`/`client_id` on auth-endpoint failures; threshold + time window | Redis counters are the right tool for exactly this kind of high-frequency, short-TTL counting. |
| Endpoint enumeration | Track distinct-404-path count per client within a window (Redis `HyperLogLog` or simple set) | A client hitting many distinct nonexistent paths is a classic enumeration signature; HLL keeps memory bounded. |
| Abnormal client | Compare a client's request pattern (endpoint diversity, request rate, error rate) against a rolling per-client baseline | Cheap behavioral fingerprinting sufficient for demo purposes; explicitly not a full UEBA system. |

All detectors write to `anomalies` (Postgres) and publish to `anomalies.detected` (Kafka) so the RCA agent and dashboard can react independently.

**8.3 OpenAPI contract monitor**
- Load the customer API's OpenAPI spec at startup (mounted file or fetched from a `/openapi.json` endpoint).
- For every request event, validate: (a) response schema matches the declared schema for that status code (using a JSON Schema validator against the OpenAPI-derived schema), (b) status code returned is one of the declared possible codes, (c) required response headers/fields present.
- Emit a `contract_violation` anomaly with a diff of expected vs. actual shape.
- Justification: this is what turns the platform from "generic APM" into "API-specific intelligence" — it's a differentiator explicitly called out in the brief and is cheap to implement with existing OpenAPI-JSON-Schema tooling (e.g., `ajv` + `openapi-schema-to-json-schema`).

**8.4 Deployment correlation**
- On a `deployments.events` message, store the deployment.
- When any anomaly fires, check: was there a deployment in the last N minutes (configurable, default 15) touching the same service? If so, attach `linked_deployment_id` to the resulting incident automatically.
- Justification: this is a simple time-window join, not ML — correctly scoped for the effort level, and it's exactly what the worked example in the brief describes.

**8.5 Health scoring**
- Computed periodically (e.g., every minute) as a weighted composite:
  - *Performance*: normalized inverse of P95 latency vs. target SLA.
  - *Reliability*: 1 − error rate, weighted by traffic volume.
  - *Security*: inverse function of open/recent security anomalies (decayed over time).
  - *API quality*: 1 − contract-violation rate.
  - *Overall*: weighted average (weights configurable, documented in code) — deliberately transparent/explainable rather than a black-box ML score, so it can be reasoned about and defended in an interview.

---

## 9. Test API + Traffic Generator (Fault Injection Harness)

This is the most important part of the story: the platform is only demonstrably good if you can *prove* detection.

**Test API (`packages/test-api`)**
- Small Express/Fastify service exposing a realistic small surface: `/login`, `/orders`, `/orders/:id`, `/users/:id`, `/products`.
- Backed by a real Postgres table (a *second* Postgres DB or schema, separate from the platform's own DB) so "slow query" faults are real, not simulated.
- Exposes a **control endpoint** (`POST /_fault/:type`) to toggle fault modes at runtime:
  - `latency_spike`: inject `setTimeout` delay on a target route.
  - `error_spike`: force a percentage of requests on a route to return 500.
  - `slow_query`: switch a query to an intentionally unindexed/sequential-scan version (e.g., drop an index at runtime, or use a query with a function-wrapped column preventing index use).
  - `schema_violation`: return a response missing a required field or wrong type, breaking the OpenAPI contract.
- Deployment simulation: a small script that "deploys" a new version (writes a `deployments` row + optionally toggles a fault) to let you demo the exact worked example from the brief (query regression tied to a deployment marker).

**Traffic generator (`packages/traffic-generator`)**
- Configurable steady-state load generator (e.g., realistic mixed traffic across routes, some legitimate variance).
- Scenario scripts, one per fault class called out in the brief:
  1. Latency spike
  2. 500-error spike
  3. Traffic flood
  4. Login flood (repeated failed `/login` from one simulated client)
  5. Schema violations
  6. Slow DB queries
  7. Kafka failures (script pauses/kills the Kafka container via Docker API or `docker compose stop kafka`)
  8. Duplicate events (replays already-sent request IDs directly onto the Kafka topic)
- Each scenario script asserts, against the platform's own API, that: (a) an anomaly of the expected type appears within X seconds, (b) if applicable, an incident with correct deployment correlation is created, (c) the RCA agent's summary mentions the right causal factor (simple keyword/semantic check, not exact match).

This closes the loop and is what makes "measure whether your platform correctly detects and explains them" verifiable rather than aspirational.

---

## 10. AI Root-Cause Analysis Agent

**Architecture:** a stateless service exposing `POST /rca/investigate { incidentId }`, invoked automatically when an incident is opened (or manually from the dashboard).

**Tools exposed to the LLM (all read-only):**
- `getMetricsForRoute(route, timeRange)` → rollup history (latency percentiles, error rate over time)
- `getAnomaliesNear(timestamp, windowMinutes)` → related anomalies
- `getRecentDeployments(service, windowMinutes)` → deployment log
- `getDbQueryStats(route, timeRange)` → DB latency + (in test-api) query plan info if available
- `getRawSamples(route, timeRange, limit)` → a handful of raw request rows for concrete evidence (status codes, sizes, sample errors)

**Provider:** the agent talks to Groq's OpenAI-compatible `/chat/completions` endpoint (a free-tier API key, model such as Llama 3.3 70B or Qwen2.5 32B — both support function/tool calling) by default. A local Ollama instance (same models family, pulled locally) is wired in behind the same `LlmProvider` interface as a no-cost, no-API-key fallback, selectable via an environment variable (`LLM_PROVIDER=groq|ollama`) — useful for offline demos or if Groq's rate limits are hit during testing. Both expose the same OpenAI-style tool-calling contract (`tools: [...]`, `tool_calls` in the response), so the agent's orchestration loop is provider-agnostic.

**Loop:** the agent is given the triggering anomaly/incident as context, then allowed multiple tool-calling turns (bounded, e.g., max 6) to gather evidence before producing a structured conclusion:
```json
{
  "summary": "…",
  "likely_root_cause": "…",
  "confidence": "high|medium|low",
  "evidence": [ { "tool": "...", "finding": "..." } ],
  "recommended_action": "…"
}
```
This is stored in `incidents.rca_summary` / `rca_raw` and pushed to the dashboard.

**Why tool-calling over a single big prompt with all data dumped in:** (1) token efficiency — most incidents don't need all five data sources; (2) it mirrors how a human SRE actually investigates (form hypothesis → check one signal → refine); (3) `rca_raw`'s full tool trace gives you an auditable "show your work" artifact, which is a much stronger demo than a black-box paragraph.

---

## 11. Dashboard (React.js SPA)

**Pages** (via React Router):
- `/` — Overview: overall health score, request volume sparkline, active anomalies/incidents feed (live via WS).
- `/endpoints` — Table of routes with P50/P95/P99, error rate, volume, sortable; click into `/endpoints/:route` for a detail view (latency histogram, status-code distribution, contract-violation history).
- `/incidents` — List + detail view; detail view shows timeline (anomaly → deployment → RCA conclusion) and the agent's evidence trace.
- `/security` — Login-flood/enumeration/abnormal-client events.
- `/deployments` — Deployment log with linked incidents.

**Data fetching:** Since the dashboard is a plain React/Vite SPA with no server-side rendering, all data comes from HTTP: a small typed REST API exposed by the analytics service (query Postgres via a typed query layer, e.g. `drizzle-orm` or `kysely`) is fetched on mount/route-change with `@tanstack/react-query` for caching, retries, and background refetch. A single WebSocket connection (opened once at the app shell level, via a small `useLiveMetrics` hook and React Context) subscribes to the analytics service's Redis-pub/sub-backed WS endpoint and pushes incremental updates into the query cache, so charts update live without polling. This keeps the frontend fully decoupled from the backend's internals — it only ever talks to one REST+WS API surface, which also makes it trivial to swap the dashboard's dev server for a static build served by Nginx/Docker in production.

---

## 12. Build Order (Execution Sequence for the Agent)

This is the order in which an agent should implement the system so that each stage is independently runnable/testable before the next depends on it:

1. **Scaffold monorepo** + `shared` package with `TelemetryEvent`, `Anomaly`, `Deployment` types and Zod schemas for runtime validation.
2. **Postgres schema + Docker Compose skeleton** (Postgres, Redis, Kafka in KRaft mode) — verify all containers boot and are reachable.
3. **Test API** with the 5 routes and its own DB-backed data, no faults yet — confirm it runs standalone.
4. **Gateway v1**: pure reverse proxy (no telemetry) — confirm client → gateway → test API round-trips correctly.
5. **Gateway v2**: add telemetry capture + async Kafka producer + local buffering/backpressure handling.
6. **Analytics service v1**: consume `telemetry.raw`, compute rollups, write to Postgres + Redis, no anomaly detection yet.
7. **Dashboard v1**: Overview + Endpoints pages reading rollups from Postgres (no live updates yet) — confirm real metrics render from real proxied traffic.
8. **Traffic generator v1**: steady-state realistic load — use this to populate meaningful dashboard data.
9. **WebSocket live layer**: Redis pub/sub → WS server → dashboard client hooks — confirm charts move in real time.
10. **Anomaly detectors** (latency/error/traffic/unusual-endpoint) — wire into `anomalies` table + `anomalies.detected` topic + dashboard incident feed.
11. **Fault injection in test API + first 4 scenario scripts** (latency spike, error spike, traffic flood, schema-adjacent groundwork) — validate detectors actually fire.
12. **Security telemetry + login-flood/enumeration/abnormal-client detectors** + scenario scripts 4–5 (login flood covered here).
13. **OpenAPI contract monitor** + schema-violation fault + scenario script.
14. **Deployments table + deployment simulation** + deployment-correlation logic in analytics + `/deployments` dashboard page.
15. **Slow-query fault** (real Postgres index toggling in test-api) + DB-latency capture in gateway/test-api instrumentation.
16. **Health scoring** job + dashboard overview card.
17. **RCA agent service** with the 5 read-only tools + incident-triggered investigation + `/incidents` detail view rendering the evidence trace.
18. **Kafka-failure and duplicate-events scenario scripts** (exercise gateway's degrade-gracefully path and analytics' dedup path).
19. **End-to-end validation pass**: run all 8 fault scenarios against the fully wired system, confirm detection + correlation + RCA summaries, fix gaps.
20. **Polish pass**: README with architecture diagram, one-command `docker compose up`, seeded demo data script, and a short "recorded scenario" walkthrough documented for portfolio presentation.

This ordering ensures every stage produces a runnable, demonstrable system rather than a large batch of untestable code, and each fault-injection scenario is validated against the specific subsystem that was *just* built, catching integration bugs early.

---

## 13. Acceptance Criteria (Definition of Done)

- `docker compose up` brings up all services with no manual steps.
- Proxied traffic through the gateway reaches the test API with correctly recorded telemetry (spot-check a raw request row).
- Dashboard shows live-updating P50/P95/P99, request volume, and status-code distribution within ~5 seconds of generated traffic.
- All 8 fault-injection scenarios produce the expected anomaly type in `anomalies` within a defined SLA (e.g., ≤15s for statistical detectors, immediate for rule-based ones like login flood).
- The deployment-correlation example from the brief is reproducible: inject a slow-query fault immediately after a simulated deployment, confirm the resulting incident links to that deployment.
- The RCA agent's summary for that scenario correctly names "database query" and "recent deployment" as contributing factors, with a visible tool-call evidence trace.
- Killing the Kafka container does not take down the gateway (requests still succeed; a dropped-event counter increases).
- Duplicate events replayed onto Kafka do not double-count in `metrics_rollup`.

---

## 14. Suggested Effort Framing for the Portfolio Write-up

Frame the final README around the causal chain, not the feature list: *"telemetry → anomaly → correlation → explanation"* is the narrative arc worth stating explicitly, since it's what separates this from a generic monitoring dashboard and is the strongest one-line pitch identified in the brief itself.
