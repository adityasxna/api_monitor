-- API Intelligence Platform Postgres Schema

CREATE TABLE requests (
  id BIGSERIAL PRIMARY KEY,
  request_id UUID NOT NULL,
  timestamp TIMESTAMPTZ NOT NULL,
  method TEXT NOT NULL,
  path TEXT NOT NULL,
  route_template TEXT,
  status_code INT NOT NULL,
  latency_ms DOUBLE PRECISION NOT NULL,
  request_size_bytes INT,
  response_size_bytes INT,
  client_ip TEXT,
  client_id TEXT,
  upstream_latency_ms DOUBLE PRECISION,
  db_latency_ms DOUBLE PRECISION,
  UNIQUE(request_id)
);
CREATE INDEX idx_requests_ts ON requests (timestamp);
CREATE INDEX idx_requests_route_ts ON requests (route_template, timestamp);
CREATE INDEX idx_requests_req_id ON requests (request_id);

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
CREATE INDEX idx_metrics_rollup_ts ON metrics_rollup (bucket_start);

CREATE TABLE anomalies (
  id BIGSERIAL PRIMARY KEY,
  detected_at TIMESTAMPTZ NOT NULL,
  type TEXT NOT NULL,
  severity TEXT NOT NULL,
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
  rca_summary TEXT,
  rca_raw JSONB
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

-- Test API specific schema for fault injection (slow query)
CREATE SCHEMA test_api;
CREATE TABLE test_api.orders (
  id BIGSERIAL PRIMARY KEY,
  user_id BIGINT NOT NULL,
  total_amount NUMERIC(10,2) NOT NULL,
  status TEXT NOT NULL,
  created_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE TABLE test_api.users (
  id BIGSERIAL PRIMARY KEY,
  email TEXT NOT NULL,
  name TEXT NOT NULL,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Index for slow query demo: We will create it here and drop it dynamically during fault injection
CREATE INDEX idx_orders_user_id ON test_api.orders(user_id);

-- Insert dummy data
INSERT INTO test_api.users (email, name)
SELECT 'user' || generate_series(1, 100) || '@example.com', 'User ' || generate_series(1, 100);

INSERT INTO test_api.orders (user_id, total_amount, status)
SELECT 
  (random() * 99 + 1)::INT,
  (random() * 1000)::NUMERIC(10,2),
  CASE WHEN random() > 0.5 THEN 'completed' ELSE 'pending' END
FROM generate_series(1, 10000);
