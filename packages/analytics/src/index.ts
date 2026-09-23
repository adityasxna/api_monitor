import { Kafka } from 'kafkajs';
import { Pool } from 'pg';
import { createClient } from 'redis';
import express from 'express';
import cors from 'cors';
import { WebSocketServer, WebSocket } from 'ws';
import { TelemetryEvent } from '@api-intelligence/shared';
import { detectAnomalies, detectSecurityAnomaly } from './detectors';
import { validateContract } from './contract-monitor';

const PORT = process.env.PORT || 3002;
const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';
const RCA_AGENT_URL = process.env.RCA_AGENT_URL || 'http://localhost:3003';

const pool = new Pool({
  user: process.env.POSTGRES_USER || 'api_monitor',
  host: process.env.POSTGRES_HOST || 'localhost',
  database: process.env.POSTGRES_DB || 'api_monitor',
  password: process.env.POSTGRES_PASSWORD || 'password',
  port: parseInt(process.env.POSTGRES_PORT || '5432', 10),
});

const redisClient = createClient({ url: REDIS_URL });
redisClient.on('error', err => console.error('Redis error', err));

const redisSubClient = redisClient.duplicate();
redisSubClient.on('error', err => console.error('Redis Sub error', err));

const kafka = new Kafka({ clientId: 'analytics', brokers: KAFKA_BROKERS });
const consumer = kafka.consumer({ groupId: 'analytics-group' });
const securityConsumer = kafka.consumer({ groupId: 'analytics-security-group' });
const producer = kafka.producer();

// ─── In-memory sliding window ─────────────────────────────────────────────────
type RouteMetrics = {
  request_count: number;
  error_count: number;
  latencies: number[];
  status_2xx: number;
  status_4xx: number;
  status_5xx: number;
  events: TelemetryEvent[]; // keep raw events for contract monitor
};

let windowBuffer: Record<string, RouteMetrics> = {};
let rawEventsBuffer: TelemetryEvent[] = [];

const BATCH_SIZE_RAW = 100;
const FLUSH_INTERVAL_MS = 5000;
const HEALTH_SCORE_INTERVAL_MS = 60_000;
const DEPLOYMENT_CORRELATION_WINDOW_MINUTES = 15;

function resetWindowBuffer() { windowBuffer = {}; }

function computePercentile(sortedValues: number[], percentile: number) {
  if (sortedValues.length === 0) return 0;
  const index = Math.ceil(percentile * sortedValues.length) - 1;
  return sortedValues[Math.max(0, index)];
}

// ─── Core flush ───────────────────────────────────────────────────────────────
async function flushMetrics() {
  const currentWindow = { ...windowBuffer };
  resetWindowBuffer();
  const bucketStart = new Date().toISOString();

  for (const [route, metrics] of Object.entries(currentWindow)) {
    if (metrics.request_count === 0) continue;

    metrics.latencies.sort((a, b) => a - b);
    const p50 = computePercentile(metrics.latencies, 0.50);
    const p95 = computePercentile(metrics.latencies, 0.95);
    const p99 = computePercentile(metrics.latencies, 0.99);

    // Run anomaly detectors
    detectAnomalies(pool, producer, route, p95, metrics.error_count, metrics.request_count)
      .catch(console.error);

    // Run contract monitor on events in this window
    for (const event of metrics.events) {
      validateContract(pool, producer, event).catch(console.error);
    }

    const rollup = {
      bucket_start: bucketStart,
      bucket_size_seconds: FLUSH_INTERVAL_MS / 1000,
      route_template: route,
      request_count: metrics.request_count,
      error_count: metrics.error_count,
      p50_latency_ms: p50,
      p95_latency_ms: p95,
      p99_latency_ms: p99,
      status_2xx: metrics.status_2xx,
      status_4xx: metrics.status_4xx,
      status_5xx: metrics.status_5xx,
    };

    try {
      await pool.query(
        `INSERT INTO metrics_rollup (
          bucket_start, bucket_size_seconds, route_template, request_count, error_count,
          p50_latency_ms, p95_latency_ms, p99_latency_ms, status_2xx, status_4xx, status_5xx
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
        [
          rollup.bucket_start, rollup.bucket_size_seconds, rollup.route_template,
          rollup.request_count, rollup.error_count,
          rollup.p50_latency_ms, rollup.p95_latency_ms, rollup.p99_latency_ms,
          rollup.status_2xx, rollup.status_4xx, rollup.status_5xx,
        ]
      );

      await redisClient.set(`route:${route}:latest`, JSON.stringify(rollup));
      await redisClient.publish('live_metrics', JSON.stringify({ type: 'metrics', data: rollup }));
    } catch (err) {
      console.error('Failed to flush metrics to DB/Redis', err);
    }
  }
}

async function flushRawEvents() {
  if (rawEventsBuffer.length === 0) return;
  const events = [...rawEventsBuffer];
  rawEventsBuffer = [];

  try {
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      for (const e of events) {
        await client.query(
          `INSERT INTO requests (
            request_id, timestamp, method, path, route_template, status_code, latency_ms,
            request_size_bytes, response_size_bytes, client_ip, client_id, upstream_latency_ms, db_latency_ms
          ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
           ON CONFLICT (request_id) DO NOTHING`,
          [
            e.request_id, e.timestamp, e.method, e.path, e.route_template,
            e.status_code, e.latency_ms, e.request_size_bytes, e.response_size_bytes,
            e.client_ip, e.client_id, e.upstream_latency_ms, e.db_latency_ms,
          ]
        );
      }
      await client.query('COMMIT');
    } catch (e) {
      await client.query('ROLLBACK');
      throw e;
    } finally {
      client.release();
    }
  } catch (err) {
    console.error('Failed to flush raw events to DB', err);
  }
}

// ─── Health scoring ───────────────────────────────────────────────────────────
async function computeHealthScore() {
  try {
    // Performance: normalized inverse of recent P95 vs 200ms target
    const perfResult = await pool.query(`
      SELECT AVG(p95_latency_ms) as avg_p95
      FROM metrics_rollup
      WHERE bucket_start > NOW() - INTERVAL '5 minutes'
    `);
    const avgP95 = parseFloat(perfResult.rows[0]?.avg_p95 || '0');
    const SLA_TARGET_MS = 200;
    const performanceScore = Math.max(0, Math.min(100, 100 - ((avgP95 - SLA_TARGET_MS) / SLA_TARGET_MS) * 100));

    // Reliability: 1 - error_rate over last 5 minutes
    const relResult = await pool.query(`
      SELECT SUM(error_count)::float / NULLIF(SUM(request_count), 0) as error_rate
      FROM metrics_rollup
      WHERE bucket_start > NOW() - INTERVAL '5 minutes'
    `);
    const errorRate = parseFloat(relResult.rows[0]?.error_rate || '0');
    const reliabilityScore = Math.max(0, Math.min(100, (1 - errorRate) * 100));

    // Security: penalize for open security anomalies in last 24h
    const secResult = await pool.query(`
      SELECT COUNT(*) as count FROM anomalies
      WHERE type = 'security' AND detected_at > NOW() - INTERVAL '24 hours'
      AND resolved_at IS NULL
    `);
    const secAnomalies = parseInt(secResult.rows[0]?.count || '0');
    const securityScore = Math.max(0, Math.min(100, 100 - secAnomalies * 10));

    // API Quality: 1 - contract_violation rate over last 24h
    const contractResult = await pool.query(`
      SELECT COUNT(*) as count FROM anomalies
      WHERE type = 'contract_violation' AND detected_at > NOW() - INTERVAL '24 hours'
    `);
    const contractViolations = parseInt(contractResult.rows[0]?.count || '0');
    const apiQualityScore = Math.max(0, Math.min(100, 100 - contractViolations * 5));

    const overallScore = (performanceScore * 0.3 + reliabilityScore * 0.3 + securityScore * 0.2 + apiQualityScore * 0.2);

    await pool.query(
      `INSERT INTO health_scores (computed_at, performance_score, reliability_score, security_score, api_quality_score, overall_score)
       VALUES ($1, $2, $3, $4, $5, $6)`,
      [
        new Date().toISOString(),
        +performanceScore.toFixed(2),
        +reliabilityScore.toFixed(2),
        +securityScore.toFixed(2),
        +apiQualityScore.toFixed(2),
        +overallScore.toFixed(2),
      ]
    );

    const scorePayload = {
      type: 'health_score',
      data: {
        performance: +performanceScore.toFixed(1),
        reliability: +reliabilityScore.toFixed(1),
        security: +securityScore.toFixed(1),
        api_quality: +apiQualityScore.toFixed(1),
        overall: +overallScore.toFixed(1),
        computed_at: new Date().toISOString(),
      },
    };
    await redisClient.publish('live_metrics', JSON.stringify(scorePayload));
  } catch (err) {
    console.error('Health score computation failed', err);
  }
}

// ─── Incident management ──────────────────────────────────────────────────────
async function openIncidentForAnomaly(anomalyId: number, anomalyType: string, route: string | null) {
  try {
    // Check for a recent deployment to correlate
    const deployResult = await pool.query(`
      SELECT id FROM deployments
      WHERE deployed_at > NOW() - INTERVAL '${DEPLOYMENT_CORRELATION_WINDOW_MINUTES} minutes'
      ORDER BY deployed_at DESC LIMIT 1
    `);
    const linkedDeploymentId = deployResult.rows[0]?.id || null;

    const result = await pool.query(
      `INSERT INTO incidents (opened_at, title, linked_anomaly_ids, linked_deployment_id)
       VALUES ($1, $2, $3, $4) RETURNING id`,
      [
        new Date().toISOString(),
        `${anomalyType.replace(/_/g, ' ')} detected${route ? ` on ${route}` : ''}`,
        [anomalyId],
        linkedDeploymentId,
      ]
    );
    const incidentId = result.rows[0].id;
    console.log(`Opened incident #${incidentId} for anomaly ${anomalyId}${linkedDeploymentId ? ` (linked to deploy #${linkedDeploymentId})` : ''}`);

    // Notify dashboard via WS
    await redisClient.publish('live_metrics', JSON.stringify({
      type: 'incident_opened',
      data: { id: incidentId, anomaly_id: anomalyId, linked_deployment_id: linkedDeploymentId },
    }));

    // Trigger RCA agent asynchronously
    triggerRCA(incidentId).catch(console.error);

    return incidentId;
  } catch (err) {
    console.error('Failed to open incident', err);
  }
}

async function triggerRCA(incidentId: number) {
  try {
    const res = await fetch(`${RCA_AGENT_URL}/rca/investigate`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ incidentId }),
    });
    if (!res.ok) {
      console.warn(`RCA agent returned ${res.status} for incident ${incidentId}`);
    }
  } catch (err) {
    // RCA agent may not be running — that's OK
    console.warn(`RCA agent not reachable for incident ${incidentId}:`, (err as any).message);
  }
}

// ─── Main init ───────────────────────────────────────────────────────────────
async function init() {
  await redisClient.connect();
  await redisSubClient.connect();

  try {
    await producer.connect();
  } catch (err) {
    console.error('Failed to connect Kafka producer', err);
  }

  // Primary telemetry consumer
  try {
    await consumer.connect();
    await consumer.subscribe({ topics: ['telemetry.raw', 'anomalies.detected'], fromBeginning: false });
    await consumer.run({
      eachMessage: async ({ topic, message }) => {
        if (!message.value) return;
        const payload = JSON.parse(message.value.toString());

        if (topic === 'telemetry.raw') {
          const event: TelemetryEvent = payload;
          rawEventsBuffer.push(event);
          if (rawEventsBuffer.length >= BATCH_SIZE_RAW) flushRawEvents();

          const route = event.route_template || event.path;
          if (!windowBuffer[route]) {
            windowBuffer[route] = { request_count: 0, error_count: 0, latencies: [], status_2xx: 0, status_4xx: 0, status_5xx: 0, events: [] };
          }
          const m = windowBuffer[route];
          m.request_count++;
          m.latencies.push(event.latency_ms);
          m.events.push(event);
          if (event.status_code >= 500) m.error_count++;
          if (event.status_code >= 200 && event.status_code < 300) m.status_2xx++;
          if (event.status_code >= 400 && event.status_code < 500) m.status_4xx++;
          if (event.status_code >= 500) m.status_5xx++;
        }

        if (topic === 'anomalies.detected') {
          // Persist to DB and open incident for high/critical anomalies
          const anomaly = payload;
          if (anomaly.severity === 'high' || anomaly.severity === 'critical') {
            // Check if an open incident already covers this anomaly type + route in last 5 min
            const existing = await pool.query(
              `SELECT id FROM incidents WHERE opened_at > NOW() - INTERVAL '5 minutes' AND title LIKE $1 AND closed_at IS NULL LIMIT 1`,
              [`%${anomaly.type}%`]
            ).catch(() => ({ rows: [] }));
            if (existing.rows.length === 0) {
              // Find the anomaly we just inserted
              const aResult = await pool.query(
                `SELECT id FROM anomalies WHERE type = $1 ORDER BY detected_at DESC LIMIT 1`,
                [anomaly.type]
              ).catch(() => ({ rows: [] }));
              if (aResult.rows.length > 0) {
                openIncidentForAnomaly(aResult.rows[0].id, anomaly.type, anomaly.route_template).catch(console.error);
              }
            }
          }
        }
      },
    });
  } catch (err) {
    console.error('Kafka consumer error (degraded mode)', err);
  }

  // Security consumer
  try {
    await securityConsumer.connect();
    await securityConsumer.subscribe({ topic: 'telemetry.security', fromBeginning: false });
    await securityConsumer.run({
      eachMessage: async ({ message }) => {
        if (!message.value) return;
        const event = JSON.parse(message.value.toString());
        await detectSecurityAnomaly(pool, producer, redisClient, event).catch(console.error);
      },
    });
  } catch (err) {
    console.error('Security Kafka consumer error (degraded mode)', err);
  }

  // Flush timers
  setInterval(() => { flushMetrics(); flushRawEvents(); }, FLUSH_INTERVAL_MS);
  setInterval(computeHealthScore, HEALTH_SCORE_INTERVAL_MS);

  // ─── REST API ───────────────────────────────────────────────────────────────
  const app = express();
  app.use(cors());
  app.use(express.json());

  const server = require('http').createServer(app);
  const wss = new WebSocketServer({ server });

  // WebSocket: push live metrics from Redis
  await redisSubClient.subscribe('live_metrics', (message) => {
    wss.clients.forEach(client => {
      if (client.readyState === WebSocket.OPEN) {
        client.send(message);
      }
    });
  });

  // ── Metrics ────────────────────────────────────────────────────────────────
  app.get('/api/metrics', async (req, res) => {
    try {
      const { route, limit = 100 } = req.query;
      let query = `
        SELECT route_template, 
               SUM(request_count) as request_count,
               AVG(p50_latency_ms) as p50_latency_ms,
               AVG(p95_latency_ms) as p95_latency_ms,
               AVG(p99_latency_ms) as p99_latency_ms,
               SUM(error_count) as error_count,
               SUM(status_2xx) as status_2xx,
               SUM(status_4xx) as status_4xx,
               SUM(status_5xx) as status_5xx,
               MAX(bucket_start) as last_seen
        FROM metrics_rollup
        WHERE bucket_start > NOW() - INTERVAL '30 minutes'
      `;
      const params: any[] = [];
      if (route) {
        query += ` AND route_template = $1`;
        params.push(route);
      }
      query += ` GROUP BY route_template ORDER BY last_seen DESC LIMIT ${parseInt(limit as string)}`;
      const result = await pool.query(query, params);
      res.json(result.rows);
    } catch (err: any) {
      res.status(500).json({ error: err.message });
    }
  });

  app.get('/api/metrics/history/:route', async (req, res) => {
    try {
      const route = decodeURIComponent(req.params.route);
      const { minutes = 60 } = req.query;
      const result = await pool.query(
        `SELECT bucket_start, request_count, p50_latency_ms, p95_latency_ms, p99_latency_ms,
                error_count, status_2xx, status_4xx, status_5xx
         FROM metrics_rollup
         WHERE route_template = $1 AND bucket_start > NOW() - INTERVAL '${parseInt(minutes as string)} minutes'
         ORDER BY bucket_start ASC`,
        [route]
      );
      res.json(result.rows);
    } catch (err: any) {
      res.status(500).json({ error: err.message });
    }
  });

  app.get('/api/requests/samples', async (req, res) => {
    try {
      const { route, limit = 20, minutes = 30 } = req.query;
      let query = `
        SELECT request_id, timestamp, method, path, route_template, status_code, 
               latency_ms, upstream_latency_ms, db_latency_ms, client_ip
        FROM requests
        WHERE timestamp > NOW() - INTERVAL '${parseInt(minutes as string)} minutes'
      `;
      const params: any[] = [];
      if (route) {
        query += ` AND route_template = $1`;
        params.push(route);
      }
      query += ` ORDER BY timestamp DESC LIMIT ${parseInt(limit as string)}`;
      const result = await pool.query(query, params);
      res.json(result.rows);
    } catch (err: any) {
      res.status(500).json({ error: err.message });
    }
  });

  // ── Anomalies ───────────────────────────────────────────────────────────────
  app.get('/api/anomalies', async (req, res) => {
    try {
      const { type, severity, limit = 50, minutes = 1440 } = req.query;
      let query = `SELECT * FROM anomalies WHERE detected_at > NOW() - INTERVAL '${parseInt(minutes as string)} minutes'`;
      const params: any[] = [];
      if (type) { query += ` AND type = $${params.length + 1}`; params.push(type); }
      if (severity) { query += ` AND severity = $${params.length + 1}`; params.push(severity); }
      query += ` ORDER BY detected_at DESC LIMIT ${parseInt(limit as string)}`;
      const result = await pool.query(query, params);
      res.json(result.rows);
    } catch (err: any) {
      res.status(500).json({ error: err.message });
    }
  });

  app.patch('/api/anomalies/:id/resolve', async (req, res) => {
    try {
      await pool.query(`UPDATE anomalies SET resolved_at = NOW() WHERE id = $1`, [req.params.id]);
      res.json({ success: true });
    } catch (err: any) {
      res.status(500).json({ error: err.message });
    }
  });

  // ── Incidents ───────────────────────────────────────────────────────────────
  app.get('/api/incidents', async (req, res) => {
    try {
      const { limit = 50 } = req.query;
      const result = await pool.query(
        `SELECT i.*, d.version as deployment_version, d.description as deployment_description
         FROM incidents i
         LEFT JOIN deployments d ON d.id = i.linked_deployment_id
         ORDER BY i.opened_at DESC LIMIT $1`,
        [parseInt(limit as string)]
      );
      res.json(result.rows);
    } catch (err: any) {
      res.status(500).json({ error: err.message });
    }
  });

  app.get('/api/incidents/:id', async (req, res) => {
    try {
      const result = await pool.query(
        `SELECT i.*, d.version as deployment_version, d.description as deployment_description, d.deployed_at
         FROM incidents i
         LEFT JOIN deployments d ON d.id = i.linked_deployment_id
         WHERE i.id = $1`,
        [req.params.id]
      );
      if (!result.rows.length) return res.status(404).json({ error: 'Not found' });
      res.json(result.rows[0]);
    } catch (err: any) {
      res.status(500).json({ error: err.message });
    }
  });

  app.post('/api/incidents/:id/rca', async (req, res) => {
    try {
      const response = await fetch(`${RCA_AGENT_URL}/rca/investigate`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ incidentId: parseInt(req.params.id) }),
      });
      const data = await response.json() as any;
      res.status(response.ok ? 200 : 502).json(data);
    } catch (err: any) {
      res.status(503).json({ error: 'RCA agent not reachable', detail: err.message });
    }
  });

  app.patch('/api/incidents/:id/close', async (req, res) => {
    try {
      await pool.query(`UPDATE incidents SET closed_at = NOW() WHERE id = $1`, [req.params.id]);
      res.json({ success: true });
    } catch (err: any) {
      res.status(500).json({ error: err.message });
    }
  });

  // ── Deployments ─────────────────────────────────────────────────────────────
  app.get('/api/deployments', async (req, res) => {
    try {
      const result = await pool.query(
        `SELECT d.*, COUNT(i.id) as incident_count
         FROM deployments d
         LEFT JOIN incidents i ON i.linked_deployment_id = d.id
         GROUP BY d.id
         ORDER BY d.deployed_at DESC LIMIT 50`
      );
      res.json(result.rows);
    } catch (err: any) {
      res.status(500).json({ error: err.message });
    }
  });

  app.post('/api/deployments', async (req, res) => {
    try {
      const { version, commit_sha, description } = req.body;
      if (!version) return res.status(400).json({ error: 'version is required' });
      const result = await pool.query(
        `INSERT INTO deployments (version, deployed_at, commit_sha, description) VALUES ($1, NOW(), $2, $3) RETURNING *`,
        [version, commit_sha, description]
      );
      const deployment = result.rows[0];
      // Also emit to Kafka
      producer.send({
        topic: 'deployments.events',
        messages: [{ key: 'test-api', value: JSON.stringify(deployment) }],
      }).catch(console.error);
      res.json(deployment);
    } catch (err: any) {
      res.status(500).json({ error: err.message });
    }
  });

  // ── Health ──────────────────────────────────────────────────────────────────
  app.get('/api/health', async (req, res) => {
    try {
      const result = await pool.query(
        `SELECT * FROM health_scores ORDER BY computed_at DESC LIMIT 1`
      );
      res.json(result.rows[0] || { overall_score: 100, performance_score: 100, reliability_score: 100, security_score: 100, api_quality_score: 100 });
    } catch (err: any) {
      res.status(500).json({ error: err.message });
    }
  });

  // ── Security events ─────────────────────────────────────────────────────────
  app.get('/api/security', async (req, res) => {
    try {
      const result = await pool.query(
        `SELECT * FROM anomalies WHERE type = 'security' ORDER BY detected_at DESC LIMIT 100`
      );
      res.json(result.rows);
    } catch (err: any) {
      res.status(500).json({ error: err.message });
    }
  });

  // ── Platform status ─────────────────────────────────────────────────────────
  app.get('/api/status', (_req, res) => {
    res.json({ status: 'ok', timestamp: new Date().toISOString() });
  });

  server.listen(PORT, () => {
    console.log(`Analytics service REST & WS API listening on port ${PORT}`);
  });
}

init().catch(console.error);
