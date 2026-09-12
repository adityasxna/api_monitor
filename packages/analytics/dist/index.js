"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const kafkajs_1 = require("kafkajs");
const pg_1 = require("pg");
const redis_1 = require("redis");
const express_1 = __importDefault(require("express"));
const cors_1 = __importDefault(require("cors"));
const ws_1 = require("ws");
const PORT = process.env.PORT || 3002;
const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';
const pool = new pg_1.Pool({
    user: process.env.POSTGRES_USER || 'api_monitor',
    host: process.env.POSTGRES_HOST || 'localhost',
    database: process.env.POSTGRES_DB || 'api_monitor',
    password: process.env.POSTGRES_PASSWORD || 'password',
    port: parseInt(process.env.POSTGRES_PORT || '5432', 10),
});
const redisClient = (0, redis_1.createClient)({ url: REDIS_URL });
redisClient.on('error', err => console.error('Redis error', err));
const redisSubClient = redisClient.duplicate();
redisSubClient.on('error', err => console.error('Redis Sub error', err));
const kafka = new kafkajs_1.Kafka({
    clientId: 'analytics',
    brokers: KAFKA_BROKERS,
});
const consumer = kafka.consumer({ groupId: 'analytics-group' });
let windowBuffer = {};
let rawEventsBuffer = [];
const BATCH_SIZE_RAW = 100;
const FLUSH_INTERVAL_MS = 5000;
function resetWindowBuffer() {
    windowBuffer = {};
}
function computePercentile(sortedValues, percentile) {
    if (sortedValues.length === 0)
        return 0;
    const index = Math.ceil(percentile * sortedValues.length) - 1;
    return sortedValues[index];
}
const detectors_1 = require("./detectors");
const producer = kafka.producer();
async function flushMetrics() {
    const currentWindow = { ...windowBuffer };
    resetWindowBuffer();
    const bucketStart = new Date().toISOString();
    for (const [route, metrics] of Object.entries(currentWindow)) {
        if (metrics.request_count === 0)
            continue;
        metrics.latencies.sort((a, b) => a - b);
        const p50 = computePercentile(metrics.latencies, 0.50);
        const p95 = computePercentile(metrics.latencies, 0.95);
        const p99 = computePercentile(metrics.latencies, 0.99);
        // Call Anomaly Detector
        (0, detectors_1.detectAnomalies)(pool, producer, route, p95, metrics.error_count, metrics.request_count).catch(console.error);
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
            // 1. Write to Postgres
            await pool.query(`INSERT INTO metrics_rollup (
          bucket_start, bucket_size_seconds, route_template, request_count, error_count, 
          p50_latency_ms, p95_latency_ms, p99_latency_ms, status_2xx, status_4xx, status_5xx
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`, [
                rollup.bucket_start, rollup.bucket_size_seconds, rollup.route_template, rollup.request_count, rollup.error_count,
                rollup.p50_latency_ms, rollup.p95_latency_ms, rollup.p99_latency_ms, rollup.status_2xx, rollup.status_4xx, rollup.status_5xx
            ]);
            // 2. Write to Redis (latest state)
            await redisClient.set(`route:${route}:latest`, JSON.stringify(rollup));
            // 3. Publish to Redis pub/sub (for WebSocket live layer)
            await redisClient.publish('live_metrics', JSON.stringify(rollup));
        }
        catch (err) {
            console.error('Failed to flush metrics to DB/Redis', err);
        }
    }
}
async function flushRawEvents() {
    if (rawEventsBuffer.length === 0)
        return;
    const events = [...rawEventsBuffer];
    rawEventsBuffer = [];
    try {
        const client = await pool.connect();
        try {
            await client.query('BEGIN');
            for (const e of events) {
                await client.query(`INSERT INTO requests (
            request_id, timestamp, method, path, route_template, status_code, latency_ms, request_size_bytes, response_size_bytes, client_ip, client_id, upstream_latency_ms, db_latency_ms
          ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
           ON CONFLICT (request_id) DO NOTHING`, // simplified duplicate handling, would need real constraint on request_id
                [
                    e.request_id, e.timestamp, e.method, e.path, e.route_template, e.status_code, e.latency_ms, e.request_size_bytes, e.response_size_bytes, e.client_ip, e.client_id, e.upstream_latency_ms, e.db_latency_ms
                ]);
            }
            await client.query('COMMIT');
        }
        catch (e) {
            await client.query('ROLLBACK');
            throw e;
        }
        finally {
            client.release();
        }
    }
    catch (err) {
        console.error('Failed to flush raw events to DB', err);
    }
}
async function init() {
    await redisClient.connect();
    try {
        await producer.connect();
        await consumer.connect();
        await consumer.subscribe({ topic: 'telemetry.raw', fromBeginning: false });
        await consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                if (!message.value)
                    return;
                const event = JSON.parse(message.value.toString());
                // Add to raw buffer
                rawEventsBuffer.push(event);
                if (rawEventsBuffer.length >= BATCH_SIZE_RAW) {
                    flushRawEvents(); // Fire async
                }
                // Add to metric window
                const route = event.route_template || event.path;
                if (!windowBuffer[route]) {
                    windowBuffer[route] = { request_count: 0, error_count: 0, latencies: [], status_2xx: 0, status_4xx: 0, status_5xx: 0 };
                }
                const m = windowBuffer[route];
                m.request_count++;
                m.latencies.push(event.latency_ms);
                if (event.status_code >= 500)
                    m.error_count++;
                if (event.status_code >= 200 && event.status_code < 300)
                    m.status_2xx++;
                if (event.status_code >= 400 && event.status_code < 500)
                    m.status_4xx++;
                if (event.status_code >= 500)
                    m.status_5xx++;
            },
        });
    }
    catch (err) {
        console.error('Kafka consumer error (degraded mode)', err);
    }
    setInterval(() => {
        flushMetrics();
        flushRawEvents();
    }, FLUSH_INTERVAL_MS);
    // REST API Layer
    const app = (0, express_1.default)();
    app.use((0, cors_1.default)());
    app.use(express_1.default.json());
    const server = require('http').createServer(app);
    const wss = new ws_1.WebSocketServer({ server });
    await redisSubClient.connect();
    await redisSubClient.subscribe('live_metrics', (message) => {
        wss.clients.forEach(client => {
            if (client.readyState === ws_1.WebSocket.OPEN) {
                client.send(message);
            }
        });
    });
    app.get('/api/metrics', async (req, res) => {
        try {
            const result = await pool.query(`
        SELECT route_template, request_count, p95_latency_ms, status_5xx 
        FROM metrics_rollup 
        ORDER BY bucket_start DESC 
        LIMIT 100
      `);
            res.json(result.rows);
        }
        catch (err) {
            res.status(500).json({ error: err.message });
        }
    });
    app.get('/api/anomalies', async (req, res) => {
        try {
            const result = await pool.query('SELECT * FROM anomalies ORDER BY detected_at DESC LIMIT 50');
            res.json(result.rows);
        }
        catch (err) {
            res.status(500).json({ error: err.message });
        }
    });
    server.listen(PORT, () => {
        console.log(`Analytics service REST & WS API listening on port ${PORT}`);
    });
}
init().catch(console.error);
