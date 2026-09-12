"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const http_1 = __importDefault(require("http"));
const http_proxy_1 = __importDefault(require("http-proxy"));
const kafkajs_1 = require("kafkajs");
const uuid_1 = require("uuid");
const UPSTREAM = process.env.UPSTREAM_URL || 'http://localhost:3001';
const PORT = process.env.PORT || 3000;
const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
const kafka = new kafkajs_1.Kafka({
    clientId: 'gateway',
    brokers: KAFKA_BROKERS,
});
const producer = kafka.producer();
// Local buffer for fire-and-forget fallback
const MAX_BUFFER_SIZE = 10000;
const telemetryBuffer = [];
let droppedEventsCount = 0;
async function initKafka() {
    try {
        await producer.connect();
        console.log('Gateway connected to Kafka');
        // Attempt to flush buffer if connected
        setInterval(async () => {
            if (telemetryBuffer.length > 0) {
                const batch = telemetryBuffer.splice(0, 500);
                try {
                    await produceTelemetryBatch(batch);
                }
                catch (err) {
                    // Put back on failure
                    if (telemetryBuffer.length + batch.length <= MAX_BUFFER_SIZE) {
                        telemetryBuffer.unshift(...batch);
                    }
                    else {
                        droppedEventsCount += batch.length;
                        console.warn(`Dropped ${batch.length} events, total dropped: ${droppedEventsCount}`);
                    }
                }
            }
        }, 1000);
    }
    catch (err) {
        console.error('Failed to connect to Kafka, running in degraded mode', err);
    }
}
async function produceTelemetryBatch(events) {
    await producer.send({
        topic: 'telemetry.raw',
        messages: events.map(e => ({
            key: e.route_template || e.path,
            value: JSON.stringify(e)
        }))
    });
}
function emitTelemetry(event) {
    // Fire and forget
    producer.send({
        topic: 'telemetry.raw',
        messages: [{
                key: event.route_template || event.path,
                value: JSON.stringify(event)
            }]
    }).catch(err => {
        // Buffer locally if Kafka is down
        if (telemetryBuffer.length < MAX_BUFFER_SIZE) {
            telemetryBuffer.push(event);
        }
        else {
            droppedEventsCount++;
            if (droppedEventsCount % 100 === 0) {
                console.warn(`Kafka unreachable. Dropped ${droppedEventsCount} events so far.`);
            }
        }
    });
}
function emitSecurityEvent(event) {
    producer.send({
        topic: 'telemetry.security',
        messages: [{
                key: event.client_id || event.client_ip || 'unknown',
                value: JSON.stringify(event)
            }]
    }).catch(() => { });
}
// Very basic route template matcher for demo purposes
function getRouteTemplate(path) {
    if (path.startsWith('/orders/') && path.split('/').length === 3)
        return '/orders/:id';
    if (path.startsWith('/users/') && path.split('/').length === 3)
        return '/users/:id';
    if (path.startsWith('/login'))
        return '/login';
    if (path.startsWith('/products'))
        return '/products';
    if (path.startsWith('/orders'))
        return '/orders';
    return path;
}
const proxy = http_proxy_1.default.createProxyServer({});
// Listen for proxy responses to calculate upstream latency and response sizes
proxy.on('proxyRes', (proxyRes, req, res) => {
    const customReq = req;
    const request_id = customReq.request_id;
    const start_time = customReq.start_time;
    const end_time = Date.now();
    const latency_ms = end_time - start_time;
    let response_size_bytes = 0;
    if (proxyRes.headers['content-length']) {
        response_size_bytes = parseInt(proxyRes.headers['content-length'], 10);
    }
    const client_ip = req.socket.remoteAddress || 'unknown';
    const event = {
        request_id,
        timestamp: new Date(start_time).toISOString(),
        method: req.method || 'GET',
        path: req.url || '/',
        route_template: getRouteTemplate(req.url || '/'),
        status_code: proxyRes.statusCode || 200,
        latency_ms,
        request_size_bytes: customReq.request_size_bytes,
        response_size_bytes,
        client_ip,
        upstream_latency_ms: latency_ms, // Simplified
    };
    emitTelemetry(event);
    // Security event heuristics
    if (event.path === '/login' && event.status_code === 401) {
        emitSecurityEvent({
            timestamp: new Date().toISOString(),
            client_ip,
            event_type: 'login_failed',
            details: { path: event.path }
        });
    }
});
const server = http_1.default.createServer((req, res) => {
    const customReq = req;
    customReq.request_id = (0, uuid_1.v4)();
    customReq.start_time = Date.now();
    customReq.request_size_bytes = parseInt(req.headers['content-length'] || '0', 10);
    proxy.web(req, res, { target: UPSTREAM }, (err) => {
        console.error('Proxy error:', err);
        // Emit 502 telemetry
        const latency_ms = Date.now() - customReq.start_time;
        emitTelemetry({
            request_id: customReq.request_id,
            timestamp: new Date(customReq.start_time).toISOString(),
            method: req.method || 'GET',
            path: req.url || '/',
            route_template: getRouteTemplate(req.url || '/'),
            status_code: 502,
            latency_ms,
            client_ip: req.socket.remoteAddress || 'unknown',
        });
        if (!res.headersSent) {
            res.writeHead(502);
            res.end('Bad Gateway');
        }
    });
});
initKafka().then(() => {
    server.listen(PORT, () => {
        console.log(`Gateway (v2) listening on port ${PORT}, routing to ${UPSTREAM}`);
    });
});
