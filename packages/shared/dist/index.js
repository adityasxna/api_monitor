"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.AnomalySchema = exports.DeploymentEventSchema = exports.SecurityEventSchema = exports.TelemetryEventSchema = void 0;
const zod_1 = require("zod");
exports.TelemetryEventSchema = zod_1.z.object({
    request_id: zod_1.z.string().uuid(),
    timestamp: zod_1.z.string().datetime(), // ISO string
    method: zod_1.z.string(),
    path: zod_1.z.string(),
    route_template: zod_1.z.string().optional(),
    status_code: zod_1.z.number(),
    latency_ms: zod_1.z.number(),
    request_size_bytes: zod_1.z.number().optional(),
    response_size_bytes: zod_1.z.number().optional(),
    client_ip: zod_1.z.string().optional(),
    client_id: zod_1.z.string().optional(),
    upstream_latency_ms: zod_1.z.number().optional(),
    db_latency_ms: zod_1.z.number().optional(),
});
exports.SecurityEventSchema = zod_1.z.object({
    timestamp: zod_1.z.string().datetime(),
    client_id: zod_1.z.string().optional(),
    client_ip: zod_1.z.string().optional(),
    event_type: zod_1.z.enum(['login_failed', 'abnormal_client', 'endpoint_enumeration']),
    details: zod_1.z.record(zod_1.z.any()),
});
exports.DeploymentEventSchema = zod_1.z.object({
    service: zod_1.z.string(),
    version: zod_1.z.string(),
    timestamp: zod_1.z.string().datetime(),
    commit_sha: zod_1.z.string().optional(),
    description: zod_1.z.string().optional(),
});
exports.AnomalySchema = zod_1.z.object({
    detected_at: zod_1.z.string().datetime(),
    type: zod_1.z.enum(['latency_spike', 'error_spike', 'traffic_spike', 'unusual_endpoint', 'security', 'contract_violation']),
    severity: zod_1.z.enum(['low', 'medium', 'high', 'critical']),
    route_template: zod_1.z.string().optional(),
    details: zod_1.z.record(zod_1.z.any()),
});
