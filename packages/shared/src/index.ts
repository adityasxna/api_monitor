import { z } from 'zod';

export const TelemetryEventSchema = z.object({
  request_id: z.string().uuid(),
  timestamp: z.string().datetime(), // ISO string
  method: z.string(),
  path: z.string(),
  route_template: z.string().optional(),
  status_code: z.number(),
  latency_ms: z.number(),
  request_size_bytes: z.number().optional(),
  response_size_bytes: z.number().optional(),
  client_ip: z.string().optional(),
  client_id: z.string().optional(),
  upstream_latency_ms: z.number().optional(),
  db_latency_ms: z.number().optional(),
});

export type TelemetryEvent = z.infer<typeof TelemetryEventSchema>;

export const SecurityEventSchema = z.object({
  timestamp: z.string().datetime(),
  client_id: z.string().optional(),
  client_ip: z.string().optional(),
  event_type: z.enum(['login_failed', 'abnormal_client', 'endpoint_enumeration']),
  details: z.record(z.any()),
});

export type SecurityEvent = z.infer<typeof SecurityEventSchema>;

export const DeploymentEventSchema = z.object({
  service: z.string(),
  version: z.string(),
  timestamp: z.string().datetime(),
  commit_sha: z.string().optional(),
  description: z.string().optional(),
});

export type DeploymentEvent = z.infer<typeof DeploymentEventSchema>;

export const AnomalySchema = z.object({
  detected_at: z.string().datetime(),
  type: z.enum(['latency_spike', 'error_spike', 'traffic_spike', 'unusual_endpoint', 'security', 'contract_violation']),
  severity: z.enum(['low', 'medium', 'high', 'critical']),
  route_template: z.string().optional(),
  details: z.record(z.any()),
});

export type Anomaly = z.infer<typeof AnomalySchema>;
