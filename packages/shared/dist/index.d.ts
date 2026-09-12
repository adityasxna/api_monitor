import { z } from 'zod';
export declare const TelemetryEventSchema: z.ZodObject<{
    request_id: z.ZodString;
    timestamp: z.ZodString;
    method: z.ZodString;
    path: z.ZodString;
    route_template: z.ZodOptional<z.ZodString>;
    status_code: z.ZodNumber;
    latency_ms: z.ZodNumber;
    request_size_bytes: z.ZodOptional<z.ZodNumber>;
    response_size_bytes: z.ZodOptional<z.ZodNumber>;
    client_ip: z.ZodOptional<z.ZodString>;
    client_id: z.ZodOptional<z.ZodString>;
    upstream_latency_ms: z.ZodOptional<z.ZodNumber>;
    db_latency_ms: z.ZodOptional<z.ZodNumber>;
}, "strip", z.ZodTypeAny, {
    request_id: string;
    timestamp: string;
    method: string;
    path: string;
    status_code: number;
    latency_ms: number;
    route_template?: string | undefined;
    request_size_bytes?: number | undefined;
    response_size_bytes?: number | undefined;
    client_ip?: string | undefined;
    client_id?: string | undefined;
    upstream_latency_ms?: number | undefined;
    db_latency_ms?: number | undefined;
}, {
    request_id: string;
    timestamp: string;
    method: string;
    path: string;
    status_code: number;
    latency_ms: number;
    route_template?: string | undefined;
    request_size_bytes?: number | undefined;
    response_size_bytes?: number | undefined;
    client_ip?: string | undefined;
    client_id?: string | undefined;
    upstream_latency_ms?: number | undefined;
    db_latency_ms?: number | undefined;
}>;
export type TelemetryEvent = z.infer<typeof TelemetryEventSchema>;
export declare const SecurityEventSchema: z.ZodObject<{
    timestamp: z.ZodString;
    client_id: z.ZodOptional<z.ZodString>;
    client_ip: z.ZodOptional<z.ZodString>;
    event_type: z.ZodEnum<["login_failed", "abnormal_client", "endpoint_enumeration"]>;
    details: z.ZodRecord<z.ZodString, z.ZodAny>;
}, "strip", z.ZodTypeAny, {
    timestamp: string;
    event_type: "login_failed" | "abnormal_client" | "endpoint_enumeration";
    details: Record<string, any>;
    client_ip?: string | undefined;
    client_id?: string | undefined;
}, {
    timestamp: string;
    event_type: "login_failed" | "abnormal_client" | "endpoint_enumeration";
    details: Record<string, any>;
    client_ip?: string | undefined;
    client_id?: string | undefined;
}>;
export type SecurityEvent = z.infer<typeof SecurityEventSchema>;
export declare const DeploymentEventSchema: z.ZodObject<{
    service: z.ZodString;
    version: z.ZodString;
    timestamp: z.ZodString;
    commit_sha: z.ZodOptional<z.ZodString>;
    description: z.ZodOptional<z.ZodString>;
}, "strip", z.ZodTypeAny, {
    timestamp: string;
    service: string;
    version: string;
    commit_sha?: string | undefined;
    description?: string | undefined;
}, {
    timestamp: string;
    service: string;
    version: string;
    commit_sha?: string | undefined;
    description?: string | undefined;
}>;
export type DeploymentEvent = z.infer<typeof DeploymentEventSchema>;
export declare const AnomalySchema: z.ZodObject<{
    detected_at: z.ZodString;
    type: z.ZodEnum<["latency_spike", "error_spike", "traffic_spike", "unusual_endpoint", "security", "contract_violation"]>;
    severity: z.ZodEnum<["low", "medium", "high", "critical"]>;
    route_template: z.ZodOptional<z.ZodString>;
    details: z.ZodRecord<z.ZodString, z.ZodAny>;
}, "strip", z.ZodTypeAny, {
    type: "latency_spike" | "error_spike" | "traffic_spike" | "unusual_endpoint" | "security" | "contract_violation";
    details: Record<string, any>;
    detected_at: string;
    severity: "low" | "medium" | "high" | "critical";
    route_template?: string | undefined;
}, {
    type: "latency_spike" | "error_spike" | "traffic_spike" | "unusual_endpoint" | "security" | "contract_violation";
    details: Record<string, any>;
    detected_at: string;
    severity: "low" | "medium" | "high" | "critical";
    route_template?: string | undefined;
}>;
export type Anomaly = z.infer<typeof AnomalySchema>;
