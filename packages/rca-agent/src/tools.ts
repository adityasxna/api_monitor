import { Pool } from 'pg';
import { LlmTool } from './providers';

const ANALYTICS_URL = process.env.ANALYTICS_URL || 'http://localhost:3002';

// ─── Tool definitions (schema for LLM) ───────────────────────────────────────
export const TOOL_DEFINITIONS: LlmTool[] = [
  {
    name: 'getMetricsForRoute',
    description: 'Get historical latency (P50/P95/P99), error rate, and request count for a specific API route over a time range. Use this to identify when performance degraded.',
    parameters: {
      type: 'object',
      properties: {
        route: { type: 'string', description: 'Route template, e.g. /orders/:id' },
        minutes: { type: 'number', description: 'How many minutes back to look (default: 30)' },
      },
      required: ['route'],
    },
  },
  {
    name: 'getAnomaliesNear',
    description: 'Get anomalies detected near a specific timestamp. Use to understand what went wrong and when.',
    parameters: {
      type: 'object',
      properties: {
        timestamp: { type: 'string', description: 'ISO timestamp to search around' },
        windowMinutes: { type: 'number', description: 'Minutes window before/after timestamp (default: 15)' },
        type: { type: 'string', description: 'Optional: filter by anomaly type (latency_spike, error_spike, traffic_spike, security, contract_violation)' },
      },
      required: ['timestamp'],
    },
  },
  {
    name: 'getRecentDeployments',
    description: 'Get recent deployment records. Use this to correlate performance degradation with code changes.',
    parameters: {
      type: 'object',
      properties: {
        windowMinutes: { type: 'number', description: 'How many minutes back to check (default: 60)' },
      },
    },
  },
  {
    name: 'getDbQueryStats',
    description: 'Get database query latency statistics for a route to identify slow queries. High db_latency_ms compared to upstream_latency_ms indicates a DB bottleneck.',
    parameters: {
      type: 'object',
      properties: {
        route: { type: 'string', description: 'Route template to inspect DB latency for' },
        minutes: { type: 'number', description: 'Time window in minutes (default: 30)' },
      },
      required: ['route'],
    },
  },
  {
    name: 'getRawSamples',
    description: 'Get raw request samples for a route to see concrete evidence (status codes, latencies, sizes). Use to verify hypothesis with specific data points.',
    parameters: {
      type: 'object',
      properties: {
        route: { type: 'string', description: 'Route template to get samples for' },
        minutes: { type: 'number', description: 'How many minutes back (default: 15)' },
        limit: { type: 'number', description: 'Max samples to return (default: 10)' },
      },
      required: ['route'],
    },
  },
];

// ─── Tool implementations (actual data fetchers) ──────────────────────────────
export async function executeTool(name: string, args: Record<string, any>): Promise<string> {
  try {
    switch (name) {
      case 'getMetricsForRoute': {
        const { route, minutes = 30 } = args;
        const res = await fetch(`${ANALYTICS_URL}/api/metrics/history/${encodeURIComponent(route)}?minutes=${minutes}`);
        if (!res.ok) return `Error fetching metrics: ${res.status}`;
        const data = await res.json() as any[];
        if (!data.length) return `No metrics found for ${route} in the last ${minutes} minutes.`;
        const latest = data.slice(-5);
        const avg_p95 = (latest.reduce((s: number, r: any) => s + parseFloat(r.p95_latency_ms || 0), 0) / latest.length).toFixed(1);
        const avg_errors = (latest.reduce((s: number, r: any) => s + parseFloat(r.error_count || 0), 0) / latest.length).toFixed(1);
        const total_requests = latest.reduce((s: number, r: any) => s + parseInt(r.request_count || 0), 0);
        const summary = {
          route,
          window_minutes: minutes,
          data_points: data.length,
          avg_p95_ms: parseFloat(avg_p95),
          avg_errors_per_window: parseFloat(avg_errors),
          total_requests_in_window: total_requests,
          first_bucket: data[0]?.bucket_start,
          last_bucket: data[data.length - 1]?.bucket_start,
          recent_trend: latest.map((r: any) => ({
            bucket: r.bucket_start,
            p95: Math.round(parseFloat(r.p95_latency_ms || 0)),
            errors: r.error_count,
            requests: r.request_count,
          })),
        };
        return JSON.stringify(summary, null, 2);
      }

      case 'getAnomaliesNear': {
        const { timestamp, windowMinutes = 15, type } = args;
        const params = new URLSearchParams({ minutes: String(windowMinutes), limit: '20' });
        if (type) params.set('type', type);
        const res = await fetch(`${ANALYTICS_URL}/api/anomalies?${params}`);
        if (!res.ok) return `Error fetching anomalies: ${res.status}`;
        const data = await res.json() as any[];
        const target = new Date(timestamp).getTime();
        const filtered = data.filter((a: any) => {
          const diff = Math.abs(new Date(a.detected_at).getTime() - target);
          return diff < windowMinutes * 60 * 1000;
        });
        if (!filtered.length) return `No anomalies found near ${timestamp} in a ${windowMinutes}min window.`;
        return JSON.stringify(filtered.map((a: any) => ({
          id: a.id,
          type: a.type,
          severity: a.severity,
          route: a.route_template,
          detected_at: a.detected_at,
          details: a.details,
        })), null, 2);
      }

      case 'getRecentDeployments': {
        const { windowMinutes = 60 } = args;
        const res = await fetch(`${ANALYTICS_URL}/api/deployments`);
        if (!res.ok) return `Error fetching deployments: ${res.status}`;
        const data = await res.json() as any[];
        const cutoff = Date.now() - windowMinutes * 60 * 1000;
        const recent = data.filter((d: any) => new Date(d.deployed_at).getTime() > cutoff);
        if (!recent.length) return `No deployments in the last ${windowMinutes} minutes.`;
        return JSON.stringify(recent.map((d: any) => ({
          id: d.id,
          version: d.version,
          deployed_at: d.deployed_at,
          commit_sha: d.commit_sha,
          description: d.description,
          incident_count: d.incident_count,
        })), null, 2);
      }

      case 'getDbQueryStats': {
        const { route, minutes = 30 } = args;
        const res = await fetch(`${ANALYTICS_URL}/api/requests/samples?route=${encodeURIComponent(route)}&minutes=${minutes}&limit=50`);
        if (!res.ok) return `Error fetching samples: ${res.status}`;
        const samples = await res.json() as any[];
        if (!samples.length) return `No DB stats available for ${route} in the last ${minutes} minutes.`;
        const withDbLatency = samples.filter((s: any) => s.db_latency_ms !== null && s.db_latency_ms !== undefined);
        if (!withDbLatency.length) return `DB latency not captured for ${route} (no db_latency_ms in samples).`;
        const avgDb = withDbLatency.reduce((s: number, r: any) => s + parseFloat(r.db_latency_ms), 0) / withDbLatency.length;
        const avgUpstream = samples.reduce((s: number, r: any) => s + parseFloat(r.upstream_latency_ms || r.latency_ms), 0) / samples.length;
        return JSON.stringify({
          route,
          samples_analyzed: samples.length,
          samples_with_db_latency: withDbLatency.length,
          avg_db_latency_ms: +avgDb.toFixed(1),
          avg_upstream_latency_ms: +avgUpstream.toFixed(1),
          db_fraction_of_total: +(avgDb / avgUpstream).toFixed(2),
          interpretation: avgDb > 500 ? 'HIGH: DB is the bottleneck' : avgDb > 100 ? 'ELEVATED: DB latency notable' : 'NORMAL',
        }, null, 2);
      }

      case 'getRawSamples': {
        const { route, minutes = 15, limit = 10 } = args;
        const res = await fetch(`${ANALYTICS_URL}/api/requests/samples?route=${encodeURIComponent(route)}&minutes=${minutes}&limit=${limit}`);
        if (!res.ok) return `Error fetching samples: ${res.status}`;
        const samples = await res.json() as any[];
        if (!samples.length) return `No samples found for ${route} in the last ${minutes} minutes.`;
        return JSON.stringify(samples.map((s: any) => ({
          timestamp: s.timestamp,
          status: s.status_code,
          latency_ms: Math.round(s.latency_ms),
          upstream_ms: Math.round(s.upstream_latency_ms || s.latency_ms),
          db_ms: s.db_latency_ms ? Math.round(s.db_latency_ms) : null,
          client_ip: s.client_ip,
        })), null, 2);
      }

      default:
        return `Unknown tool: ${name}`;
    }
  } catch (err: any) {
    return `Tool execution error: ${err.message}`;
  }
}
