import { Pool } from 'pg';
import { createClient } from 'redis';
import { Anomaly } from '@api-intelligence/shared';

type RedisClient = ReturnType<typeof createClient>;

// Per-route trailing baseline (last N rollups) for deviation detection
const BASELINE_WINDOW = 10; // number of historical rollups to compare against
const LATENCY_SPIKE_RATIO = 2.5; // current P95 must be 2.5x the trailing median
const ERROR_RATE_SPIKE_THRESHOLD = 0.15; // 15% error rate
const TRAFFIC_ZSCORE_THRESHOLD = 2.5;
const MIN_SAMPLES = 5; // ignore windows with too few requests

// In-memory trailing baselines: route → list of recent rollup values
const latencyBaseline: Record<string, number[]> = {};
const errorRateBaseline: Record<string, number[]> = {};
const trafficBaseline: Record<string, number[]> = {};
const knownRoutes = new Set<string>();

function median(arr: number[]): number {
  if (arr.length === 0) return 0;
  const sorted = [...arr].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 !== 0 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
}

function mean(arr: number[]): number {
  if (arr.length === 0) return 0;
  return arr.reduce((a, b) => a + b, 0) / arr.length;
}

function stddev(arr: number[]): number {
  if (arr.length < 2) return 0;
  const m = mean(arr);
  return Math.sqrt(arr.reduce((sum, v) => sum + (v - m) ** 2, 0) / arr.length);
}

function pushBaseline(map: Record<string, number[]>, key: string, value: number) {
  if (!map[key]) map[key] = [];
  map[key].push(value);
  if (map[key].length > BASELINE_WINDOW) map[key].shift();
}

async function writeAnomaly(pool: Pool, producer: any, anomaly: Anomaly) {
  try {
    await pool.query(
      `INSERT INTO anomalies (detected_at, type, severity, route_template, details)
       VALUES ($1, $2, $3, $4, $5)`,
      [anomaly.detected_at, anomaly.type, anomaly.severity, anomaly.route_template, JSON.stringify(anomaly.details)]
    );
  } catch (e) {
    console.error('Failed to write anomaly to DB', e);
  }

  producer.send({
    topic: 'anomalies.detected',
    messages: [{ key: anomaly.type, value: JSON.stringify(anomaly) }]
  }).catch((e: any) => console.error('Failed to publish anomaly to Kafka', e));
}

export async function detectAnomalies(
  pool: Pool,
  producer: any,
  route: string,
  currentP95: number,
  currentErrorCount: number,
  currentRequestCount: number
) {
  const now = new Date().toISOString();
  const currentErrorRate = currentRequestCount > 0 ? currentErrorCount / currentRequestCount : 0;

  // --- 1. Latency Spike (baseline deviation) ---
  if (currentRequestCount >= MIN_SAMPLES) {
    const baselineP95 = latencyBaseline[route] || [];
    if (baselineP95.length >= 3) {
      const baselineMedian = median(baselineP95);
      if (baselineMedian > 0 && currentP95 > baselineMedian * LATENCY_SPIKE_RATIO && currentP95 > 100) {
        await writeAnomaly(pool, producer, {
          detected_at: now,
          type: 'latency_spike',
          severity: currentP95 > baselineMedian * 5 ? 'high' : 'medium',
          route_template: route,
          details: {
            current_p95: Math.round(currentP95),
            baseline_median_p95: Math.round(baselineMedian),
            ratio: +(currentP95 / baselineMedian).toFixed(2),
          },
        });
      }
    } else if (currentP95 > 500) {
      // No baseline yet — use absolute threshold
      await writeAnomaly(pool, producer, {
        detected_at: now,
        type: 'latency_spike',
        severity: currentP95 > 2000 ? 'high' : 'medium',
        route_template: route,
        details: { current_p95: Math.round(currentP95), note: 'no baseline yet — using absolute threshold' },
      });
    }
  }

  // --- 2. Error Spike (baseline deviation) ---
  if (currentRequestCount >= MIN_SAMPLES) {
    const baselineErrorRates = errorRateBaseline[route] || [];
    if (baselineErrorRates.length >= 3) {
      const baselineMedianRate = median(baselineErrorRates);
      if (currentErrorRate > baselineMedianRate + 0.1 && currentErrorRate >= ERROR_RATE_SPIKE_THRESHOLD) {
        await writeAnomaly(pool, producer, {
          detected_at: now,
          type: 'error_spike',
          severity: currentErrorRate > 0.5 ? 'critical' : 'high',
          route_template: route,
          details: {
            current_error_rate: +currentErrorRate.toFixed(3),
            baseline_error_rate: +baselineMedianRate.toFixed(3),
            error_count: currentErrorCount,
            request_count: currentRequestCount,
          },
        });
      }
    } else if (currentErrorRate >= 0.3 && currentErrorCount >= 3) {
      // No baseline — absolute threshold
      await writeAnomaly(pool, producer, {
        detected_at: now,
        type: 'error_spike',
        severity: currentErrorRate > 0.7 ? 'critical' : 'high',
        route_template: route,
        details: { current_error_rate: +currentErrorRate.toFixed(3), note: 'no baseline yet' },
      });
    }
  }

  // --- 3. Traffic Spike / Flood (Z-score) ---
  if (currentRequestCount >= MIN_SAMPLES) {
    const baselineTraffic = trafficBaseline[route] || [];
    if (baselineTraffic.length >= 5) {
      const m = mean(baselineTraffic);
      const s = stddev(baselineTraffic);
      if (s > 0) {
        const zScore = (currentRequestCount - m) / s;
        if (zScore > TRAFFIC_ZSCORE_THRESHOLD) {
          await writeAnomaly(pool, producer, {
            detected_at: now,
            type: 'traffic_spike',
            severity: zScore > 4 ? 'high' : 'medium',
            route_template: route,
            details: {
              current_rps: currentRequestCount,
              baseline_mean: Math.round(m),
              z_score: +zScore.toFixed(2),
            },
          });
        }
      }
    }
  }

  // --- 4. Unusual Endpoint (route not previously seen) ---
  if (!knownRoutes.has(route)) {
    if (knownRoutes.size > 0) {
      // Only flag after we've established a baseline set
      await writeAnomaly(pool, producer, {
        detected_at: now,
        type: 'unusual_endpoint',
        severity: 'low',
        route_template: route,
        details: { message: `Route ${route} has not been seen before`, known_routes: knownRoutes.size },
      });
    }
    knownRoutes.add(route);
  }

  // Update baselines AFTER detection (so current window doesn't bias its own check)
  pushBaseline(latencyBaseline, route, currentP95);
  pushBaseline(errorRateBaseline, route, currentErrorRate);
  pushBaseline(trafficBaseline, route, currentRequestCount);
}

// --- Security Detectors (called on telemetry.security events) ---

// Per-IP / per-client sliding counters (in-memory, with Redis for persistence)
const loginFailCounters: Record<string, { count: number; windowStart: number }> = {};
const enumCounters: Record<string, Set<string>> = {};

const LOGIN_FLOOD_THRESHOLD = 10; // failures within window
const LOGIN_FLOOD_WINDOW_MS = 60_000; // 1 minute
const ENUM_THRESHOLD = 15; // distinct 404 paths within window
const ENUM_WINDOW_MS = 60_000;

export async function detectSecurityAnomaly(
  pool: Pool,
  producer: any,
  redis: RedisClient,
  event: { timestamp: string; client_ip?: string; client_id?: string; event_type: string; details: Record<string, any> }
) {
  const now = Date.now();
  const clientKey = event.client_id || event.client_ip || 'unknown';
  const anomalyNow = new Date().toISOString();

  if (event.event_type === 'login_failed') {
    // --- 5. Login Flood Detector ---
    if (!loginFailCounters[clientKey] || now - loginFailCounters[clientKey].windowStart > LOGIN_FLOOD_WINDOW_MS) {
      loginFailCounters[clientKey] = { count: 0, windowStart: now };
    }
    loginFailCounters[clientKey].count++;

    if (loginFailCounters[clientKey].count === LOGIN_FLOOD_THRESHOLD) {
      await writeAnomaly(pool, producer, {
        detected_at: anomalyNow,
        type: 'security',
        severity: 'high',
        route_template: '/login',
        details: {
          subtype: 'login_flood',
          client: clientKey,
          failures_in_window: loginFailCounters[clientKey].count,
          window_seconds: LOGIN_FLOOD_WINDOW_MS / 1000,
        },
      });
    }
  }

  if (event.event_type === 'endpoint_enumeration') {
    // --- 6. Endpoint Enumeration Detector ---
    const path = event.details?.path as string;
    if (path) {
      if (!enumCounters[clientKey]) enumCounters[clientKey] = new Set();
      enumCounters[clientKey].add(path);

      if (enumCounters[clientKey].size === ENUM_THRESHOLD) {
        await writeAnomaly(pool, producer, {
          detected_at: anomalyNow,
          type: 'security',
          severity: 'medium',
          route_template: undefined,
          details: {
            subtype: 'endpoint_enumeration',
            client: clientKey,
            distinct_paths_probed: enumCounters[clientKey].size,
          },
        });
      }

      // Reset after threshold to avoid repeated alerts per event
      if (enumCounters[clientKey].size > ENUM_THRESHOLD * 2) {
        enumCounters[clientKey] = new Set();
      }
    }
  }
}
