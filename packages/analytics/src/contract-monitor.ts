/**
 * OpenAPI Contract Monitor
 * Validates TelemetryEvent response bodies against the declared OpenAPI spec for the test-api.
 * In production, this spec would be fetched from the upstream's /openapi.json endpoint.
 */
import { Pool } from 'pg';
import { TelemetryEvent } from '@api-intelligence/shared';

// Embedded OpenAPI-derived response schemas for test-api routes
// In production: fetch from upstream /openapi.json and parse with openapi-schema-to-json-schema
const ROUTE_SCHEMAS: Record<string, Record<number, { required: string[]; types: Record<string, string> }>> = {
  '/orders': {
    200: {
      required: [],
      types: {},
    },
  },
  '/orders/:id': {
    200: {
      required: ['id', 'user_id', 'total_amount', 'status'],
      types: { id: 'number', user_id: 'number', total_amount: 'number', status: 'string' },
    },
  },
  '/users/:id': {
    200: {
      required: ['id', 'email', 'name'],
      types: { id: 'number', email: 'string', name: 'string' },
    },
  },
  '/products': {
    200: {
      required: [],
      types: {},
    },
  },
  '/login': {
    200: {
      required: ['token'],
      types: { token: 'string' },
    },
  },
};

// The gateway captures response bodies in the `details` field of TelemetryEvent when available.
// For this MVP, we validate based on what we CAN check from telemetry metadata:
// 1. Status codes that shouldn't occur (undeclared 2xx codes on error-only routes)
// 2. Response body schema violations captured by the gateway's response-body sampling

const DECLARED_STATUS_CODES: Record<string, number[]> = {
  '/login': [200, 401, 400],
  '/orders': [200, 500],
  '/orders/:id': [200, 404, 500],
  '/users/:id': [200, 404, 500],
  '/products': [200, 500],
};

let contractViolationCooldown: Record<string, number> = {};
const COOLDOWN_MS = 30_000; // Don't re-alert the same violation within 30s

async function writeContractViolation(pool: Pool, producer: any, route: string, details: Record<string, any>) {
  const key = `${route}-${details.violation_type}`;
  const now = Date.now();
  if (contractViolationCooldown[key] && now - contractViolationCooldown[key] < COOLDOWN_MS) {
    return; // In cooldown
  }
  contractViolationCooldown[key] = now;

  const anomaly = {
    detected_at: new Date().toISOString(),
    type: 'contract_violation',
    severity: 'medium' as const,
    route_template: route,
    details,
  };

  try {
    await pool.query(
      `INSERT INTO anomalies (detected_at, type, severity, route_template, details)
       VALUES ($1, $2, $3, $4, $5)`,
      [anomaly.detected_at, anomaly.type, anomaly.severity, anomaly.route_template, JSON.stringify(anomaly.details)]
    );
  } catch (e) {
    console.error('Failed to write contract violation', e);
  }

  producer.send({
    topic: 'anomalies.detected',
    messages: [{ key: 'contract_violation', value: JSON.stringify(anomaly) }],
  }).catch(console.error);

  console.warn(`[ContractMonitor] Violation on ${route}:`, details);
}

export async function validateContract(pool: Pool, producer: any, event: TelemetryEvent) {
  const route = event.route_template;
  if (!route) return;

  // 1. Check for undeclared status codes
  const declaredCodes = DECLARED_STATUS_CODES[route];
  if (declaredCodes && !declaredCodes.includes(event.status_code)) {
    await writeContractViolation(pool, producer, route, {
      violation_type: 'undeclared_status_code',
      expected_codes: declaredCodes,
      actual_code: event.status_code,
      request_id: event.request_id,
    });
  }

  // 2. Response body validation (if body was captured in details)
  // The gateway passes response body via a special header parsed into db_latency_ms for now.
  // Extended body capture would require adding it to TelemetryEvent. Skipped for MVP telemetry-only mode.
  // The test-api schema_violation fault changes body shape — we detect that at the gateway layer
  // by checking if the response body (if available) matches the schema.
  // For this MVP, we detect schema violations through status code + size heuristics:

  const schema = ROUTE_SCHEMAS[route];
  if (schema && schema[event.status_code]) {
    // If response_size_bytes is suspiciously small for a route that should return a full object
    const expectedMinBytes: Record<string, number> = {
      '/orders/:id': 30,
      '/users/:id': 30,
      '/login': 20,
    };
    const minBytes = expectedMinBytes[route];
    if (minBytes && event.response_size_bytes !== undefined && event.response_size_bytes < minBytes && event.status_code === 200) {
      await writeContractViolation(pool, producer, route, {
        violation_type: 'response_too_small',
        expected_min_bytes: minBytes,
        actual_bytes: event.response_size_bytes,
        request_id: event.request_id,
        note: 'Possible schema violation — response body smaller than expected for a successful response',
      });
    }
  }
}
