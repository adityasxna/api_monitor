import { Pool } from 'pg';
import { Kafka } from 'kafkajs';
import { Anomaly } from '@api-intelligence/shared';

export async function detectAnomalies(
  pool: Pool,
  producer: any,
  route: string,
  currentP95: number,
  currentErrorCount: number,
  currentRequestCount: number
) {
  // Very simplified anomaly detection based on hardcoded baseline for demo purposes.
  // In a real system, we'd query trailing baseline from DB.
  
  const anomaliesToEmit: Anomaly[] = [];
  const now = new Date().toISOString();

  // 1. Latency Spike (assuming baseline is ~50ms)
  if (currentP95 > 150) {
    anomaliesToEmit.push({
      detected_at: now,
      type: 'latency_spike',
      severity: currentP95 > 500 ? 'high' : 'medium',
      route_template: route,
      details: { current_p95: currentP95, baseline_p95: 50 },
    });
  }

  // 2. Error Spike (assuming baseline is < 2)
  if (currentErrorCount > 5) {
    anomaliesToEmit.push({
      detected_at: now,
      type: 'error_spike',
      severity: 'critical',
      route_template: route,
      details: { error_count: currentErrorCount, threshold: 5 },
    });
  }

  for (const anomaly of anomaliesToEmit) {
    // Write to Postgres
    await pool.query(
      `INSERT INTO anomalies (detected_at, type, severity, route_template, details)
       VALUES ($1, $2, $3, $4, $5)`,
      [anomaly.detected_at, anomaly.type, anomaly.severity, anomaly.route_template, JSON.stringify(anomaly.details)]
    ).catch(e => console.error('Failed to write anomaly to DB', e));

    // Publish to Kafka (fire and forget)
    producer.send({
      topic: 'anomalies.detected',
      messages: [{ key: anomaly.type, value: JSON.stringify(anomaly) }]
    }).catch((e: any) => console.error('Failed to publish anomaly to Kafka', e));
  }
}
