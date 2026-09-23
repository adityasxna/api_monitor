/**
 * Shared utilities for scenario scripts
 */
import axios from 'axios';

export const GATEWAY_URL = process.env.GATEWAY_URL || 'http://localhost:3000';
export const TEST_API_URL = process.env.TEST_API_URL || 'http://localhost:3001';
export const ANALYTICS_URL = process.env.ANALYTICS_URL || 'http://localhost:3002';

export const colors = {
  green: (s: string) => `\x1b[32m${s}\x1b[0m`,
  red: (s: string) => `\x1b[31m${s}\x1b[0m`,
  yellow: (s: string) => `\x1b[33m${s}\x1b[0m`,
  cyan: (s: string) => `\x1b[36m${s}\x1b[0m`,
  bold: (s: string) => `\x1b[1m${s}\x1b[0m`,
};

export async function enableFault(type: string, options: Record<string, any> = {}) {
  const res = await axios.post(`${TEST_API_URL}/_fault/${type}`, { action: 'enable', ...options });
  console.log(colors.yellow(`  [fault] Enabled: ${type}`, ), options);
  return res.data;
}

export async function disableFault(type: string) {
  const res = await axios.post(`${TEST_API_URL}/_fault/${type}`, { action: 'disable' });
  console.log(colors.yellow(`  [fault] Disabled: ${type}`));
  return res.data;
}

export async function waitForAnomaly(
  type: string,
  timeoutMs: number = 45_000,
  pollIntervalMs: number = 3_000
): Promise<boolean> {
  const start = Date.now();
  const since = new Date(start - 5000).toISOString(); // small buffer

  console.log(colors.cyan(`  [wait] Polling for anomaly type="${type}" (timeout: ${timeoutMs / 1000}s)...`));

  while (Date.now() - start < timeoutMs) {
    try {
      const res = await axios.get(`${ANALYTICS_URL}/api/anomalies`, {
        params: { type, limit: 5, minutes: 2 },
      });
      if (res.data.length > 0) {
        const newest = res.data[0];
        if (new Date(newest.detected_at) >= new Date(since)) {
          console.log(colors.green(`  [found] Anomaly detected: ${JSON.stringify(newest.details).substring(0, 120)}`));
          return true;
        }
      }
    } catch (_) {}
    await sleep(pollIntervalMs);
  }
  return false;
}

export async function generateTraffic(rps: number, durationMs: number, routes?: string[]) {
  const defaultRoutes = ['/orders', '/products', '/users/1', '/orders/1'];
  const paths = routes || defaultRoutes;
  const delayMs = 1000 / rps;
  const end = Date.now() + durationMs;

  let sent = 0;
  while (Date.now() < end) {
    const path = paths[sent % paths.length];
    axios.get(`${GATEWAY_URL}${path}`, { timeout: 10_000 }).catch(() => {});
    sent++;
    await sleep(delayMs);
  }
  return sent;
}

export async function sleep(ms: number) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

export function printResult(scenarioName: string, passed: boolean, note?: string) {
  const status = passed ? colors.green('PASS') : colors.red('FAIL');
  console.log(`\n${colors.bold(scenarioName)}: ${status}${note ? ` — ${note}` : ''}`);
}
