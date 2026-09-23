/**
 * Scenario 05: Endpoint Enumeration
 * Simulates an attacker probing many unknown paths to trigger endpoint_enumeration security anomaly.
 */
import axios from 'axios';
import { waitForAnomaly, sleep, printResult, colors, GATEWAY_URL } from './utils';

const PROBE_PATHS = [
  '/admin', '/admin/users', '/api/keys', '/config', '/env', '/.env',
  '/api/v1/internal', '/debug', '/metrics', '/health', '/actuator',
  '/api/v2/users', '/api/v3/orders', '/private', '/secret',
  '/backup', '/db', '/console', '/phpmyadmin', '/wp-admin',
  '/api/tokens', '/api/export', '/system', '/manage', '/ops',
  '/api/admin/reset', '/internal/stats', '/api/billing',
  '/api/v1/delete-all', '/test', '/staging', '/dev',
];

async function run() {
  console.log(colors.bold('\n═══════════════════════════════════════════'));
  console.log(colors.bold(' Scenario 05: Endpoint Enumeration'));
  console.log(colors.bold('═══════════════════════════════════════════'));

  console.log(`\n1. Probing ${PROBE_PATHS.length} unknown paths...`);
  for (const path of PROBE_PATHS) {
    axios.get(`${GATEWAY_URL}${path}`, {
      validateStatus: () => true,
      timeout: 3000,
      headers: { 'X-Client-ID': 'scanner-bot-001' }, // consistent client ID
    }).catch(() => {});
    await sleep(150);
  }
  console.log(colors.yellow(`  Probed ${PROBE_PATHS.length} distinct paths`));

  console.log('\n2. Waiting for security anomaly (endpoint_enumeration)...');
  const detected = await waitForAnomaly('security', 40_000);

  printResult('Scenario 05: Endpoint Enumeration', detected,
    detected ? 'security/endpoint_enumeration anomaly detected' : 'Anomaly NOT detected');

  process.exit(detected ? 0 : 1);
}

run().catch(err => { console.error(err); process.exit(1); });
