/**
 * Scenario 07: Slow Query
 * Enables slow_query fault on /users/:id (bypasses DB index),
 * asserts latency_spike anomaly fires with DB latency evidence.
 */
import { enableFault, disableFault, waitForAnomaly, generateTraffic, printResult, colors } from './utils';

async function run() {
  console.log(colors.bold('\n═══════════════════════════════════════════'));
  console.log(colors.bold(' Scenario 07: Slow DB Query'));
  console.log(colors.bold('═══════════════════════════════════════════'));

  console.log('\n1. Generating baseline traffic on /users/:id (15s)...');
  await generateTraffic(8, 15_000, ['/users/1', '/users/2', '/users/3']);

  console.log('\n2. Enabling slow_query fault (unindexed JOIN)...');
  await enableFault('slow_query');

  console.log('\n3. Generating traffic with slow query active (25s)...');
  generateTraffic(8, 25_000, ['/users/1', '/users/2', '/users/3']);

  console.log('\n4. Waiting for latency_spike anomaly...');
  const detected = await waitForAnomaly('latency_spike', 55_000);

  console.log('\n5. Disabling fault...');
  await disableFault('slow_query');

  printResult('Scenario 07: Slow DB Query', detected,
    detected ? 'latency_spike anomaly detected (DB query regression)' : 'Anomaly NOT detected');

  process.exit(detected ? 0 : 1);
}

run().catch(err => { console.error(err); process.exit(1); });
