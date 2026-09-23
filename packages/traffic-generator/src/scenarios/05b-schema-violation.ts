/**
 * Scenario 05b: Schema Violation (OpenAPI Contract Breach)
 * Enables schema_violation fault on test-api, runs traffic to /orders/:id,
 * asserts contract_violation anomaly fires.
 */
import { enableFault, disableFault, waitForAnomaly, generateTraffic, printResult, colors } from './utils';

async function run() {
  console.log(colors.bold('\n═══════════════════════════════════════════'));
  console.log(colors.bold(' Scenario 05b: Schema Violation (Contract Monitor)'));
  console.log(colors.bold('═══════════════════════════════════════════'));

  console.log('\n1. Generating baseline traffic (15s)...');
  await generateTraffic(8, 15_000, ['/orders/1']);

  console.log('\n2. Enabling schema_violation fault on /orders/:id...');
  await enableFault('schema_violation');

  console.log('\n3. Generating traffic with schema violation active (20s)...');
  generateTraffic(10, 20_000, ['/orders/1']);

  console.log('\n4. Waiting for contract_violation anomaly...');
  const detected = await waitForAnomaly('contract_violation', 50_000);

  console.log('\n5. Disabling fault...');
  await disableFault('schema_violation');

  printResult('Scenario 05b: Schema Violation', detected,
    detected ? 'contract_violation anomaly detected' : 'Anomaly NOT detected — check contract monitor logs');

  process.exit(detected ? 0 : 1);
}

run().catch(err => { console.error(err); process.exit(1); });
