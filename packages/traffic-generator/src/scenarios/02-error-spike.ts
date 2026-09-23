/**
 * Scenario 02: Error Spike
 * Enables 80% 500-error rate on all routes, asserts error_spike anomaly fires.
 */
import { enableFault, disableFault, waitForAnomaly, generateTraffic, printResult, colors } from './utils';

async function run() {
  console.log(colors.bold('\n═══════════════════════════════════════════'));
  console.log(colors.bold(' Scenario 02: Error Spike'));
  console.log(colors.bold('═══════════════════════════════════════════'));

  console.log('\n1. Generating baseline traffic (15s)...');
  await generateTraffic(8, 15_000);

  console.log('\n2. Enabling error_spike fault at 80% rate...');
  await enableFault('error_spike', { rate: 0.8 });

  console.log('\n3. Generating traffic with fault active (20s)...');
  generateTraffic(10, 20_000);

  console.log('\n4. Waiting for error_spike anomaly...');
  const detected = await waitForAnomaly('error_spike', 50_000);

  console.log('\n5. Disabling fault...');
  await disableFault('error_spike');

  printResult('Scenario 02: Error Spike', detected,
    detected ? 'error_spike anomaly detected' : 'Anomaly NOT detected');

  process.exit(detected ? 0 : 1);
}

run().catch(err => { console.error(err); process.exit(1); });
