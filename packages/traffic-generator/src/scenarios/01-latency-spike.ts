/**
 * Scenario 01: Latency Spike
 * Enables 3-second delay on /orders, runs traffic, asserts latency_spike anomaly fires.
 */
import { enableFault, disableFault, waitForAnomaly, generateTraffic, sleep, printResult, colors } from './utils';

async function run() {
  console.log(colors.bold('\n═══════════════════════════════════════════'));
  console.log(colors.bold(' Scenario 01: Latency Spike'));
  console.log(colors.bold('═══════════════════════════════════════════'));

  console.log('\n1. Generating baseline traffic (15s) to build detector baseline...');
  await generateTraffic(8, 15_000);

  console.log('\n2. Enabling latency_spike fault on /orders...');
  await enableFault('latency_spike', { targetRoute: '/orders' });

  console.log('\n3. Generating traffic with fault active (20s)...');
  generateTraffic(8, 20_000, ['/orders', '/orders/1']);

  console.log('\n4. Waiting for latency_spike anomaly...');
  const detected = await waitForAnomaly('latency_spike', 50_000);

  console.log('\n5. Disabling fault and returning to steady state...');
  await disableFault('latency_spike');

  printResult('Scenario 01: Latency Spike', detected,
    detected ? 'latency_spike anomaly detected within window' : 'Anomaly NOT detected — check analytics logs');

  process.exit(detected ? 0 : 1);
}

run().catch(err => { console.error(err); process.exit(1); });
