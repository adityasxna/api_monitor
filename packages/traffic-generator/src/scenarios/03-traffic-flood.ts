/**
 * Scenario 03: Traffic Flood
 * Bursts to 80 RPS for 30 seconds to trigger a traffic_spike anomaly.
 */
import { waitForAnomaly, generateTraffic, sleep, printResult, colors } from './utils';

async function run() {
  console.log(colors.bold('\n═══════════════════════════════════════════'));
  console.log(colors.bold(' Scenario 03: Traffic Flood'));
  console.log(colors.bold('═══════════════════════════════════════════'));

  console.log('\n1. Generating baseline traffic (20s) at 8 RPS...');
  await generateTraffic(8, 20_000);

  console.log('\n2. Bursting to 80 RPS for 30s...');
  generateTraffic(80, 30_000);

  console.log('\n3. Waiting for traffic_spike anomaly...');
  const detected = await waitForAnomaly('traffic_spike', 50_000);

  printResult('Scenario 03: Traffic Flood', detected,
    detected ? 'traffic_spike anomaly detected' : 'Anomaly NOT detected — may need more baseline windows');

  process.exit(detected ? 0 : 1);
}

run().catch(err => { console.error(err); process.exit(1); });
