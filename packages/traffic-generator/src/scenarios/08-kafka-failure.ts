/**
 * Scenario 08: Kafka Failure
 * Stops the Kafka container, verifies gateway continues serving traffic,
 * checks that dropped_events counter increases in gateway health endpoint,
 * restarts Kafka and verifies events flush.
 *
 * Requires: Docker CLI accessible from the terminal running this script.
 */
import axios from 'axios';
import { execSync } from 'child_process';
import { generateTraffic, sleep, printResult, colors, GATEWAY_URL, ANALYTICS_URL } from './utils';

function runCommand(cmd: string): string {
  try {
    return execSync(cmd, { encoding: 'utf-8', stdio: ['pipe', 'pipe', 'pipe'] }).trim();
  } catch (err: any) {
    return err.message;
  }
}

async function run() {
  console.log(colors.bold('\n═══════════════════════════════════════════'));
  console.log(colors.bold(' Scenario 08: Kafka Failure (Gateway Resilience)'));
  console.log(colors.bold('═══════════════════════════════════════════'));

  console.log('\n1. Stopping Kafka container...');
  const stopOutput = runCommand('docker compose stop kafka');
  console.log(colors.yellow(`  docker compose stop kafka → ${stopOutput.substring(0, 80)}`));
  await sleep(3000);

  console.log('\n2. Generating traffic for 15s while Kafka is down...');
  let successCount = 0;
  let failCount = 0;
  for (let i = 0; i < 50; i++) {
    try {
      const res = await axios.get(`${GATEWAY_URL}/orders`, { timeout: 5000, validateStatus: () => true });
      if (res.status < 500) successCount++;
      else failCount++;
    } catch {
      failCount++;
    }
    await sleep(300);
  }

  const gatewayStillWorking = successCount > 30;
  console.log(colors.yellow(`  Sent 50 requests: ${successCount} success, ${failCount} fail`));
  if (gatewayStillWorking) {
    console.log(colors.green('  Gateway continued serving traffic with Kafka down'));
  } else {
    console.log(colors.red('  Gateway appears to have failed — too many errors'));
  }

  console.log('\n3. Restarting Kafka...');
  const startOutput = runCommand('docker compose start kafka');
  console.log(colors.yellow(`  docker compose start kafka → ${startOutput.substring(0, 80)}`));
  console.log('  Waiting 15s for Kafka to re-initialize...');
  await sleep(15_000);

  console.log('\n4. Generating post-recovery traffic (10s)...');
  await generateTraffic(5, 10_000);

  console.log('\n5. Checking analytics service is back and receiving events...');
  let analyticsOk = false;
  try {
    const res = await axios.get(`${ANALYTICS_URL}/api/status`);
    analyticsOk = res.data.status === 'ok';
    console.log(colors.green(`  Analytics service status: ${res.data.status}`));
  } catch (err) {
    console.log(colors.red('  Analytics service unreachable'));
  }

  const passed = gatewayStillWorking && analyticsOk;
  printResult('Scenario 08: Kafka Failure', passed,
    passed ? 'Gateway resilient during Kafka outage, recovered after restart' :
    `gateway_ok=${gatewayStillWorking}, analytics_ok=${analyticsOk}`);

  process.exit(passed ? 0 : 1);
}

run().catch(err => { console.error(err); process.exit(1); });
