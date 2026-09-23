/**
 * Scenario 04: Login Flood (Brute Force)
 * Sends 15 rapid failed login attempts from one client to trigger login_flood security anomaly.
 */
import axios from 'axios';
import { waitForAnomaly, sleep, printResult, colors, GATEWAY_URL } from './utils';

async function run() {
  console.log(colors.bold('\n═══════════════════════════════════════════'));
  console.log(colors.bold(' Scenario 04: Login Flood (Brute Force)'));
  console.log(colors.bold('═══════════════════════════════════════════'));

  console.log('\n1. Sending 15 rapid failed login attempts...');
  const requests = [];
  for (let i = 0; i < 15; i++) {
    requests.push(
      axios.post(`${GATEWAY_URL}/login`, { username: 'attacker', password: `wrong-${i}` }, {
        validateStatus: () => true, // accept 401s
        timeout: 5000,
      })
    );
    await sleep(100); // 100ms apart — fast enough to trigger flood detection
  }
  await Promise.all(requests);
  console.log(colors.yellow(`  Sent 15 failed login attempts via gateway`));

  console.log('\n2. Waiting for security anomaly (login_flood)...');
  const detected = await waitForAnomaly('security', 30_000);

  printResult('Scenario 04: Login Flood', detected,
    detected ? 'security/login_flood anomaly detected' : 'Anomaly NOT detected');

  process.exit(detected ? 0 : 1);
}

run().catch(err => { console.error(err); process.exit(1); });
