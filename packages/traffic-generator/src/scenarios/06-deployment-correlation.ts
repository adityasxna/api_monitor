/**
 * Scenario 06: Deployment Correlation
 * 1. Simulates a deployment marker
 * 2. Enables slow_query fault
 * 3. Asserts that the resulting anomaly/incident is linked to the deployment
 */
import axios from 'axios';
import { enableFault, disableFault, waitForAnomaly, generateTraffic, sleep, printResult, colors, ANALYTICS_URL } from './utils';

async function run() {
  console.log(colors.bold('\n═══════════════════════════════════════════'));
  console.log(colors.bold(' Scenario 06: Deployment Correlation'));
  console.log(colors.bold('═══════════════════════════════════════════'));

  console.log('\n1. Generating baseline traffic (15s)...');
  await generateTraffic(8, 15_000, ['/users/1']);

  console.log('\n2. Recording a simulated deployment...');
  const deployRes = await axios.post(`${ANALYTICS_URL}/api/deployments`, {
    version: `v2.1.${Date.now().toString().slice(-4)}`,
    commit_sha: 'abc1234',
    description: 'Optimized user query — removed index accidentally',
  });
  const deployment = deployRes.data;
  console.log(colors.yellow(`  Deployment recorded: ${deployment.version} (id=${deployment.id})`));

  await sleep(2000); // Give the DB a moment

  console.log('\n3. Enabling slow_query fault (simulating regression after deploy)...');
  await enableFault('slow_query');

  console.log('\n4. Generating traffic hitting /users/:id with slow query (20s)...');
  generateTraffic(10, 20_000, ['/users/1']);

  console.log('\n5. Waiting for latency_spike anomaly...');
  const anomalyDetected = await waitForAnomaly('latency_spike', 50_000);

  await sleep(5000); // Allow incident to be created and linked

  console.log('\n6. Checking if incident was linked to deployment...');
  let deploymentLinked = false;
  try {
    const incidentsRes = await axios.get(`${ANALYTICS_URL}/api/incidents`, { params: { limit: 5 } });
    const incidents = incidentsRes.data;
    for (const inc of incidents) {
      if (inc.linked_deployment_id === deployment.id) {
        deploymentLinked = true;
        console.log(colors.green(`  Incident #${inc.id} correctly linked to deployment #${deployment.id} (${deployment.version})`));
        break;
      }
    }
  } catch (err) {
    console.error('Failed to query incidents', err);
  }

  console.log('\n7. Disabling fault...');
  await disableFault('slow_query');

  const passed = anomalyDetected && deploymentLinked;
  printResult('Scenario 06: Deployment Correlation', passed,
    passed ? 'anomaly detected AND incident linked to deployment' :
    `anomaly=${anomalyDetected}, deployment_linked=${deploymentLinked}`);

  process.exit(passed ? 0 : 1);
}

run().catch(err => { console.error(err); process.exit(1); });
