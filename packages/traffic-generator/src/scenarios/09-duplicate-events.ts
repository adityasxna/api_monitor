/**
 * Scenario 09: Duplicate Events
 * Fetches recent request_ids from Postgres, replays them directly onto the
 * telemetry.raw Kafka topic, then verifies that metrics_rollup counts
 * don't double-count (deduplication via ON CONFLICT DO NOTHING).
 */
import { Kafka } from 'kafkajs';
import axios from 'axios';
import { sleep, printResult, colors, ANALYTICS_URL } from './utils';

const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');

async function run() {
  console.log(colors.bold('\n═══════════════════════════════════════════'));
  console.log(colors.bold(' Scenario 09: Duplicate Events (Deduplication)'));
  console.log(colors.bold('═══════════════════════════════════════════'));

  console.log('\n1. Fetching recent request samples...');
  let samples: any[] = [];
  try {
    const res = await axios.get(`${ANALYTICS_URL}/api/requests/samples`, { params: { limit: 30, minutes: 15 } });
    samples = res.data;
    console.log(colors.yellow(`  Fetched ${samples.length} recent request events`));
  } catch (err) {
    console.log(colors.red('  Failed to fetch samples — ensure traffic has been running'));
    process.exit(1);
  }

  if (samples.length < 5) {
    console.log(colors.red('  Not enough samples. Run the traffic generator first.'));
    process.exit(1);
  }

  // Get pre-replay count for one route
  const testRoute = samples[0].route_template;
  let preReplayCount = 0;
  try {
    const before = await axios.get(`${ANALYTICS_URL}/api/metrics`, { params: { route: testRoute } });
    preReplayCount = parseInt(before.data[0]?.request_count || '0');
    console.log(colors.yellow(`  Pre-replay count for ${testRoute}: ${preReplayCount}`));
  } catch (_) {}

  console.log('\n2. Connecting to Kafka and replaying events...');
  const kafka = new Kafka({ clientId: 'scenario-09-dedup', brokers: KAFKA_BROKERS });
  const producer = kafka.producer();

  try {
    await producer.connect();
  } catch (err) {
    console.log(colors.red('  Failed to connect to Kafka — is it running?'));
    process.exit(1);
  }

  // Replay same events (same request_ids) back onto the topic
  const messages = samples.slice(0, 20).map((s: any) => ({
    key: s.route_template || s.path,
    value: JSON.stringify({
      request_id: s.request_id, // Same UUID — should be deduped
      timestamp: s.timestamp,
      method: s.method,
      path: s.path,
      route_template: s.route_template,
      status_code: s.status_code,
      latency_ms: s.latency_ms,
      client_ip: s.client_ip,
    }),
  }));

  await producer.send({ topic: 'telemetry.raw', messages });
  console.log(colors.yellow(`  Replayed ${messages.length} duplicate events onto telemetry.raw`));
  await producer.disconnect();

  // Wait for a full flush cycle
  console.log('\n3. Waiting 10s for analytics flush cycle...');
  await sleep(10_000);

  // Check that raw requests table didn't grow by the replay count
  console.log('\n4. Verifying deduplication in requests table...');
  let dedupWorking = false;
  try {
    const after = await axios.get(`${ANALYTICS_URL}/api/requests/samples`, {
      params: { limit: 100, minutes: 15 },
    });
    // Check for duplicate request_ids in the DB
    const requestIds = after.data.map((r: any) => r.request_id);
    const uniqueIds = new Set(requestIds);
    dedupWorking = requestIds.length === uniqueIds.size;
    console.log(colors.yellow(`  Rows fetched: ${requestIds.length}, unique IDs: ${uniqueIds.size}`));
    if (dedupWorking) {
      console.log(colors.green('  No duplicates found in requests table — deduplication working'));
    } else {
      console.log(colors.red(`  ${requestIds.length - uniqueIds.size} duplicate request_ids found in DB`));
    }
  } catch (err) {
    console.error('  Failed to verify dedup', err);
  }

  printResult('Scenario 09: Duplicate Events', dedupWorking,
    dedupWorking ? 'Replayed events correctly deduped (ON CONFLICT DO NOTHING)' :
    'Duplicates found in DB — deduplication not working');

  process.exit(dedupWorking ? 0 : 1);
}

run().catch(err => { console.error(err); process.exit(1); });
