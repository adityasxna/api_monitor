/**
 * Run All Scenarios — End-to-End Validation Orchestrator
 * Runs scenarios 01–09 in sequence and prints a pass/fail summary table.
 *
 * Usage:
 *   npx ts-node src/run-all-scenarios.ts
 *
 * Prerequisites:
 *   - docker compose up (all services running)
 *   - Traffic generator running (npm run dev) OR scenarios will generate their own traffic
 */
import { execSync } from 'child_process';
import { sleep } from './scenarios/utils';

const colors = {
  green: (s: string) => `\x1b[32m${s}\x1b[0m`,
  red: (s: string) => `\x1b[31m${s}\x1b[0m`,
  yellow: (s: string) => `\x1b[33m${s}\x1b[0m`,
  cyan: (s: string) => `\x1b[36m${s}\x1b[0m`,
  bold: (s: string) => `\x1b[1m${s}\x1b[0m`,
};

interface ScenarioResult {
  name: string;
  file: string;
  status: 'PASS' | 'FAIL' | 'SKIP' | 'ERROR';
  duration: number;
  error?: string;
}

const SCENARIOS = [
  { name: '01: Latency Spike', file: 'src/scenarios/01-latency-spike.ts' },
  { name: '02: Error Spike', file: 'src/scenarios/02-error-spike.ts' },
  { name: '03: Traffic Flood', file: 'src/scenarios/03-traffic-flood.ts' },
  { name: '04: Login Flood', file: 'src/scenarios/04-login-flood.ts' },
  { name: '05: Endpoint Enumeration', file: 'src/scenarios/05-endpoint-enumeration.ts' },
  { name: '05b: Schema Violation', file: 'src/scenarios/05b-schema-violation.ts' },
  { name: '06: Deployment Correlation', file: 'src/scenarios/06-deployment-correlation.ts' },
  { name: '07: Slow Query', file: 'src/scenarios/07-slow-query.ts' },
  { name: '08: Kafka Failure', file: 'src/scenarios/08-kafka-failure.ts' },
  { name: '09: Duplicate Events', file: 'src/scenarios/09-duplicate-events.ts' },
];

function runScenario(file: string, timeoutMs: number = 120_000): { exitCode: number; duration: number } {
  const start = Date.now();
  try {
    execSync(`npx ts-node ${file}`, {
      cwd: process.cwd(),
      timeout: timeoutMs,
      stdio: 'inherit',
      env: { ...process.env },
    });
    return { exitCode: 0, duration: Date.now() - start };
  } catch (err: any) {
    return { exitCode: err.status || 1, duration: Date.now() - start };
  }
}

function printSummary(results: ScenarioResult[]) {
  console.log('\n');
  console.log(colors.bold('═══════════════════════════════════════════════════════════════'));
  console.log(colors.bold('                   END-TO-END VALIDATION RESULTS              '));
  console.log(colors.bold('═══════════════════════════════════════════════════════════════'));
  console.log('');

  const colW = [30, 8, 10];
  console.log(
    colors.bold('Scenario'.padEnd(colW[0])) +
    colors.bold('Status'.padEnd(colW[1])) +
    colors.bold('Duration')
  );
  console.log('─'.repeat(55));

  let passed = 0;
  let failed = 0;
  for (const r of results) {
    const statusStr = r.status === 'PASS'
      ? colors.green('PASS')
      : r.status === 'SKIP' ? colors.yellow('SKIP') : colors.red(r.status);
    const durStr = `${(r.duration / 1000).toFixed(1)}s`;
    console.log(`${r.name.padEnd(colW[0])}${statusStr.padEnd(colW[1] + 9)}${durStr}`);
    if (r.status === 'PASS') passed++;
    else if (r.status === 'FAIL' || r.status === 'ERROR') failed++;
  }

  console.log('─'.repeat(55));
  console.log(`\nTotal: ${results.length} scenarios | ${colors.green(`${passed} PASS`)} | ${failed > 0 ? colors.red(`${failed} FAIL`) : colors.green('0 FAIL')}`);

  if (failed === 0) {
    console.log(colors.bold(colors.green('\n✓ All scenarios passed — platform validated!')));
  } else {
    console.log(colors.bold(colors.red(`\n✗ ${failed} scenario(s) failed — check logs above`)));
  }
}

async function main() {
  console.log(colors.bold(colors.cyan('\nAPI Intelligence Platform — End-to-End Validation')));
  console.log(colors.cyan(`Running ${SCENARIOS.length} fault-injection scenarios...\n`));

  const results: ScenarioResult[] = [];

  for (const scenario of SCENARIOS) {
    console.log(colors.bold(colors.cyan(`\n▶ Running ${scenario.name}...`)));
    const { exitCode, duration } = runScenario(scenario.file);
    results.push({
      name: scenario.name,
      file: scenario.file,
      status: exitCode === 0 ? 'PASS' : 'FAIL',
      duration,
    });

    // Cool-down between scenarios to let anomaly cooldowns reset
    console.log(`\n  [cooldown] Waiting 10s between scenarios...`);
    await sleep(10_000);
  }

  printSummary(results);
  const anyFailed = results.some(r => r.status === 'FAIL' || r.status === 'ERROR');
  process.exit(anyFailed ? 1 : 0);
}

main().catch(err => { console.error(err); process.exit(1); });
