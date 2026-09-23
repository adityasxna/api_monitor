import express from 'express';
import cors from 'cors';
import { Pool } from 'pg';
import { createProvider, PROVIDER_CONFIGURED } from './providers';
import { investigate } from './agent';

const PORT = process.env.PORT || 3003;
const ANALYTICS_URL = process.env.ANALYTICS_URL || 'http://localhost:3002';

const pool = new Pool({
  user: process.env.POSTGRES_USER || 'api_monitor',
  host: process.env.POSTGRES_HOST || 'localhost',
  database: process.env.POSTGRES_DB || 'api_monitor',
  password: process.env.POSTGRES_PASSWORD || 'password',
  port: parseInt(process.env.POSTGRES_PORT || '5432', 10),
});

let provider: ReturnType<typeof createProvider>;
try {
  provider = createProvider();
} catch (err: any) {
  console.error('[RCA] Provider initialization failed:', err.message);
  process.exit(1);
}

const app = express();
app.use(cors());
app.use(express.json());

// POST /rca/investigate { incidentId }
app.post('/rca/investigate', async (req, res) => {
  const { incidentId } = req.body;
  if (!incidentId) {
    return res.status(400).json({ error: 'incidentId is required' });
  }

  console.log(`[RCA] Starting investigation for incident #${incidentId}`);

  try {
    // Fetch incident details
    const result = await pool.query(
      `SELECT i.*, d.version as deployment_version, d.deployed_at
       FROM incidents i
       LEFT JOIN deployments d ON d.id = i.linked_deployment_id
       WHERE i.id = $1`,
      [incidentId]
    );

    if (!result.rows.length) {
      return res.status(404).json({ error: `Incident #${incidentId} not found` });
    }

    const incident = result.rows[0];

    if (!PROVIDER_CONFIGURED) {
      // Write a placeholder RCA summary so UI shows something
      const placeholderSummary = 'RCA agent not configured. Set GROQ_API_KEY environment variable or LLM_PROVIDER=ollama to enable AI root-cause analysis.';
      await pool.query(
        `UPDATE incidents SET rca_summary = $1, rca_raw = $2 WHERE id = $3`,
        [placeholderSummary, JSON.stringify({ provider: 'none', note: 'No LLM provider configured' }), incidentId]
      );
      return res.json({ incidentId, rca_summary: placeholderSummary, provider: 'none' });
    }

    // Run investigation
    const rcaResult = await investigate(provider, {
      id: incident.id,
      title: incident.title,
      opened_at: incident.opened_at,
      linked_anomaly_ids: incident.linked_anomaly_ids || [],
      linked_deployment_id: incident.linked_deployment_id,
    });

    const summaryText = rcaResult.conclusion
      ? `${rcaResult.conclusion.summary}\n\nRoot cause: ${rcaResult.conclusion.likely_root_cause}\n\nRecommended: ${rcaResult.conclusion.recommended_action}`
      : 'Investigation completed but no conclusion was reached.';

    // Persist to incident record
    await pool.query(
      `UPDATE incidents SET rca_summary = $1, rca_raw = $2 WHERE id = $3`,
      [summaryText, JSON.stringify(rcaResult), incidentId]
    );

    // Notify analytics WS layer
    await fetch(`${ANALYTICS_URL}/api/status`).catch(() => {});

    console.log(`[RCA] Completed investigation for incident #${incidentId} (${rcaResult.turns} turns)`);

    res.json({
      incidentId,
      conclusion: rcaResult.conclusion,
      tool_trace: rcaResult.tool_trace,
      turns: rcaResult.turns,
      rca_summary: summaryText,
    });
  } catch (err: any) {
    console.error('[RCA] Investigation failed:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// GET /rca/status
app.get('/rca/status', (_req, res) => {
  res.json({
    status: 'ok',
    provider_configured: PROVIDER_CONFIGURED,
    provider: process.env.LLM_PROVIDER || 'none',
    timestamp: new Date().toISOString(),
  });
});

app.listen(PORT, () => {
  console.log(`RCA Agent service listening on port ${PORT}`);
  console.log(`LLM provider: ${process.env.LLM_PROVIDER || 'none'}${PROVIDER_CONFIGURED ? '' : ' (set GROQ_API_KEY to enable)'}`);
});
