import { LlmProvider, LlmMessage } from './providers';
import { TOOL_DEFINITIONS, executeTool } from './tools';

const MAX_TURNS = 6;

export interface RcaConclusion {
  summary: string;
  likely_root_cause: string;
  confidence: 'high' | 'medium' | 'low';
  evidence: Array<{ tool: string; finding: string }>;
  recommended_action: string;
}

export interface RcaResult {
  conclusion: RcaConclusion | null;
  tool_trace: Array<{ tool: string; args: Record<string, any>; result: string }>;
  turns: number;
  provider_available: boolean;
  raw_content?: string;
}

const SYSTEM_PROMPT = `You are an expert Site Reliability Engineer (SRE) AI assistant for the API Intelligence Platform.
Your job is to investigate incidents and determine their root cause using the available data tools.

You have access to these tools:
- getMetricsForRoute: Get latency percentiles and error rates for an API route
- getAnomaliesNear: Find anomalies around a specific time
- getRecentDeployments: Check for recent code deployments
- getDbQueryStats: Analyze database query performance
- getRawSamples: Get individual request samples for concrete evidence

Investigation strategy:
1. Start by looking at the anomalies that triggered this incident
2. Identify which route(s) are affected using metrics
3. Check for recent deployments that correlate with the issue timing
4. If latency is high, check DB query stats for that route
5. Use raw samples to verify your hypothesis with concrete data

Always conclude with a structured JSON response in this exact format:
{
  "summary": "Brief one-sentence summary",
  "likely_root_cause": "Specific root cause",
  "confidence": "high|medium|low",
  "evidence": [{"tool": "toolName", "finding": "key finding"}],
  "recommended_action": "What should be done to fix this"
}`;

export async function investigate(
  provider: LlmProvider,
  incident: {
    id: number;
    title: string;
    opened_at: string;
    linked_anomaly_ids: number[];
    linked_deployment_id?: number;
    rca_summary?: string;
  }
): Promise<RcaResult> {
  const toolTrace: RcaResult['tool_trace'] = [];

  const initialContext = `
Incident #${incident.id}: "${incident.title}"
Opened at: ${incident.opened_at}
${incident.linked_deployment_id ? `⚠️ Linked to deployment #${incident.linked_deployment_id}` : ''}
Anomaly IDs involved: ${incident.linked_anomaly_ids?.join(', ') || 'unknown'}

Please investigate this incident and determine the root cause.
Start by looking at anomalies near the incident time, then use metrics and other tools to build your case.
`;

  const messages: LlmMessage[] = [
    { role: 'system', content: SYSTEM_PROMPT },
    { role: 'user', content: initialContext },
  ];

  let turns = 0;
  let finalContent: string | null = null;

  while (turns < MAX_TURNS) {
    turns++;
    let response;
    try {
      response = await provider.chat(messages, TOOL_DEFINITIONS);
    } catch (err: any) {
      console.error(`[RCA] LLM error on turn ${turns}:`, err.message);
      break;
    }

    if (response.content) {
      finalContent = response.content;
    }

    if (!response.tool_calls || response.tool_calls.length === 0 || response.done) {
      break;
    }

    // Add the assistant message with tool calls
    messages.push({
      role: 'assistant',
      content: response.content || '',
    });

    // Execute each tool call
    for (const tc of response.tool_calls) {
      console.log(`[RCA] Calling tool: ${tc.name}(${JSON.stringify(tc.arguments).substring(0, 80)}...)`);
      const result = await executeTool(tc.name, tc.arguments);
      toolTrace.push({ tool: tc.name, args: tc.arguments, result: result.substring(0, 500) });

      messages.push({
        role: 'tool',
        content: result,
        tool_call_id: tc.id,
        name: tc.name,
      });
    }
  }

  // Parse the conclusion from the final content
  let conclusion: RcaConclusion | null = null;
  if (finalContent) {
    try {
      const jsonMatch = finalContent.match(/\{[\s\S]*"summary"[\s\S]*\}/);
      if (jsonMatch) {
        conclusion = JSON.parse(jsonMatch[0]);
      } else {
        // Wrap unstructured response
        conclusion = {
          summary: finalContent.substring(0, 200),
          likely_root_cause: 'Could not determine — see summary',
          confidence: 'low',
          evidence: toolTrace.map(t => ({ tool: t.tool, finding: t.result.substring(0, 100) })),
          recommended_action: 'Manual investigation required',
        };
      }
    } catch {
      conclusion = {
        summary: finalContent.substring(0, 300),
        likely_root_cause: 'Parse error — see raw content',
        confidence: 'low',
        evidence: [],
        recommended_action: 'Check raw_content field',
      };
    }
  }

  return {
    conclusion,
    tool_trace: toolTrace,
    turns,
    provider_available: true,
    raw_content: finalContent || undefined,
  };
}
