import OpenAI from 'openai';

const GROQ_API_KEY = process.env.GROQ_API_KEY;
const OLLAMA_BASE_URL = process.env.OLLAMA_BASE_URL || 'http://localhost:11434/v1';
const LLM_PROVIDER = process.env.LLM_PROVIDER || (GROQ_API_KEY ? 'groq' : 'none');

export interface LlmMessage {
  role: 'system' | 'user' | 'assistant' | 'tool';
  content: string;
  tool_call_id?: string;
  name?: string;
}

export interface LlmTool {
  name: string;
  description: string;
  parameters: Record<string, any>;
}

export interface LlmToolCall {
  id: string;
  name: string;
  arguments: Record<string, any>;
}

export interface LlmResponse {
  content: string | null;
  tool_calls?: LlmToolCall[];
  done: boolean;
}

export interface LlmProvider {
  chat(messages: LlmMessage[], tools: LlmTool[]): Promise<LlmResponse>;
}

// ─── Groq Provider ────────────────────────────────────────────────────────────
class GroqProvider implements LlmProvider {
  private client: OpenAI;
  private model: string;

  constructor() {
    this.client = new OpenAI({
      apiKey: GROQ_API_KEY,
      baseURL: 'https://api.groq.com/openai/v1',
    });
    this.model = process.env.GROQ_MODEL || 'llama-3.3-70b-versatile';
  }

  async chat(messages: LlmMessage[], tools: LlmTool[]): Promise<LlmResponse> {
    const response = await this.client.chat.completions.create({
      model: this.model,
      messages: messages as any,
      tools: tools.map(t => ({
        type: 'function' as const,
        function: {
          name: t.name,
          description: t.description,
          parameters: t.parameters,
        },
      })),
      tool_choice: 'auto',
      temperature: 0.1,
      max_tokens: 2048,
    });

    const choice = response.choices[0];
    const msg = choice.message;

    return {
      content: msg.content,
      done: choice.finish_reason === 'stop',
      tool_calls: msg.tool_calls?.map(tc => ({
        id: tc.id,
        name: tc.function.name,
        arguments: JSON.parse(tc.function.arguments || '{}'),
      })),
    };
  }
}

// ─── Ollama Provider ──────────────────────────────────────────────────────────
class OllamaProvider implements LlmProvider {
  private client: OpenAI;
  private model: string;

  constructor() {
    this.client = new OpenAI({
      apiKey: 'ollama', // Ollama doesn't require a key
      baseURL: OLLAMA_BASE_URL,
    });
    this.model = process.env.OLLAMA_MODEL || 'llama3.1';
  }

  async chat(messages: LlmMessage[], tools: LlmTool[]): Promise<LlmResponse> {
    const response = await this.client.chat.completions.create({
      model: this.model,
      messages: messages as any,
      tools: tools.map(t => ({
        type: 'function' as const,
        function: {
          name: t.name,
          description: t.description,
          parameters: t.parameters,
        },
      })),
      tool_choice: 'auto',
    });

    const choice = response.choices[0];
    const msg = choice.message;

    return {
      content: msg.content,
      done: choice.finish_reason === 'stop',
      tool_calls: msg.tool_calls?.map(tc => ({
        id: tc.id,
        name: tc.function.name,
        arguments: JSON.parse(tc.function.arguments || '{}'),
      })),
    };
  }
}

// ─── No-op Provider (when no key is configured) ───────────────────────────────
class NoProvider implements LlmProvider {
  async chat(_messages: LlmMessage[], _tools: LlmTool[]): Promise<LlmResponse> {
    return {
      content: 'RCA agent not configured. Set GROQ_API_KEY or LLM_PROVIDER=ollama to enable AI analysis.',
      done: true,
    };
  }
}

export function createProvider(): LlmProvider {
  switch (LLM_PROVIDER) {
    case 'groq':
      if (!GROQ_API_KEY) throw new Error('GROQ_API_KEY is required when LLM_PROVIDER=groq');
      console.log('[RCA] Using Groq provider');
      return new GroqProvider();
    case 'ollama':
      console.log(`[RCA] Using Ollama provider at ${OLLAMA_BASE_URL}`);
      return new OllamaProvider();
    default:
      console.warn('[RCA] No LLM provider configured. Set GROQ_API_KEY or LLM_PROVIDER=ollama');
      return new NoProvider();
  }
}

export const PROVIDER_CONFIGURED = LLM_PROVIDER !== 'none';
