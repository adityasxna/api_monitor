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
  private modelResolved = false;

  constructor() {
    this.client = new OpenAI({
      apiKey: GROQ_API_KEY,
      baseURL: 'https://api.groq.com/openai/v1',
    });
    this.model = process.env.GROQ_MODEL || 'llama-3.3-70b-versatile';
  }

  private async resolveModel(): Promise<void> {
    if (this.modelResolved) return;
    if (process.env.GROQ_MODEL) {
      this.modelResolved = true;
      return;
    }

    try {
      const modelsList = await this.client.models.list();
      const availableIds = modelsList.data.map(m => m.id);
      
      const preferred = [
        'llama-3.3-70b-versatile',
        'llama-3.1-70b-versatile',
        'llama-3.1-8b-instant',
        'llama3-70b-8192',
        'llama3-8b-8192',
        'mixtral-8x7b-32768',
      ];

      const matched = preferred.find(id => availableIds.includes(id));
      if (matched) {
        this.model = matched;
        console.log(`[RCA] Selected Groq model: ${this.model}`);
      } else if (availableIds.length > 0) {
        this.model = availableIds[0];
        console.log(`[RCA] Using available Groq model: ${this.model}`);
      }
    } catch (err: any) {
      console.warn(`[RCA] Could not auto-detect Groq models (${err.message}). Using default: ${this.model}`);
    } finally {
      this.modelResolved = true;
    }
  }

  async chat(messages: LlmMessage[], tools: LlmTool[]): Promise<LlmResponse> {
    await this.resolveModel();

    try {
      return await this.executeChat(this.model, messages, tools);
    } catch (err: any) {
      if (err?.status === 404 || err?.message?.includes('404') || err?.message?.includes('does not exist')) {
        console.warn(`[RCA] Model '${this.model}' returned 404. Attempting fallback model...`);
        this.modelResolved = false;
        try {
          const modelsList = await this.client.models.list();
          const availableIds = modelsList.data.map(m => m.id);
          const fallback = availableIds.find(id => id !== this.model) || 'llama-3.1-8b-instant';
          this.model = fallback;
          console.log(`[RCA] Retrying with fallback model: ${this.model}`);
          return await this.executeChat(this.model, messages, tools);
        } catch (fallbackErr: any) {
          throw err;
        }
      }
      throw err;
    }
  }

  private async executeChat(modelName: string, messages: LlmMessage[], tools: LlmTool[]): Promise<LlmResponse> {
    const response = await this.client.chat.completions.create({
      model: modelName,
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
