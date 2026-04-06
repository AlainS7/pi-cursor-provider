/**
 * Cursor Provider Extension for pi
 *
 * Provides access to Cursor models (Claude, GPT, Gemini, etc.) via:
 * 1. Browser-based PKCE OAuth login to Cursor
 * 2. Local proxy translating OpenAI format → Cursor gRPC protocol
 *
 * Usage:
 *   /login cursor    — authenticate via browser
 *   /model           — select any Cursor model
 *
 * Based on https://github.com/ephraimduncan/opencode-cursor by Ephraim Duncan.
 */

import type { ExtensionAPI } from "@mariozechner/pi-coding-agent";
import type { OAuthCredentials, OAuthLoginCallbacks } from "@mariozechner/pi-ai";
import {
  generateCursorAuthParams,
  getTokenExpiry,
  pollCursorAuth,
  refreshCursorToken,
} from "./auth.js";
import { getCursorModels, startProxy, type CursorModel } from "./proxy.js";

// ── Cost estimation ──

interface ModelCost {
  input: number;
  output: number;
  cacheRead: number;
  cacheWrite: number;
}

const MODEL_COST_TABLE: Record<string, ModelCost> = {
  "claude-4-sonnet":         { input: 3, output: 15, cacheRead: 0.3, cacheWrite: 3.75 },
  "claude-4.5-haiku":        { input: 1, output: 5, cacheRead: 0.1, cacheWrite: 1.25 },
  "claude-4.5-opus":         { input: 5, output: 25, cacheRead: 0.5, cacheWrite: 6.25 },
  "claude-4.5-sonnet":       { input: 3, output: 15, cacheRead: 0.3, cacheWrite: 3.75 },
  "claude-4.6-opus":         { input: 5, output: 25, cacheRead: 0.5, cacheWrite: 6.25 },
  "claude-4.6-sonnet":       { input: 3, output: 15, cacheRead: 0.3, cacheWrite: 3.75 },
  "composer-1":              { input: 1.25, output: 10, cacheRead: 0.125, cacheWrite: 0 },
  "composer-1.5":            { input: 3.5, output: 17.5, cacheRead: 0.35, cacheWrite: 0 },
  "composer-2":              { input: 0.5, output: 2.5, cacheRead: 0.2, cacheWrite: 0 },
  "gemini-2.5-flash":        { input: 0.3, output: 2.5, cacheRead: 0.03, cacheWrite: 0 },
  "gemini-3-flash":          { input: 0.5, output: 3, cacheRead: 0.05, cacheWrite: 0 },
  "gemini-3-pro":            { input: 2, output: 12, cacheRead: 0.2, cacheWrite: 0 },
  "gemini-3.1-pro":          { input: 2, output: 12, cacheRead: 0.2, cacheWrite: 0 },
  "gpt-5":                   { input: 1.25, output: 10, cacheRead: 0.125, cacheWrite: 0 },
  "gpt-5-mini":              { input: 0.25, output: 2, cacheRead: 0.025, cacheWrite: 0 },
  "gpt-5.2":                 { input: 1.75, output: 14, cacheRead: 0.175, cacheWrite: 0 },
  "gpt-5.2-codex":           { input: 1.75, output: 14, cacheRead: 0.175, cacheWrite: 0 },
  "gpt-5.3-codex":           { input: 1.75, output: 14, cacheRead: 0.175, cacheWrite: 0 },
  "gpt-5.4":                 { input: 2.5, output: 15, cacheRead: 0.25, cacheWrite: 0 },
  "gpt-5.4-mini":            { input: 0.75, output: 4.5, cacheRead: 0.075, cacheWrite: 0 },
  "grok-4.20":               { input: 2, output: 6, cacheRead: 0.2, cacheWrite: 0 },
  "kimi-k2.5":               { input: 0.6, output: 3, cacheRead: 0.1, cacheWrite: 0 },
};

const MODEL_COST_PATTERNS: Array<{ match: (id: string) => boolean; cost: ModelCost }> = [
  { match: (id) => /claude.*opus.*fast/i.test(id),   cost: { input: 30, output: 150, cacheRead: 3, cacheWrite: 37.5 } },
  { match: (id) => /claude.*opus/i.test(id),         cost: MODEL_COST_TABLE["claude-4.6-opus"]! },
  { match: (id) => /claude.*haiku/i.test(id),        cost: MODEL_COST_TABLE["claude-4.5-haiku"]! },
  { match: (id) => /claude.*sonnet/i.test(id),       cost: MODEL_COST_TABLE["claude-4.6-sonnet"]! },
  { match: (id) => /composer/i.test(id),             cost: MODEL_COST_TABLE["composer-1"]! },
  { match: (id) => /gpt-5\.4.*mini/i.test(id),      cost: MODEL_COST_TABLE["gpt-5.4-mini"]! },
  { match: (id) => /gpt-5\.4/i.test(id),            cost: MODEL_COST_TABLE["gpt-5.4"]! },
  { match: (id) => /gpt-5\.3/i.test(id),            cost: MODEL_COST_TABLE["gpt-5.3-codex"]! },
  { match: (id) => /gpt-5\.2/i.test(id),            cost: MODEL_COST_TABLE["gpt-5.2"]! },
  { match: (id) => /gpt-5.*mini/i.test(id),          cost: MODEL_COST_TABLE["gpt-5-mini"]! },
  { match: (id) => /gpt-5/i.test(id),                cost: MODEL_COST_TABLE["gpt-5"]! },
  { match: (id) => /gemini.*3\.1/i.test(id),        cost: MODEL_COST_TABLE["gemini-3.1-pro"]! },
  { match: (id) => /gemini.*flash/i.test(id),        cost: MODEL_COST_TABLE["gemini-2.5-flash"]! },
  { match: (id) => /gemini/i.test(id),               cost: MODEL_COST_TABLE["gemini-3-pro"]! },
  { match: (id) => /grok/i.test(id),                 cost: MODEL_COST_TABLE["grok-4.20"]! },
  { match: (id) => /kimi/i.test(id),                 cost: MODEL_COST_TABLE["kimi-k2.5"]! },
];

const DEFAULT_COST: ModelCost = { input: 3, output: 15, cacheRead: 0.3, cacheWrite: 0 };

function estimateModelCost(modelId: string): ModelCost {
  const normalized = modelId.toLowerCase();
  const exact = MODEL_COST_TABLE[normalized];
  if (exact) return exact;
  const stripped = normalized.replace(/-(high|medium|low|preview|thinking|spark-preview|fast)$/g, "");
  const strippedMatch = MODEL_COST_TABLE[stripped];
  if (strippedMatch) return strippedMatch;
  return MODEL_COST_PATTERNS.find((p) => p.match(normalized))?.cost ?? DEFAULT_COST;
}

function modelConfig(m: CursorModel) {
  return {
    id: m.id,
    name: m.name,
    reasoning: m.reasoning,
    input: ["text"] as ("text" | "image")[],
    cost: estimateModelCost(m.id),
    contextWindow: m.contextWindow,
    maxTokens: m.maxTokens,
    compat: {
      supportsDeveloperRole: false,
      supportsReasoningEffort: false,
      maxTokensField: "max_tokens" as const,
    },
  };
}

// ── Fallback models (shown before model discovery) ──

const FALLBACK_MODELS: CursorModel[] = [
  { id: "composer-1", name: "Composer 1", reasoning: true, contextWindow: 200_000, maxTokens: 64_000 },
  { id: "composer-1.5", name: "Composer 1.5", reasoning: true, contextWindow: 200_000, maxTokens: 64_000 },
  { id: "claude-4.6-opus-high", name: "Claude 4.6 Opus", reasoning: true, contextWindow: 200_000, maxTokens: 128_000 },
  { id: "claude-4.6-sonnet-medium", name: "Claude 4.6 Sonnet", reasoning: true, contextWindow: 200_000, maxTokens: 64_000 },
  { id: "claude-4.5-sonnet", name: "Claude 4.5 Sonnet", reasoning: true, contextWindow: 200_000, maxTokens: 64_000 },
  { id: "gpt-5.4-medium", name: "GPT-5.4", reasoning: true, contextWindow: 272_000, maxTokens: 128_000 },
  { id: "gpt-5.2", name: "GPT-5.2", reasoning: true, contextWindow: 400_000, maxTokens: 128_000 },
  { id: "gpt-5.2-codex", name: "GPT-5.2 Codex", reasoning: true, contextWindow: 400_000, maxTokens: 128_000 },
  { id: "gpt-5.3-codex", name: "GPT-5.3 Codex", reasoning: true, contextWindow: 400_000, maxTokens: 128_000 },
  { id: "gemini-3.1-pro", name: "Gemini 3.1 Pro", reasoning: true, contextWindow: 1_000_000, maxTokens: 64_000 },
];

// ── Extension ──

export default function (pi: ExtensionAPI) {
  // Current access token, updated by login/refresh/getApiKey
  let currentToken = "";

  // Start proxy eagerly — it just binds a port, no auth needed until a request arrives.
  // The getAccessToken callback reads currentToken at request time.
  const proxyReady = startProxy(async () => {
    if (!currentToken) throw new Error("Not logged in to Cursor. Run /login cursor");
    return currentToken;
  });

  // We don't have the port yet (async), so register with fallback models first.
  // Once the proxy is ready, re-register with the real baseUrl.
  proxyReady.then((port) => {
    register(pi, port, FALLBACK_MODELS);
  }).catch(() => {
    // Proxy failed to start — models will show but requests will fail with a clear error
  });

  // Initial registration with placeholder baseUrl so models appear immediately
  register(pi, 0, FALLBACK_MODELS);

  function register(pi: ExtensionAPI, port: number, models: CursorModel[]) {
    const baseUrl = port > 0 ? `http://127.0.0.1:${port}/v1` : "http://localhost:1";

    pi.registerProvider("cursor", {
      baseUrl,
      apiKey: "cursor-proxy",
      api: "openai-completions",
      models: models.map(modelConfig),
      oauth: {
        name: "Cursor",

        async login(callbacks: OAuthLoginCallbacks): Promise<OAuthCredentials> {
          const { verifier, uuid, loginUrl } = await generateCursorAuthParams();
          callbacks.onAuth({ url: loginUrl });
          const { accessToken, refreshToken } = await pollCursorAuth(uuid, verifier);
          currentToken = accessToken;

          // Discover real models and re-register
          const realPort = await proxyReady;
          const discovered = await getCursorModels(accessToken);
          register(pi, realPort, discovered);

          return {
            refresh: refreshToken,
            access: accessToken,
            expires: getTokenExpiry(accessToken),
          };
        },

        async refreshToken(credentials: OAuthCredentials): Promise<OAuthCredentials> {
          const refreshed = await refreshCursorToken(credentials.refresh);
          currentToken = refreshed.access;

          // Discover real models on refresh too
          const realPort = await proxyReady;
          const discovered = await getCursorModels(refreshed.access);
          register(pi, realPort, discovered);

          return refreshed;
        },

        getApiKey(credentials: OAuthCredentials): string {
          currentToken = credentials.access;
          return "cursor-proxy";
        },
      },
    });
  }
}
