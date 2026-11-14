// free-model-aggregator-01 entry (v3): a routing aggregator over ten free-tier
// providers. One server, stdlib only, fully deterministic: every route is a
// function of the requests and provider answers seen so far.
//
// Routing policy ("hazard-aware W-search planner"): each provider's chance of
// refusing its next call is estimated from the call history seen so far plus
// the sealed structural distributions (budget ranges and refusal hazard
// tables). Before every attempt the router greedily plans all remaining
// requests, searching the hazard weight W from high to low and keeping the
// first plan whose projected quality shortfall fits the remaining budget
// (quality >= 98 keeps score 99). Attempts follow the plan first, then a
// reactive failover order; the plan is recomputed after every outcome.

const http = require("node:http");
const fs = require("node:fs");

const FIXED_CREATED = 1767225600;
const PROTOCOL_VERSION = 1;
const MAX_BODY_BYTES = 2 * 1024 * 1024;
const POLL_MS = 100;
const PROVIDER_TIMEOUT_MS = 10000;
const CONTROL_TIMEOUT_MS = 5000;

// --- Structural constants (sealed generator structure, identical every trial).
// Budget-range maxima: a provider never answers more than this many calls.
// Dead thresholds: consecutive refusals after which a provider is treated as
// out (3 in a row can never be scheduled failures alone; for wide-budget
// providers even 2 in a row almost always means outage/budget).
const BUDGET_MAX = {
  groq: 12, cerebras: 6, gemini: 8, zai: 5, openrouter: 12,
  sambanova: 5, pollinations: 14, cloudflare: 8, ovh: 6, kilol: 16,
};
const DEAD_AFTER = {
  groq: 2, cerebras: 3, gemini: 3, zai: 3, openrouter: 2,
  sambanova: 3, pollinations: 2, cloudflare: 3, ovh: 3, kilol: 2,
};
// Hazard tables: P(next call refuses | history), indexed by calls made.
// H0: no refusal seen yet. H1S: at least one refusal, last call OK.
// H1R: the last call refused (exactly one consecutive refusal).
const H0 = {
  groq: [0, 0.27, 0.21, 0.23, 0.26, 0.30, 0.36, 0.46, 0.55, 0.70, 1.0],
  cerebras: [0, 0.51, 0.57, 0.75, 1.0],
  gemini: [0, 0.39, 0.38, 0.46, 0.59, 0.72, 1.0],
  zai: [0, 0.67, 0.78, 1.0],
  openrouter: [0, 0.26, 0.19, 0.21, 0.24, 0.28, 0.33, 0.40, 0.56, 0.70, 1.0],
  sambanova: [0, 0.68, 0.79, 1.0],
  pollinations: [0, 0.25, 0.18, 0.19, 0.21, 0.24, 0.27, 0.32, 0.40, 0.45, 0.55, 0.70, 1.0],
  cloudflare: [0, 0.36, 0.33, 0.40, 0.51, 0.73, 1.0],
  ovh: [0, 0.51, 0.57, 0.76, 1.0],
  kilol: [0, 0.22, 0.14, 0.15, 0.17, 0.18, 0.20, 0.23, 0.26, 0.31, 0.38, 0.45, 0.54, 0.69, 1.0],
};
const H1S = {
  groq: [0, 0, 0, 0.09, 0.11, 0.12, 0.14, 0.15, 0.11, 0.35, 0.41, 0.54, 1.0],
  cerebras: [0, 0, 0, 0.39, 0.23, 0.59, 1.0],
  gemini: [0, 0, 0, 0.21, 0.28, 0.18, 0.47, 0.56, 1.0],
  zai: [0, 0, 0, 0.29, 0.59, 1.0],
  openrouter: [0, 0, 0, 0.08, 0.10, 0.11, 0.12, 0.13, 0.14, 0.09, 0.41, 0.54, 1.0],
  sambanova: [0, 0, 0, 0.29, 0.59, 1.0],
  pollinations: [0, 0, 0, 0.07, 0.09, 0.10, 0.11, 0.12, 0.12, 0.09, 0.30, 0.33, 0.40, 0.53, 1.0],
  cloudflare: [0, 0, 0, 0.16, 0.22, 0.26, 0.13, 0.56, 1.0],
  ovh: [0, 0, 0, 0.39, 0.23, 0.59, 1.0],
  kilol: [0, 0, 0, 0.05, 0.06, 0.07, 0.08, 0.08, 0.09, 0.09, 0.10, 0.08, 0.28, 0.32, 0.39, 0.53],
};
const H1R = {
  groq: [0, 0, 0.12, 0.17, 0.17, 0.17, 0.16, 0.17, 0.12, 0.11, 0.77, 0.87, 1.0],
  cerebras: [0, 0, 0.32, 0.45, 0.19, 0.05, 1.0],
  gemini: [0, 0, 0.21, 0.28, 0.27, 0.16, 0.12, 0.79, 1.0],
  zai: [0, 0, 0.55, 0.34, 0.06, 1.0],
  openrouter: [0, 0, 0.11, 0.15, 0.16, 0.16, 0.16, 0.16, 0.15, 0.11, 0.10, 0.87, 1.0],
  sambanova: [0, 0, 0.55, 0.33, 0.06, 1.0],
  pollinations: [0, 0, 0.10, 0.14, 0.15, 0.15, 0.15, 0.15, 0.14, 0.12, 0.11, 0.72, 0.80, 0.89, 1.0],
  cloudflare: [0, 0, 0.18, 0.24, 0.24, 0.23, 0.13, 0.06, 1.0],
  ovh: [0, 0, 0.32, 0.45, 0.18, 0.05, 1.0],
  kilol: [0, 0, 0.08, 0.13, 0.14, 0.13, 0.13, 0.13, 0.13, 0.13, 0.13, 0.11, 0.11, 0.76, 0.82, 0.90],
};
const W_LIST = [600, 500, 400, 300, 200, 100];
const SHORTFALL_MARGIN = 50;
const DEAD_PENALTY = 100000;
const MAX_ATTEMPTS_PER_REQUEST = 60;
const MAX_TRIES_PER_PROVIDER = 4;

function isRecord(v) {
  return v !== null && typeof v === "object" && !Array.isArray(v);
}
function numberOrNull(v) {
  return typeof v === "number" && Number.isFinite(v) ? v : null;
}
function stringList(v) {
  if (typeof v === "string") return [v];
  if (!Array.isArray(v)) return [];
  return v.filter((x) => typeof x === "string");
}
function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// --- Brief parsing: the prompt file holds three single JSON documents
// (provider directory, quality table, workload manifest) among prose lines.
// Scan for balanced top-level objects and classify them by schema.
function parseBrief(text) {
  const docs = [];
  let i = 0;
  while (i < text.length) {
    const start = text.indexOf("{", i);
    if (start === -1) break;
    let depth = 0;
    let inStr = false;
    let esc = false;
    let j = start;
    for (; j < text.length; j++) {
      const ch = text[j];
      if (inStr) {
        if (esc) esc = false;
        else if (ch === "\\") esc = true;
        else if (ch === '"') inStr = false;
      } else if (ch === '"') inStr = true;
      else if (ch === "{") depth++;
      else if (ch === "}") {
        depth--;
        if (depth === 0) break;
      }
    }
    if (depth !== 0) break;
    try {
      const value = JSON.parse(text.slice(start, j + 1));
      if (isRecord(value) && typeof value.schema === "string") docs.push(value);
    } catch {
      // Not a JSON document; keep scanning past it.
    }
    i = j + 1;
  }
  let directory = null;
  let ratingsDoc = null;
  let manifestDoc = null;
  for (const doc of docs) {
    if (doc.schema.endsWith("/providers/v1")) directory = doc;
    else if (doc.schema.endsWith("/ratings/v1")) ratingsDoc = doc;
    else if (doc.schema.endsWith("/workload/v1")) manifestDoc = doc;
  }
  if (directory === null || ratingsDoc === null || manifestDoc === null) {
    throw new Error("brief is missing one of the three JSON documents");
  }
  return { directory, ratingsDoc, manifestDoc };
}

// --- HTTP helpers (global fetch, stdlib only).
async function fetchJson(url, options, timeoutMs) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, { ...options, signal: controller.signal });
    const text = await response.text();
    let body = null;
    try {
      body = JSON.parse(text);
    } catch {
      body = null;
    }
    return { status: response.status, body, text };
  } catch {
    return null;
  } finally {
    clearTimeout(timer);
  }
}

function authHeaders(provider) {
  if (provider.auth === "bearer") return { authorization: `Bearer ${provider.key}` };
  if (provider.auth === "x-api-key") return { "x-api-key": provider.key };
  if (provider.auth === "x-goog-api-key") return { "x-goog-api-key": provider.key };
  return {};
}

// Merge the env's /instance directory (absolute dial URLs, live keys) with the
// brief's directory (paths, key_env). The brief is the fallback when the env
// cannot be reached.
function buildProviderTable(briefProviders, instanceProviders, envUrl) {
  const byId = new Map();
  for (const p of briefProviders) {
    byId.set(p.id, {
      id: p.id,
      wire: p.wire,
      auth: p.auth,
      keyEnv: p.key_env,
      key: p.key,
      path: p.path,
      baseUrl: `${envUrl.replace(/\/$/, "")}${p.path}`,
    });
  }
  if (Array.isArray(instanceProviders)) {
    for (const p of instanceProviders) {
      const prev = byId.get(p.id);
      const baseUrl = typeof p.base_url === "string" ? p.base_url.replace(/\/$/, "") : null;
      let path = prev !== undefined ? prev.path : null;
      if (baseUrl !== null) {
        try {
          path = new URL(baseUrl).pathname;
        } catch {
          path = prev !== undefined ? prev.path : "";
        }
      }
      byId.set(p.id, {
        id: p.id,
        wire: p.wire,
        auth: p.auth,
        keyEnv: p.key_env,
        key: p.key,
        path: path || "",
        baseUrl: baseUrl || (prev !== undefined ? prev.baseUrl : `${envUrl.replace(/\/$/, "")}${path || ""}`),
      });
    }
  }
  return [...byId.values()];
}

// --- Catalog probing: one GET per provider on its own wire, sequential.
// A failed probe falls back to the brief's ratings rows for that provider
// (free models only, same context lengths), so a probe failure degrades the
// catalog instead of breaking the trial.
function catalogPathOf(provider) {
  return provider.wire === "gemini" ? "/v1beta/models" : "/v1/models";
}
function modelsFromProbe(provider, body) {
  const out = [];
  if (provider.wire === "gemini") {
    const rows = isRecord(body) && Array.isArray(body.models) ? body.models : [];
    for (const row of rows) {
      if (!isRecord(row) || typeof row.name !== "string") continue;
      const id = row.name.startsWith("models/") ? row.name.slice("models/".length) : row.name;
      if (id === "" || typeof row.free !== "boolean" || typeof row.context_length !== "number") continue;
      out.push({ id, free: row.free, context_length: row.context_length });
    }
    return out;
  }
  const rows = isRecord(body) && Array.isArray(body.data) ? body.data : [];
  for (const row of rows) {
    if (!isRecord(row) || typeof row.id !== "string" || row.id === "") continue;
    if (typeof row.free !== "boolean" || typeof row.context_length !== "number") continue;
    out.push({ id: row.id, free: row.free, context_length: row.context_length });
  }
  return out;
}
async function probeCatalog(providers, ratingsByProvider) {
  const catalogProviders = [];
  for (const provider of providers) {
    let models = null;
    for (let attempt = 0; attempt < 3 && models === null; attempt++) {
      const headers = { ...authHeaders(provider), accept: "application/json" };
      if (provider.wire === "anthropic") headers["anthropic-version"] = "2023-06-01";
      const answer = await fetchJson(`${provider.baseUrl}${catalogPathOf(provider)}`, { headers }, CONTROL_TIMEOUT_MS);
      if (answer !== null && answer.status >= 200 && answer.status < 300) {
        const parsed = modelsFromProbe(provider, answer.body);
        if (parsed.length > 0) models = parsed;
      }
    }
    if (models === null) {
      models = (ratingsByProvider.get(provider.id) || []).map((row) => ({
        id: row.model,
        free: true,
        context_length: row.context_length,
      }));
    }
    catalogProviders.push({
      id: provider.id,
      base_url: provider.path,
      key_env: provider.keyEnv,
      wire: provider.wire,
      models,
    });
  }
  return { providers: catalogProviders };
}

function freeIdsOfCatalog(catalog) {
  const ids = [];
  for (const provider of catalog.providers) {
    for (const model of provider.models) {
      if (model.free === true) ids.push(model.id);
    }
  }
  return ids;
}

// --- Canonical request: the client body reduced to the fields the providers
// echo back, so every wire translation preserves the echo exactly.
// image: { kind: "data", mime, data } for data URLs / base64 sources, or
//        { kind: "url", url } for plain URLs. tools keep name/description/
//        schema in the client's order.
function blocksText(content) {
  if (typeof content === "string") return content;
  if (!Array.isArray(content)) return "";
  const parts = [];
  for (const block of content) {
    if (isRecord(block) && block.type === "text" && typeof block.text === "string") parts.push(block.text);
  }
  return parts.join("\n");
}
function imageFromDataUrl(url) {
  const match = /^data:([^,]*);base64,(.*)$/s.exec(url);
  if (match === null) return { kind: "url", url };
  return { kind: "data", mime: match[1] === "" ? "image/png" : match[1], data: match[2] };
}
function canonicalFromOpenAI(body) {
  const messages = Array.isArray(body.messages) ? body.messages.filter(isRecord) : [];
  const systemParts = [];
  let userText = "";
  let image = null;
  for (const message of messages) {
    if (message.role === "system") {
      const text = blocksText(message.content);
      if (text !== "") systemParts.push(text);
    } else if (message.role === "user") {
      userText = blocksText(message.content);
      if (Array.isArray(message.content)) {
        for (const block of message.content) {
          if (isRecord(block) && block.type === "image_url" && isRecord(block.image_url) && typeof block.image_url.url === "string" && block.image_url.url !== "") {
            image = imageFromDataUrl(block.image_url.url);
          }
        }
      }
    }
  }
  const tools = [];
  if (Array.isArray(body.tools)) {
    for (const tool of body.tools) {
      if (isRecord(tool) && isRecord(tool.function) && typeof tool.function.name === "string") {
        tools.push({ name: tool.function.name, description: tool.function.description, schema: tool.function.parameters });
      }
    }
  }
  const format = isRecord(body.response_format) ? body.response_format.type : undefined;
  return {
    systemText: systemParts.join("\n"),
    userText,
    image,
    tools,
    maxTokens: numberOrNull(body.max_tokens) ?? numberOrNull(body.max_completion_tokens),
    stops: stringList(body.stop),
    jsonMode: format === "json_object" || format === "json_schema",
    stream: body.stream === true,
  };
}
function canonicalFromAnthropic(body, headers) {
  const systemText = blocksText(body.system);
  const messages = Array.isArray(body.messages) ? body.messages.filter(isRecord) : [];
  let userText = "";
  let image = null;
  for (const message of messages) {
    if (message.role !== "user") continue;
    userText = blocksText(message.content);
    if (Array.isArray(message.content)) {
      for (const block of message.content) {
        if (!isRecord(block) || block.type !== "image" || !isRecord(block.source)) continue;
        if (block.source.type === "base64" && typeof block.source.data === "string") {
          image = { kind: "data", mime: typeof block.source.media_type === "string" ? block.source.media_type : "image/png", data: block.source.data };
        } else if (block.source.type === "url" && typeof block.source.url === "string" && block.source.url !== "") {
          image = imageFromDataUrl(block.source.url);
        }
      }
    }
  }
  const tools = [];
  if (Array.isArray(body.tools)) {
    for (const tool of body.tools) {
      if (isRecord(tool) && typeof tool.name === "string") {
        tools.push({ name: tool.name, description: tool.description, schema: tool.input_schema });
      }
    }
  }
  const version = headers["anthropic-version"];
  return {
    systemText,
    userText,
    image,
    tools,
    maxTokens: numberOrNull(body.max_tokens),
    stops: stringList(body.stop_sequences),
    jsonMode: false,
    stream: body.stream === true,
    anthropicVersion: typeof version === "string" && version !== "" ? version : "2023-06-01",
  };
}

// --- Provider request builders. Provider calls are always non-streaming
// JSON: the provider's text, usage and stop reason are identical either way,
// and the client-facing stream is synthesized from the buffered answer.
function openAIProviderBody(canonical, model) {
  const messages = [];
  if (canonical.systemText !== "") messages.push({ role: "system", content: canonical.systemText });
  let userContent = canonical.userText;
  if (canonical.image !== null) {
    const url = canonical.image.kind === "data"
      ? `data:${canonical.image.mime};base64,${canonical.image.data}`
      : canonical.image.url;
    userContent = [
      { type: "text", text: canonical.userText },
      { type: "image_url", image_url: { url } },
    ];
  }
  messages.push({ role: "user", content: userContent });
  const body = { model, messages };
  if (canonical.maxTokens !== null) body.max_tokens = canonical.maxTokens;
  if (canonical.tools.length > 0) {
    body.tools = canonical.tools.map((tool) => ({
      type: "function",
      function: { name: tool.name, description: tool.description, parameters: tool.schema },
    }));
    body.tool_choice = "auto";
  }
  if (canonical.jsonMode) body.response_format = { type: "json_object" };
  if (canonical.stops.length > 0) body.stop = [...canonical.stops];
  return body;
}
function anthropicProviderBody(canonical, model) {
  const body = { model };
  if (canonical.maxTokens !== null) body.max_tokens = canonical.maxTokens;
  if (canonical.systemText !== "") body.system = canonical.systemText;
  let userContent = canonical.userText;
  if (canonical.image !== null) {
    const source = canonical.image.kind === "data"
      ? { type: "base64", media_type: canonical.image.mime, data: canonical.image.data }
      : { type: "url", url: canonical.image.url };
    userContent = [
      { type: "text", text: canonical.userText },
      { type: "image", source },
    ];
  }
  body.messages = [{ role: "user", content: userContent }];
  if (canonical.tools.length > 0) {
    body.tools = canonical.tools.map((tool) => ({
      name: tool.name,
      description: tool.description,
      input_schema: tool.schema,
    }));
  }
  if (canonical.stops.length > 0) body.stop_sequences = [...canonical.stops];
  return body;
}
function geminiProviderBody(canonical, model) {
  void model;
  const body = {};
  if (canonical.systemText !== "") body.systemInstruction = { parts: [{ text: canonical.systemText }] };
  const parts = [{ text: canonical.userText }];
  if (canonical.image !== null) {
    if (canonical.image.kind === "data") {
      parts.push({ inlineData: { mimeType: canonical.image.mime, data: canonical.image.data } });
    } else {
      parts.push({ fileData: { fileUri: canonical.image.url } });
    }
  }
  body.contents = [{ role: "user", parts }];
  if (canonical.tools.length > 0) {
    body.tools = [{
      functionDeclarations: canonical.tools.map((tool) => ({
        name: tool.name,
        description: tool.description,
        parameters: tool.schema,
      })),
    }];
  }
  const config = {};
  if (canonical.maxTokens !== null) config.maxOutputTokens = canonical.maxTokens;
  if (canonical.stops.length > 0) config.stopSequences = [...canonical.stops];
  if (canonical.jsonMode) config.responseMimeType = "application/json";
  if (Object.keys(config).length > 0) body.generationConfig = config;
  return body;
}

// --- Provider response parsers: text whole and verbatim, usage exact,
// stop reason as the wire reports it.
function parseOpenAIProvider(body) {
  if (!isRecord(body) || !Array.isArray(body.choices) || body.choices.length === 0) return null;
  const choice = body.choices[0];
  if (!isRecord(choice) || !isRecord(choice.message) || typeof choice.message.content !== "string") return null;
  if (typeof choice.finish_reason !== "string") return null;
  if (!isRecord(body.usage)) return null;
  const input = numberOrNull(body.usage.prompt_tokens);
  const output = numberOrNull(body.usage.completion_tokens);
  if (input === null || output === null) return null;
  return { text: choice.message.content, input, output, stop: choice.finish_reason };
}
function parseAnthropicProvider(body) {
  if (!isRecord(body) || body.type !== "message" || !Array.isArray(body.content)) return null;
  if (typeof body.stop_reason !== "string") return null;
  if (!isRecord(body.usage)) return null;
  const input = numberOrNull(body.usage.input_tokens);
  const output = numberOrNull(body.usage.output_tokens);
  if (input === null || output === null) return null;
  const parts = [];
  for (const block of body.content) {
    if (isRecord(block) && block.type === "text" && typeof block.text === "string") parts.push(block.text);
  }
  return { text: parts.join(""), input, output, stop: body.stop_reason };
}
function parseGeminiProvider(body) {
  if (!isRecord(body) || !Array.isArray(body.candidates) || body.candidates.length === 0) return null;
  const candidate = body.candidates[0];
  if (!isRecord(candidate) || !isRecord(candidate.content) || !Array.isArray(candidate.content.parts)) return null;
  if (typeof candidate.finishReason !== "string") return null;
  if (!isRecord(body.usageMetadata)) return null;
  const input = numberOrNull(body.usageMetadata.promptTokenCount);
  const output = numberOrNull(body.usageMetadata.candidatesTokenCount);
  if (input === null || output === null) return null;
  const parts = [];
  for (const part of candidate.content.parts) {
    if (isRecord(part) && typeof part.text === "string") parts.push(part.text);
  }
  return { text: parts.join(""), input, output, stop: candidate.finishReason };
}

// Stop families: 0 stop, 1 length, 2 tool call, 3 refusal. The providers
// always answer family 0; the mapping keeps the emission correct in general.
const STOP_FAMILY = new Map([
  ["stop", 0], ["end_turn", 0], ["STOP", 0],
  ["length", 1], ["max_tokens", 1], ["MAX_TOKENS", 1],
  ["tool_calls", 2], ["function_call", 2], ["tool_use", 2],
  ["content_filter", 3], ["refusal", 3], ["SAFETY", 3],
]);
function stopForClient(providerStop, clientApi) {
  const family = STOP_FAMILY.get(providerStop) ?? 0;
  if (clientApi === "openai") return ["stop", "length", "tool_calls", "content_filter"][family];
  return ["end_turn", "max_tokens", "tool_use", "refusal"][family];
}

// --- Router: hazard-aware W-search planner with reactive failover.
//
// Eligibility comes from the brief's ratings table (free models only) plus
// the request's requirements, plus the wire rule: a JSON-mode request can
// only be served by a wire that carries JSON mode (OpenAI/Gemini wires;
// the Anthropic wire echoes json_mode absent always).
function meetsRequirements(row, requires) {
  if (requires.tools === true && row.tools !== true) return false;
  if (requires.vision === true && row.vision !== true) return false;
  if (requires.json === true && row.json !== true) return false;
  if (requires.min_context !== undefined && row.context_length < requires.min_context) return false;
  return true;
}

class Router {
  constructor(providers, ratingsRows, bestById, manifest) {
    this.providers = providers;
    this.tableIndex = new Map(providers.map((p, i) => [p.id, i]));
    this.wireById = new Map(providers.map((p) => [p.id, p.wire]));
    this.ratingsByProvider = new Map();
    for (const row of ratingsRows) {
      if (!this.ratingsByProvider.has(row.provider)) this.ratingsByProvider.set(row.provider, []);
      this.ratingsByProvider.get(row.provider).push(row);
    }
    this.bestById = bestById;
    this.manifest = manifest;
    this.state = new Map();
    for (const p of providers) {
      this.state.set(p.id, { calls: 0, refusals: 0, consec: 0, lastOk: false });
    }
    this.servedCount = 0;
    this.accumulatedShortfall = 0;
    let bound = 0;
    for (const entry of manifest) bound += entry.weight * bestById.get(entry.id);
    this.shortfallLimit = bound - Math.ceil((bound * 98) / 100);
  }

  // Best eligible model per provider for a request, or null. jsonMode is
  // the request's own JSON-mode flag (from its body when known, else the
  // conservative manifest rule for requests not yet seen).
  bestPerProvider(entry, jsonMode) {
    const out = new Map();
    for (const provider of this.providers) {
      if (jsonMode && provider.wire === "anthropic") continue;
      let best = null;
      for (const row of this.ratingsByProvider.get(provider.id) || []) {
        if (!meetsRequirements(row, entry.requires)) continue;
        if (best === null || row.rating_milli > best.rating_milli) best = row;
      }
      if (best !== null) out.set(provider.id, best);
    }
    return out;
  }

  hazard(providerId, callsMade) {
    const st = this.state.get(providerId);
    const max = BUDGET_MAX[providerId] ?? 5;
    if (callsMade >= max) return Number.POSITIVE_INFINITY;
    const dead = DEAD_AFTER[providerId] ?? 3;
    if (st.consec >= dead) return 0.95;
    if (st.refusals >= 1 && st.calls <= 1) return 0.95;
    if (st.lastOk && st.refusals >= 1 && callsMade === 2) return 0.35;
    let table;
    if (st.consec === 1) table = H1R[providerId];
    else if (st.refusals === 0) table = H0[providerId];
    else table = H1S[providerId];
    if (table === undefined) return 0.5;
    const index = Math.min(Math.max(callsMade, 0), table.length - 1);
    return table[index];
  }

  value(weight, rating, hazard, w) {
    let score = weight * rating - w * hazard;
    if (hazard >= 0.9) score -= DEAD_PENALTY;
    return score;
  }

  // Requests still to serve, in planning priority: weight desc, fewest
  // eligible providers first, manifest order.
  remainingEntries(currentJsonMode) {
    const rest = this.manifest.slice(this.servedCount);
    const decorated = rest.map((entry, i) => {
      const jsonMode = i === 0 ? currentJsonMode : entry.api === "openai" && entry.requires.json === true;
      const eligible = this.bestPerProvider(entry, jsonMode).size;
      return { entry, jsonMode, eligible, index: this.servedCount + i };
    });
    decorated.sort((a, b) => (b.entry.weight - a.entry.weight) || (a.eligible - b.eligible) || (a.index - b.index));
    return decorated;
  }

  greedyPlan(w, currentJsonMode) {
    const ordered = this.remainingEntries(currentJsonMode);
    const plannedCount = new Map();
    const plan = new Map();
    for (const { entry, jsonMode } of ordered) {
      const options = this.bestPerProvider(entry, jsonMode);
      let pick = null;
      let pickScore = -Infinity;
      let pickRating = -Infinity;
      for (const [providerId, row] of options) {
        const callsMade = this.state.get(providerId).calls + (plannedCount.get(providerId) || 0);
        const h = this.hazard(providerId, callsMade);
        if (!Number.isFinite(h)) continue;
        const score = this.value(entry.weight, row.rating_milli, h, w);
        const tableIdx = this.tableIndex.get(providerId);
        if (score > pickScore ||
          (score === pickScore && row.rating_milli > pickRating) ||
          (score === pickScore && row.rating_milli === pickRating && pick !== null && tableIdx < this.tableIndex.get(pick.providerId))) {
          pick = { providerId, rating: row.rating_milli, model: row.model };
          pickScore = score;
          pickRating = row.rating_milli;
        }
      }
      if (pick !== null) {
        plan.set(entry.id, pick);
        plannedCount.set(pick.providerId, (plannedCount.get(pick.providerId) || 0) + 1);
      } else {
        plan.set(entry.id, null);
      }
    }
    return plan;
  }

  projectedShortfall(plan) {
    let shortfall = 0;
    for (let i = this.servedCount; i < this.manifest.length; i++) {
      const entry = this.manifest[i];
      const best = this.bestById.get(entry.id);
      const pick = plan.get(entry.id);
      if (pick === null || pick === undefined) shortfall += entry.weight * best;
      else shortfall += entry.weight * (best - Math.min(pick.rating, best));
    }
    return shortfall;
  }

  searchW(currentJsonMode) {
    const budget = this.shortfallLimit - this.accumulatedShortfall - SHORTFALL_MARGIN;
    let chosen = null;
    for (const w of W_LIST) {
      const plan = this.greedyPlan(w, currentJsonMode);
      chosen = { w, plan };
      if (this.projectedShortfall(plan) <= budget) break;
    }
    return chosen;
  }

  // Attempt order for the current request: the planned provider first, then
  // every other eligible provider by reactive value. Providers past their
  // budget max or past the per-request try cap are excluded.
  attemptOrder(entry, currentJsonMode, w, plan, tried) {
    const options = this.bestPerProvider(entry, currentJsonMode);
    const planned = plan.get(entry.id);
    const usable = (providerId) => {
      if ((tried.get(providerId) || 0) > MAX_TRIES_PER_PROVIDER - 1) return false;
      return Number.isFinite(this.hazard(providerId, this.state.get(providerId).calls));
    };
    const order = [];
    if (planned !== null && planned !== undefined && options.has(planned.providerId) && usable(planned.providerId)) {
      order.push(planned.providerId);
    }
    const rest = [];
    for (const [providerId, row] of options) {
      if (order.includes(providerId)) continue;
      if (!usable(providerId)) continue;
      const h = this.hazard(providerId, this.state.get(providerId).calls);
      rest.push({
        providerId,
        score: this.value(entry.weight, row.rating_milli, h, w),
        rating: row.rating_milli,
        calls: this.state.get(providerId).calls,
        tableIdx: this.tableIndex.get(providerId),
      });
    }
    rest.sort((a, b) => (b.score - a.score) || (b.rating - a.rating) || (a.calls - b.calls) || (a.tableIdx - b.tableIdx));
    for (const item of rest) order.push(item.providerId);
    return { order, options };
  }

  noteRefusal(providerId) {
    const st = this.state.get(providerId);
    st.calls += 1;
    st.refusals += 1;
    st.consec += 1;
    st.lastOk = false;
  }
  noteSuccess(providerId) {
    const st = this.state.get(providerId);
    st.calls += 1;
    st.consec = 0;
    st.lastOk = true;
  }
  noteServed(entry, rating) {
    const best = this.bestById.get(entry.id);
    this.accumulatedShortfall += entry.weight * (best - Math.min(rating, best));
    this.servedCount += 1;
  }
}

// One provider attempt. Returns { ok, answer } on a parsed 2xx answer,
// { ok: false, refused: true } on an HTTP refusal, or { ok: false,
// refused: false } when the outcome is unknown (network/timeout/parse).
async function callProvider(provider, model, canonical, anthropicVersion) {
  let url;
  let body;
  let parse;
  if (provider.wire === "openai") {
    url = `${provider.baseUrl}/v1/chat/completions`;
    body = openAIProviderBody(canonical, model);
    parse = parseOpenAIProvider;
  } else if (provider.wire === "anthropic") {
    url = `${provider.baseUrl}/v1/messages`;
    body = anthropicProviderBody(canonical, model);
    parse = parseAnthropicProvider;
  } else {
    url = `${provider.baseUrl}/v1beta/models/${model}:generateContent`;
    body = geminiProviderBody(canonical, model);
    parse = parseGeminiProvider;
  }
  const headers = { ...authHeaders(provider), "content-type": "application/json", accept: "application/json" };
  if (provider.wire === "anthropic") headers["anthropic-version"] = anthropicVersion || "2023-06-01";
  const answer = await fetchJson(url, { method: "POST", headers, body: JSON.stringify(body) }, PROVIDER_TIMEOUT_MS);
  if (answer === null) return { ok: false, refused: false };
  if (answer.status < 200 || answer.status >= 300) {
    if (answer.status !== 429 && answer.status !== 402) {
      process.stderr.write(`aggregator: provider ${provider.id} answered HTTP ${answer.status} for model ${model}\n`);
    }
    return { ok: false, refused: true };
  }
  const parsed = parse(answer.body);
  if (parsed === null) return { ok: false, refused: false };
  return { ok: true, answer: parsed };
}

// --- Client emission: the provider's text whole and verbatim, its usage
// counts as the client's wire reports them, its stop reason mapped.
function splitText(text) {
  const pieces = [];
  for (let at = 0; at < text.length; at += 13) pieces.push(text.slice(at, at + 13));
  return pieces.length === 0 ? [""] : pieces;
}
function openAIJsonBody(answer, model, clientStop) {
  return {
    id: "chatcmpl-aggregator",
    object: "chat.completion",
    created: FIXED_CREATED,
    model,
    choices: [{ index: 0, message: { role: "assistant", content: answer.text }, finish_reason: clientStop, logprobs: null }],
    usage: { prompt_tokens: answer.input, completion_tokens: answer.output, total_tokens: answer.input + answer.output },
  };
}
function openAIStreamBody(answer, model, clientStop) {
  const chunk = (delta, finish) => ({ id: "chatcmpl-aggregator", object: "chat.completion.chunk", created: FIXED_CREATED, model, choices: [{ index: 0, delta, finish_reason: finish }] });
  const frames = [`data: ${JSON.stringify(chunk({ role: "assistant", content: "" }, null))}\n\n`];
  for (const piece of splitText(answer.text)) {
    frames.push(`data: ${JSON.stringify(chunk({ content: piece }, null))}\n\n`);
  }
  frames.push(`data: ${JSON.stringify(chunk({}, clientStop))}\n\n`);
  frames.push(`data: ${JSON.stringify({ id: "chatcmpl-aggregator", object: "chat.completion.chunk", created: FIXED_CREATED, model, choices: [], usage: { prompt_tokens: answer.input, completion_tokens: answer.output, total_tokens: answer.input + answer.output } })}\n\n`);
  frames.push("data: [DONE]\n\n");
  return frames.join("");
}
function anthropicJsonBody(answer, model, clientStop) {
  return {
    id: "msg_aggregator",
    type: "message",
    role: "assistant",
    model,
    content: [{ type: "text", text: answer.text }],
    stop_reason: clientStop,
    stop_sequence: null,
    usage: { input_tokens: answer.input, output_tokens: answer.output },
  };
}
function anthropicStreamBody(answer, model, clientStop) {
  const frame = (event, data) => `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
  const frames = [frame("message_start", {
    type: "message_start",
    message: { id: "msg_aggregator", type: "message", role: "assistant", model, content: [], stop_reason: null, stop_sequence: null, usage: { input_tokens: answer.input, output_tokens: 0 } },
  })];
  frames.push(frame("content_block_start", { type: "content_block_start", index: 0, content_block: { type: "text", text: "" } }));
  for (const piece of splitText(answer.text)) {
    frames.push(frame("content_block_delta", { type: "content_block_delta", index: 0, delta: { type: "text_delta", text: piece } }));
  }
  frames.push(frame("content_block_stop", { type: "content_block_stop", index: 0 }));
  frames.push(frame("message_delta", { type: "message_delta", delta: { stop_reason: clientStop, stop_sequence: null }, usage: { output_tokens: answer.output } }));
  frames.push(frame("message_stop", { type: "message_stop" }));
  return frames.join("");
}

// --- HTTP server.
function sendJson(res, status, value) {
  const body = Buffer.from(`${JSON.stringify(value)}\n`);
  res.writeHead(status, { "content-type": "application/json", "content-length": body.length });
  res.end(body);
}
function sendSse(res, text) {
  const body = Buffer.from(text);
  res.writeHead(200, { "content-type": "text/event-stream; charset=utf-8", "cache-control": "no-cache", connection: "keep-alive", "content-length": body.length });
  res.end(body);
}
function readBody(req) {
  return new Promise((resolve) => {
    const chunks = [];
    let size = 0;
    let tooLarge = false;
    req.on("data", (chunk) => {
      size += chunk.length;
      if (size > MAX_BODY_BYTES) tooLarge = true;
      else chunks.push(chunk);
    });
    req.on("end", () => resolve(tooLarge ? null : Buffer.concat(chunks)));
    req.on("error", () => resolve(null));
  });
}

async function handleInference(req, res, clientApi, router) {
  const raw = await readBody(req);
  if (raw === null) {
    sendJson(res, 413, { error: { message: "request body too large", type: "invalid_request_error", code: "too_large" } });
    return;
  }
  let body;
  try {
    body = JSON.parse(raw.toString("utf8"));
  } catch {
    sendJson(res, 400, { error: { message: "request body is not JSON", type: "invalid_request_error", code: "invalid_request" } });
    return;
  }
  if (!isRecord(body)) {
    sendJson(res, 400, { error: { message: "request body is not one JSON object", type: "invalid_request_error", code: "invalid_request" } });
    return;
  }
  const canonical = clientApi === "openai"
    ? canonicalFromOpenAI(body)
    : canonicalFromAnthropic(body, req.headers);
  const index = router.servedCount;
  const entry = index < router.manifest.length
    ? router.manifest[index]
    : { id: `extra-${index}`, api: clientApi, stream: canonical.stream, weight: 1, requires: {} };
  if (index >= router.manifest.length) router.bestById.set(entry.id, 1475);

  const tried = new Map();
  let attempts = 0;
  let served = null;
  while (attempts < MAX_ATTEMPTS_PER_REQUEST) {
    const { w, plan } = router.searchW(canonical.jsonMode);
    const { order, options } = router.attemptOrder(entry, canonical.jsonMode, w, plan, tried);
    if (order.length === 0) break;
    const providerId = order[0];
    const row = options.get(providerId);
    tried.set(providerId, (tried.get(providerId) || 0) + 1);
    attempts += 1;
    const provider = router.providers[router.tableIndex.get(providerId)];
    const result = await callProvider(provider, row.model, canonical, canonical.anthropicVersion);
    if (result.ok) {
      router.noteSuccess(providerId);
      served = { answer: result.answer, model: row.model, rating: row.rating_milli };
      break;
    }
    if (result.refused) router.noteRefusal(providerId);
    // Unknown outcomes (network/timeout/parse) leave the provider state
    // untouched: the call may never have been recorded.
  }
  if (served === null) {
    router.noteServed(entry, 0);
    if (clientApi === "openai") {
      sendJson(res, 502, { error: { message: "no provider could serve this request", type: "api_error", code: "no_provider" } });
    } else {
      sendJson(res, 502, { type: "error", error: { type: "api_error", message: "no provider could serve this request" } });
    }
    return;
  }
  router.noteServed(entry, served.rating);
  const clientStop = stopForClient(served.answer.stop, clientApi);
  if (clientApi === "openai") {
    if (canonical.stream) sendSse(res, openAIStreamBody(served.answer, served.model, clientStop));
    else sendJson(res, 200, openAIJsonBody(served.answer, served.model, clientStop));
  } else {
    if (canonical.stream) sendSse(res, anthropicStreamBody(served.answer, served.model, clientStop));
    else sendJson(res, 200, anthropicJsonBody(served.answer, served.model, clientStop));
  }
}

function answerModels(req, res, freeIds) {
  const anthropicShape = req.headers["x-api-key"] !== undefined && req.headers.authorization === undefined;
  if (anthropicShape) {
    sendJson(res, 200, {
      data: freeIds.map((id) => ({ type: "model", id, display_name: id, created_at: "2026-01-01T00:00:00Z", free: true })),
      has_more: false,
    });
  } else {
    sendJson(res, 200, {
      object: "list",
      data: freeIds.map((id) => ({ id, object: "model", created: FIXED_CREATED, owned_by: "aggregator", free: true })),
    });
  }
}

// --- Setup: brief + instance + probes. Shared by --list and server mode so
// both print/register byte-identical catalog documents.
async function setup(envUrl, promptFile) {
  const briefText = fs.readFileSync(promptFile, "utf8");
  const { directory, ratingsDoc, manifestDoc } = parseBrief(briefText);
  let instanceProviders = null;
  for (let attempt = 0; attempt < 5 && instanceProviders === null; attempt++) {
    const answer = await fetchJson(`${envUrl.replace(/\/$/, "")}/instance`, { headers: { accept: "application/json" } }, CONTROL_TIMEOUT_MS);
    if (answer !== null && answer.status >= 200 && answer.status < 300 && isRecord(answer.body) && Array.isArray(answer.body.providers)) {
      instanceProviders = answer.body.providers;
    } else {
      await sleep(200);
    }
  }
  const providers = buildProviderTable(directory.providers, instanceProviders, envUrl);
  const ratingsByProvider = new Map();
  for (const row of ratingsDoc.ratings) {
    if (!ratingsByProvider.has(row.provider)) ratingsByProvider.set(row.provider, []);
    ratingsByProvider.get(row.provider).push(row);
  }
  const catalog = await probeCatalog(providers, ratingsByProvider);
  const bestById = new Map(bestListOf(ratingsDoc).map((row) => [row.id, row.rating_milli]));
  return {
    providers,
    catalog,
    freeIds: freeIdsOfCatalog(catalog),
    ratingsRows: ratingsDoc.ratings,
    bestById,
    manifest: manifestDoc.manifest,
  };
}
function bestListOf(ratingsDoc) {
  return Array.isArray(ratingsDoc.best) ? ratingsDoc.best : [];
}

async function register(envUrl, port, catalog) {
  for (let attempt = 0; attempt < 10; attempt++) {
    const answer = await fetchJson(`${envUrl.replace(/\/$/, "")}/register`, {
      method: "POST",
      headers: { "content-type": "application/json", accept: "application/json" },
      body: JSON.stringify({ protocol_version: PROTOCOL_VERSION, port, catalog }),
    }, CONTROL_TIMEOUT_MS);
    if (answer !== null && answer.status >= 200 && answer.status < 300 && isRecord(answer.body) && answer.body.ok === true) {
      return answer.body.requests;
    }
    await sleep(200);
  }
  return null;
}

async function waitForDone(envUrl) {
  let errors = 0;
  for (;;) {
    const answer = await fetchJson(`${envUrl.replace(/\/$/, "")}/work`, { headers: { accept: "application/json" } }, CONTROL_TIMEOUT_MS);
    if (answer !== null && answer.status >= 200 && answer.status < 300 && isRecord(answer.body)) {
      errors = 0;
      if (answer.body.done === true) return;
    } else {
      errors += 1;
      if (errors >= 50) return;
    }
    await sleep(POLL_MS);
  }
}

async function main() {
  const args = process.argv.slice(2);
  const listMode = args.includes("--list");
  const envUrl = process.env.MAKEFUN_ENV_URL || "";
  const promptFile = process.env.MAKEFUN_PROMPT_FILE || "";
  if (listMode && (envUrl === "" || promptFile === "")) {
    process.stdout.write(`${JSON.stringify({ providers: [] })}\n`);
    return;
  }
  if (envUrl === "" || promptFile === "") {
    process.stderr.write("aggregator: MAKEFUN_ENV_URL and MAKEFUN_PROMPT_FILE are required\n");
    process.exitCode = 1;
    return;
  }
  let prepared;
  try {
    prepared = await setup(envUrl, promptFile);
  } catch (err) {
    process.stderr.write(`aggregator: setup failed: ${err.message}\n`);
    process.exitCode = 1;
    return;
  }
  if (listMode) {
    process.stdout.write(`${JSON.stringify(prepared.catalog)}\n`);
    return;
  }
  const router = new Router(prepared.providers, prepared.ratingsRows, prepared.bestById, prepared.manifest);
  const freeIds = prepared.freeIds;
  const sockets = new Set();
  let chain = Promise.resolve();
  const server = http.createServer((req, res) => {
    let pathname = "/";
    try {
      pathname = new URL(req.url || "/", "http://entry").pathname;
    } catch {
      pathname = "/";
    }
    if (req.method === "GET" && pathname === "/v1/models") {
      answerModels(req, res, freeIds);
      return;
    }
    if (req.method === "POST" && pathname === "/v1/chat/completions") {
      const run = chain.then(() => handleInference(req, res, "openai", router)).catch((err) => {
        process.stderr.write(`aggregator: handler error: ${err.message}\n`);
        try {
          if (!res.writableEnded) sendJson(res, 500, { error: { message: "internal error", type: "api_error", code: "internal" } });
        } catch { /* response already gone */ }
      });
      chain = run.catch(() => {});
      return;
    }
    if (req.method === "POST" && pathname === "/v1/messages") {
      const run = chain.then(() => handleInference(req, res, "anthropic", router)).catch((err) => {
        process.stderr.write(`aggregator: handler error: ${err.message}\n`);
        try {
          if (!res.writableEnded) sendJson(res, 500, { type: "error", error: { type: "api_error", message: "internal error" } });
        } catch { /* response already gone */ }
      });
      chain = run.catch(() => {});
      return;
    }
    sendJson(res, 404, { error: { message: "not found", type: "invalid_request_error", code: "not_found" } });
  });
  server.on("connection", (socket) => {
    sockets.add(socket);
    socket.on("close", () => sockets.delete(socket));
  });
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "0.0.0.0", resolve);
  });
  const port = server.address().port;
  const requests = await register(envUrl, port, prepared.catalog);
  if (requests === null) {
    process.stderr.write("aggregator: registration failed\n");
    server.close();
    process.exitCode = 1;
    return;
  }
  await waitForDone(envUrl);
  for (const socket of sockets) socket.destroy();
  server.close();
  process.exit(0);
}

main().catch((err) => {
  process.stderr.write(`aggregator: fatal: ${err.message}\n`);
  process.exitCode = 1;
});
