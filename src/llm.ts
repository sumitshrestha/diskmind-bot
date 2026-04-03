import * as fs from "fs-extra";
import * as path from "path";
import { Ollama } from "ollama";
import OpenAI from "openai";
import { DiskNode } from "./scanner";
import { PotentialSaving } from "./database";

const RUN_ID = new Date().toISOString().replace(/[.:]/g, "-");
const LOG_DIR = path.join(process.cwd(), "logs");
const LOG_PATH = path.join(LOG_DIR, `llm-calls-${RUN_ID}.log`);

const DECISION_SCHEMA = {
  type: "object",
  required: ["action", "reasoning"],
  properties: {
    action: { type: "string", enum: ["DELVE", "PLAN", "REPORT"] },
    target: { type: "string" },
    reasoning: { type: "string" }
  },
  additionalProperties: false
} as const;

const FINAL_PLAN_SCHEMA = {
  type: "object",
  required: ["summary", "zeroRiskPowershell", "mediumRiskChecklist", "highRiskChecklist", "disclaimers"],
  properties: {
    summary: { type: "string" },
    zeroRiskPowershell: { type: "string" },
    mediumRiskChecklist: { type: "array", items: { type: "string" } },
    highRiskChecklist: { type: "array", items: { type: "string" } },
    disclaimers: { type: "array", items: { type: "string" } }
  },
  additionalProperties: false
} as const;

async function llmLog(entry: {
  call: string;
  provider: string;
  model: string;
  prompt: string;
  raw: string;
  parseOk: boolean;
  error?: string;
}): Promise<void> {
  try {
    await fs.ensureDir(LOG_DIR);

    const line =
      JSON.stringify({
        ts: new Date().toISOString(),
        runId: RUN_ID,
        call: entry.call,
        provider: entry.provider,
        model: entry.model,
        parseOk: entry.parseOk,
        error: entry.error,
        prompt: entry.prompt,
        raw: entry.raw
      }) + "\n";
    await fs.appendFile(LOG_PATH, line, "utf8");
  } catch {
    // Non-fatal — never let logging break the main flow.
  }
}

export interface AgentDecision {
  action: "DELVE" | "PLAN" | "REPORT";
  target?: string;
  reasoning: string;
}

export interface FinalPlan {
  summary: string;
  zeroRiskPowershell: string;
  mediumRiskChecklist: string[];
  highRiskChecklist: string[];
  disclaimers: string[];
}

export interface OllamaInventory {
  availableModels: string[];
  runningModels: string[];
}

const openai = process.env.OPENAI_API_KEY ? new OpenAI({ apiKey: process.env.OPENAI_API_KEY }) : null;
const ollama = new Ollama({ host: process.env.OLLAMA_HOST });
let ollamaRequestQueue: Promise<void> = Promise.resolve();

async function runWithOllamaLock<T>(operation: () => Promise<T>): Promise<T> {
  const run = ollamaRequestQueue.then(operation, operation);
  ollamaRequestQueue = run.then(
    () => undefined,
    () => undefined
  );
  return run;
}

export async function getOllamaInventory(): Promise<OllamaInventory> {
  try {
    const listResponse = await ollama.list();
    const availableModels = ((listResponse as { models?: Array<{ model?: string; name?: string }> }).models ?? [])
      .map((model) => model.model ?? model.name ?? "")
      .filter((name) => Boolean(name));

    let runningModels: string[] = [];
    try {
      const psResponse = await ollama.ps();
      runningModels = ((psResponse as { models?: Array<{ model?: string; name?: string }> }).models ?? [])
        .map((model) => model.model ?? model.name ?? "")
        .filter((name) => Boolean(name));
    } catch {
      runningModels = [];
    }

    return {
      availableModels: [...new Set(availableModels)],
      runningModels: [...new Set(runningModels)]
    };
  } catch {
    return {
      availableModels: [],
      runningModels: []
    };
  }
}

export async function decideNextAction(input: {
  currentPath: string;
  nodes: DiskNode[];
  topFiles: DiskNode[];
  visitedPaths: string[];
}): Promise<AgentDecision> {
  const compactNodes = summarizeNodes(input.nodes, 35);
  const compactTopFiles = summarizeNodes(input.topFiles, 20);
  const prompt = [
    "You are DiskMind's decision engine.",
    "Choose one action:",
    "- DELVE: continue scanning a folder",
    "- PLAN: enough data to propose cleanup",
    "- REPORT: stop and summarize current findings",
    "Respond with ONLY a single JSON object. No explanation, no markdown, no extra text.",
    "Use exactly this structure:",
    '{"action":"DELVE","target":"C:\\\\SomeFolder","reasoning":"short text"}',
    "The action field must be exactly one of: DELVE, PLAN, REPORT",
    "The target field is only required for DELVE.",
    "Rules:",
    "- Prefer DELVE into the largest unvisited directory.",
    "- Do not DELVE into protected system internals unless obviously huge.",
    "- If confidence is low, choose PLAN.",
    "- DELVE only if target is a directory path.",
    `Current path: ${input.currentPath}`,
    `Visited paths: ${JSON.stringify(input.visitedPaths)}`,
    `Current summary (largest first): ${compactNodes}`,
    `Global top files: ${compactTopFiles}`
  ].join("\n");

  try {
    const raw = await chat(prompt, DECISION_SCHEMA);
    console.debug(`[DiskMind LLM] decideNextAction raw response (${raw.length} chars):`, raw.slice(0, 300));
    const parsed = parseJson<AgentDecision>(raw);
    const parseOk = parsed !== null && ["DELVE", "PLAN", "REPORT"].includes(parsed.action);
    void llmLog({
      call: "decideNextAction",
      provider: process.env.DISKMIND_LLM_PROVIDER ?? "ollama",
      model: process.env.DISKMIND_OLLAMA_MODEL ?? process.env.DISKMIND_OPENAI_MODEL ?? "unknown",
      prompt,
      raw,
      parseOk
    });

    if (parseOk) {
      return parsed;
    }
  } catch (error) {
    const message = getErrorMessage(error);
    console.warn(`DiskMind LLM warning [decideNextAction]: ${message}`);
    void llmLog({
      call: "decideNextAction",
      provider: process.env.DISKMIND_LLM_PROVIDER ?? "ollama",
      model: process.env.DISKMIND_OLLAMA_MODEL ?? process.env.DISKMIND_OPENAI_MODEL ?? "unknown",
      prompt,
      raw: "",
      parseOk: false,
      error: message
    });
  }

  const fallbackTarget = input.nodes.find((node) => node.isDirectory && !input.visitedPaths.includes(node.path));
  if (fallbackTarget) {
    return {
      action: "DELVE",
      target: fallbackTarget.path,
      reasoning: "Fallback to largest unvisited directory"
    };
  }

  return {
    action: "PLAN",
    reasoning: "Fallback to planning due to unparseable model response"
  };
}

export async function createFinalPlan(input: {
  potentialSavings: PotentialSaving[];
  topFiles: DiskNode[];
  scannedPaths: string[];
}): Promise<FinalPlan> {
  const compactSavings = summarizeSavings(input.potentialSavings, 120);
  const compactTopFiles = summarizeNodes(input.topFiles, 40);
  const prompt = [
    "You are DiskMind's cleanup strategist.",
    "Analyze the potential savings and classify each item by risk.",
    "Risk buckets:",
    "- Zero Risk: Caches, Temp files, Prefetch, logs safe to clear automatically",
    "- Medium Risk: old apps, duplicate AI models, stale SDK caches",
    "- High Risk/User Action: personal media, project folders, unknown binaries",
    "Respond with ONLY a single JSON object. No explanation, no markdown, no extra text.",
    "Use exactly this structure (all fields are required):",
    JSON.stringify({
      summary: "One paragraph describing what was found and top recommendations.",
      zeroRiskPowershell: "# PowerShell script\nRemove-Item -Path 'C:\\Windows\\Temp\\*' -Recurse -Force -WhatIf",
      mediumRiskChecklist: ["Example: Review large unused app at C:\\SomePath"],
      highRiskChecklist: ["Example: Inspect personal folder at C:\\Users\\Name"],
      disclaimers: ["Always review the script before running it."]
    }),
    "Rules for zeroRiskPowershell:",
    "- Readable, commented, idempotent PowerShell",
    "- Must only remove obvious cache/temp/log paths from the potential savings list",
    "- Include -WhatIf in all Remove-Item calls",
    "- Include a comment above each Remove-Item explaining what it removes",
    `Scanned paths: ${JSON.stringify(input.scannedPaths)}`,
    `Potential savings: ${compactSavings}`,
    `Top files: ${compactTopFiles}`
  ].join("\n");

  try {
    const raw = await chat(prompt, FINAL_PLAN_SCHEMA);
    console.debug(`[DiskMind LLM] createFinalPlan raw response (${raw.length} chars):`, raw.slice(0, 500));
    const parsed = parseJson<FinalPlan>(raw);
    const parseOk = isUsableFinalPlan(parsed);
    void llmLog({
      call: "createFinalPlan",
      provider: process.env.DISKMIND_LLM_PROVIDER ?? "ollama",
      model: process.env.DISKMIND_OLLAMA_MODEL ?? process.env.DISKMIND_OPENAI_MODEL ?? "unknown",
      prompt,
      raw,
      parseOk
    });

    if (parsed && parseOk) {
      return {
        summary: parsed.summary ?? "No summary produced.",
        zeroRiskPowershell: parsed.zeroRiskPowershell ?? "# No script generated",
        mediumRiskChecklist: Array.isArray(parsed.mediumRiskChecklist) ? parsed.mediumRiskChecklist : [],
        highRiskChecklist: Array.isArray(parsed.highRiskChecklist) ? parsed.highRiskChecklist : [],
        disclaimers: Array.isArray(parsed.disclaimers) ? parsed.disclaimers : ["Review all recommendations manually before running scripts."]
      };
    }
  } catch (error) {
    const message = getErrorMessage(error);
    console.warn(`DiskMind LLM warning [createFinalPlan]: ${message}`);
    void llmLog({
      call: "createFinalPlan",
      provider: process.env.DISKMIND_LLM_PROVIDER ?? "ollama",
      model: process.env.DISKMIND_OLLAMA_MODEL ?? process.env.DISKMIND_OPENAI_MODEL ?? "unknown",
      prompt,
      raw: "",
      parseOk: false,
      error: message
    });
  }

  return {
    summary: "Model response could not be parsed. Manual review required.",
    zeroRiskPowershell: "# Manual script creation required",
    mediumRiskChecklist: ["Review large unused applications."],
    highRiskChecklist: ["Inspect personal media and project folders manually."],
    disclaimers: ["LLM output parsing failed; no automated recommendations trusted."]
  };
}

async function chat(prompt: string, jsonSchema?: object): Promise<string> {
  const provider = (process.env.DISKMIND_LLM_PROVIDER ?? "ollama").toLowerCase();

  if (provider === "openai") {
    const openaiModel = process.env.DISKMIND_OPENAI_MODEL ?? "gpt-4o-mini";

    if (!openai) {
      throw new Error("OPENAI_API_KEY is required for provider=openai");
    }

    const completion = await openai.responses.create({
      model: openaiModel,
      input: prompt,
      temperature: 0.1
    });

    return completion.output_text;
  }

  const response = await runWithOllamaLock(() =>
    ollama.chat({
      model: process.env.DISKMIND_OLLAMA_MODEL ?? "llama3.1:8b",
      messages: [
        { role: "system", content: "You are a strict JSON API. Return only JSON that matches the schema." },
        { role: "user", content: prompt }
      ],
      format: jsonSchema ?? "json",
      options: { temperature: 0.1 }
    })
  );

  return response.message.content;
}

function parseJson<T>(raw: string): T | null {
  return parseJsonInternal<T>(raw, 0);
}

function parseJsonInternal<T>(raw: string, depth: number): T | null {
  const trimmed = raw.trim();
  if (!trimmed) {
    return null;
  }
  if (depth > 3) {
    return null;
  }

  // Some local models prepend chain-of-thought style tags before JSON.
  const withoutThinkingBlocks = trimmed.replace(/<think>[\s\S]*?<\/think>/gi, "").trim();
  const direct = tryParseJsonUnknown(withoutThinkingBlocks);
  if (direct !== null) {
    const unwrapped = unwrapEnvelopeContent(direct);
    if (unwrapped && unwrapped !== withoutThinkingBlocks) {
      const nested = parseJsonInternal<T>(unwrapped, depth + 1);
      if (nested) {
        return nested;
      }
    }

    return direct as T;
  }

  const fenced = withoutThinkingBlocks.match(/```(?:json)?\s*([\s\S]*?)```/i);
  if (fenced?.[1]) {
    const fencedParsed = parseJsonInternal<T>(fenced[1].trim(), depth + 1);
    if (fencedParsed) {
      return fencedParsed;
    }
  }

  const extracted = extractFirstJsonObject(withoutThinkingBlocks);
  if (!extracted) {
    return null;
  }

  return parseJsonInternal<T>(extracted, depth + 1);
}

function tryParseJson<T>(text: string): T | null {
  try {
    return JSON.parse(text) as T;
  } catch {
    return null;
  }
}

function tryParseJsonUnknown(text: string): unknown | null {
  return tryParseJson<unknown>(text);
}

function unwrapEnvelopeContent(value: unknown): string | null {
  if (!value || typeof value !== "object") {
    return null;
  }

  const obj = value as Record<string, unknown>;
  const contentCandidate = obj.content ?? obj.response ?? obj.text;
  if (typeof contentCandidate === "string") {
    return contentCandidate;
  }

  const messageCandidate = obj.message;
  if (messageCandidate && typeof messageCandidate === "object") {
    const nested = (messageCandidate as Record<string, unknown>).content;
    if (typeof nested === "string") {
      return nested;
    }
  }

  return null;
}

function summarizeNodes(nodes: DiskNode[], limit: number): string {
  return JSON.stringify(
    nodes.slice(0, limit).map((node) => ({
      path: node.path,
      sizeGB: node.sizeGB,
      isDirectory: node.isDirectory,
      extension: node.extension
    }))
  );
}

function summarizeSavings(savings: PotentialSaving[], limit: number): string {
  return JSON.stringify(
    savings.slice(0, limit).map((saving) => ({
      path: saving.path,
      sizeGB: saving.sizeGB,
      reason: saving.reason,
      riskHint: saving.riskHint
    }))
  );
}

function isUsableFinalPlan(plan: FinalPlan | null): boolean {
  if (!plan) {
    return false;
  }

  return (
    typeof plan.summary === "string" &&
    typeof plan.zeroRiskPowershell === "string" &&
    Array.isArray(plan.mediumRiskChecklist) &&
    Array.isArray(plan.highRiskChecklist) &&
    Array.isArray(plan.disclaimers)
  );
}

function getErrorMessage(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }
  return String(error);
}

function extractFirstJsonObject(text: string): string | null {
  let inString = false;
  let escapeNext = false;
  let depth = 0;
  let start = -1;

  for (let i = 0; i < text.length; i += 1) {
    const ch = text[i];

    if (inString) {
      if (escapeNext) {
        escapeNext = false;
        continue;
      }

      if (ch === "\\") {
        escapeNext = true;
        continue;
      }

      if (ch === '"') {
        inString = false;
      }

      continue;
    }

    if (ch === '"') {
      inString = true;
      continue;
    }

    if (ch === "{") {
      if (depth === 0) {
        start = i;
      }
      depth += 1;
      continue;
    }

    if (ch === "}" && depth > 0) {
      depth -= 1;
      if (depth === 0 && start >= 0) {
        return text.slice(start, i + 1);
      }
    }
  }

  return null;
}
