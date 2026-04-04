import * as fs from "fs-extra";
import * as path from "path";
import { Ollama } from "ollama";
import OpenAI from "openai";
import { DiskNode } from "./scanner";
import { PotentialSaving } from "./database";

const RUN_ID = new Date().toISOString().replace(/[.:]/g, "-");
const LOG_DIR = path.join(process.cwd(), "logs");
const LOG_PATH = path.join(LOG_DIR, `llm-calls-${RUN_ID}.log`);
const OLLAMA_MAX_RETRIES = Number(process.env.DISKMIND_OLLAMA_RETRIES ?? 2);
const OLLAMA_RETRY_DELAY_MS = Number(process.env.DISKMIND_OLLAMA_RETRY_DELAY_MS ?? 2000);

interface PromptBudget {
  decisionNodeLimit: number;
  decisionTopFilesLimit: number;
  planSavingsLimit: number;
  planTopFilesLimit: number;
}

const DEFAULT_PROMPT_BUDGET: PromptBudget = {
  decisionNodeLimit: 35,
  decisionTopFilesLimit: 20,
  planSavingsLimit: 120,
  planTopFilesLimit: 40
};

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
  required: [
    "summary",
    "zeroRiskPowershell",
    "mediumRiskChecklist",
    "highRiskChecklist",
    "offloadChecklist",
    "semanticActions",
    "disclaimers"
  ],
  properties: {
    summary: { type: "string" },
    zeroRiskPowershell: { type: "string" },
    mediumRiskChecklist: { type: "array", items: { type: "string" } },
    highRiskChecklist: { type: "array", items: { type: "string" } },
    offloadChecklist: { type: "array", items: { type: "string" } },
    semanticActions: { type: "array", items: { type: "string" } },
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
  offloadChecklist: string[];
  semanticActions: string[];
  disclaimers: string[];
}

export interface OllamaInventory {
  availableModels: string[];
  runningModels: string[];
}

const openai = process.env.OPENAI_API_KEY ? new OpenAI({ apiKey: process.env.OPENAI_API_KEY }) : null;
const ollama = new Ollama({ host: process.env.OLLAMA_HOST });
let ollamaRequestQueue: Promise<void> = Promise.resolve();
let promptBudgetCache: Promise<PromptBudget> | null = null;

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
  const budget = await getPromptBudget();
  const compactNodes = summarizeNodes(input.nodes, budget.decisionNodeLimit);
  const compactTopFiles = summarizeNodes(input.topFiles, budget.decisionTopFilesLimit);
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
    const parseOk = parsed !== null && isValidDecision(parsed, input.nodes);
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
  let failureReason: string | null = null;
  const fallbackPlan = buildDeterministicFallbackPlan(input);
  const budget = await getPromptBudget();
  const compactSavings = summarizeSavings(input.potentialSavings, budget.planSavingsLimit);
  const compactTopFiles = summarizeNodes(input.topFiles, budget.planTopFilesLimit);
  const prompt = [
    "You are the Storage Intelligence Agent for DiskMind.",
    "Analyze the potential savings and classify each item by risk and retention value.",
    "Operate on semantic clusters (folder-level aggregates), not only individual files.",
    "Risk buckets:",
    "- Zero Risk: Caches, Temp files, Prefetch, logs safe to clear automatically",
    "- Medium Risk: old apps, duplicate AI models, stale SDK caches",
    "- High Risk/User Action: personal media, project folders, unknown binaries",
    "- Offload Candidates: large low-access files better moved to external storage, NAS, or cloud",
    "Action tags for every finding:",
    "- [PURGE]: safe cache/temp/log cleanup",
    "- [OFFLOAD]: important but cold data on hot/system drive",
    "- [COMPRESS]: large occasionally-used folder that should be zipped/archived",
    "- [RETAIN]: active data that should stay on current drive",
    "Respond with ONLY a single JSON object. No explanation, no markdown, no extra text.",
    "Use exactly this structure (all fields are required):",
    JSON.stringify({
      summary: "One paragraph describing what was found and top recommendations.",
      zeroRiskPowershell: "# PowerShell script\nRemove-Item -Path 'C:\\Windows\\Temp\\*' -Recurse -Force -WhatIf",
      mediumRiskChecklist: ["Example: Review large unused app at C:\\SomePath and list top files"],
      highRiskChecklist: ["Example: Inspect personal folder at C:\\Users\\Name before deleting anything"],
      offloadChecklist: ["Example: Move C:\\Users\\Name\\Videos\\LargeFile.mp4 (last accessed 2023-08-01) to external drive"],
      semanticActions: [
        "[OFFLOAD] C:\\Users\\Name\\Documents\\backup (16.3 GB) is on hot system drive and looks archive-like; last accessed 120 days ago, low activity; move to external storage.",
        "[COMPRESS] C:\\Users\\Name\\Media\\Footage (8.4 GB) has occasional activity; compress older chunks into archives.",
        "[PURGE] C:\\Windows\\Temp (0.5 GB) is transient cache/log data with low retention value; safe to clear with -WhatIf.",
        "[RETAIN] C:\\Users\\Name\\Projects\\active-app (2.1 GB) shows recent access and active edits; keep on SSD."
      ],
      disclaimers: ["Always review the script before running it."]
    }),
    "Rules for zeroRiskPowershell:",
    "- Readable, commented, idempotent PowerShell",
    "- Must only remove obvious cache/temp/log paths from the potential savings list",
    "- Include -WhatIf in all Remove-Item calls",
    "- Include a comment above each Remove-Item explaining what it removes",
    "Rules for checklists:",
    "- Every checklist item must include one or more concrete file/folder paths from the input.",
    "- Use lastAccessedISO to prioritize stale files for offload (older access date = higher offload priority).",
    "- Do not output placeholders, truncation markers, or incomplete paths.",
    "- Every semanticActions item must start with [PURGE], [OFFLOAD], [COMPRESS], or [RETAIN] and include concrete path + reason.",
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
      const mediumRiskChecklist = sanitizeChecklist(parsed.mediumRiskChecklist);
      const highRiskChecklist = sanitizeChecklist(parsed.highRiskChecklist);
      const offloadChecklist = sanitizeChecklist(parsed.offloadChecklist);
      const semanticActions = sanitizeSemanticActions(parsed.semanticActions);
      const candidatePlan: FinalPlan = {
        summary: parsed.summary ?? "No summary produced.",
        zeroRiskPowershell: parsed.zeroRiskPowershell ?? "# No script generated",
        mediumRiskChecklist,
        highRiskChecklist,
        offloadChecklist,
        semanticActions,
        disclaimers: Array.isArray(parsed.disclaimers) ? parsed.disclaimers : ["Review all recommendations manually before running scripts."]
      };

      if (passesSemanticPlanValidation(candidatePlan, input)) {
        return candidatePlan;
      }

      void llmLog({
        call: "createFinalPlan",
        provider: process.env.DISKMIND_LLM_PROVIDER ?? "ollama",
        model: process.env.DISKMIND_OLLAMA_MODEL ?? process.env.DISKMIND_OPENAI_MODEL ?? "unknown",
        prompt: "semantic-validation",
        raw: JSON.stringify(candidatePlan),
        parseOk: false,
        error: "Plan rejected by semantic validation; deterministic fallback used."
      });

      return fallbackPlan;
    }
  } catch (error) {
    const message = getErrorMessage(error);
    failureReason = message;
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

  const unavailable = failureReason && isLikelyTransportFailure(failureReason);

  return {
    ...fallbackPlan,
    summary: unavailable
      ? `LLM request failed (likely local Ollama availability/timeout). ${fallbackPlan.summary}`
      : fallbackPlan.summary,
    disclaimers: [
      unavailable
        ? `LLM request failed: ${failureReason}; deterministic fallback recommendations were used.`
        : "LLM output parsing/validation failed; deterministic fallback recommendations were used."
    ]
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

  const model = process.env.DISKMIND_OLLAMA_MODEL ?? "llama3.1:8b";
  let lastError: unknown = null;

  for (let attempt = 0; attempt <= OLLAMA_MAX_RETRIES; attempt += 1) {
    try {
      const response = await runWithOllamaLock(() =>
        ollama.chat({
          model,
          messages: [
            { role: "system", content: "You are a strict JSON API. Return only JSON that matches the schema." },
            { role: "user", content: prompt }
          ],
          format: jsonSchema ?? "json",
          options: { temperature: 0.1 }
        })
      );

      return response.message.content;
    } catch (error) {
      lastError = error;
      const message = getErrorMessage(error);
      const canRetry = attempt < OLLAMA_MAX_RETRIES && isLikelyTransportFailure(message);

      if (!canRetry) {
        throw error;
      }

      const waitMs = OLLAMA_RETRY_DELAY_MS * (attempt + 1);
      console.warn(
        `DiskMind LLM transient failure on model ${model}; retrying (${attempt + 1}/${OLLAMA_MAX_RETRIES}) in ${waitMs}ms: ${message}`
      );
      await delay(waitMs);
    }
  }

  throw lastError instanceof Error ? lastError : new Error(getErrorMessage(lastError));
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
      extension: node.extension,
      lastAccessedISO: node.lastAccessedISO,
      lastModifiedISO: node.lastModifiedISO
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
    Array.isArray(plan.offloadChecklist) &&
    Array.isArray(plan.semanticActions) &&
    Array.isArray(plan.disclaimers)
  );
}

function isValidDecision(decision: AgentDecision, nodes: DiskNode[]): boolean {
  if (!["DELVE", "PLAN", "REPORT"].includes(decision.action)) {
    return false;
  }

  if (decision.action !== "DELVE") {
    return true;
  }

  if (!decision.target || !isLiteralWindowsPath(decision.target)) {
    return false;
  }

  const knownDirectories = new Set(nodes.filter((node) => node.isDirectory).map((node) => node.path.toLowerCase()));
  return knownDirectories.has(decision.target.toLowerCase());
}

function passesSemanticPlanValidation(
  plan: FinalPlan,
  input: { potentialSavings: PotentialSaving[]; topFiles: DiskNode[]; scannedPaths: string[] }
): boolean {
  const zeroRiskPaths = new Set(input.potentialSavings.map((item) => item.path.toLowerCase()));
  const topFilePaths = input.topFiles.map((file) => file.path);
  const genericSummary = plan.summary.trim().length < 40 || /^total size:/i.test(plan.summary.trim());
  if (genericSummary) {
    return false;
  }

  if (plan.zeroRiskPowershell.includes("Remove-Item") && !plan.zeroRiskPowershell.includes("-WhatIf")) {
    return false;
  }

  const scriptPaths = Array.from(plan.zeroRiskPowershell.matchAll(/[A-Za-z]:\\[^'"\r\n]+/g)).map((match) => match[0].toLowerCase());
  if (scriptPaths.some((path) => !Array.from(zeroRiskPaths).some((allowed) => path.startsWith(allowed)))) {
    return false;
  }

  if (!containsConcretePath(plan.mediumRiskChecklist, topFilePaths)) {
    return false;
  }

  if (!containsConcretePath(plan.offloadChecklist, topFilePaths)) {
    return false;
  }

  if (plan.semanticActions.length < 3) {
    return false;
  }

  const validTags = ["[PURGE]", "[OFFLOAD]", "[COMPRESS]", "[RETAIN]"];
  if (!plan.semanticActions.every((item) => validTags.some((tag) => item.startsWith(tag)))) {
    return false;
  }

  if (!containsConcretePath(plan.semanticActions, topFilePaths) && !containsPotentialPath(plan.semanticActions, input.potentialSavings)) {
    return false;
  }

  if (plan.offloadChecklist.some((item) => !/last accessed/i.test(item))) {
    return false;
  }

  return true;
}

function sanitizeChecklist(items: string[]): string[] {
  const output: string[] = [];

  for (const item of items) {
    if (typeof item !== "string") {
      continue;
    }

    const normalized = item.replace(/\s+/g, " ").trim();
    if (!normalized) {
      continue;
    }

    const lower = normalized.toLowerCase();
    const looksTruncated =
      lower.includes("truncated") ||
      normalized.endsWith("...") ||
      /[A-Za-z]:\\[^ ]{0,3}$/.test(normalized);

    if (looksTruncated) {
      continue;
    }

    output.push(normalized);
  }

  return output.slice(0, 20);
}

function sanitizeSemanticActions(items: string[]): string[] {
  return sanitizeChecklist(items)
    .filter(
      (item) =>
        item.startsWith("[PURGE]") ||
        item.startsWith("[OFFLOAD]") ||
        item.startsWith("[COMPRESS]") ||
        item.startsWith("[RETAIN]")
    )
    .slice(0, 30);
}

function buildFallbackOffloadChecklist(topFiles: DiskNode[]): string[] {
  return topFiles
    .filter((file) => !file.isDirectory)
    .slice(0, 8)
    .map((file) => {
      const access = file.lastAccessedISO ? ` last accessed ${file.lastAccessedISO}` : " last access unknown";
      return `Consider offloading ${file.path} (${file.sizeGB.toFixed(3)} GB,${access}) to external storage.`;
    });
}

function containsConcretePath(items: string[], knownPaths: string[]): boolean {
  const lowerKnown = knownPaths.map((path) => path.toLowerCase());
  return items.some((item) => lowerKnown.some((path) => item.toLowerCase().includes(path.toLowerCase())));
}

function containsPotentialPath(items: string[], potentialSavings: PotentialSaving[]): boolean {
  const paths = potentialSavings.map((item) => item.path.toLowerCase());
  return items.some((item) => paths.some((path) => item.toLowerCase().includes(path)));
}

function buildDeterministicFallbackPlan(input: {
  potentialSavings: PotentialSaving[];
  topFiles: DiskNode[];
  scannedPaths: string[];
}): FinalPlan {
  const zeroRiskCount = input.potentialSavings.length;
  const zeroRiskTotal = input.potentialSavings.reduce((sum, item) => sum + item.sizeGB, 0);
  const staleTopFiles = [...input.topFiles]
    .filter((file) => !file.isDirectory)
    .sort(compareByStalenessThenSize)
    .slice(0, 8);
  const mediumRiskChecklist = input.topFiles
    .filter((file) => !file.isDirectory)
    .slice(0, 6)
    .map((file) => `Review ${file.path} (${file.sizeGB.toFixed(3)} GB) before deleting or archiving.`);
  const highRiskChecklist = staleTopFiles
    .slice(0, 6)
    .map((file) => `Verify ownership and importance of ${file.path} (${file.sizeGB.toFixed(3)} GB) before any destructive action.`);

  const semanticActions = buildSemanticClusterActions(input);

  return {
    summary:
      `Detected ${zeroRiskCount} low-risk cleanup targets totaling ${zeroRiskTotal.toFixed(3)} GB. ` +
      `Large non-system files should be reviewed for archive, compression, or offload rather than deleted outright. ` +
      `Start with the generated zero-risk cleanup script, then offload stale clusters and compress occasional-use folders.`,
    zeroRiskPowershell: buildDeterministicZeroRiskScript(input.potentialSavings),
    mediumRiskChecklist,
    highRiskChecklist,
    offloadChecklist: staleTopFiles.map((file) => {
      const access = file.lastAccessedISO ?? "unknown";
      return `Move ${file.path} (${file.sizeGB.toFixed(3)} GB, last accessed ${access}) to external storage if no longer needed locally.`;
    }),
    semanticActions,
    disclaimers: ["Review all recommendations manually before running scripts or moving files."]
  };
}

function buildSemanticClusterActions(input: {
  potentialSavings: PotentialSaving[];
  topFiles: DiskNode[];
  scannedPaths: string[];
}): string[] {
  const systemDrive = (process.env.SystemDrive ?? "C:").replace(/\\$/, "").toUpperCase();
  const clusters = aggregateSemanticClusters(input.topFiles);
  const actions: string[] = [];

  for (const cluster of clusters.slice(0, 12)) {
    const hotDrive = cluster.drive.toUpperCase() === systemDrive;
    const clues = semanticClues(cluster.path);
    const frequency = frequencyFromDays(cluster.averageDaysSinceAccess);
    const percentHot = hotDrive && cluster.totalHotGb > 0 ? ((cluster.sizeGB / cluster.totalHotGb) * 100).toFixed(1) : "0.0";

    if (clues.purge) {
      actions.push(
        `[PURGE] ${cluster.path} (${cluster.sizeGB.toFixed(3)} GB) matches cache/temp/log semantics and ${frequency} activity; clear with -WhatIf to reclaim space safely.`
      );
      continue;
    }

    if (hotDrive && (frequency === "low" || clues.offload)) {
      actions.push(
        `[OFFLOAD] ${cluster.path} (${cluster.sizeGB.toFixed(3)} GB) appears ${clues.offload ? "archive/backup-like" : "inactive"} ` +
          `(last accessed ~${cluster.averageDaysSinceAccess} days ago, ${frequency} activity) on hot system drive ${cluster.drive}; ` +
          `moving to external storage can reclaim about ${percentHot}% of analyzed hot-drive cluster footprint.`
      );
      continue;
    }

    if (frequency === "medium" && cluster.sizeGB >= 5) {
      actions.push(
        `[COMPRESS] ${cluster.path} (${cluster.sizeGB.toFixed(3)} GB) has occasional activity ` +
          `(last accessed ~${cluster.averageDaysSinceAccess} days ago); compress into archives to reduce footprint while keeping local access.`
      );
      continue;
    }

    actions.push(
      `[RETAIN] ${cluster.path} (${cluster.sizeGB.toFixed(3)} GB) shows ${frequency} activity ` +
        `(last accessed ~${cluster.averageDaysSinceAccess} days ago); keep on current ${hotDrive ? "hot" : "cold"} drive for performance.`
    );
  }

  if (actions.length === 0) {
    actions.push("[RETAIN] No strong semantic clusters identified from sampled top files; keep current placement and continue monitoring.");
  }

  return actions.slice(0, 20);
}

interface SemanticCluster {
  path: string;
  drive: string;
  sizeGB: number;
  averageDaysSinceAccess: number;
  totalHotGb: number;
}

function aggregateSemanticClusters(topFiles: DiskNode[]): SemanticCluster[] {
  const now = Date.now();
  const grouped = new Map<string, { sizeGB: number; days: number[]; drive: string }>();

  for (const file of topFiles) {
    if (file.isDirectory) {
      continue;
    }

    const root = semanticClusterRoot(file.path);
    const drive = extractDrive(file.path);
    const days = daysSince(file.lastAccessedISO, now);
    const existing = grouped.get(root);

    if (existing) {
      existing.sizeGB += file.sizeGB;
      existing.days.push(days);
    } else {
      grouped.set(root, { sizeGB: file.sizeGB, days: [days], drive });
    }
  }

  const systemDrive = (process.env.SystemDrive ?? "C:").replace(/\\$/, "").toUpperCase();
  const totalHotGb = Array.from(grouped.values())
    .filter((item) => item.drive.toUpperCase() === systemDrive)
    .reduce((sum, item) => sum + item.sizeGB, 0);

  return Array.from(grouped.entries())
    .map(([clusterPath, value]) => ({
      path: clusterPath,
      drive: value.drive,
      sizeGB: value.sizeGB,
      averageDaysSinceAccess: Math.round(value.days.reduce((sum, d) => sum + d, 0) / Math.max(1, value.days.length)),
      totalHotGb
    }))
    .sort((a, b) => b.sizeGB - a.sizeGB);
}

function semanticClusterRoot(filePath: string): string {
  const parts = filePath.split("\\").filter(Boolean);
  if (parts.length <= 2) {
    return parts.join("\\");
  }

  if (parts[1]?.toLowerCase() === "users" && parts.length >= 5) {
    if (["documents", "desktop"].includes(parts[3]?.toLowerCase()) && parts[4]) {
      return `${parts[0]}\\${parts[1]}\\${parts[2]}\\${parts[3]}\\${parts[4]}`;
    }

    return `${parts[0]}\\${parts[1]}\\${parts[2]}\\${parts[3]}`;
  }

  if (["windows", "program files", "program files (x86)", "programdata"].includes(parts[1]?.toLowerCase())) {
    return parts.length >= 3 ? `${parts[0]}\\${parts[1]}\\${parts[2]}` : `${parts[0]}\\${parts[1]}`;
  }

  return `${parts[0]}\\${parts[1]}\\${parts[2]}`;
}

function semanticClues(clusterPath: string): { purge: boolean; offload: boolean } {
  const lower = clusterPath.toLowerCase();
  return {
    purge: /(\\temp\\|\\cache\\|\\logs?\\|\$recycle\.bin|installer\\msi.*\.tmp|windows\\temp)/.test(lower),
    offload: /(backup|archive|_v\d+|old|exports?|media|videos?|downloads?)/.test(lower)
  };
}

function frequencyFromDays(days: number): "high" | "medium" | "low" {
  if (days <= 7) {
    return "high";
  }
  if (days <= 45) {
    return "medium";
  }
  return "low";
}

function daysSince(iso: string | undefined, nowMs: number): number {
  if (!iso) {
    return 365;
  }

  const ts = Date.parse(iso);
  if (Number.isNaN(ts)) {
    return 365;
  }

  return Math.max(0, Math.round((nowMs - ts) / (1000 * 60 * 60 * 24)));
}

function extractDrive(filePath: string): string {
  const match = filePath.match(/^[A-Za-z]:/);
  return match ? match[0] : "UNKNOWN";
}

function isLiteralWindowsPath(value: string): boolean {
  return /^[A-Za-z]:\\/.test(value);
}

function buildDeterministicZeroRiskScript(potentialSavings: PotentialSaving[]): string {
  const uniquePaths = Array.from(new Set(potentialSavings.map((item) => item.path))).slice(0, 20);
  const lines = [
    "# Deterministic zero-risk cleanup script",
    "# Review before running. All removals use -WhatIf.",
    ""
  ];

  for (const path of uniquePaths) {
    const normalized = path.replace(/'/g, "''");
    const wildcard = /\\Temp$/i.test(path) || /\$Recycle\.Bin$/i.test(path) ? "\\*" : "";
    lines.push(`# Remove zero-risk path: ${path}`);
    lines.push(`Remove-Item -Path '${normalized}${wildcard}' -Recurse -Force -WhatIf -ErrorAction SilentlyContinue`);
    lines.push("");
  }

  return lines.join("\n").trim();
}

function compareByStalenessThenSize(left: DiskNode, right: DiskNode): number {
  const leftAccess = left.lastAccessedISO ? Date.parse(left.lastAccessedISO) : Number.POSITIVE_INFINITY;
  const rightAccess = right.lastAccessedISO ? Date.parse(right.lastAccessedISO) : Number.POSITIVE_INFINITY;

  if (leftAccess !== rightAccess) {
    return leftAccess - rightAccess;
  }

  return right.sizeGB - left.sizeGB;
}

function getErrorMessage(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }
  return String(error);
}

function isLikelyTransportFailure(message: string): boolean {
  const lower = message.toLowerCase();
  return (
    lower.includes("fetch failed") ||
    lower.includes("econn") ||
    lower.includes("timed out") ||
    lower.includes("timeout") ||
    lower.includes("socket") ||
    lower.includes("network") ||
    lower.includes("connection")
  );
}

async function delay(ms: number): Promise<void> {
  await new Promise<void>((resolve) => setTimeout(resolve, ms));
}

async function getPromptBudget(): Promise<PromptBudget> {
  if (!promptBudgetCache) {
    promptBudgetCache = resolvePromptBudget();
  }

  return promptBudgetCache;
}

async function resolvePromptBudget(): Promise<PromptBudget> {
  const provider = (process.env.DISKMIND_LLM_PROVIDER ?? "ollama").toLowerCase();
  if (provider !== "ollama") {
    return DEFAULT_PROMPT_BUDGET;
  }

  const envVram = Number(process.env.DISKMIND_OLLAMA_VRAM_GB ?? "");
  if (Number.isFinite(envVram) && envVram > 0) {
    return budgetFromVramGb(envVram);
  }

  const detected = await detectOllamaVramGb();
  if (!detected) {
    return DEFAULT_PROMPT_BUDGET;
  }

  return budgetFromVramGb(detected);
}

async function detectOllamaVramGb(): Promise<number | null> {
  try {
    const psResponse = await ollama.ps();
    const models = ((psResponse as unknown as { models?: Array<Record<string, unknown>> }).models ?? []);
    if (models.length === 0) {
      return null;
    }

    const bytes = models
      .map((model) => parseVramBytes(model.size_vram))
      .filter((value): value is number => value !== null)
      .reduce((sum, value) => sum + value, 0);

    if (bytes <= 0) {
      return null;
    }

    return bytes / 1024 / 1024 / 1024;
  } catch {
    return null;
  }
}

function parseVramBytes(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value) && value > 0) {
    return value;
  }

  if (typeof value !== "string") {
    return null;
  }

  const direct = Number(value);
  if (Number.isFinite(direct) && direct > 0) {
    return direct;
  }

  const match = value.trim().toUpperCase().match(/^([0-9]+(?:\.[0-9]+)?)\s*(B|KB|MB|GB|TB)$/);
  if (!match) {
    return null;
  }

  const amount = Number(match[1]);
  const unit = match[2];
  const multiplier =
    unit === "TB"
      ? 1024 ** 4
      : unit === "GB"
        ? 1024 ** 3
        : unit === "MB"
          ? 1024 ** 2
          : unit === "KB"
            ? 1024
            : 1;

  return amount * multiplier;
}

function budgetFromVramGb(vramGb: number): PromptBudget {
  if (vramGb >= 40) {
    return {
      decisionNodeLimit: 120,
      decisionTopFilesLimit: 80,
      planSavingsLimit: 420,
      planTopFilesLimit: 180
    };
  }

  if (vramGb >= 24) {
    return {
      decisionNodeLimit: 90,
      decisionTopFilesLimit: 60,
      planSavingsLimit: 320,
      planTopFilesLimit: 130
    };
  }

  if (vramGb >= 16) {
    return {
      decisionNodeLimit: 70,
      decisionTopFilesLimit: 45,
      planSavingsLimit: 240,
      planTopFilesLimit: 95
    };
  }

  if (vramGb >= 10) {
    return {
      decisionNodeLimit: 50,
      decisionTopFilesLimit: 30,
      planSavingsLimit: 170,
      planTopFilesLimit: 60
    };
  }

  return DEFAULT_PROMPT_BUDGET;
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
