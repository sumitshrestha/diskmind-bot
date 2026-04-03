import { Ollama } from "ollama";
import OpenAI from "openai";
import { DiskNode } from "./scanner";
import { PotentialSaving } from "./database";

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
  const prompt = [
    "You are DiskMind's decision engine.",
    "Choose one action:",
    "- DELVE: continue scanning a folder",
    "- PLAN: enough data to propose cleanup",
    "- REPORT: stop and summarize current findings",
    "Output strict JSON only:",
    '{"action":"DELVE|PLAN|REPORT","target":"optional_path","reasoning":"short text"}',
    "Rules:",
    "- Prefer DELVE into the largest unvisited directory.",
    "- Do not DELVE into protected system internals unless obviously huge.",
    "- If confidence is low, choose PLAN.",
    `Current path: ${input.currentPath}`,
    `Visited paths: ${JSON.stringify(input.visitedPaths)}`,
    `Current summary (largest first): ${JSON.stringify(input.nodes.slice(0, 60))}`,
    `Global top files: ${JSON.stringify(input.topFiles.slice(0, 50))}`
  ].join("\n");

  const raw = await chat(prompt);
  const parsed = parseJson<AgentDecision>(raw);

  if (parsed && ["DELVE", "PLAN", "REPORT"].includes(parsed.action)) {
    return parsed;
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
  const prompt = [
    "You are DiskMind's cleanup strategist.",
    "Analyze potential deletions and classify risk.",
    "Risk buckets:",
    "- Zero Risk: Caches, Temp files, Prefetch, logs safe to clear",
    "- Medium Risk: old apps, duplicate AI models, stale SDK caches",
    "- High Risk/User Action: personal media, project folders, unknown binaries",
    "Return strict JSON only with keys:",
    "summary, zeroRiskPowershell, mediumRiskChecklist, highRiskChecklist, disclaimers",
    "Rules for zeroRiskPowershell:",
    "- Readable, commented, idempotent",
    "- Must only remove obvious cache/temp/log paths",
    "- Include -WhatIf in Remove-Item examples",
    `Scanned paths: ${JSON.stringify(input.scannedPaths)}`,
    `Potential savings: ${JSON.stringify(input.potentialSavings.slice(0, 500))}`,
    `Top files: ${JSON.stringify(input.topFiles.slice(0, 100))}`
  ].join("\n");

  const raw = await chat(prompt);
  const parsed = parseJson<FinalPlan>(raw);

  if (parsed) {
    return {
      summary: parsed.summary ?? "No summary produced.",
      zeroRiskPowershell: parsed.zeroRiskPowershell ?? "# No script generated",
      mediumRiskChecklist: Array.isArray(parsed.mediumRiskChecklist) ? parsed.mediumRiskChecklist : [],
      highRiskChecklist: Array.isArray(parsed.highRiskChecklist) ? parsed.highRiskChecklist : [],
      disclaimers: Array.isArray(parsed.disclaimers) ? parsed.disclaimers : ["Review all recommendations manually before running scripts."]
    };
  }

  return {
    summary: "Model response could not be parsed. Manual review required.",
    zeroRiskPowershell: "# Manual script creation required",
    mediumRiskChecklist: ["Review large unused applications."],
    highRiskChecklist: ["Inspect personal media and project folders manually."],
    disclaimers: ["LLM output parsing failed; no automated recommendations trusted."]
  };
}

async function chat(prompt: string): Promise<string> {
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

  const response = await ollama.chat({
    model: process.env.DISKMIND_OLLAMA_MODEL ?? "llama3.1:8b",
    messages: [{ role: "user", content: prompt }],
    options: { temperature: 0.1 }
  });

  return response.message.content;
}

function parseJson<T>(raw: string): T | null {
  const trimmed = raw.trim();

  try {
    return JSON.parse(trimmed) as T;
  } catch {
    const fenced = trimmed.match(/```(?:json)?\s*([\s\S]*?)```/i);
    if (fenced?.[1]) {
      try {
        return JSON.parse(fenced[1]) as T;
      } catch {
        return null;
      }
    }

    const firstBrace = trimmed.indexOf("{");
    const lastBrace = trimmed.lastIndexOf("}");
    if (firstBrace >= 0 && lastBrace > firstBrace) {
      try {
        return JSON.parse(trimmed.slice(firstBrace, lastBrace + 1)) as T;
      } catch {
        return null;
      }
    }

    return null;
  }
}
