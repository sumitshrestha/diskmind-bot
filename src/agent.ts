import * as fs from "fs-extra";
import * as path from "path";
import bloatwareRules from "../config/bloatware.json";
import {
  getDirectorySummary,
  getDefaultRoots,
  getExistingRoots,
  listLargestFiles,
  DiskNode
} from "./scanner";
import {
  loadMap,
  prepareMapForRun,
  saveMap,
  updateSnapshot,
  mergePotentialSavings,
  mergeTopFiles,
  PotentialSaving
} from "./database";
import { createFinalPlan, decideNextAction } from "./llm";

export interface AgentOptions {
  maxIterations?: number;
  rootSummaryDepth?: number;
  diveSummaryDepth?: number;
  topFilesLimit?: number;
  topFileScanDepth?: number;
  topFileScanMaxFiles?: number;
  roots?: string[];
}

export async function startAnalysis(options: AgentOptions = {}): Promise<{ reportPath: string; scriptPath: string }> {
  const maxIterations = options.maxIterations ?? 10;
  const rootSummaryDepth = options.rootSummaryDepth ?? 1;
  const diveSummaryDepth = options.diveSummaryDepth ?? 2;
  const topFilesLimit = options.topFilesLimit ?? 50;
  const topFileScanDepth = options.topFileScanDepth ?? Number(process.env.DISKMIND_TOP_SCAN_DEPTH ?? 4);
  const topFileScanMaxFiles = options.topFileScanMaxFiles ?? Number(process.env.DISKMIND_TOP_SCAN_MAX_FILES ?? 25000);

  const map = await prepareMapForRun(await loadMap());
  const visited = new Set<string>();

  const rootCandidates = options.roots && options.roots.length > 0 ? options.roots : getDefaultRoots();
  const roots = await getExistingRoots(rootCandidates);
  if (roots.length === 0) {
    throw new Error("No readable roots found. Set DISKMIND_ROOTS to one or more accessible paths.");
  }

  console.log("DiskMind Bot: analyzing root overview...");

  for (const root of roots) {
    const snapshot = await getDirectorySummary(root, {
      depth: rootSummaryDepth,
      folderSizeDepth: rootSummaryDepth,
      maxItems: 250
    });

    updateSnapshot(map, snapshot);
    mergePotentialSavings(map, detectPotentialSavings(snapshot.nodes));
    mergeTopFiles(map, pickTopFilesFromNodes(snapshot.nodes, topFilesLimit), topFilesLimit);

    // Perform one bounded deep scan per root for better global heavy-hitter detection.
    const rootTop = await listLargestFiles(root, Math.ceil(topFilesLimit / Math.max(1, roots.length)), {
      maxDepth: topFileScanDepth,
      maxFilesVisited: topFileScanMaxFiles
    });
    mergeTopFiles(map, rootTop, topFilesLimit);
    visited.add(root);
    await saveMap(map);
  }

  let currentPath = roots[0];
  let iteration = 0;

  while (iteration < maxIterations) {
    iteration += 1;

    const snapshot = await getDirectorySummary(currentPath, {
      depth: diveSummaryDepth,
      folderSizeDepth: diveSummaryDepth,
      maxItems: 300
    });

    updateSnapshot(map, snapshot);
    mergePotentialSavings(map, detectPotentialSavings(snapshot.nodes));
    mergeTopFiles(map, pickTopFilesFromNodes(snapshot.nodes, topFilesLimit), topFilesLimit);
    await saveMap(map);

    const decision = await decideNextAction({
      currentPath,
      nodes: snapshot.nodes,
      topFiles: map.topFiles,
      visitedPaths: Array.from(visited)
    });

    console.log(`DiskMind decision [${iteration}/${maxIterations}]: ${decision.action} - ${decision.reasoning}`);

    if (decision.action === "DELVE" && decision.target && !visited.has(decision.target)) {
      currentPath = decision.target;
      visited.add(decision.target);
      continue;
    }

    break;
  }

  const finalPlan = await createFinalPlan({
    potentialSavings: map.potentialSavings,
    topFiles: map.topFiles,
    scannedPaths: Object.keys(map.scannedPaths)
  });

  return writeReports(finalPlan, map.potentialSavings, Object.keys(map.scannedPaths));
}

function pickTopFilesFromNodes(nodes: DiskNode[], limit: number): DiskNode[] {
  return nodes
    .filter((node) => !node.isDirectory)
    .sort((a, b) => b.sizeGB - a.sizeGB)
    .slice(0, limit);
}

function detectPotentialSavings(nodes: DiskNode[]): PotentialSaving[] {
  const lowerPatterns = bloatwareRules.knownPatterns.map((pattern) => pattern.toLowerCase());
  const lowRiskExtensions = bloatwareRules.extensionsLowRisk.map((ext) => ext.toLowerCase());

  return nodes
    .filter((node) => {
      const lowerPath = node.path.replace(/\\/g, "/").toLowerCase();
      const ext = (node.extension ?? "").toLowerCase();
      const byPattern = lowerPatterns.some((pattern) => lowerPath.includes(pattern));
      const byExtension = !node.isDirectory && lowRiskExtensions.includes(ext);
      return byPattern || byExtension;
    })
    .map((node) => ({
      path: node.path,
      sizeGB: node.sizeGB,
      reason: node.isDirectory ? "Matches known cache/temp pattern" : "Low-risk extension candidate",
      riskHint: "zero" as const
    }));
}

async function writeReports(
  finalPlan: {
    summary: string;
    zeroRiskPowershell: string;
    mediumRiskChecklist: string[];
    highRiskChecklist: string[];
    offloadChecklist: string[];
    semanticActions: string[];
    disclaimers: string[];
  },
  potentialSavings: PotentialSaving[],
  scannedPaths: string[]
): Promise<{ reportPath: string; scriptPath: string }> {
  const reportsDir = path.join(process.cwd(), "reports");
  await fs.ensureDir(reportsDir);

  const stamp = new Date().toISOString().replace(/[.:]/g, "-");
  const reportPath = path.join(reportsDir, `cleanup-plan-${stamp}.html`);
  const scriptPath = path.join(reportsDir, `zero-risk-cleanup-${stamp}.ps1`);

  const rows = buildReportRows(finalPlan.semanticActions, potentialSavings).sort((a, b) => b.sizeGB - a.sizeGB);
  const targetDrive = resolveTargetDrive(scannedPaths);
  const totalSizeGb = rows.reduce((sum, row) => sum + row.sizeGB, 0);
  const potentialRecoveryGb = rows
    .filter((row) => row.action === "PURGE" || row.action === "OFFLOAD")
    .reduce((sum, row) => sum + row.sizeGB, 0);

  const tableRows = rows.length
    ? rows
        .map((row) => {
          const style = actionStyle(row.action);
          return [
            `<tr style="background-color: ${style.background}; border-bottom: 1px solid ${style.border};">`,
            `  <td style="padding: 10px;">${escapeHtml(row.path)}</td>`,
            `  <td style="padding: 10px;">${row.sizeGB.toFixed(3)} GB</td>`,
            `  <td style="padding: 10px;">${escapeHtml(row.category)}</td>`,
            `  <td style="padding: 10px;"><strong>[${row.action}]</strong></td>`,
            `  <td style="padding: 10px;">${escapeHtml(row.justification)}</td>`,
            `</tr>`
          ].join("\n");
        })
        .join("\n")
    : [
        `<tr style="background-color: #d4edda; border-bottom: 1px solid #c3e6cb;">`,
        `  <td style="padding: 10px;" colspan="5">No actionable clusters detected in this run. Continue monitoring.</td>`,
        `</tr>`
      ].join("\n");

  const disclaimerHtml = finalPlan.disclaimers
    .map((item) => `<li style="margin-bottom: 4px;">${escapeHtml(item)}</li>`)
    .join("\n");

  const reportHtml = [
    `<div style="font-family: Segoe UI, Tahoma, Arial, sans-serif; padding: 20px; color: #1f2933;">`,
    `  <h2 style="color: #1f2933; margin: 0 0 8px;">Storage Analysis: Drive ${escapeHtml(targetDrive)}</h2>`,
    `  <p style="margin: 0 0 4px;"><strong>Target Drive:</strong> ${escapeHtml(targetDrive)}</p>`,
    `  <p style="margin: 0 0 4px;"><strong>Total Size:</strong> ${totalSizeGb.toFixed(3)} GB</p>`,
    `  <p style="margin: 0 0 16px;"><strong>Potential Space Recovery:</strong> ${potentialRecoveryGb.toFixed(3)} GB</p>`,
    `  <table style="width: 100%; border-collapse: collapse;">`,
    `    <tr style="background-color: #f8f9fa; border-bottom: 2px solid #dee2e6;">`,
    `      <th style="padding: 10px; text-align: left;">Path</th>`,
    `      <th style="padding: 10px; text-align: left;">Size</th>`,
    `      <th style="padding: 10px; text-align: left;">Category</th>`,
    `      <th style="padding: 10px; text-align: left;">Action</th>`,
    `      <th style="padding: 10px; text-align: left;">Justification</th>`,
    `    </tr>`,
    tableRows,
    `  </table>`,
    `  <p style="margin-top: 16px;"><strong>Summary:</strong> ${escapeHtml(finalPlan.summary)}</p>`,
    `  <ul style="margin-top: 10px; padding-left: 20px;">`,
    disclaimerHtml,
    `    <li style="margin-bottom: 4px;">DiskMind is read-only and does not execute delete operations.</li>`,
    `    <li style="margin-bottom: 4px;">Review the generated PowerShell script before running it manually.</li>`,
    `  </ul>`,
    `</div>`
  ].join("\n");

  const scriptHeader = [
    "# DiskMind generated script",
    "# Review before running. This script is NOT auto-executed.",
    "# Recommended test mode:",
    "#   PowerShell -ExecutionPolicy Bypass -File <this-script> -WhatIf",
    ""
  ].join("\n");

  await fs.writeFile(reportPath, reportHtml, "utf8");
  await fs.writeFile(scriptPath, `${scriptHeader}${finalPlan.zeroRiskPowershell}\n`, "utf8");

  return { reportPath, scriptPath };
}

interface ReportRow {
  path: string;
  sizeGB: number;
  category: string;
  action: "PURGE" | "OFFLOAD" | "RETAIN";
  justification: string;
}

function buildReportRows(semanticActions: string[], potentialSavings: PotentialSaving[]): ReportRow[] {
  const rows: ReportRow[] = [];
  const seen = new Set<string>();

  for (const item of semanticActions) {
    const parsed = parseSemanticAction(item);
    if (!parsed) {
      continue;
    }

    const key = `${parsed.action}|${parsed.path}`.toLowerCase();
    if (seen.has(key)) {
      continue;
    }

    seen.add(key);
    rows.push(parsed);
  }

  for (const item of potentialSavings) {
    const pathValue = toPlaceholderPath(item.path);
    const key = `PURGE|${pathValue}`.toLowerCase();
    if (seen.has(key)) {
      continue;
    }

    seen.add(key);
    rows.push({
      path: pathValue,
      sizeGB: item.sizeGB,
      category: "Temp/Cache/Redundant",
      action: "PURGE",
      justification: `${item.reason}. Candidate identified as a high-confidence deletion target.`
    });
  }

  return rows;
}

function parseSemanticAction(item: string): ReportRow | null {
  const match = item.match(/^\[(PURGE|OFFLOAD|RETAIN)\]\s+(.+?)\s+\(([\d.]+)\s+GB\)\s+(.+)$/);
  if (!match) {
    return null;
  }

  const action = match[1] as "PURGE" | "OFFLOAD" | "RETAIN";
  const pathValue = toPlaceholderPath(match[2].trim());
  const sizeGB = Number(match[3]);
  const reasoning = ensureSentence(match[4].trim());

  return {
    path: pathValue,
    sizeGB: Number.isFinite(sizeGB) ? sizeGB : 0,
    category: categoryForAction(action),
    action,
    justification: reasoning
  };
}

function categoryForAction(action: "PURGE" | "OFFLOAD" | "RETAIN"): string {
  if (action === "PURGE") {
    return "Temp/Cache/Redundant";
  }
  if (action === "OFFLOAD") {
    return "Low-Frequency Cluster";
  }
  return "High-Frequency Cluster";
}

function actionStyle(action: "PURGE" | "OFFLOAD" | "RETAIN"): { background: string; border: string } {
  if (action === "PURGE") {
    return { background: "#ffcccc", border: "#f5b4b4" };
  }
  if (action === "OFFLOAD") {
    return { background: "#fff3cd", border: "#ffeeba" };
  }
  return { background: "#d4edda", border: "#c3e6cb" };
}

function resolveTargetDrive(scannedPaths: string[]): string {
  for (const scannedPath of scannedPaths) {
    const match = scannedPath.match(/^[A-Za-z]:/);
    if (match) {
      return match[0];
    }
  }

  const envDrive = process.env.SystemDrive?.replace(/\\$/, "");
  return envDrive || "${DRIVE_LETTER}";
}

function toPlaceholderPath(rawPath: string): string {
  const normalized = rawPath.replace(/\//g, "\\");
  return normalized.replace(/^([A-Za-z]:\\Users\\)[^\\]+/i, "$1${USER_NAME}");
}

function ensureSentence(text: string): string {
  if (!text) {
    return "No additional justification available.";
  }
  return /[.!?]$/.test(text) ? text : `${text}.`;
}

function escapeHtml(value: string): string {
  return value
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\"/g, "&quot;")
    .replace(/'/g, "&#39;");
}
