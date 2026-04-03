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
}

export async function startAnalysis(options: AgentOptions = {}): Promise<{ reportPath: string; scriptPath: string }> {
  const maxIterations = options.maxIterations ?? 10;
  const rootSummaryDepth = options.rootSummaryDepth ?? 1;
  const diveSummaryDepth = options.diveSummaryDepth ?? 2;
  const topFilesLimit = options.topFilesLimit ?? 50;

  const map = await loadMap();
  const visited = new Set<string>();

  const roots = await getExistingRoots(getDefaultRoots());
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
    const rootTop = await listLargestFiles(root, Math.ceil(topFilesLimit / Math.max(1, roots.length)));
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
    const localTopFiles = await listLargestFiles(currentPath, topFilesLimit);
    mergeTopFiles(map, localTopFiles, topFilesLimit);
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

  return writeReports(finalPlan, map.potentialSavings);
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
    disclaimers: string[];
  },
  potentialSavings: PotentialSaving[]
): Promise<{ reportPath: string; scriptPath: string }> {
  const reportsDir = path.join(process.cwd(), "reports");
  await fs.ensureDir(reportsDir);

  const stamp = new Date().toISOString().replace(/[.:]/g, "-");
  const reportPath = path.join(reportsDir, `cleanup-plan-${stamp}.txt`);
  const scriptPath = path.join(reportsDir, `zero-risk-cleanup-${stamp}.ps1`);

  const reportLines = [
    "DiskMind Cleanup Plan",
    `Generated: ${new Date().toISOString()}`,
    "",
    "Summary",
    finalPlan.summary,
    "",
    "Top Potential Savings",
    ...potentialSavings.slice(0, 100).map((item) => `- ${item.sizeGB.toFixed(3)} GB | ${item.path} | ${item.reason}`),
    "",
    "Medium Risk Checklist",
    ...finalPlan.mediumRiskChecklist.map((item) => `- ${item}`),
    "",
    "High Risk / User Action Checklist",
    ...finalPlan.highRiskChecklist.map((item) => `- ${item}`),
    "",
    "Disclaimers",
    ...finalPlan.disclaimers.map((item) => `- ${item}`),
    "",
    "Safety",
    "- DiskMind is read-only and does not execute delete operations.",
    "- Review the generated PowerShell script before running it manually."
  ].join("\n");

  const scriptHeader = [
    "# DiskMind generated script",
    "# Review before running. This script is NOT auto-executed.",
    "# Recommended test mode:",
    "#   PowerShell -ExecutionPolicy Bypass -File <this-script> -WhatIf",
    ""
  ].join("\n");

  await fs.writeFile(reportPath, reportLines, "utf8");
  await fs.writeFile(scriptPath, `${scriptHeader}${finalPlan.zeroRiskPowershell}\n`, "utf8");

  return { reportPath, scriptPath };
}
