import * as fs from "fs-extra";
import * as path from "path";
import { statfs } from "node:fs/promises";
import bloatwareRules from "../config/bloatware.json";
import {
  getDirectorySummary,
  getDefaultRoots,
  getExistingRoots,
  shouldExcludeDirectory,
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
import { createFinalPlan } from "./llm";

export interface AgentOptions {
  maxIterations?: number;
  rootSummaryDepth?: number;
  diveSummaryDepth?: number;
  topFilesLimit?: number;
  maxItemsPerDirectory?: number;
  roots?: string[];
}

interface ScanCoverage {
  roots: string[];
  maxIterations: number;
  processedDirectories: number;
  discoveredDirectories: number;
  scannedPathCount: number;
  remainingQueue: number;
  reachedIterationCap: boolean;
  maxItemsPerDirectory: number;
  diveSummaryDepth: number;
  folderSizeDepth: number;
}

export async function startAnalysis(options: AgentOptions = {}): Promise<{ reportPath: string; scriptPath: string; logPath: string }> {
  const runId = new Date().toISOString().replace(/[.:]/g, "-");
  const logDir = path.join(process.cwd(), "logs");
  const runtimeLogPath = path.join(logDir, `scan-progress-${runId}.log`);

  await fs.ensureDir(logDir);
  await archiveTopLevelFilesToPrev(logDir);
  await fs.writeFile(runtimeLogPath, `# DiskMind scan log\n# runId=${runId}\n`, "utf8");

  const maxIterations = options.maxIterations ?? Number(process.env.DISKMIND_MAX_ITERATIONS ?? 10);
  const targetCoverage = Math.min(100, Math.max(1, Number(process.env.DISKMIND_TARGET_COVERAGE ?? 75)));
  const rootSummaryDepth = options.rootSummaryDepth ?? 1;
  const diveSummaryDepth = options.diveSummaryDepth ?? 1;
  const folderSizeDepth = Number(process.env.DISKMIND_FOLDER_SIZE_DEPTH ?? 1);
  const folderSizeMaxEntries = Number(process.env.DISKMIND_FOLDER_SIZE_MAX_ENTRIES ?? 2000);
  const topFilesLimit = options.topFilesLimit ?? Number(process.env.DISKMIND_TOP_FILES_LIMIT ?? 500);
  const maxItemsPerDirectory =
    options.maxItemsPerDirectory ?? Number(process.env.DISKMIND_MAX_ITEMS_PER_DIR ?? 5000);
  const maxDirsToQueuePerScan = Number(process.env.DISKMIND_MAX_DIRS_ENQUEUE_PER_SCAN ?? 64);
  const minDirsToQueuePerScan = Number(process.env.DISKMIND_MIN_DIRS_ENQUEUE_PER_SCAN ?? 8);
  const adaptiveQueueEnabled = (process.env.DISKMIND_ADAPTIVE_QUEUE ?? "true").toLowerCase() !== "false";
  const queuePressureMultiplier = Number(process.env.DISKMIND_QUEUE_PRESSURE_MULTIPLIER ?? 2);
  const queueRelaxMultiplier = Number(process.env.DISKMIND_QUEUE_RELAX_MULTIPLIER ?? 0.75);
  const adaptiveRebalanceEvery = Number(process.env.DISKMIND_ADAPTIVE_REBALANCE_EVERY ?? 20);
  const saveEveryDirectories = Number(process.env.DISKMIND_SAVE_EVERY_DIRS ?? 25);
  const progressLogEveryDirectories = Number(process.env.DISKMIND_PROGRESS_EVERY_DIRS ?? 10);
  const progressLogIntervalMs = Number(process.env.DISKMIND_PROGRESS_INTERVAL_MS ?? 5000);
  const heartbeatIntervalMs = Number(process.env.DISKMIND_HEARTBEAT_INTERVAL_MS ?? 15000);

  const map = await prepareMapForRun(await loadMap());
  const visited = new Set<string>();
  const queued = new Set<string>();
  const runStartedAt = Date.now();
  let processedDirectories = 0;
  let discoveredDirectories = 0;
  let pendingSaveCount = 0;
  let lastProgressLogAt = 0;
  let lastSaveAt = Date.now();
  let currentQueueFanout = Math.max(1, maxDirsToQueuePerScan);
  let effectiveMaxIterations = maxIterations;
  const queue: string[] = [];
  const heartbeatTimer = setInterval(() => {
    emit(makeProgressLine("heartbeat"));
  }, heartbeatIntervalMs);
  heartbeatTimer.unref();

  try {
    const rootCandidates = options.roots && options.roots.length > 0 ? options.roots : getDefaultRoots();
    const roots = await getExistingRoots(rootCandidates);
    if (roots.length === 0) {
      throw new Error("No readable roots found. Set DISKMIND_ROOTS to one or more accessible paths.");
    }

    emit("DiskMind Bot: analyzing root overview...");
    emit(
      `Scan settings: maxIterations=${maxIterations}, maxItemsPerDirectory=${maxItemsPerDirectory}, ` +
        `folderSizeDepth=${folderSizeDepth}, queueFanout=${maxDirsToQueuePerScan}, adaptiveQueue=${adaptiveQueueEnabled}`
    );
    emit(`Runtime scan log: ${runtimeLogPath}`);

    for (const root of roots) {
      emit(`Root scan start: ${root}`);
      const snapshot = await getDirectorySummary(root, {
        depth: rootSummaryDepth,
        folderSizeDepth,
        folderSizeMaxEntries,
        maxItems: maxItemsPerDirectory
      });

      updateSnapshot(map, snapshot);
      mergePotentialSavings(map, detectPotentialSavings(snapshot.nodes));
      mergeTopFiles(map, pickTopFilesFromNodes(snapshot.nodes, topFilesLimit), topFilesLimit);

      queue.push(root);
      queued.add(root);
      discoveredDirectories += 1;
      pendingSaveCount += 1;
      await maybeCheckpoint(true);
      logProgress(`root complete ${root}`, true);
    }

    while (queue.length > 0 && processedDirectories < effectiveMaxIterations) {
      const currentPath = queue.shift();
      if (!currentPath) {
        continue;
      }
      queued.delete(currentPath);

      if (visited.has(currentPath)) {
        continue;
      }

      visited.add(currentPath);

      const snapshot = await getDirectorySummary(currentPath, {
        depth: diveSummaryDepth,
        folderSizeDepth,
        folderSizeMaxEntries,
        maxItems: maxItemsPerDirectory
      });

      updateSnapshot(map, snapshot);
      mergePotentialSavings(map, detectPotentialSavings(snapshot.nodes));
      mergeTopFiles(map, pickTopFilesFromNodes(snapshot.nodes, topFilesLimit), topFilesLimit);
      processedDirectories += 1;
      pendingSaveCount += 1;

      const nextDirs = snapshot.nodes
        .filter((node) => {
          if (!node.isDirectory || !isLiteralWindowsPath(node.path)) return false;
          return !shouldExcludeDirectory(node.path);
        })
        .sort((a, b) => b.sizeGB - a.sizeGB)
        .slice(0, currentQueueFanout);

      for (const node of nextDirs) {
        if (!visited.has(node.path) && !queued.has(node.path)) {
          queue.push(node.path);
          queued.add(node.path);
          discoveredDirectories += 1;
        }
      }

      await maybeCheckpoint(false);
      logProgress(`scanned ${currentPath}`, false);

      if (adaptiveQueueEnabled && processedDirectories % Math.max(1, adaptiveRebalanceEvery) === 0) {
        rebalanceQueuePressure();
      }

      if (queue.length > 0 && processedDirectories >= effectiveMaxIterations) {
        if (shouldExtendIterationBudgetForCoverage()) {
          const extension = 100;
          effectiveMaxIterations += extension;
          const currentCoverage = discoveredDirectories > 0 
            ? Math.round((processedDirectories / discoveredDirectories) * 100) 
            : 0;
          emit(
            `[Coverage] Extended iterations by ${extension}; new cap=${effectiveMaxIterations}, ` +
              `current coverage=${currentCoverage}%, target=${targetCoverage}%, queue=${queue.length}`
          );
        }
      }
    }

    if (queue.length > 0 && processedDirectories >= effectiveMaxIterations) {
      emit(
        `Reached iteration cap (${effectiveMaxIterations}). ` +
          `Generating report from current dataset with ${Object.keys(map.scannedPaths).length} scanned paths.`
      );
    }

    await saveMap(map);
    logProgress("final checkpoint saved", true);

    const finalPlan = await createFinalPlan({
      potentialSavings: map.potentialSavings,
      topFiles: map.topFiles,
      scannedPaths: Object.keys(map.scannedPaths)
    });

    const scannedPathKeys = Object.keys(map.scannedPaths);
    const coverage: ScanCoverage = {
      roots,
      maxIterations: effectiveMaxIterations,
      processedDirectories,
      discoveredDirectories,
      scannedPathCount: scannedPathKeys.length,
      remainingQueue: queue.length,
      reachedIterationCap: queue.length > 0 && processedDirectories >= effectiveMaxIterations,
      maxItemsPerDirectory,
      diveSummaryDepth,
      folderSizeDepth
    };

    const reports = await writeReports(finalPlan, map.potentialSavings, map.topFiles, scannedPathKeys, coverage);
    emit(`Analysis complete. Report: ${reports.reportPath}`);
    emit(`Analysis complete. Script: ${reports.scriptPath}`);

    return {
      ...reports,
      logPath: runtimeLogPath
    };
  } finally {
    clearInterval(heartbeatTimer);
  }

  async function maybeCheckpoint(force: boolean): Promise<void> {
    const now = Date.now();
    const dueByCount = pendingSaveCount >= saveEveryDirectories;
    const dueByTime = now - lastSaveAt >= 15000;

    if (!force && !dueByCount && !dueByTime) {
      return;
    }

    await saveMap(map);
    pendingSaveCount = 0;
    lastSaveAt = Date.now();
  }

  function logProgress(event: string, force: boolean): void {
    const now = Date.now();
    const dueByCount = processedDirectories > 0 && processedDirectories % progressLogEveryDirectories === 0;
    const dueByTime = now - lastProgressLogAt >= progressLogIntervalMs;
    if (!force && !dueByCount && !dueByTime) {
      return;
    }

    emit(makeProgressLine(event));

    lastProgressLogAt = now;
  }

  function makeProgressLine(event: string): string {
    const now = Date.now();

    const elapsedMs = now - runStartedAt;
    const avgMsPerDir = processedDirectories > 0 ? elapsedMs / processedDirectories : 0;
    const etaSeconds = avgMsPerDir > 0 ? Math.round((queue.length * avgMsPerDir) / 1000) : 0;
    const etaText = queue.length > 0 ? `${etaSeconds}s` : "0s";

    return (
      `[Progress] ${event} | processed=${processedDirectories}/${effectiveMaxIterations} ` +
      `queue=${queue.length} discovered=${discoveredDirectories} scannedPaths=${Object.keys(map.scannedPaths).length} ` +
      `savings=${map.potentialSavings.length} topFiles=${map.topFiles.length} elapsed=${formatElapsed(elapsedMs)} eta=${etaText}`
    );
  }

  function rebalanceQueuePressure(): void {
    const remainingBudget = Math.max(1, effectiveMaxIterations - processedDirectories);
    const pressureThreshold = Math.ceil(remainingBudget * Math.max(1, queuePressureMultiplier));
    const relaxThreshold = Math.floor(remainingBudget * Math.max(0.1, queueRelaxMultiplier));

    if (queue.length > pressureThreshold && currentQueueFanout > minDirsToQueuePerScan) {
      const nextFanout = Math.max(minDirsToQueuePerScan, currentQueueFanout - 4);
      if (nextFanout !== currentQueueFanout) {
        currentQueueFanout = nextFanout;
        emit(
          `[Adaptive] High queue pressure detected (queue=${queue.length}, budget=${remainingBudget}). ` +
            `Reducing fanout to ${currentQueueFanout}.`
        );
      }
      return;
    }

    if (queue.length < relaxThreshold && currentQueueFanout < maxDirsToQueuePerScan) {
      const nextFanout = Math.min(maxDirsToQueuePerScan, currentQueueFanout + 2);
      if (nextFanout !== currentQueueFanout) {
        currentQueueFanout = nextFanout;
        emit(
          `[Adaptive] Queue pressure relaxed (queue=${queue.length}, budget=${remainingBudget}). ` +
            `Increasing fanout to ${currentQueueFanout}.`
        );
      }
    }
  }

  function shouldExtendIterationBudgetForCoverage(): boolean {
    // Hard limit: don't go beyond 5x the initial budget
    const absoluteHardCap = maxIterations * 5;
    if (effectiveMaxIterations >= absoluteHardCap) {
      emit(`[Coverage] Reached hard cap (${absoluteHardCap}). No more extensions.`);
      return false;
    }

    // If we haven't discovered much yet, extend to gather more data
    if (discoveredDirectories < 100) {
      return true;
    }

    // Calculate current coverage
    const currentCoverage = (processedDirectories / discoveredDirectories) * 100;

    // Extend if we're below target AND queue has items AND we have found useful data
    const belowTarget = currentCoverage < targetCoverage;
    const hasQueue = queue.length > 0;
    const hasData = map.potentialSavings.length > 5 || map.topFiles.length > 10;

    if (belowTarget && hasQueue && hasData) {
      console.debug(
        `[Coverage Check] current=${currentCoverage.toFixed(1)}% target=${targetCoverage}% ` +
        `processed=${processedDirectories}/${discoveredDirectories} → extending`
      );
      return true;
    }

    if (!belowTarget) {
      emit(`[Coverage] Target coverage (${targetCoverage}%) reached at ${currentCoverage.toFixed(1)}%. Stopping.`);
    }

    return false;
  }

  function emit(message: string): void {
    console.log(message);
    void appendRuntimeLog(message).catch(() => undefined);
  }

  async function appendRuntimeLog(message: string): Promise<void> {
    const line = `[${new Date().toISOString()}] ${message}\n`;
    await fs.appendFile(runtimeLogPath, line, "utf8");
  }
}

function formatElapsed(milliseconds: number): string {
  const totalSeconds = Math.max(0, Math.floor(milliseconds / 1000));
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;

  if (hours > 0) {
    return `${hours}h ${minutes}m ${seconds}s`;
  }

  if (minutes > 0) {
    return `${minutes}m ${seconds}s`;
  }

  return `${seconds}s`;
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
  topFiles: DiskNode[],
  scannedPaths: string[],
  coverage: ScanCoverage
): Promise<{ reportPath: string; scriptPath: string }> {
  const reportsDir = path.join(process.cwd(), "reports");
  await fs.ensureDir(reportsDir);
  await archiveTopLevelFilesToPrev(reportsDir);

  const stamp = new Date().toISOString().replace(/[.:]/g, "-");
  const reportPath = path.join(reportsDir, `cleanup-plan-${stamp}.html`);
  const scriptPath = path.join(reportsDir, `zero-risk-cleanup-${stamp}.ps1`);

  const rows = buildReportRows(finalPlan.semanticActions, potentialSavings, topFiles).sort((a, b) => b.sizeGB - a.sizeGB);
  const targetDrive = resolveTargetDrive(scannedPaths);
  const totalSizeGb = rows.reduce((sum, row) => sum + row.sizeGB, 0);
  const potentialRecoveryGb = rows
    .filter((row) => row.action === "PURGE" || row.action === "OFFLOAD")
    .reduce((sum, row) => sum + row.sizeGB, 0);
  const freeSpaceInfo = await getDriveSpaceInfo(targetDrive);
  const projectedFreeGb = freeSpaceInfo.freeGb !== null ? freeSpaceInfo.freeGb + potentialRecoveryGb : null;
  const coverageClass = coverage.reachedIterationCap ? "#fff3cd" : "#d4edda";
  const coverageBorder = coverage.reachedIterationCap ? "#ffeeba" : "#c3e6cb";
  const coverageStatus = coverage.reachedIterationCap
    ? "Partial scan: iteration cap reached before queue exhaustion."
    : "Complete queue scan within configured bounds.";
  const rootsLabel = coverage.roots.join(", ");

  const tableRows = rows.length
    ? rows
        .map((row) => {
          const style = actionStyle(row.action);
          return [
            `<tr style="background-color: ${style.background}; border-bottom: 1px solid ${style.border};">`,
            `  <td style="padding: 10px;">${escapeHtml(row.path)}</td>`,
            `  <td style="padding: 10px;">${formatSizeFromGb(row.sizeGB)}</td>`,
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
    `  <div style="background-color: ${coverageClass}; border: 1px solid ${coverageBorder}; padding: 10px; margin: 0 0 14px;">`,
    `    <p style="margin: 0 0 6px;"><strong>Scan Coverage:</strong> ${escapeHtml(coverageStatus)}</p>`,
    `    <p style="margin: 0 0 4px;"><strong>Roots:</strong> ${escapeHtml(rootsLabel)}</p>`,
    `    <p style="margin: 0 0 4px;"><strong>Directories Processed:</strong> ${coverage.processedDirectories} / ${coverage.maxIterations} (iteration cap)</p>`,
    `    <p style="margin: 0 0 4px;"><strong>Directories Discovered:</strong> ${coverage.discoveredDirectories}</p>`,
    `    <p style="margin: 0 0 4px;"><strong>Paths Snapshotted:</strong> ${coverage.scannedPathCount}</p>`,
    `    <p style="margin: 0 0 4px;"><strong>Remaining Queue:</strong> ${coverage.remainingQueue}</p>`,
    `    <p style="margin: 0;"><strong>Scan Bounds:</strong> maxItemsPerDirectory=${coverage.maxItemsPerDirectory}, diveDepth=${coverage.diveSummaryDepth}, folderSizeDepth=${coverage.folderSizeDepth}</p>`,
    `  </div>`,
    `  <h3 style="color: #1f2933; margin: 0 0 8px;">Space Recovery Projection</h3>`,
    `  <p style="margin: 0 0 4px;"><strong>Current Free Space:</strong> ${formatSizeFromGb(freeSpaceInfo.freeGb)}</p>`,
    `  <p style="margin: 0 0 4px;"><strong>Projected Free Space (after Purge & Offload):</strong> ${formatSizeFromGb(projectedFreeGb)}</p>`,
    `  <p style="margin: 0 0 12px;"><strong>Total Recovery Potential:</strong> ${formatSizeFromGb(potentialRecoveryGb)}</p>`,
    `  <p style="margin: 0 0 4px;"><strong>Target Drive:</strong> ${escapeHtml(targetDrive)}</p>`,
    `  <p style="margin: 0 0 4px;"><strong>Total Size:</strong> ${formatSizeFromGb(totalSizeGb)}</p>`,
    `  <p style="margin: 0 0 16px;"><strong>Potential Space Recovery:</strong> ${formatSizeFromGb(potentialRecoveryGb)}</p>`,
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

async function archiveTopLevelFilesToPrev(targetDir: string): Promise<void> {
  const previousDir = path.join(targetDir, "prev");
  await fs.ensureDir(previousDir);

  const entries = await fs.readdir(targetDir, { withFileTypes: true });
  const filesToArchive = entries
    .filter((entry) => entry.isFile())
    .map((entry) => entry.name);

  await Promise.all(
    filesToArchive.map((name) =>
      fs.move(path.join(targetDir, name), path.join(previousDir, name), { overwrite: true })
    )
  );
}

interface ReportRow {
  path: string;
  sizeGB: number;
  category: string;
  action: "PURGE" | "OFFLOAD" | "COMPRESS" | "RETAIN";
  justification: string;
}

function buildReportRows(semanticActions: string[], potentialSavings: PotentialSaving[], topFiles: DiskNode[]): ReportRow[] {
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
    const pathValue = normalizeWindowsPath(item.path);
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

  const compressRows = buildCompressionRows(topFiles, seen);
  rows.push(...compressRows);

  return rows;
}

function parseSemanticAction(item: string): ReportRow | null {
  const match = item.match(/^\[(PURGE|OFFLOAD|COMPRESS|RETAIN)\]\s+(.+?)\s+\(([\d.]+)\s+GB\)\s+(.+)$/);
  if (!match) {
    return null;
  }

  const action = match[1] as "PURGE" | "OFFLOAD" | "COMPRESS" | "RETAIN";
  const pathValue = normalizeWindowsPath(match[2].trim());
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

function categoryForAction(action: "PURGE" | "OFFLOAD" | "COMPRESS" | "RETAIN"): string {
  if (action === "PURGE") {
    return "Temp/Cache/Redundant";
  }
  if (action === "OFFLOAD") {
    return "Low-Frequency Cluster";
  }
  if (action === "COMPRESS") {
    return "Occasional-Use Cluster";
  }
  return "High-Frequency Cluster";
}

function actionStyle(action: "PURGE" | "OFFLOAD" | "COMPRESS" | "RETAIN"): { background: string; border: string } {
  if (action === "PURGE") {
    return { background: "#ffcccc", border: "#f5b4b4" };
  }
  if (action === "OFFLOAD") {
    return { background: "#fff3cd", border: "#ffeeba" };
  }
  if (action === "COMPRESS") {
    return { background: "#d0e7ff", border: "#b9dbff" };
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

function normalizeWindowsPath(rawPath: string): string {
  return rawPath.replace(/\//g, "\\");
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

function isLiteralWindowsPath(value: string): boolean {
  return /^[A-Za-z]:\\/.test(value);
}

function buildCompressionRows(topFiles: DiskNode[], seen: Set<string>): ReportRow[] {
  const grouped = new Map<string, { sizeGB: number; days: number[] }>();
  const now = Date.now();

  for (const file of topFiles) {
    if (file.isDirectory) {
      continue;
    }

    const cluster = compressionClusterPath(file.path);
    if (!cluster) {
      continue;
    }

    const existing = grouped.get(cluster) ?? { sizeGB: 0, days: [] };
    existing.sizeGB += file.sizeGB;
    existing.days.push(daysSince(file.lastAccessedISO, now));
    grouped.set(cluster, existing);
  }

  const candidates = Array.from(grouped.entries())
    .map(([cluster, value]) => ({
      cluster,
      sizeGB: value.sizeGB,
      avgDays: Math.round(value.days.reduce((sum, day) => sum + day, 0) / Math.max(1, value.days.length))
    }))
    .filter((item) => item.sizeGB >= 5 && item.avgDays >= 30 && item.avgDays <= 240)
    .sort((a, b) => b.sizeGB - a.sizeGB)
    .slice(0, 8);

  const rows: ReportRow[] = [];
  for (const candidate of candidates) {
    const clusterPath = normalizeWindowsPath(candidate.cluster);
    const key = `COMPRESS|${clusterPath}`.toLowerCase();
    if (seen.has(key)) {
      continue;
    }

    seen.add(key);
    rows.push({
      path: clusterPath,
      sizeGB: Number(candidate.sizeGB.toFixed(3)),
      category: "Occasional-Use Cluster",
      action: "COMPRESS",
      justification:
        `This semantic cluster is large and intermittently used (average access age ~${candidate.avgDays} days). ` +
        `Compressing it into archival bundles can recover space while retaining local availability.`
    });
  }

  return rows;
}

function compressionClusterPath(filePath: string): string | null {
  const parts = filePath.split("\\").filter(Boolean);
  if (parts.length < 3) {
    return null;
  }

  if (parts[1]?.toLowerCase() === "users" && parts.length >= 5) {
    return `${parts[0]}\\${parts[1]}\\${parts[2]}\\${parts[3]}\\${parts[4]}`;
  }

  return `${parts[0]}\\${parts[1]}\\${parts[2]}`;
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

async function getDriveSpaceInfo(targetDrive: string): Promise<{ freeGb: number | null; totalGb: number | null }> {
  const normalized = targetDrive.match(/^[A-Za-z]:/) ? `${targetDrive}\\` : targetDrive;
  try {
    const stats = await statfs(normalized);
    const freeBytes = Number(stats.bavail) * Number(stats.bsize);
    const totalBytes = Number(stats.blocks) * Number(stats.bsize);
    if (!Number.isFinite(freeBytes) || !Number.isFinite(totalBytes) || totalBytes <= 0) {
      return { freeGb: null, totalGb: null };
    }

    return {
      freeGb: Number((freeBytes / (1024 ** 3)).toFixed(3)),
      totalGb: Number((totalBytes / (1024 ** 3)).toFixed(3))
    };
  } catch {
    return { freeGb: null, totalGb: null };
  }
}

function formatGb(value: number | null): string {
  if (value === null) {
    return "Unknown";
  }

  return `${value.toFixed(3)} GB`;
}

function formatSizeFromGb(valueGb: number | null): string {
  if (valueGb === null || !Number.isFinite(valueGb)) {
    return "Unknown";
  }

  const bytes = Math.max(0, valueGb * 1024 ** 3);
  if (bytes >= 1024 ** 4) {
    return `${(bytes / 1024 ** 4).toFixed(3)} TB`;
  }

  if (bytes >= 1024 ** 3) {
    return `${(bytes / 1024 ** 3).toFixed(3)} GB`;
  }

  if (bytes >= 1024 ** 2) {
    return `${(bytes / 1024 ** 2).toFixed(2)} MB`;
  }

  if (bytes >= 1024) {
    return `${(bytes / 1024).toFixed(2)} KB`;
  }

  return `${Math.round(bytes)} B`;
}
