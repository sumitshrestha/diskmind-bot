import "dotenv/config";
import { startAnalysis } from "./agent";
import { getDatabasePath } from "./database";

async function main(): Promise<void> {
  try {
    console.log("DiskMind starting in read-only mode...");
    const result = await startAnalysis({
      maxIterations: Number(process.env.DISKMIND_MAX_ITERATIONS ?? 10),
      rootSummaryDepth: Number(process.env.DISKMIND_ROOT_DEPTH ?? 1),
      diveSummaryDepth: Number(process.env.DISKMIND_DIVE_DEPTH ?? 2),
      topFilesLimit: Number(process.env.DISKMIND_TOP_FILES_LIMIT ?? 50)
    });

    console.log("DiskMind completed analysis.");
    console.log(`Map database: ${getDatabasePath()}`);
    console.log(`Cleanup report: ${result.reportPath}`);
    console.log(`PowerShell script (manual execution only): ${result.scriptPath}`);
  } catch (error) {
    console.error("DiskMind failed:", error instanceof Error ? error.message : error);
    process.exitCode = 1;
  }
}

void main();
