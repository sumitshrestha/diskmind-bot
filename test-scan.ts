import "dotenv/config";
import { startAnalysis } from "./src/agent";
import { getDatabasePath } from "./src/database";

async function testScan(): Promise<void> {
  const startedAt = Date.now();
  
  try {
    console.log("Starting test scan with new exclusions and validation...");
    const result = await startAnalysis({
      maxIterations: Number(process.env.DISKMIND_MAX_ITERATIONS ?? 2000),
      rootSummaryDepth: 1,
      diveSummaryDepth: 1,
      topFilesLimit: 50,
      roots: ["E:\\"]
    });

    console.log("✓ Scan completed successfully.");
    console.log(`Map database: ${getDatabasePath()}`);
    console.log(`Runtime log: ${result.logPath}`);
    console.log(`Cleanup report: ${result.reportPath}`);
    console.log(`PowerShell script: ${result.scriptPath}`);
    const elapsed = Math.floor((Date.now() - startedAt) / 1000);
    console.log(`Total time: ${elapsed}s`);
  } catch (error) {
    console.error("✗ Test scan failed:", error instanceof Error ? error.message : error);
    process.exitCode = 1;
  }
}

void testScan();
