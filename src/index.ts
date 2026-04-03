import "dotenv/config";
import { createInterface } from "node:readline/promises";
import { stdin as input, stdout as output } from "node:process";
import { startAnalysis } from "./agent";
import { getDatabasePath } from "./database";
import { getDefaultRoots, getExistingRoots } from "./scanner";

async function main(): Promise<void> {
  try {
    console.log("DiskMind starting in read-only mode...");
    const startupConfig = await askStartupOptions();

    const result = await startAnalysis({
      maxIterations: Number(process.env.DISKMIND_MAX_ITERATIONS ?? 10),
      rootSummaryDepth: Number(process.env.DISKMIND_ROOT_DEPTH ?? 1),
      diveSummaryDepth: Number(process.env.DISKMIND_DIVE_DEPTH ?? 2),
      topFilesLimit: Number(process.env.DISKMIND_TOP_FILES_LIMIT ?? 50),
      roots: [startupConfig.selectedDrive]
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

async function askStartupOptions(): Promise<{ selectedDrive: string }> {
  const rl = createInterface({ input, output });

  try {
    const currentModel = process.env.DISKMIND_OLLAMA_MODEL ?? "llama3.1:8b";
    const modelInput = await rl.question(`Ollama model to use [${currentModel}]: `);
    const chosenModel = modelInput.trim() || currentModel;
    process.env.DISKMIND_OLLAMA_MODEL = chosenModel;

    const drives = await getExistingRoots(getDefaultRoots());
    if (drives.length === 0) {
      throw new Error("No readable drives found. Set DISKMIND_ROOTS to one or more accessible paths.");
    }

    console.log("Detected drives:");
    drives.forEach((drive, index) => {
      console.log(`${index + 1}. ${drive}`);
    });

    const driveAnswer = await rl.question("Choose drive number to scan [1]: ");
    const parsedIndex = Number.parseInt(driveAnswer.trim(), 10);
    const selectedIndex = Number.isNaN(parsedIndex) ? 1 : parsedIndex;
    const selectedDrive = drives[selectedIndex - 1] ?? drives[0];

    console.log(`Selected model: ${chosenModel}`);
    console.log(`Selected drive: ${selectedDrive}`);

    return { selectedDrive };
  } finally {
    rl.close();
  }
}

void main();
