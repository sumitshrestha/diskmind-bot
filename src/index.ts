import "dotenv/config";
import { createInterface } from "node:readline/promises";
import { stdin as input, stdout as output } from "node:process";
import { startAnalysis } from "./agent";
import { getDatabasePath } from "./database";
import { getDefaultRoots, getExistingRoots } from "./scanner";
import { getOllamaInventory } from "./llm";

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
    console.log("Scanning for available drives and Ollama models...");
    const [drives, inventory] = await Promise.all([
      getExistingRoots(getDefaultRoots()),
      getOllamaInventory()
    ]);

    if (drives.length === 0) {
      throw new Error("No readable drives found. Set DISKMIND_ROOTS to one or more accessible paths.");
    }

    console.log("Drive scan complete.");
    console.log("Detected drives:");
    drives.forEach((drive, index) => {
      console.log(`${index + 1}. ${drive}`);
    });

    console.log("Ollama model scan complete.");
    if (inventory.availableModels.length > 0) {
      console.log("Installed Ollama models:");
      inventory.availableModels.forEach((model, index) => {
        const isRunning = inventory.runningModels.includes(model) ? " (running)" : "";
        console.log(`${index + 1}. ${model}${isRunning}`);
      });
    } else {
      console.log("Installed Ollama models: none detected");
      console.log("Tip: run 'ollama pull llama3.1:8b' then retry.");
    }

    const defaultModel = process.env.DISKMIND_OLLAMA_MODEL ?? inventory.availableModels[0] ?? "llama3.1:8b";
    let chosenModel = defaultModel;
    if (inventory.availableModels.length > 0) {
      const modelAnswer = await rl.question(`Choose Ollama model number [1] or press Enter for ${defaultModel}: `);
      const modelIndex = Number.parseInt(modelAnswer.trim(), 10);
      if (!Number.isNaN(modelIndex) && inventory.availableModels[modelIndex - 1]) {
        chosenModel = inventory.availableModels[modelIndex - 1];
      }
    } else {
      const modelInput = await rl.question(`Ollama model to use [${defaultModel}]: `);
      chosenModel = modelInput.trim() || defaultModel;
    }
    process.env.DISKMIND_OLLAMA_MODEL = chosenModel;

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
