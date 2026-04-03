import * as fs from "fs-extra";
import * as path from "path";
import { DirectorySnapshot, DiskNode } from "./scanner";

export interface PotentialSaving {
  path: string;
  sizeGB: number;
  reason: string;
  riskHint: "zero" | "medium" | "high";
}

export interface DiskMindMap {
  scannedPaths: Record<string, DirectorySnapshot>;
  potentialSavings: PotentialSaving[];
  topFiles: DiskNode[];
  updatedAtISO: string;
}

const DATA_DIR = path.join(process.cwd(), "data");
const DB_PATH = path.join(DATA_DIR, "diskmind-map.json");

export async function loadMap(): Promise<DiskMindMap> {
  await fs.ensureDir(DATA_DIR);

  if (!(await fs.pathExists(DB_PATH))) {
    const initial: DiskMindMap = {
      scannedPaths: {},
      potentialSavings: [],
      topFiles: [],
      updatedAtISO: new Date().toISOString()
    };
    await fs.writeJson(DB_PATH, initial, { spaces: 2 });
    return initial;
  }

  return fs.readJson(DB_PATH) as Promise<DiskMindMap>;
}

export async function saveMap(map: DiskMindMap): Promise<void> {
  map.updatedAtISO = new Date().toISOString();
  await fs.ensureDir(DATA_DIR);
  await fs.writeJson(DB_PATH, map, { spaces: 2 });
}

export function updateSnapshot(map: DiskMindMap, snapshot: DirectorySnapshot): DiskMindMap {
  map.scannedPaths[snapshot.scannedPath] = snapshot;
  map.updatedAtISO = new Date().toISOString();
  return map;
}

export function mergePotentialSavings(map: DiskMindMap, savings: PotentialSaving[]): DiskMindMap {
  for (const saving of savings) {
    const existing = map.potentialSavings.find((item) => item.path.toLowerCase() === saving.path.toLowerCase());
    if (existing) {
      existing.sizeGB = Math.max(existing.sizeGB, saving.sizeGB);
      existing.reason = saving.reason;
      existing.riskHint = saving.riskHint;
    } else {
      map.potentialSavings.push(saving);
    }
  }

  map.potentialSavings.sort((a, b) => b.sizeGB - a.sizeGB);
  map.updatedAtISO = new Date().toISOString();
  return map;
}

export function mergeTopFiles(map: DiskMindMap, files: DiskNode[], limit = 50): DiskMindMap {
  for (const file of files) {
    if (file.isDirectory) {
      continue;
    }

    const existing = map.topFiles.find((item) => item.path.toLowerCase() === file.path.toLowerCase());
    if (existing) {
      existing.sizeGB = Math.max(existing.sizeGB, file.sizeGB);
      existing.lastModifiedISO = file.lastModifiedISO;
      existing.lastAccessedISO = file.lastAccessedISO;
      existing.extension = file.extension;
    } else {
      map.topFiles.push(file);
    }
  }

  map.topFiles = map.topFiles.sort((a, b) => b.sizeGB - a.sizeGB).slice(0, limit);
  map.updatedAtISO = new Date().toISOString();
  return map;
}

export function getDatabasePath(): string {
  return DB_PATH;
}
