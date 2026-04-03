import * as fs from "fs-extra";
import * as path from "path";

const BYTES_PER_GB = 1024 ** 3;

export interface DiskNode {
  path: string;
  name: string;
  sizeGB: number;
  isDirectory: boolean;
  extension?: string;
  lastModifiedISO?: string;
}

export interface ScanOptions {
  depth?: number;
  maxItems?: number;
  folderSizeDepth?: number;
  fileLimitForTopScan?: number;
}

export interface DirectorySnapshot {
  scannedPath: string;
  scannedAtISO: string;
  nodes: DiskNode[];
}

export async function getDirectorySummary(dirPath: string, options: ScanOptions = {}): Promise<DirectorySnapshot> {
  const depth = options.depth ?? 1;
  const maxItems = options.maxItems ?? 250;
  const folderSizeDepth = options.folderSizeDepth ?? Math.max(1, depth);

  let entries: string[] = [];
  try {
    entries = await fs.readdir(dirPath);
  } catch {
    return {
      scannedPath: dirPath,
      scannedAtISO: new Date().toISOString(),
      nodes: []
    };
  }

  const limited = entries.slice(0, maxItems);
  const nodes = await mapLimit(limited, 16, async (entry): Promise<DiskNode | null> => {
    const fullPath = path.join(dirPath, entry);

    try {
      const stats = await fs.stat(fullPath);
      const isDirectory = stats.isDirectory();
      const sizeBytes = isDirectory
        ? await calculateFolderSize(fullPath, folderSizeDepth)
        : stats.size;

      return {
        path: fullPath,
        name: entry,
        sizeGB: Number((sizeBytes / BYTES_PER_GB).toFixed(3)),
        isDirectory,
        extension: isDirectory ? undefined : path.extname(entry).toLowerCase() || undefined,
        lastModifiedISO: stats.mtime.toISOString()
      };
    } catch {
      return null;
    }
  });

  return {
    scannedPath: dirPath,
    scannedAtISO: new Date().toISOString(),
    nodes: nodes.filter((node): node is DiskNode => Boolean(node)).sort((a, b) => b.sizeGB - a.sizeGB)
  };
}

export async function listLargestFiles(rootPath: string, limit = 50): Promise<DiskNode[]> {
  const collected: DiskNode[] = [];
  await walkFiles(rootPath, async (filePath, stats) => {
    collected.push({
      path: filePath,
      name: path.basename(filePath),
      sizeGB: Number((stats.size / BYTES_PER_GB).toFixed(3)),
      isDirectory: false,
      extension: path.extname(filePath).toLowerCase() || undefined,
      lastModifiedISO: stats.mtime.toISOString()
    });
  });

  return collected.sort((a, b) => b.sizeGB - a.sizeGB).slice(0, limit);
}

export function getDefaultRoots(): string[] {
  if (process.platform === "win32") {
    const configured = process.env.DISKMIND_ROOTS;
    if (configured) {
      return configured.split(";").map((item) => item.trim()).filter(Boolean);
    }

    return ["C:\\", "D:\\", "E:\\"];
  }

  return ["/"];
}

export async function getExistingRoots(candidates: string[]): Promise<string[]> {
  const checks = await Promise.all(
    candidates.map(async (candidate) => ({
      candidate,
      exists: await fs.pathExists(candidate)
    }))
  );

  return checks.filter((check) => check.exists).map((check) => check.candidate);
}

async function calculateFolderSize(dirPath: string, depth: number): Promise<number> {
  if (depth <= 0) {
    return 0;
  }

  let total = 0;
  let entries: string[] = [];

  try {
    entries = await fs.readdir(dirPath);
  } catch {
    return 0;
  }

  for (const entry of entries) {
    const fullPath = path.join(dirPath, entry);
    try {
      const stats = await fs.stat(fullPath);
      if (stats.isDirectory()) {
        total += await calculateFolderSize(fullPath, depth - 1);
      } else {
        total += stats.size;
      }
    } catch {
      continue;
    }
  }

  return total;
}

async function walkFiles(
  rootPath: string,
  onFile: (filePath: string, stats: fs.Stats) => Promise<void>
): Promise<void> {
  let entries: string[] = [];

  try {
    entries = await fs.readdir(rootPath);
  } catch {
    return;
  }

  for (const entry of entries) {
    const fullPath = path.join(rootPath, entry);

    try {
      const stats = await fs.stat(fullPath);
      if (stats.isDirectory()) {
        await walkFiles(fullPath, onFile);
      } else {
        await onFile(fullPath, stats);
      }
    } catch {
      continue;
    }
  }
}

async function mapLimit<TIn, TOut>(
  list: TIn[],
  concurrency: number,
  mapper: (item: TIn) => Promise<TOut>
): Promise<TOut[]> {
  const output: TOut[] = [];
  let index = 0;

  const workers = Array.from({ length: Math.min(concurrency, list.length) }).map(async () => {
    while (index < list.length) {
      const current = index;
      index += 1;
      output[current] = await mapper(list[current]);
    }
  });

  await Promise.all(workers);
  return output;
}
