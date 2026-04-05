import * as fs from "fs-extra";
import * as path from "path";

const BYTES_PER_GB = 1024 ** 3;

// High-branching directories to skip for efficiency
const EXCLUDED_DIR_PATTERNS = [
  /[\\\/]node_modules$/i,                      // npm packages
  /[\\\/]\.git[\\\/]objects$/i,               // git objects
  /[\\\/]\.git[\\\/]info$/i,                  // git refs
  /[\\\/]__pycache__$/i,                        // Python bytecode
  /[\\\/]\.pnpm-store$/i,                      // pnpm cache
  /[\\\/]pnpm-store[\\\/]v\d+[\\\/]files$/i, // pnpm versioned cache
  /[\\\/]PIP_CACHE_DIR[\\\/]/i,              // pip cache
  /[\\\/]UV_CACHE_DIR[\\\/]/i,               // uv cache
  /[\\\/]TORCH_HOME[\\\/]/i,                 // PyTorch cache
  /[\\\/]npm-cache[\\\/]/i,                  // npm cache
  /[\\\/]\.yarn[\\\/]cache$/i,              // yarn cache
  /[\\\/]\.cache[\\\/]/i,                    // generic cache
  /[\\\/]\.gradle[\\\/]caches$/i,           // gradle cache
  /[\\\/]\.m2[\\\/]repository$/i,           // maven cache
];

export interface DiskNode {
  path: string;
  name: string;
  sizeGB: number;
  isDirectory: boolean;
  extension?: string;
  lastModifiedISO?: string;
  lastAccessedISO?: string;
}

export interface ScanOptions {
  depth?: number;
  maxItems?: number;
  folderSizeDepth?: number;
  fileLimitForTopScan?: number;
  statConcurrency?: number;
  folderSizeMaxEntries?: number;
}

export interface LargestFilesOptions {
  maxDepth?: number;
  maxFilesVisited?: number;
  maxEntriesPerDirectory?: number;
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
  const statConcurrency = options.statConcurrency ?? 24;
  const folderSizeMaxEntries = options.folderSizeMaxEntries ?? 2000;

  const entries = await readDirLimited(dirPath, maxItems);
  if (entries.length === 0) {
    return {
      scannedPath: dirPath,
      scannedAtISO: new Date().toISOString(),
      nodes: []
    };
  }

  const nodes = await mapLimit(entries, statConcurrency, async (entry): Promise<DiskNode | null> => {
    const fullPath = path.join(dirPath, entry);

    try {
      const stats = await fs.lstat(fullPath);
      if (stats.isSymbolicLink()) {
        return null;
      }

      const isDirectory = stats.isDirectory();
      const sizeBytes = isDirectory
        ? await calculateFolderSize(fullPath, folderSizeDepth, folderSizeMaxEntries)
        : stats.size;

      return {
        path: fullPath,
        name: entry,
        sizeGB: sizeBytes / BYTES_PER_GB,
        isDirectory,
        extension: isDirectory ? undefined : path.extname(entry).toLowerCase() || undefined,
        lastModifiedISO: stats.mtime.toISOString(),
        lastAccessedISO: stats.atime.toISOString()
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

export async function listLargestFiles(
  rootPath: string,
  limit = 50,
  options: LargestFilesOptions = {}
): Promise<DiskNode[]> {
  const collected: DiskNode[] = [];
  const state: WalkState = {
    filesVisited: 0,
    maxFilesVisited: options.maxFilesVisited,
    stop: false
  };

  await walkFiles(rootPath, async (filePath, stats) => {
    collected.push({
      path: filePath,
      name: path.basename(filePath),
      sizeGB: stats.size / BYTES_PER_GB,
      isDirectory: false,
      extension: path.extname(filePath).toLowerCase() || undefined,
      lastModifiedISO: stats.mtime.toISOString(),
      lastAccessedISO: stats.atime.toISOString()
    });
  }, options.maxDepth ?? Number.POSITIVE_INFINITY, state, 0, options.maxEntriesPerDirectory);

  return collected.sort((a, b) => b.sizeGB - a.sizeGB).slice(0, limit);
}

export function shouldExcludeDirectory(dirPath: string): boolean {
  const normalized = dirPath.replace(/\//g, "\\");
  return EXCLUDED_DIR_PATTERNS.some((pattern) => pattern.test(normalized));
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

async function calculateFolderSize(dirPath: string, depth: number, maxEntriesPerDirectory: number): Promise<number> {
  if (depth <= 0) {
    return 0;
  }

  let total = 0;
  const stack: Array<{ path: string; depth: number }> = [{ path: dirPath, depth }];

  while (stack.length > 0) {
    const current = stack.pop();
    if (!current || current.depth <= 0) {
      continue;
    }

    const entries = await readDirLimited(current.path, maxEntriesPerDirectory);
    for (const entry of entries) {
      const fullPath = path.join(current.path, entry);
      try {
        const stats = await fs.lstat(fullPath);
        if (stats.isSymbolicLink()) {
          continue;
        }

        if (stats.isDirectory()) {
          stack.push({ path: fullPath, depth: current.depth - 1 });
        } else {
          total += stats.size;
        }
      } catch {
        continue;
      }
    }
  }

  return total;
}

interface WalkState {
  filesVisited: number;
  maxFilesVisited?: number;
  stop: boolean;
}

async function walkFiles(
  rootPath: string,
  onFile: (filePath: string, stats: fs.Stats) => Promise<void>,
  maxDepth: number,
  state: WalkState,
  depth: number,
  maxEntriesPerDirectory?: number
): Promise<void> {
  if (state.stop || depth > maxDepth) {
    return;
  }

  const entries = await readDirLimited(rootPath, maxEntriesPerDirectory);

  for (const entry of entries) {
    if (state.stop) {
      return;
    }

    const fullPath = path.join(rootPath, entry);

    try {
      const stats = await fs.lstat(fullPath);
      if (stats.isSymbolicLink()) {
        continue;
      }

      if (stats.isDirectory()) {
        await walkFiles(fullPath, onFile, maxDepth, state, depth + 1, maxEntriesPerDirectory);
      } else {
        state.filesVisited += 1;
        await onFile(fullPath, stats);
        if (state.maxFilesVisited && state.filesVisited >= state.maxFilesVisited) {
          state.stop = true;
          return;
        }
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

async function readDirLimited(dirPath: string, maxEntries?: number): Promise<string[]> {
  const entries: string[] = [];
  const limit = maxEntries && maxEntries > 0 ? maxEntries : Number.POSITIVE_INFINITY;

  let dir: fs.Dir | null = null;
  try {
    dir = await fs.opendir(dirPath);

    while (entries.length < limit) {
      const entry = await dir.read();
      if (!entry) {
        break;
      }

      entries.push(entry.name);
    }
  } catch {
    return [];
  } finally {
    await dir?.close().catch(() => undefined);
  }

  return entries;
}
