# DiskMind Bot

Read-only, AI-assisted disk analysis bot for large file systems using a hierarchical chunking strategy.

DiskMind is designed to avoid context-window overload by scanning in stages:

1. Root overview (top-level folders/drives)
2. Guided deep-dive (LLM picks next path)
3. Global heavy hitters (top large files)
4. Final cleanup plan generation (script + checklist)

## Features

- Hierarchical chunking instead of full-drive dump
- Agentic loop (`DELVE`, `PLAN`, `REPORT`) driven by LLM output
- Read-only scanner (no automatic deletion)
- Global `PotentialSavings` and top-files memory across iterations
- Report generation to `.txt` and `.ps1` artifacts
- Supports `ollama` (default) or `openai`

## Project Structure

```text
/diskmind-bot
├── src/
│   ├── scanner.ts      # File system walker + summaries + top files
│   ├── llm.ts          # Ollama/OpenAI decision and plan wrapper
│   ├── agent.ts        # Agentic loop and chunk orchestration
│   ├── database.ts     # JSON-backed map/persistence
│   └── index.ts        # CLI entrypoint
├── config/
│   └── bloatware.json  # Known cache/log patterns and low-risk extensions
├── data/               # Generated state map (created at runtime)
├── reports/            # Generated cleanup plan + PS script
└── package.json
```

## Requirements

- Node.js 18+
- npm
- One LLM provider:
  - Local: Ollama running with a pulled model
  - Cloud: OpenAI API key

## Install

```bash
npm install
```

## Configuration

Create an `.env` file in repo root (optional but recommended):

```env
# LLM provider: ollama (default) or openai
DISKMIND_LLM_PROVIDER=ollama

# Ollama options
DISKMIND_OLLAMA_MODEL=llama3.1:8b
# OLLAMA_HOST=http://localhost:11434
DISKMIND_OLLAMA_RETRIES=2
DISKMIND_OLLAMA_RETRY_DELAY_MS=2000
# Optional VRAM override (GB) to scale prompt payload size, e.g. 16, 24, 48
# DISKMIND_OLLAMA_VRAM_GB=16

# OpenAI options (used only if provider=openai)
# OPENAI_API_KEY=your_key
# DISKMIND_OPENAI_MODEL=gpt-4o-mini

# Scanner options
DISKMIND_MAX_ITERATIONS=10
DISKMIND_ROOT_DEPTH=1
DISKMIND_DIVE_DEPTH=2
DISKMIND_TOP_FILES_LIMIT=50
DISKMIND_TOP_SCAN_DEPTH=4
DISKMIND_TOP_SCAN_MAX_FILES=25000

# Windows roots to scan (semicolon-separated)
# DISKMIND_ROOTS=C:\\;D:\\
```

Notes:

- If `DISKMIND_ROOTS` is not set on Windows, defaults are `C:\`, `D:\`, `E:\`.
- Only existing/readable roots are scanned.

## Usage

### Development

```bash
npm run dev
```

### Build and Run

```bash
npm run build
npm start
```

Expected console output includes:

- Path to map database JSON
- Path to generated cleanup report
- Path to generated PowerShell script

## How It Works

1. **Root Overview Chunk**
   - Scans available root drives/folders.
   - Stores summaries in map state.
2. **Deep Dive Chunk**
   - LLM receives current folder summary + visited paths + global top files.
   - LLM returns action JSON: `DELVE`, `PLAN`, or `REPORT`.
3. **Heavy Hitters Chunk**
   - Maintains global top large files list (`topFiles`).
4. **Final Plan**
   - Sends `potentialSavings` + `topFiles` + scanned paths to LLM.
   - Produces:
     - Zero-risk PowerShell script
     - Medium-risk checklist
     - High-risk checklist
     - Disclaimers

## Output Artifacts

Generated at runtime:

- `data/diskmind-map.json`
- `reports/cleanup-plan-<timestamp>.txt`
- `reports/zero-risk-cleanup-<timestamp>.ps1`

## Safety Model

DiskMind is intentionally read-only by design.

- Scanner uses read/stat operations only.
- No delete operations are executed by the app.
- Cleanup script is generated as a file for manual review and manual execution.
- Script output includes `-WhatIf` guidance.

## Knowledge Base

`config/bloatware.json` contains:

- `knownPatterns`: cache/temp/log-like path patterns
- `extensionsLowRisk`: file extensions considered low-risk candidates

Tune this file to match your environment and policies.

## Troubleshooting

### No readable roots found

Set `DISKMIND_ROOTS` explicitly to accessible paths.

### OpenAI provider error

If `DISKMIND_LLM_PROVIDER=openai`, make sure `OPENAI_API_KEY` is set.

### Slow scan on large volumes

Reduce scan scope and compute intensity:

- Lower `DISKMIND_MAX_ITERATIONS`
- Lower `DISKMIND_DIVE_DEPTH`
- Set specific `DISKMIND_ROOTS`
- Lower `DISKMIND_TOP_SCAN_DEPTH`
- Lower `DISKMIND_TOP_SCAN_MAX_FILES`

## Roadmap Ideas

- Add interactive CLI flags (`--roots`, `--max-iterations`, etc.)
- Add ignore rules for system/protected folders
- Add SQLite backend option for larger persistent maps
- Add deterministic pre-filtering before LLM calls

## Disclaimer

Always review generated recommendations and scripts before deleting anything. You are responsible for executing cleanup actions in your environment.
