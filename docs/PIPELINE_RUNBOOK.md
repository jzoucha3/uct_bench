# Pipeline Runbook

This document covers the full local workflow for getting the pipeline running:

1. Clone the repository
2. Create a Python environment
3. Install dependencies
4. Configure credentials and local paths
5. Run a dry check
6. Run a real pipeline case
7. Find the outputs written to disk

The examples below use the `scripts/run_pipeline_demo.py` entry point because it is the most direct way to run the pipeline end to end from the terminal.

## Related Documentation

Use this runbook for execution. Use the linked docs below when you need deeper detail on a specific part of the workflow:

- [Documentation Index](/home/joey/jzouca_UCTBench_minimal/docs/README.md)
- [Pipeline System Guide](/home/joey/jzouca_UCTBench_minimal/docs/PIPELINE_SYSTEM_GUIDE.md)
- [Missingness, Preprocessing, and Simulation](/home/joey/jzouca_UCTBench_minimal/docs/MISSINGNESS_PREPROCESSING_AND_SIMULATION.md)
- [Result Artifacts Guide](/home/joey/jzouca_UCTBench_minimal/docs/RESULT_ARTIFACTS_GUIDE.md)
- [Outputs and Database Inspection](/home/joey/jzouca_UCTBench_minimal/docs/OUTPUTS_AND_DATABASE_INSPECTION.md)

## What the Demo Runner Does

`scripts/run_pipeline_demo.py`:

- loads environment variables from `.env`
- resolves UDL credentials
- creates a dataset record in the configured database
- runs one of the two pipeline modes:
  - `standard`
  - `statevector-first`
- persists generated data and artifacts
- writes a JSON run report to `reports/pipeline_runs/`

For the most reliable local path today, use `--pipeline-mode statevector-first` with DuckDB.

## Prerequisites

Required:

- Python 3.12 or newer
- a working UDL credential:
  - `UDL_TOKEN`, or
  - `UDL_USERNAME` and `UDL_PASSWORD`

Optional:

- `ESA_TOKEN` for supplemental enrichment
- Node.js and npm if you also want to run the frontend

The repository already includes an `orekit-data-main/` directory. The default `OREKIT_DATA_PATH=./orekit-data-main` in `.env.example` is valid for a normal clone.

## 1. Clone the Repository

If you do not already have the code locally:

```bash
git clone <your-repo-url> jzouca_UCTBench_minimal
cd jzouca_UCTBench_minimal
```

If you already have a clone, just `cd` into it.

## 2. Create and Activate a Python Environment

Create a local virtual environment in the repo:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
```

If your machine uses `python3` for Python 3.12, this is also fine:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

## 3. Install Python Dependencies

Install the package in editable mode:

```bash
python -m pip install --upgrade pip
python -m pip install -e .
```

If you also want test tooling:

```bash
python -m pip install -e .[dev]
```

## 4. Configure Environment Variables

Copy the example file:

```bash
cp .env.example .env
```

Edit `.env` and set at least the UDL credentials section.

Minimum required configuration:

```dotenv
UDL_TOKEN=your_real_udl_token
OREKIT_DATA_PATH=./orekit-data-main
DATABASE_BACKEND=duckdb
```

Alternative credential path:

```dotenv
UDL_USERNAME=your_udl_username
UDL_PASSWORD=your_udl_password
OREKIT_DATA_PATH=./orekit-data-main
DATABASE_BACKEND=duckdb
```

Notes:

- `UDL_TOKEN` is preferred if you already have one.
- If `UDL_TOKEN` is not set, the script will try to generate one from `UDL_USERNAME` and `UDL_PASSWORD`.
- DuckDB is the simplest local backend and is the recommended choice for local validation.
- If `DATABASE_PATH` is not set, the default DuckDB location is `data/database/uct_benchmark.duckdb`.

## 5. Confirm the Runner Loads Correctly

Before making live API calls, run a dry check:

```bash
python scripts/run_pipeline_demo.py --dry-run
```

This prints the resolved config and confirms the script imports cleanly without calling UDL.

## 6. Run a Real Pipeline Case

The demo script builds a date window from:

- `--timeframe`
- `--timeunit`
- `--end-offset-days`

By default, it does not accept explicit `--start-date` and `--end-date` arguments from the CLI. Instead:

- `end_date = today - end_offset_days`
- `start_date = end_date - timeframe`

Example:

- `--timeframe 1 --timeunit days --end-offset-days 1`
- queries the previous full UTC day

### Recommended Local Flags

For local runs, these options keep the behavior predictable:

```bash
--pipeline-mode statevector-first
--db-path /tmp/uct_benchmark_demo.duckdb
```

Why:

- `statevector-first` is currently the most reliable path for direct terminal validation
- a per-run `/tmp` database avoids file locking or collisions with a shared local DuckDB file

### Case A: Date-Range Run Over the Default Satellite Pool

This is the best example of a "date-range" run without targeting a specific satellite.

It uses the requested time window but does not pass `--satellites`. In that case, the pipeline samples from the internal `DEFAULT_SATELLITES` list, capped by `--object-count`.

```bash
python scripts/run_pipeline_demo.py \
  --pipeline-mode statevector-first \
  --db-path /tmp/uct_benchmark_range_case.duckdb \
  --tier T2 \
  --regime LEO \
  --object-count 3 \
  --timeframe 1 \
  --timeunit days \
  --end-offset-days 1 \
  --search-strategy auto
```

What this does:

- runs the statevector-first pipeline
- builds a one-day window ending one day before the current UTC date
- chooses up to 3 satellites from the default calibration list
- pulls UDL data for that window
- performs inspection, preprocessing, and any tier-driven routing
- writes results to `/tmp/uct_benchmark_range_case.duckdb`

Important:

- this is not "all satellites in that date range"
- it is a bounded sample from the default internal satellite list

### Case B: Simpler Single-Satellite Run

This is the simplest real validation case and the easiest one to debug.

The example below targets the ISS (`25544`):

```bash
python scripts/run_pipeline_demo.py \
  --pipeline-mode statevector-first \
  --db-path /tmp/uct_benchmark_iss_case.duckdb \
  --tier T2 \
  --regime LEO \
  --object-count 1 \
  --timeframe 1 \
  --timeunit days \
  --end-offset-days 1 \
  --search-strategy auto \
  --satellites 25544
```

What this changes from Case A:

- `--satellites 25544` forces a specific NORAD ID
- `--object-count 1` keeps the requested scope aligned with that single target

## 7. Understanding the Console Output

The demo runner prints four major sections:

1. `SECTION 1: PIPELINE CONFIGURATION`
2. `SECTION 2: CREDENTIAL RESOLUTION`
3. `SECTION 3: DATASET RECORD CREATION`
4. `SECTION 4-11: ORCHESTRATED EXECUTION`

If the run completes, it ends with a `SUMMARY` block showing:

- dataset ID
- resolved search strategy
- observation count
- satellite count returned
- runtime window
- report path

## 8. What the Pipeline Actually Does During a Run

At a high level, a successful run can include:

1. Data acquisition from UDL
2. Missingness inspection
3. Deterministic preprocessing
4. Optional downsampling
5. Optional simulation
6. Evaluation artifact generation
7. Database persistence

### Missingness Inspection

The pipeline inspects the returned dataframe and writes artifacts under:

- `reports/missingness/<run-name>/`

Typical outputs:

- `missingness_report.json`
- `missingness_heatmap.png`
- `missingness_indicator_corr_heatmap.png`

### Preprocessing

Preprocessing may:

- assign or repair `trackId`
- flag sparse satellites
- fill deterministic fields such as range-derived values when sufficient inputs exist

### Downsampling

Downsampling only runs when it is explicitly enabled from the CLI:

```bash
--enable-downsampling
```

Useful companion flags:

```bash
--target-coverage 0.05
--target-gap 2.0
--max-obs-per-sat 30
```

If `Downsampling Metadata` is `null` in the run report, then downsampling did not run.

### Simulation

Simulation can run in two ways:

1. Explicitly:

```bash
--enable-simulation
```

2. Automatically:

- some tiers and missingness conditions can auto-enable simulation when the data is sparse or high-missingness

If simulation runs but `simulated_count` is `0`, that means the step executed but did not add synthetic observations for that dataset.

## 9. Where the Outputs Go

### Run Report

Every run writes a JSON report to:

- `reports/pipeline_runs/run-<timestamp>.json`

This is the best first place to inspect what happened.

### Database

If you pass `--db-path`, the DuckDB database is written to that location.

Example:

- `/tmp/uct_benchmark_iss_case.duckdb`

If you do not pass `--db-path` and are using DuckDB, the default is:

- `data/database/uct_benchmark.duckdb`

### Evaluation Artifacts

Evaluation artifacts are written under:

- `reports/evaluation/<run-name>/`

### Missingness Artifacts

Missingness artifacts are written under:

- `reports/missingness/<run-name>/`

## 10. How to Open the Run Report

From the repository root:

```bash
less reports/pipeline_runs/run-<timestamp>.json
```

Or pretty-print it:

```bash
python -m json.tool reports/pipeline_runs/run-<timestamp>.json
```

## 11. Common Local Troubleshooting

### Missing UDL Credentials

Symptom:

- the runner stops before execution and reports missing credentials

Fix:

- set `UDL_TOKEN` in `.env`, or
- set `UDL_USERNAME` and `UDL_PASSWORD`

### DuckDB Locking or Unexpected State

Symptom:

- local DB conflicts between runs

Fix:

- pass a unique `--db-path` under `/tmp`

Example:

```bash
--db-path /tmp/uct_benchmark_run_01.duckdb
```

### Downsampling Did Not Happen

Symptom:

- the run completed but the report shows `"Downsampling Metadata": null`

Fix:

- add `--enable-downsampling`

### Simulation Ran but Added Zero Synthetic Rows

Symptom:

- the report shows successful simulation but `simulated_count: 0`

Meaning:

- the simulation stage executed, but the dataset did not contain usable gaps or enough conditions to synthesize new observations

### Tkinter `Image.__del__` Noise at Exit

Symptom:

- a traceback mentioning `Image.__del__` and `main thread is not in main loop` appears after the summary

Meaning:

- the pipeline already finished
- this is a Python shutdown-time GUI cleanup issue, not the main pipeline failing

## 12. Optional Frontend and Backend Setup

The pipeline can run without the web app, but if you want the full stack:

Backend:

```bash
source .venv/bin/activate
uvicorn backend_api.main:app --reload --port 8000
```

Frontend:

```bash
cd frontend
npm install
npm run dev
```

Endpoints:

- frontend: `http://localhost:5173`
- backend: `http://localhost:8000`

## 13. Recommended First Validation Sequence

Use this order when bringing up a fresh clone:

1. `python scripts/run_pipeline_demo.py --dry-run`
2. Run the single-satellite example with `--satellites 25544`
3. Open the generated JSON report
4. Run the date-range example without `--satellites`
5. Inspect the new DuckDB file and artifact folders
