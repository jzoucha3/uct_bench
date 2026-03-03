# jzouca_UCTBench_minimal

Minimal full-stack UCT Benchmark workspace.

This repository supports:

1. Pulling source data from UDL
2. Running the benchmark pipeline
3. Inspecting missingness, preprocessing, downsampling, and simulation behavior
4. Persisting outputs to DuckDB or PostgreSQL
5. Serving a backend API and demo frontend

## Documentation

- [Documentation Index](/home/joey/jzouca_UCTBench_minimal/docs/README.md)
- [Pipeline Runbook](/home/joey/jzouca_UCTBench_minimal/docs/PIPELINE_RUNBOOK.md)
- [Pipeline System Guide](/home/joey/jzouca_UCTBench_minimal/docs/PIPELINE_SYSTEM_GUIDE.md)
- [Missingness, Preprocessing, and Simulation](/home/joey/jzouca_UCTBench_minimal/docs/MISSINGNESS_PREPROCESSING_AND_SIMULATION.md)
- [Result Artifacts Guide](/home/joey/jzouca_UCTBench_minimal/docs/RESULT_ARTIFACTS_GUIDE.md)
- [Outputs and Database Inspection](/home/joey/jzouca_UCTBench_minimal/docs/OUTPUTS_AND_DATABASE_INSPECTION.md)

Use the documentation index as the landing page for the docs set.
Use the runbook for setup and terminal execution. Use the system guide for architecture and pipeline-stage behavior.
Use the method and artifact guides to understand why the pipeline chose a given path and how to inspect what it produced.

## Quick Start

Clone and enter the repository:

```bash
git clone <your-repo-url> jzouca_UCTBench_minimal
cd jzouca_UCTBench_minimal
```

Create a virtual environment and install dependencies:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e .
```

Create your local environment file:

```bash
cp .env.example .env
```

Set at least these values in `.env`:

```dotenv
UDL_TOKEN=your_real_udl_token
OREKIT_DATA_PATH=./orekit-data-main
DATABASE_BACKEND=duckdb
```

Run a dry check:

```bash
python scripts/run_pipeline_demo.py --dry-run
```

## Real Pipeline Examples

Recommended local pattern:

- use `--pipeline-mode statevector-first`
- use `--db-path /tmp/...` to isolate each run

Date-range run over the default internal satellite pool:

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

Single-satellite run for ISS (`25544`):

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

Each successful run writes a JSON report under `reports/pipeline_runs/`.

## Where Outputs Are Saved

Run reports:

- `reports/pipeline_runs/`

Missingness artifacts and images:

- `reports/missingness/<run-name>/`

Evaluation artifacts and images:

- `reports/evaluation/<run-name>/`

Datasets:

- if you pass `--db-path`, the DuckDB file is written exactly there
- otherwise, with local DuckDB, the default is `data/database/uct_benchmark.duckdb`

The exact artifact paths for a completed run are also recorded inside the run report JSON.

## Backend

```bash
source .venv/bin/activate
uvicorn backend_api.main:app --reload --port 8000
```

Backend: `http://localhost:8000`

## Frontend

```bash
cd frontend
npm install
npm run dev
```

Frontend: `http://localhost:5173`
