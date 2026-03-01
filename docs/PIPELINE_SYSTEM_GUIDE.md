# Pipeline System Guide

This repository is a minimal full-stack UCT benchmark workspace intended to:

1. Acquire source data from UDL
2. Normalize and inspect the returned data
3. Apply deterministic preprocessing
4. Optionally downsample and/or simulate observations
5. Save internal evaluation artifacts
6. Persist the generated dataset in DuckDB
7. Expose the process through a backend API and a frontend UI

## System Layout

- `uct_benchmark/api/`
  - UDL queries, token handling, Orekit setup, and the main data-generation logic.
- `uct_benchmark/data/`
  - Missingness inspection, deterministic preprocessing, downsampling, and simulation helpers.
- `uct_benchmark/evaluation/`
  - Observation-level comparison metrics and saved artifact generation.
- `uct_benchmark/pipeline/`
  - High-level orchestration and controller logic.
- `backend_api/`
  - FastAPI application, request models, background job workers, and API routes.
- `frontend/`
  - React UI used to configure and submit dataset-generation jobs.
- `data/database/`
  - DuckDB file storage.
- `reports/`
  - Saved run reports, missingness artifacts, and evaluation artifacts.

## Main Pipeline Paths

There are two runtime paths.

1. Standard observation-first path
- Entry point: `uct_benchmark.api.apiIntegration.generateDataset(...)`
- Intended flow:
  - query observation data
  - inspect missingness
  - deterministic preprocessing
  - optional downsampling
  - optional simulation
  - fetch state vectors and element sets
  - save evaluation artifacts
  - persist results

2. Statevector-first path
- Entry point: `uct_benchmark.pipeline.statevector_first.execute_statevector_first_pipeline(...)`
- Intended flow:
  - query state vectors first
  - derive an observation-like dataframe from `epoch`
  - inspect missingness
  - deterministic preprocessing
  - optional downsampling
  - optional simulation
  - fetch current element sets
  - save evaluation artifacts
  - persist results

The statevector-first path exists so the system can still run when an observation endpoint is unavailable or returns no usable rows.

## Data Acquisition

### Credentials

The code expects:

- `UDL_TOKEN`
  - preferred when already available
- or `UDL_USERNAME` + `UDL_PASSWORD`
  - used to generate `UDL_TOKEN`
- optional `ESA_TOKEN`
  - used only for supplemental Discosweb enrichment

### Orekit

Orekit initialization is handled in `uct_benchmark/api/apiIntegration.py`.

Runtime behavior:

1. Use `OREKIT_DATA_PATH` if it points to a valid directory
2. If missing, attempt to clone Orekit data into the configured path
3. If clone fails, fall back to the local `orekit_jpype` data path

This keeps the simulation path usable even when a local Orekit data checkout is not already present.

### Search Strategy

Search strategy is resolved automatically unless explicitly set.

Available values:

- `fast`
- `hybrid`
- `windowed`
- `auto`

Automatic routing:

- small, short requests prefer `fast`
- large or long-range requests prefer `windowed`
- everything else uses `hybrid`

This is intended to balance completeness against API call count and latency.

## Missingness Inspection

Immediately after data pull, the system inspects the dataframe before any modification.

Implemented in:

- `uct_benchmark.data.missingness.inspect_missingness(...)`
- `uct_benchmark.data.missingness.summarize_inspection(...)`
- `uct_benchmark.data.missingness.save_artifacts(...)`

Outputs include:

- missing fraction per column
- missingness-indicator correlation structure
- temporal missing-run statistics
- group-level missingness clustering
- basic numeric and categorical diagnostics
- recommendation tags

Current tags:

- `TEMPORAL_BLOCK_MISSINGNESS`
- `JOINT_DROPOUT_ACROSS_FIELDS`
- `LIKELY_LOW_RANK_STRUCTURE`
- `HIGH_MISSINGNESS_REQUIRES_RESIM`
- `POSSIBLE_MNAR_RISK`

Why this exists:

- the pipeline should react to the actual returned data, not assume the same missingness pattern every run
- the saved artifacts provide a record of why preprocessing or simulation decisions were taken

## Deterministic Preprocessing

Deterministic preprocessing runs before any generic routing-based method family and before simulation.

Implemented in:

- `uct_benchmark.data.missingness.apply_missingness_driven_preprocessing(...)`
- also reflected in `uct_benchmark.pipeline.pipeline_controller.run_pipeline(...)`

Execution order:

1. `range_km` fill
2. `range_rate_km_s` adjacent derivative
3. `range_rate_km_s` mean fallback
4. `trackId` grouping fill
5. sparse-satellite detection

### 1. `range_km` Fill

Method:

- physics-based geometric estimate from `elevation`

Why:

- this is a local deterministic fill
- it uses a direct physical relationship
- it is lower risk than statistical imputation when `elevation` is present

Trigger:

- `range_km` missing
- `elevation` exists

Side effect:

- if legacy `range` exists, it is kept in sync

### 2. `range_rate_km_s` Fill, Stage 1

Method:

- derive rate from adjacent observations for the same satellite

Why:

- if the time ordering is valid and range values exist, local finite-difference behavior is more defensible than a model-based guess

Rules:

- grouped by `satNo`
- sorted by `obTime`
- both adjacent ranges present
- both adjacent times present
- `0 < dt <= 120 seconds`
- `abs(rate) <= 8.0 km/s`

### 3. `range_rate_km_s` Fill, Stage 2

Method:

- mean fallback

Why:

- only used for leftover gaps after the deterministic derivative pass
- simple, bounded, and transparent
- this is a conservative fallback, not the preferred primary fill

Trigger:

- missing remains after stage 1
- observed mean exists

### 4. `trackId` Fill

Method:

- grouping / track assignment

Why:

- `trackId` is structural, not a continuous physical measurement
- grouping based on object, sensor identity, and time gaps is a more appropriate reconstruction than statistical imputation

Grouping priority:

1. `satNo`
2. sensor identity:
   - `idSensor`
   - else `sensorName`
   - else location fields

Split rule:

- start a new track when time gap is greater than `120 seconds`

### 5. Sparse-Satellite Detection

Method:

- rule-based count check

Why:

- some data issues are not “missing cells”; they are insufficient coverage
- that condition is better handled by simulation than by field-level fills

Trigger:

- any `satNo` with fewer than `100` rows

## Diagnose-Then-Route Layer

The repository includes a deterministic routing layer for future extension.

Implemented in:

- `uct_benchmark.pipeline.routing_methods`
- `uct_benchmark.pipeline.pipeline_controller`

What it does today:

- runs the deterministic preprocessing steps first
- builds a per-column routing table for method families
- decides whether simulation should run
- records decisions

What it does not do yet:

- it does not execute the advanced method families for general columns
- it only plans and records those routes

Supported method families:

- `STRUCTURAL_MISSING`
- `LEAVE_AS_MISSING`
- `DROP_COLUMN`
- `SIMPLE_UNIVARIATE`
- `DEPENDENCY_BASED_MI`
- `LOW_RANK_COMPLETION`
- `TIME_SERIES_INTERP`
- `STATE_SPACE_SMOOTHING`
- `PHYSICS_PROPAGATION`
- `RESIMULATE_MEASUREMENTS`

These exist so the system can grow without rewriting the control flow later.

## Downsampling

Implemented in:

- `uct_benchmark.data.dataManipulation.apply_downsampling(...)`

Purpose:

- intentionally reduce data density or continuity
- create more challenging benchmark conditions

Important distinction:

- downsampling is not a repair step
- it deliberately removes or thins real observations

Why it exists:

- benchmark generation sometimes needs controlled degradation, not just reconstruction

Typical controls:

- target coverage
- target gap
- max observations per satellite
- preserve track boundaries

## Simulation

Implemented in:

- `uct_benchmark.data.dataManipulation.apply_simulation_to_gaps(...)`

Purpose:

- add synthetic observations to fill gaps or increase usable coverage

Method:

- uses TLE / element-set data
- uses sensor definitions
- generates synthetic observations at selected epochs
- can apply realistic noise

Why simulation is separate from preprocessing:

- preprocessing changes existing rows with local deterministic logic
- simulation creates new rows when the issue is broader coverage loss

### Simulation Triggers

Simulation runs when any of these are true:

1. explicitly enabled in configuration
2. sparse satellites are detected and simulation was not explicitly disabled
3. inspection includes `HIGH_MISSINGNESS_REQUIRES_RESIM`
4. temporal block conditions indicate larger structured gaps

### Simulation Constraints

- requires TLE / element-set data
- respects `max_synthetic_ratio`
- records metadata about added synthetic rows

## Evaluation Artifacts

The pipeline performs internal observation-level comparison after transformation.

Implemented in:

- `uct_benchmark.evaluation.datasetComparison`

Purpose:

- compare the transformed observation dataset against a real/reference baseline
- support internal testing and tuning before later benchmark-stage comparisons

Artifacts written under `reports/evaluation/<dataset>/`:

- `observation_evaluation.json`
- `observation_match_counts.png`
- `observation_rmse.png` when numeric matched columns exist

Why this is file-based:

- the current system is focused on dataset generation, not end-user evaluation display
- internal metrics and plots are more useful right now than exposing raw values in the UI

## Database Persistence

Storage uses DuckDB through the repository layer.

Primary entry point:

- `uct_benchmark.database.connection.DatabaseManager`

Key repositories:

- `db.datasets`
- `db.observations`
- `db.state_vectors`
- `db.element_sets`

What is persisted:

- dataset metadata and status
- observations
- state vectors
- element sets

Why the DB layer matters:

- it gives a durable record of generated datasets
- it supports later comparison, reuse, and API access

## Backend and Frontend

### Backend API

The FastAPI backend:

- creates generation jobs
- stores job status
- loads optional evaluation reference data
- resolves CSV uploads for reference comparisons
- calls the pipeline

Important files:

- `backend_api/main.py`
- `backend_api/routers/datasets.py`
- `backend_api/jobs/workers.py`

### Frontend

The React frontend:

- collects dataset-generation parameters
- submits requests to the backend
- supports evaluation reference selection
- supports CSV upload for reference comparison

Important file:

- `frontend/src/pages/DatasetGeneratorPage.tsx`

## Hypothetical Normal Usage

If all runtime services are available, a normal end-to-end flow looks like this.

### CLI

Dry run:

```bash
cd /home/joey/jzouca_UCTBench_minimal
source /home/joey/jzoucha_UCTBench/.venv/bin/activate
python scripts/run_pipeline_demo.py --dry-run
```

Standard pipeline:

```bash
python scripts/run_pipeline_demo.py \
  --db-path /tmp/uct_benchmark_run.duckdb \
  --tier T2 \
  --regime LEO \
  --object-count 1 \
  --timeframe 1 \
  --search-strategy auto \
  --satellites 25544
```

Statevector-first pipeline:

```bash
python scripts/run_pipeline_demo.py \
  --pipeline-mode statevector-first \
  --db-path /tmp/uct_benchmark_run.duckdb \
  --tier T2 \
  --regime LEO \
  --object-count 1 \
  --timeframe 1 \
  --search-strategy auto \
  --satellites 25544
```

With an external evaluation reference:

```bash
python scripts/run_pipeline_demo.py \
  --db-path /tmp/uct_benchmark_run.duckdb \
  --tier T2 \
  --regime LEO \
  --object-count 1 \
  --timeframe 1 \
  --search-strategy auto \
  --satellites 25544 \
  --evaluation-reference-csv /home/joey/reference_obs.csv
```

### Backend + Frontend

Backend:

```bash
uvicorn backend_api.main:app --reload --port 8000
```

Frontend:

```bash
cd frontend
npm install
npm run dev
```

Then:

1. Open the frontend
2. Configure regime, tier, timeframe, and satellites
3. Optionally enable downsampling or simulation
4. Optionally upload a reference CSV or enter a reference dataset ID
5. Submit generation
6. Monitor completion in the backend-managed job flow
7. Inspect the saved reports in `reports/`

## Current Blocker

The current blocker is not the preprocessing, downsampling, simulation, or storage code.

The blocker is upstream data retrieval from UDL for the tested satellite and account:

- the observation-first path can fail when no usable observation rows are returned
- the statevector-first path can also fail when no historical statevector rows are returned

What is currently known:

- the system initializes correctly
- credentials load correctly
- the database and local processing stack run
- the pipeline structure is in place
- the tested live runs are stopping because the relevant UDL query path is returning no usable data

What this means:

- the current gap is at external service access or endpoint behavior
- local pipeline logic after successful data retrieval is ready for iterative testing

## Planned Next Step

The next practical step is to add a focused UDL diagnostic probe and use it before full pipeline execution.

That probe should test:

1. broad observation query by time window
2. historical statevector query by time range
3. current statevector availability
4. current element-set availability
5. returned row counts and returned columns

Reason:

- this isolates account access and endpoint behavior from pipeline logic
- it tells us which live entry path is actually available for this environment
- it prevents continued debugging of downstream code when the real issue is upstream data availability

Once that probe identifies the reliable live source, the pipeline should standardize on that source for the next test cycle.
