# Outputs and Database Inspection

This document explains where the pipeline saves datasets and artifacts, and how to inspect the resulting DuckDB files after a run.

## 1. Where Data Is Saved

The pipeline writes two classes of output:

1. Structured files on disk
2. Database records

## 2. Run Report Location

Every run writes a JSON summary file:

- `reports/pipeline_runs/run-<timestamp>.json`

This file links the rest of the outputs together.

## 3. Missingness and Evaluation Image Locations

Image artifacts are written under:

- `reports/missingness/<run-name>/`
- `reports/evaluation/<run-name>/`

Common image files:

- `missingness_heatmap.png`
- `missingness_indicator_corr_heatmap.png`
- `observation_match_counts.png`
- `observation_rmse.png`

## 4. Database Location

### 4.1 If You Pass `--db-path`

The run writes to the exact file you provide.

Example:

```bash
python scripts/run_pipeline_demo.py \
  --pipeline-mode statevector-first \
  --db-path /tmp/uct_benchmark_iss_case.duckdb \
  --satellites 25544
```

In that case, the dataset is stored in:

- `/tmp/uct_benchmark_iss_case.duckdb`

### 4.2 If You Do Not Pass `--db-path`

With `DATABASE_BACKEND=duckdb`, the default local database file is:

- `data/database/uct_benchmark.duckdb`

## 5. Quick Terminal Inspection

To list recent pipeline reports:

```bash
ls -lt reports/pipeline_runs
```

To list all generated artifacts:

```bash
find reports -type f | sort
```

To inspect a run report:

```bash
python -m json.tool reports/pipeline_runs/run-<timestamp>.json
```

## 6. Inspecting DuckDB with Python

If you do not want to rely on a separate DuckDB CLI install, use Python directly.

Example:

```bash
python - <<'PY'
import duckdb

db_path = "/tmp/uct_benchmark_iss_case.duckdb"
conn = duckdb.connect(db_path, read_only=True)

print("Tables:")
print(conn.execute("SHOW TABLES").fetchall())

print("\nDatasets:")
print(conn.execute("SELECT id, name, status, observation_count, satellite_count FROM datasets ORDER BY id DESC LIMIT 5").fetchdf())

print("\nObservations:")
print(conn.execute("SELECT COUNT(*) AS count FROM observations").fetchdf())

print("\nState vectors:")
print(conn.execute("SELECT COUNT(*) AS count FROM state_vectors").fetchdf())

print("\nElement sets:")
print(conn.execute("SELECT COUNT(*) AS count FROM element_sets").fetchdf())

conn.close()
PY
```

This is the most portable local inspection method because it uses the installed Python dependency.

## 7. Inspecting DuckDB with the DuckDB CLI

If `duckdb` is installed on your machine, you can inspect the file directly.

Open the database:

```bash
duckdb /tmp/uct_benchmark_iss_case.duckdb
```

Useful SQL once inside:

```sql
SHOW TABLES;
SELECT id, name, status, observation_count, satellite_count FROM datasets ORDER BY id DESC LIMIT 5;
SELECT COUNT(*) FROM observations;
SELECT COUNT(*) FROM state_vectors;
SELECT COUNT(*) FROM element_sets;
```

Inspect a recent dataset record:

```sql
SELECT *
FROM datasets
ORDER BY id DESC
LIMIT 1;
```

Inspect a few observations:

```sql
SELECT sat_no, ob_time, sensor_name, data_mode, track_id
FROM observations
ORDER BY ob_time DESC
LIMIT 10;
```

Inspect simulated rows:

```sql
SELECT sat_no, ob_time, data_mode
FROM observations
WHERE data_mode = 'SIMULATED'
ORDER BY ob_time DESC
LIMIT 10;
```

## 8. What to Check After a Run

For a quick sanity check, verify:

1. The run report exists in `reports/pipeline_runs/`
2. The database file exists where you expected
3. The `datasets` table has a new row with `status='available'`
4. Observation, state-vector, and element-set counts are nonzero if the run was expected to persist them
5. Missingness and evaluation artifact folders exist for the run name

## 9. How to Match a Report to a Database Record

Use these fields:

- run report `dataset_id`
- run report `dataset_name`

Then query the `datasets` table for the same `id` or `name`.

Example SQL:

```sql
SELECT id, name, status, created_at, observation_count, satellite_count
FROM datasets
WHERE id = 3;
```

## 10. Common Inspection Patterns

### Did preprocessing change anything?

Check the run report:

- `performance.Preprocessing Metadata`

Look at:

- `range_km_filled`
- `range_rate_physics_filled`
- `range_rate_mean_filled`
- `track_id_filled`
- `applied_steps`

### Did downsampling run?

Check:

- `performance.Downsampling Metadata`

If it is `null`, downsampling did not run.

### Did simulation add rows?

Check:

- `performance.Simulation Metadata.simulated_count`
- `performance.Simulation Metadata.synthetic_ratio`

### Where are the exact images?

Check:

- `performance.Missingness Artifacts`
- `performance.Observation Evaluation Artifacts`

Those entries contain the exact saved file paths.
