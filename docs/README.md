# Documentation Index

This directory contains the primary project documentation for local setup, pipeline execution, interpretation, and inspection.

## Start Here

If you are new to the repository, read these in order:

1. [Pipeline Runbook](PIPELINE_RUNBOOK.md)
2. [Outputs and Database Inspection](OUTPUTS_AND_DATABASE_INSPECTION.md)
3. [Result Artifacts Guide](RESULT_ARTIFACTS_GUIDE.md)

That sequence gets you from setup to running the pipeline to inspecting what it produced.

## Full Documentation Map

- [Pipeline Runbook](PIPELINE_RUNBOOK.md)
  - End-to-end local setup and terminal usage
  - Includes clone, environment setup, credentials, and real run examples

- [Pipeline System Guide](PIPELINE_SYSTEM_GUIDE.md)
  - Architecture and high-level pipeline behavior
  - Describes the main subsystems and runtime paths

- [Missingness, Preprocessing, and Simulation](MISSINGNESS_PREPROCESSING_AND_SIMULATION.md)
  - Missingness heuristics and recommendation tags
  - Deterministic preprocessing rules
  - Downsampling and simulation decision logic
  - Explanation of current imputation and synthetic-data methods

- [Result Artifacts Guide](RESULT_ARTIFACTS_GUIDE.md)
  - What image and JSON artifacts are written
  - How to read the missingness and evaluation plots

- [Outputs and Database Inspection](OUTPUTS_AND_DATABASE_INSPECTION.md)
  - Where reports, images, and database files are saved
  - How to inspect DuckDB outputs after a run

## Common Questions

### How do I get the pipeline running locally?

Start with:

- [Pipeline Runbook](PIPELINE_RUNBOOK.md)

### How do I understand why preprocessing or simulation happened?

Start with:

- [Missingness, Preprocessing, and Simulation](MISSINGNESS_PREPROCESSING_AND_SIMULATION.md)

### How do I interpret the generated PNG files?

Start with:

- [Result Artifacts Guide](RESULT_ARTIFACTS_GUIDE.md)

### How do I find the saved dataset and query it?

Start with:

- [Outputs and Database Inspection](OUTPUTS_AND_DATABASE_INSPECTION.md)
