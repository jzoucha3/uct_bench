# Result Artifacts Guide

This document explains what files the pipeline writes during a run and how to read the generated result images.

## Artifact Families

A successful run can produce four main output families:

1. Run report JSON
2. Missingness artifacts
3. Evaluation artifacts
4. Database records

The image files are primarily created in the missingness and evaluation artifact folders.

## 1. Run Report JSON

Location:

- `reports/pipeline_runs/run-<timestamp>.json`

Purpose:

- high-level audit record for the run
- configuration used
- counts
- pipeline context
- metadata for preprocessing, downsampling, and simulation
- paths to image artifacts

This is the main "table of contents" for understanding a completed run.

## 2. Missingness Artifacts

Typical location:

- `reports/missingness/<run-name>/`

Typical files:

- `missingness_report.json`
- `missingness_heatmap.png`
- `missingness_indicator_corr_heatmap.png`

## 2.1 `missingness_report.json`

This is the raw structured output of the missingness inspector.

Use it when you need:

- exact missing fractions
- exact tag triggers
- temporal run stats
- group disparity stats

## 2.2 How to Read `missingness_heatmap.png`

What it shows:

- a sampled missingness matrix
- columns are sorted from most missing to least missing
- each row in the image corresponds to a sampled observation row
- each horizontal band corresponds to a data column

Color meaning:

- dark cells indicate missing values
- light cells indicate observed values

How to interpret patterns:

- vertical dark streaks:
  - specific rows have many fields missing at once
  - suggests row-level dropout

- thick dark bands in one column:
  - one field is frequently absent
  - suggests a field-specific data issue

- dark blocks spanning time-ordered rows:
  - prolonged contiguous missingness
  - consistent with temporal outages

- mostly light image with isolated dark points:
  - low missingness
  - sporadic nulls rather than structural failure

Limitations:

- it is a sampled view, not necessarily every row
- it is best used for visual pattern recognition, not exact counts

## 2.3 How to Read `missingness_indicator_corr_heatmap.png`

What it shows:

- correlation between binary "is this field missing?" indicators

Axes:

- both axes list only columns that had at least one missing value

Color meaning:

- values near `+1`:
  - two fields tend to be missing together

- values near `0`:
  - little relationship between those fields' missingness patterns

- values near `-1`:
  - one field tends to be missing when the other is present
  - less common in this use case

How to interpret:

- bright/high-correlation clusters:
  - related fields are dropping out together
  - supports joint-dropout reasoning

- weak/no structure:
  - missingness is less coupled across fields

## 3. Evaluation Artifacts

Typical location:

- `reports/evaluation/<run-name>/`

Typical files:

- `observation_evaluation.json`
- `observation_match_counts.png`
- `observation_rmse.png`

These artifacts compare the final observation dataset against a reference observation dataset.

## 3.1 `observation_evaluation.json`

This JSON stores the structured comparison result.

Important fields:

- `candidate_row_count`
- `reference_row_count`
- `matched_row_count`
- `candidate_only_row_count`
- `reference_only_row_count`
- `retention_ratio_vs_reference`
- `candidate_simulated_count`
- `candidate_simulated_fraction`
- `per_column_metrics`

Use this file for exact numeric interpretation.

## 3.2 How to Read `observation_match_counts.png`

What it shows:

- a three-bar comparison:
  - `matched`
  - `candidate_only`
  - `reference_only`

Meaning:

- `matched`:
  - rows present in both the pipeline result and the reference

- `candidate_only`:
  - rows present only in the pipeline output
  - may indicate simulated rows, transformed rows, or unmatched output

- `reference_only`:
  - rows present only in the reference
  - may indicate dropped rows, unresolved joins, or coverage loss

How to interpret:

- high `matched` with low unmatched bars:
  - strong overlap with the reference

- high `candidate_only`:
  - output contains many rows not present in the reference
  - check whether simulation was enabled

- high `reference_only`:
  - output failed to retain or recreate much of the reference coverage

## 3.3 How to Read `observation_rmse.png`

What it shows:

- one bar per numeric field included in evaluation
- bar height is RMSE for that field on matched rows

Common fields:

- `ra`
- `declination`
- `elevation`
- `azimuth`
- `range`
- `range_km`
- `range_rate_km_s`

How to interpret:

- smaller bars:
  - closer agreement between candidate and reference on that field

- larger bars:
  - more error or more transformation drift on that field

Important:

- RMSE is scale-dependent
- compare RMSE relative to the units and expected operating range of that field

Example:

- a low RMSE in angular fields may still be meaningful
- a larger absolute RMSE in `range_km` may be acceptable depending on use case

## 4. No-Change Cases

Some artifact sets are valid even when little changed.

Examples:

- preprocessing ran but filled nothing
- simulation ran but added zero synthetic rows
- evaluation ran on a tiny dataset with limited overlap

In these cases, the plots are still useful:

- they show what was attempted
- they help confirm that the stage executed

## 5. Finding the Artifact Paths

The easiest way to locate the exact files for a run is:

1. open the run report JSON
2. inspect:
   - `performance.Missingness Artifacts`
   - `performance.Observation Evaluation Artifacts`

Those entries record the exact relative paths written during the run.

## 6. Opening the Images Locally

From the terminal, you can list the files:

```bash
find reports -type f | sort
```

To inspect the JSON:

```bash
python -m json.tool reports/pipeline_runs/run-<timestamp>.json
```

To open the image files with a desktop viewer, use the image viewer available on your machine. If you are in a headless shell, inspect the file paths and copy them to a local machine or open them through your editor.
