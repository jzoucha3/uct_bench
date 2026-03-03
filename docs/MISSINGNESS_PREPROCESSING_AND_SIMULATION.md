# Missingness, Preprocessing, and Simulation

This document explains how the pipeline:

1. Classifies missingness patterns
2. Chooses deterministic preprocessing steps
3. Decides when to downsample or simulate
4. Applies the current imputation and simulation methods

The implementation described here is based on the code currently used by the local pipeline runner.

## Scope

Primary implementation points:

- `uct_benchmark.data.missingness.inspect_missingness(...)`
- `uct_benchmark.data.missingness.summarize_inspection(...)`
- `uct_benchmark.data.missingness.apply_missingness_driven_preprocessing(...)`
- `uct_benchmark.data.dataManipulation.apply_downsampling(...)`
- `uct_benchmark.data.dataManipulation.apply_simulation_to_gaps(...)`

## 1. Missingness Classification

The pipeline does not use a formal academic classifier that labels data as MCAR, MAR, or MNAR with statistical certainty. Instead, it computes practical diagnostics and emits heuristic recommendation tags.

That means:

- the system measures observable missingness structure
- it derives operational tags
- those tags influence follow-on handling
- the tags are heuristics, not proof of a missingness mechanism

## 2. What `inspect_missingness(...)` Measures

The missingness inspector builds a JSON-safe report containing:

- `row_count`
- `column_count`
- `missing_fraction` per column
- missingness-indicator correlation matrix
- temporal missing-run statistics
- group-level missingness rates and disparity
- numeric diagnostics
- categorical diagnostics
- plot-ready sampled data

### 2.1 Column Missing Fraction

For each column:

- compute the fraction of rows that are null
- sort columns from most missing to least missing

Why it matters:

- highlights the largest data quality gaps
- drives the top-level "high missingness" heuristic

### 2.2 Missingness-Indicator Correlation

For each column with at least one missing value:

- convert missingness into a binary indicator
- compute the correlation matrix across those indicators

Interpretation:

- strong positive correlation means fields tend to disappear together
- near-zero correlation means missingness is less coupled across those fields

Why it matters:

- helps detect joint dropout patterns
- supports the `JOINT_DROPOUT_ACROSS_FIELDS` and `LIKELY_LOW_RANK_STRUCTURE` tags

### 2.3 Temporal Missing Runs

If a valid time column is provided:

- sort by grouping columns plus time
- measure consecutive runs of missing values
- record longest run, average run length, and longest-run fraction

Interpretation:

- isolated nulls suggest sporadic loss
- long blocks suggest temporal outages or sustained gaps

Why it matters:

- supports the `TEMPORAL_BLOCK_MISSINGNESS` tag

### 2.4 Group-Level Missingness Disparity

If grouping columns are provided:

- compute per-group missing fractions
- compare min/max/spread across groups

Interpretation:

- if one satellite or sensor is much worse than others, missingness is concentrated rather than uniform

Why it matters:

- large spread can indicate operational bias or subgroup-specific gaps
- this supports the `POSSIBLE_MNAR_RISK` heuristic

### 2.5 Numeric and Categorical Diagnostics

The inspector also stores:

- numeric means, standard deviations, skewness, kurtosis, zero fraction, observed fraction
- top categorical values and their counts

These are primarily for inspection and artifact review. They are not the main routing trigger today.

## 3. How Recommendation Tags Are Assigned

After inspection, `summarize_inspection(...)` generates a human-readable summary and a set of recommendation tags.

Current tags:

- `HIGH_MISSINGNESS_REQUIRES_RESIM`
- `POSSIBLE_MNAR_RISK`
- `TEMPORAL_BLOCK_MISSINGNESS`
- `JOINT_DROPOUT_ACROSS_FIELDS`
- `LIKELY_LOW_RANK_STRUCTURE`

### 3.1 `HIGH_MISSINGNESS_REQUIRES_RESIM`

Triggered when:

- the maximum column-level missing fraction is at least `50%`

Meaning:

- at least one field is missing in a large share of rows
- the dataset may need augmentation or simulation rather than only field-level repair

### 3.2 `POSSIBLE_MNAR_RISK`

Triggered when either:

- the maximum column-level missing fraction is at least `80%`, or
- the maximum group-level missingness spread is at least `40%`

Meaning:

- the missingness may be highly concentrated or operationally biased
- this is a cautionary heuristic, not a formal proof of MNAR

### 3.3 `TEMPORAL_BLOCK_MISSINGNESS`

Triggered when either:

- the longest missing run covers at least `5%` of the dataset, or
- the longest missing run has length at least `10` rows

Meaning:

- missingness is clustered into contiguous time blocks

### 3.4 `JOINT_DROPOUT_ACROSS_FIELDS`

Triggered when:

- the maximum absolute off-diagonal missingness-indicator correlation is at least `0.6`

Meaning:

- multiple fields tend to be missing together

### 3.5 `LIKELY_LOW_RANK_STRUCTURE`

Triggered when:

- the mean absolute off-diagonal missingness-indicator correlation is at least `0.35`, and
- there are at least `3` missingness columns in play

Meaning:

- the missingness may be driven by a smaller set of common causes

## 4. Current Preprocessing Methods

The current pipeline uses deterministic, local rules first. It does not currently apply a general-purpose learned imputer.

That is important:

- the system prefers physically interpretable or structure-based repairs
- if those repairs are not justified by the available columns, it leaves values missing

## 5. Deterministic Imputation Methods

These are the current field-level repair methods in `apply_missingness_driven_preprocessing(...)`.

## 5.1 Range Fill: `FILL_RANGE_KM_FROM_ELEVATION`

Target field:

- `range_km`

Trigger:

- `elevation` exists
- `range_km` is missing

Method:

- estimate slant range from elevation using a simple Earth-geometry model

Behavior:

- fills `range_km`
- if legacy `range` exists, it is updated to match

Why this method is used:

- it is deterministic
- it uses a direct geometric relationship
- it is more defensible than statistical guessing when elevation is available

Limitations:

- it is an approximation
- it depends on the simplified geometry assumptions used by the function

## 5.2 Range-Rate Derivation: `DERIVE_RANGE_RATE_FROM_CONSECUTIVE_OBS`

Target field:

- `range_rate_km_s`

Trigger:

- `satNo` exists
- `obTime` exists
- consecutive rows for the same satellite have usable time and range values

Method:

- sort observations by `satNo` and `obTime`
- compute finite-difference range change between adjacent rows
- only accept a derived rate when:
  - `0 < dt <= 120` seconds
  - `abs(rate) <= 8.0 km/s`

Why this method is used:

- it is local and physically interpretable
- it is safer than a generic global fill when time ordering is valid

Limitations:

- it only works when adjacent range values exist
- it intentionally rejects implausible jumps

## 5.3 Range-Rate Mean Fill: `MEAN_FILL_RANGE_RATE`

Target field:

- remaining nulls in `range_rate_km_s`

Trigger:

- nulls remain after the physics-based derivation step
- the column has at least one observed value to compute a mean

Method:

- fill remaining nulls with the observed column mean

Why this method is used:

- it is a conservative fallback after the physically motivated step
- it keeps the behavior simple and transparent

Limitations:

- it reduces variability
- it is not physically specific
- it can bias downstream analyses if overused

## 5.4 Track Reconstruction: `ASSIGN_TRACK_IDS`

Target field:

- `trackId`

Trigger:

- `trackId` is missing

Method:

- sort by `satNo` and `obTime`
- choose the sensor grouping key in this priority:
  - `idSensor`
  - else `sensorName`
  - else `senlat|senlon|senalt`
- start a new track if the time gap is greater than `120` seconds

Why this method is used:

- `trackId` is a structural grouping field, not a continuous measurement
- reconstructing tracks from object, sensor, and timing is more appropriate than numeric imputation

Limitations:

- if the sensor identity columns are weak or inconsistent, track grouping quality drops

## 5.5 Sparse-Satellite Detection: `FLAG_SPARSE_SATELLITES`

Target field:

- this does not fill a field directly

Trigger:

- any `satNo` has fewer than `100` rows

Method:

- count observations per satellite
- mark satellites below the threshold as sparse

Why this method is used:

- some data quality problems are coverage problems rather than cell-level nulls
- sparse coverage is handled downstream by simulation logic more naturally than by imputation

## 6. How Method Selection Works

The current selection logic is straightforward:

1. Inspect the returned data
2. Generate summary tags
3. Apply deterministic preprocessing
4. Optionally downsample
5. Optionally simulate

This is not a broad "model chooses any imputer" framework today. The selection is rule-based.

## 7. When Downsampling Runs

Downsampling is not triggered solely by missingness tags.

It runs when:

- the pipeline configuration enables it

In the demo CLI, that means:

```bash
--enable-downsampling
```

If it is not enabled, the run report will usually show:

- `Downsampling Metadata: null`

### 7.1 What Downsampling Does

`apply_downsampling(...)` reduces the number of observation rows using tier-aware settings.

Inputs it uses:

- observation dataframe
- satellite parameters or orbital elements derived from TLEs
- tier (`T1` to `T4`)

High-level behavior:

- higher-quality tiers retain more data
- lower-quality tiers downsample more aggressively

Typical outcomes:

- smaller observation counts
- lower retention ratio
- potentially larger track gaps

This is not an imputation method. It intentionally removes observations.

## 8. When Simulation Runs

Simulation can run in two ways.

### 8.1 Explicit Simulation

Triggered when the config explicitly enables it.

In the demo CLI:

```bash
--enable-simulation
```

### 8.2 Automatic Simulation

In the current `statevector-first` path, simulation can be auto-enabled when:

- sparse satellites are detected, or
- the missingness summary includes `HIGH_MISSINGNESS_REQUIRES_RESIM`

In those cases, the pipeline builds a default simulation config and attempts to fill observation gaps.

## 9. What the Simulation Method Does

`apply_simulation_to_gaps(...)` is gap-filling by synthetic observation generation, not field-level imputation.

High-level steps:

1. Build orbital context from TLE / element-set data
2. Analyze each satellite's observation gaps
3. Propose epochs to simulate
4. Enforce a maximum synthetic ratio
5. Generate synthetic observations from the orbit and sensor geometry
6. Add optional sensor noise
7. Merge simulated rows with the original observations

Key constraints:

- requires element-set data
- requires sensor data
- requires a usable orbit for the target satellite

If those are missing, simulation returns the original observations unchanged.

### 9.1 Maximum Synthetic Ratio

The simulation step limits synthetic growth using:

- `max_synthetic_ratio`

That cap prevents the run from replacing most of the dataset with synthetic data.

### 9.2 Sensor Noise

When enabled:

- position noise and angular noise are injected into simulated rows

This is intended to make synthetic observations less unrealistically clean.

### 9.3 Simulation Output Markers

Simulated rows are marked with:

- `is_simulated = True`
- `dataMode = "SIMULATED"`

Original rows are preserved as:

- `is_simulated = False`
- `dataMode = "REAL"` if absent before merge

## 10. Important Practical Interpretation

If a run report shows:

- preprocessing `status: success`
- simulation `status: success`

that only means the steps executed.

It does not necessarily mean they changed the dataset.

Examples:

- a preprocessing step may run but fill `0` values
- simulation may run but add `0` synthetic rows
- downsampling may be skipped entirely

Always inspect the run report fields:

- `Preprocessing Metadata`
- `Downsampling Metadata`
- `Simulation Metadata`

## 11. What the Pipeline Does Not Do Today

The current implementation does not provide:

- a generic multivariate statistical imputer
- a learned deep imputation model
- a formal probabilistic MCAR/MAR/MNAR classifier
- automatic downsampling based purely on missingness tags

The implemented strategy today is:

- inspect
- apply deterministic local repairs
- optionally thin data
- optionally synthesize gap-filling observations
