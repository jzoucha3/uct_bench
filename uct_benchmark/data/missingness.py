"""Missingness inspection, preprocessing, and artifact generation utilities."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def _to_python(value: Any) -> Any:
    """Convert numpy/pandas scalar values into JSON-safe Python values."""
    if pd.isna(value):
        return None
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return float(value)
    if isinstance(value, (np.bool_,)):
        return bool(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return value


def _normalize_cols(cols: Optional[Iterable[str]], fallback: List[str]) -> List[str]:
    """Return explicit columns or sensible defaults."""
    if cols is None:
        return list(fallback)
    return [c for c in cols if c in fallback]


def _run_lengths(mask: pd.Series, group_breaks: Optional[pd.Series] = None) -> List[int]:
    """Collect consecutive True run lengths, resetting on group boundaries."""
    lengths: List[int] = []
    current = 0
    bool_mask = mask.fillna(False).astype(bool).tolist()
    breaks = (
        group_breaks.fillna(True).astype(bool).tolist() if group_breaks is not None else [False] * len(mask)
    )

    for idx, is_missing in enumerate(bool_mask):
        if breaks[idx] and current > 0:
            lengths.append(current)
            current = 0
        if is_missing:
            current += 1
        elif current > 0:
            lengths.append(current)
            current = 0
    if current > 0:
        lengths.append(current)
    return lengths


def inspect_missingness(
    df: pd.DataFrame,
    time_col: Optional[str] = None,
    group_cols: Optional[Iterable[str]] = None,
    id_cols: Optional[Iterable[str]] = None,
    numeric_cols: Optional[Iterable[str]] = None,
    categorical_cols: Optional[Iterable[str]] = None,
) -> Dict[str, Any]:
    """
    Inspect missingness patterns and lightweight distribution diagnostics.

    Returns a JSON-serializable report containing summary statistics and
    sampled plot-ready data.
    """
    if df is None:
        raise ValueError("df must be a DataFrame")

    report: Dict[str, Any] = {
        "row_count": int(len(df)),
        "column_count": int(len(df.columns)),
        "columns": list(df.columns),
    }
    if df.empty:
        report.update(
            {
                "missing_fraction": {},
                "indicator_correlation": {"columns": [], "matrix": []},
                "temporal_runs": {},
                "group_missingness": {},
                "numeric_diagnostics": {},
                "categorical_diagnostics": {},
                "plot_data": {"columns_sorted": [], "missingness_matrix_sample": []},
            }
        )
        return report

    group_cols_list = [c for c in (group_cols or []) if c in df.columns]
    id_cols_list = [c for c in (id_cols or []) if c in df.columns]
    default_numeric = df.select_dtypes(include=[np.number]).columns.tolist()
    numeric_cols_list = _normalize_cols(numeric_cols, default_numeric)
    default_categorical = [
        c
        for c in df.columns
        if c not in numeric_cols_list and c not in group_cols_list and c not in id_cols_list
    ]
    categorical_cols_list = _normalize_cols(categorical_cols, default_categorical)

    report["config"] = {
        "time_col": time_col if time_col in df.columns else None,
        "group_cols": group_cols_list,
        "id_cols": id_cols_list,
        "numeric_cols": numeric_cols_list,
        "categorical_cols": categorical_cols_list,
    }

    missing_fraction = df.isna().mean().sort_values(ascending=False)
    report["missing_fraction"] = {col: float(val) for col, val in missing_fraction.items()}

    # Missingness indicator correlation
    missing_cols = [c for c in df.columns if df[c].isna().any()]
    if missing_cols:
        indicators = df[missing_cols].isna().astype(float)
        corr = indicators.corr().fillna(0.0)
        report["indicator_correlation"] = {
            "columns": missing_cols,
            "matrix": corr.round(6).values.tolist(),
        }
    else:
        report["indicator_correlation"] = {"columns": [], "matrix": []}

    # Temporal run diagnostics
    temporal_runs: Dict[str, Any] = {}
    if time_col and time_col in df.columns:
        ordered = df.copy()
        ordered[time_col] = pd.to_datetime(ordered[time_col], errors="coerce")
        sort_cols = group_cols_list + [time_col]
        ordered = ordered.sort_values(sort_cols, na_position="last").reset_index(drop=True)

        group_breaks = None
        if group_cols_list:
            curr_keys = ordered[group_cols_list].astype(str).agg("|".join, axis=1)
            group_breaks = curr_keys.ne(curr_keys.shift(1)).fillna(True)

        for col in missing_cols:
            lengths = _run_lengths(ordered[col].isna(), group_breaks=group_breaks)
            longest = max(lengths) if lengths else 0
            temporal_runs[col] = {
                "run_lengths": lengths,
                "run_count": int(len(lengths)),
                "max_run_length": int(longest),
                "mean_run_length": float(np.mean(lengths)) if lengths else 0.0,
                "pct_missing_in_longest_run": float(longest / len(ordered)) if len(ordered) else 0.0,
            }
    report["temporal_runs"] = temporal_runs

    # Group missingness clustering
    group_missingness: Dict[str, Any] = {}
    if group_cols_list:
        grouped = df.groupby(group_cols_list, dropna=False)
        group_records: List[Dict[str, Any]] = []
        for keys, group_df in grouped:
            key_tuple = keys if isinstance(keys, tuple) else (keys,)
            row = group_df.isna().mean()
            record = {
                "group": {
                    col: _to_python(val) for col, val in zip(group_cols_list, key_tuple)
                },
                "row_count": int(len(group_df)),
                "missing_fraction": {col: float(row[col]) for col in df.columns},
            }
            group_records.append(record)

        # Also keep a compact disparity summary to drive flags.
        disparity = {}
        for col in df.columns:
            col_rates = [rec["missing_fraction"][col] for rec in group_records]
            disparity[col] = {
                "min": float(np.min(col_rates)) if col_rates else 0.0,
                "max": float(np.max(col_rates)) if col_rates else 0.0,
                "spread": float((np.max(col_rates) - np.min(col_rates))) if col_rates else 0.0,
            }
        group_missingness = {"group_rates": group_records, "disparity": disparity}
    report["group_missingness"] = group_missingness

    # Numeric diagnostics
    numeric_report: Dict[str, Any] = {}
    for col in numeric_cols_list:
        series = pd.to_numeric(df[col], errors="coerce")
        numeric_report[col] = {
            "mean": float(series.mean()) if series.notna().any() else None,
            "std": float(series.std()) if series.notna().any() else None,
            "skewness": float(series.skew()) if series.notna().sum() > 2 else None,
            "kurtosis": float(series.kurt()) if series.notna().sum() > 3 else None,
            "zero_fraction": float((series.fillna(np.nan) == 0).mean()),
            "observed_fraction": float(series.notna().mean()),
        }
    report["numeric_diagnostics"] = numeric_report

    # Categorical diagnostics
    categorical_report: Dict[str, Any] = {}
    for col in categorical_cols_list:
        ser = df[col]
        top_counts = ser.value_counts(dropna=True).head(5)
        categorical_report[col] = {
            "missing_fraction": float(ser.isna().mean()),
            "top_k": [{"value": _to_python(idx), "count": int(cnt)} for idx, cnt in top_counts.items()],
        }
    report["categorical_diagnostics"] = categorical_report

    # Plot data: column-sorted missingness matrix sample and corr heatmap input
    columns_sorted = list(missing_fraction.index)
    sample_rows = min(500, len(df))
    sample_idx = np.linspace(0, len(df) - 1, sample_rows, dtype=int) if len(df) > sample_rows else None
    plot_df = df.iloc[sample_idx] if sample_idx is not None else df
    miss_matrix = plot_df[columns_sorted].isna().astype(int)
    report["plot_data"] = {
        "columns_sorted": columns_sorted,
        "missingness_matrix_sample": miss_matrix.values.tolist(),
        "indicator_corr_columns": report["indicator_correlation"]["columns"],
        "indicator_corr_matrix": report["indicator_correlation"]["matrix"],
    }

    return report


def summarize_inspection(report: Dict[str, Any]) -> str:
    """Create a concise human-readable summary with recommendation tags only."""
    row_count = int(report.get("row_count", 0))
    missing_fraction = report.get("missing_fraction", {})
    temporal_runs = report.get("temporal_runs", {})
    indicator_corr = report.get("indicator_correlation", {})
    group_missingness = report.get("group_missingness", {})

    tags: List[str] = []
    lines = [
        f"Rows: {row_count}",
        f"Columns analyzed: {len(report.get('columns', []))}",
    ]

    if missing_fraction:
        top_missing = sorted(missing_fraction.items(), key=lambda x: x[1], reverse=True)[:5]
        lines.append("Top missing columns:")
        for col, frac in top_missing:
            lines.append(f"- {col}: {frac:.1%}")

        max_missing = max(missing_fraction.values())
        if max_missing >= 0.5:
            tags.append("HIGH_MISSINGNESS_REQUIRES_RESIM")
        if max_missing >= 0.8:
            tags.append("POSSIBLE_MNAR_RISK")

    # Temporal block missingness
    if temporal_runs:
        max_run_frac = max(v.get("pct_missing_in_longest_run", 0.0) for v in temporal_runs.values())
        max_run_len = max(v.get("max_run_length", 0) for v in temporal_runs.values())
        lines.append(
            f"Largest temporal missing block: {max_run_len} rows ({max_run_frac:.1%} of dataset)"
        )
        if max_run_frac >= 0.05 or max_run_len >= 10:
            tags.append("TEMPORAL_BLOCK_MISSINGNESS")

    # Joint dropout / low-rank heuristics from indicator correlations
    corr_cols = indicator_corr.get("columns", [])
    corr_matrix = np.array(indicator_corr.get("matrix", []), dtype=float)
    if corr_cols and corr_matrix.size:
        if corr_matrix.shape[0] > 1:
            off_diag = corr_matrix[~np.eye(corr_matrix.shape[0], dtype=bool)]
            max_abs = float(np.max(np.abs(off_diag))) if off_diag.size else 0.0
            mean_abs = float(np.mean(np.abs(off_diag))) if off_diag.size else 0.0
            lines.append(
                f"Missingness indicator correlation: max |r|={max_abs:.2f}, mean |r|={mean_abs:.2f}"
            )
            if max_abs >= 0.6:
                tags.append("JOINT_DROPOUT_ACROSS_FIELDS")
            if mean_abs >= 0.35 and len(corr_cols) >= 3:
                tags.append("LIKELY_LOW_RANK_STRUCTURE")

    # Group concentration heuristic
    disparity = group_missingness.get("disparity", {})
    if disparity:
        max_spread = max(v.get("spread", 0.0) for v in disparity.values())
        lines.append(f"Max group-level missingness spread: {max_spread:.1%}")
        if max_spread >= 0.4:
            tags.append("POSSIBLE_MNAR_RISK")

    # Deduplicate while preserving order
    deduped_tags: List[str] = []
    for tag in tags:
        if tag not in deduped_tags:
            deduped_tags.append(tag)
    report["summary_tags"] = deduped_tags

    lines.append("Recommendation tags:")
    if deduped_tags:
        for tag in deduped_tags:
            lines.append(f"- {tag}")
    else:
        lines.append("- NONE")

    return "\n".join(lines)


def save_artifacts(report: Dict[str, Any], out_dir: str | Path) -> Dict[str, str]:
    """Save JSON report and heatmap plots."""
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    json_path = out_path / "missingness_report.json"
    with json_path.open("w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    saved = {"json_report": str(json_path)}

    plot_data = report.get("plot_data", {})
    columns_sorted = plot_data.get("columns_sorted", [])
    matrix = np.array(plot_data.get("missingness_matrix_sample", []), dtype=float)
    if matrix.size and columns_sorted:
        fig, ax = plt.subplots(figsize=(max(8, len(columns_sorted) * 0.35), 6))
        ax.imshow(matrix.T, aspect="auto", interpolation="nearest", cmap="Greys")
        ax.set_title("Missingness Matrix (Column-Sorted Sample)")
        ax.set_xlabel("Sampled Rows")
        ax.set_ylabel("Columns")
        ax.set_yticks(range(len(columns_sorted)))
        ax.set_yticklabels(columns_sorted, fontsize=8)
        heatmap_path = out_path / "missingness_heatmap.png"
        fig.tight_layout()
        fig.savefig(heatmap_path, dpi=150)
        plt.close(fig)
        saved["missingness_heatmap"] = str(heatmap_path)

    corr_cols = plot_data.get("indicator_corr_columns", [])
    corr_matrix = np.array(plot_data.get("indicator_corr_matrix", []), dtype=float)
    if corr_matrix.size and corr_cols:
        fig, ax = plt.subplots(figsize=(max(6, len(corr_cols) * 0.45), max(5, len(corr_cols) * 0.45)))
        im = ax.imshow(corr_matrix, vmin=-1.0, vmax=1.0, cmap="coolwarm")
        ax.set_title("Missingness Indicator Correlation")
        ax.set_xticks(range(len(corr_cols)))
        ax.set_xticklabels(corr_cols, rotation=90, fontsize=8)
        ax.set_yticks(range(len(corr_cols)))
        ax.set_yticklabels(corr_cols, fontsize=8)
        fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
        corr_path = out_path / "missingness_indicator_corr_heatmap.png"
        fig.tight_layout()
        fig.savefig(corr_path, dpi=150)
        plt.close(fig)
        saved["indicator_corr_heatmap"] = str(corr_path)

    return saved


def _estimate_range_from_elevation(elevation_deg: pd.Series, altitude_km: float = 500.0) -> pd.Series:
    """Approximate slant range from elevation angle using a simple Earth-geometry model."""
    earth_radius_km = 6371.0
    el_rad = np.radians(pd.to_numeric(elevation_deg, errors="coerce"))
    return -earth_radius_km * np.sin(el_rad) + np.sqrt(
        (earth_radius_km * np.sin(el_rad)) ** 2 + altitude_km**2 + 2 * earth_radius_km * altitude_km
    )


def _assign_track_ids(df: pd.DataFrame, gap_threshold_seconds: float = 120.0) -> pd.Series:
    """Assign numeric track IDs using sat + sensor/location + time gap rules."""
    if "obTime" not in df.columns or "satNo" not in df.columns:
        return pd.Series([None] * len(df), index=df.index, dtype="object")

    work = df.copy()
    work["obTime"] = pd.to_datetime(work["obTime"], errors="coerce")
    work = work.sort_values(["satNo", "obTime"]).reset_index()

    if "idSensor" in work.columns:
        sensor_key = work["idSensor"].fillna("")
    elif "sensorName" in work.columns:
        sensor_key = work["sensorName"].fillna("")
    elif all(col in work.columns for col in ["senlat", "senlon", "senalt"]):
        sensor_key = (
            work["senlat"].astype(str) + "|" + work["senlon"].astype(str) + "|" + work["senalt"].astype(str)
        )
    else:
        sensor_key = pd.Series([""] * len(work))

    work["_group_key"] = work["satNo"].astype(str) + "|" + sensor_key.astype(str)
    assigned = pd.Series([None] * len(work), index=work.index, dtype="object")
    track_counter = 1

    for _, group in work.groupby("_group_key", sort=False):
        previous_time = None
        current_track_rows: List[int] = []
        for row_idx, row in group.iterrows():
            current_time = row["obTime"]
            if pd.isna(current_time):
                continue
            if previous_time is None:
                current_track_rows = [row_idx]
            else:
                gap_seconds = (current_time - previous_time).total_seconds()
                if gap_seconds <= gap_threshold_seconds:
                    current_track_rows.append(row_idx)
                else:
                    for idx in current_track_rows:
                        assigned.iloc[idx] = track_counter
                    track_counter += 1
                    current_track_rows = [row_idx]
            previous_time = current_time
        for idx in current_track_rows:
            assigned.iloc[idx] = track_counter
        if current_track_rows:
            track_counter += 1

    assigned.index = work["index"]
    return assigned.sort_index()


def apply_missingness_driven_preprocessing(
    df: pd.DataFrame,
    report: Optional[Dict[str, Any]] = None,
    sparse_threshold: int = 100,
) -> tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Apply deterministic preprocessing based on observed missingness.

    Runtime rules mirror the current practical production methodology:
    - Fill `range_km` from elevation where derivable
    - Derive `range_rate_km_s` from consecutive observations, then mean-fill remainder
    - Fill `trackId` by grouping observations into tracks
    - Flag sparse satellites for potential simulation
    """
    if df is None:
        raise ValueError("df must be a DataFrame")
    if df.empty:
        return df.copy(), {
            "status": "empty_input",
            "range_km_filled": 0,
            "range_rate_physics_filled": 0,
            "range_rate_mean_filled": 0,
            "track_id_filled": 0,
            "sparse_satellites": [],
            "sparse_threshold": sparse_threshold,
        }

    work = df.copy()
    metadata: Dict[str, Any] = {
        "status": "success",
        "range_km_filled": 0,
        "range_rate_physics_filled": 0,
        "range_rate_mean_filled": 0,
        "track_id_filled": 0,
        "sparse_satellites": [],
        "sparse_threshold": sparse_threshold,
        "applied_steps": [],
        "trigger_tags": (report or {}).get("summary_tags", []),
    }

    # Normalize useful columns if they are absent.
    if "range_km" not in work.columns:
        if "range" in work.columns:
            work["range_km"] = pd.to_numeric(work["range"], errors="coerce")
        else:
            work["range_km"] = np.nan

    if "range_rate_km_s" not in work.columns:
        work["range_rate_km_s"] = np.nan

    if "trackId" not in work.columns:
        work["trackId"] = np.nan
    work["trackId"] = work["trackId"].astype("object")

    # 1) Fill range_km from elevation if missing
    if "elevation" in work.columns:
        range_missing_mask = work["range_km"].isna()
        if range_missing_mask.any():
            estimated = _estimate_range_from_elevation(work.loc[range_missing_mask, "elevation"])
            work.loc[range_missing_mask, "range_km"] = estimated
            # Keep legacy UDL-style `range` in sync when present.
            if "range" in work.columns:
                work.loc[range_missing_mask, "range"] = work.loc[range_missing_mask, "range_km"]
            metadata["range_km_filled"] = int(range_missing_mask.sum() - work["range_km"].isna().sum())
            if metadata["range_km_filled"] > 0:
                metadata["applied_steps"].append("FILL_RANGE_KM_FROM_ELEVATION")

    # 2) Derive range_rate_km_s from consecutive observations (per satellite)
    if "satNo" in work.columns and "obTime" in work.columns:
        work["obTime"] = pd.to_datetime(work["obTime"], errors="coerce")
        for sat_id, sat_idx in work.groupby("satNo").groups.items():
            sat_rows = work.loc[sat_idx].sort_values("obTime")
            times = sat_rows["obTime"].tolist()
            ranges = pd.to_numeric(sat_rows["range_km"], errors="coerce").to_numpy()
            derived = pd.Series(np.nan, index=sat_rows.index, dtype=float)
            for i in range(1, len(sat_rows)):
                if pd.isna(ranges[i]) or pd.isna(ranges[i - 1]) or pd.isna(times[i]) or pd.isna(times[i - 1]):
                    continue
                dt_seconds = (times[i] - times[i - 1]).total_seconds()
                if 0 < dt_seconds <= 120:
                    rate = (ranges[i] - ranges[i - 1]) / dt_seconds
                    if abs(rate) <= 8.0:
                        derived.iloc[i] = rate
            fill_mask = work.loc[sat_rows.index, "range_rate_km_s"].isna() & derived.notna()
            work.loc[sat_rows.index[fill_mask], "range_rate_km_s"] = derived.loc[fill_mask]
            metadata["range_rate_physics_filled"] += int(fill_mask.sum())

        if metadata["range_rate_physics_filled"] > 0:
            metadata["applied_steps"].append("DERIVE_RANGE_RATE_FROM_CONSECUTIVE_OBS")

        # 3) Mean-fill any remaining range_rate gaps
        rr_missing_mask = work["range_rate_km_s"].isna()
        rr_mean = pd.to_numeric(work["range_rate_km_s"], errors="coerce").mean()
        if rr_missing_mask.any() and not pd.isna(rr_mean):
            work.loc[rr_missing_mask, "range_rate_km_s"] = rr_mean
            metadata["range_rate_mean_filled"] = int(rr_missing_mask.sum())
            if metadata["range_rate_mean_filled"] > 0:
                metadata["applied_steps"].append("MEAN_FILL_RANGE_RATE")

    # 4) Assign track IDs when missing
    track_missing_mask = work["trackId"].isna()
    if track_missing_mask.any():
        assigned = _assign_track_ids(work)
        to_fill = track_missing_mask & assigned.notna()
        work.loc[to_fill, "trackId"] = assigned[to_fill]
        metadata["track_id_filled"] = int(to_fill.sum())
        if metadata["track_id_filled"] > 0:
            metadata["applied_steps"].append("ASSIGN_TRACK_IDS")

    # 5) Sparse satellite detection
    if "satNo" in work.columns:
        counts = work.groupby("satNo").size()
        sparse = counts[counts < sparse_threshold]
        metadata["sparse_satellites"] = [int(s) for s in sparse.index.tolist()]
        metadata["sparse_satellite_count"] = int(len(sparse))
        metadata["sparse_min_observation_count"] = int(sparse.min()) if len(sparse) else None
        if len(sparse) > 0:
            metadata["applied_steps"].append("FLAG_SPARSE_SATELLITES")

    return work, metadata
