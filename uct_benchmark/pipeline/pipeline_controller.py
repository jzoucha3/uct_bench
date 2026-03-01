"""Deterministic diagnose-then-route controller for the UCT pipeline."""

from __future__ import annotations

from dataclasses import asdict, is_dataclass
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd

from .routing_methods import (
    DEPENDENCY_BASED_MI,
    LEAVE_AS_MISSING,
    PHYSICS_PROPAGATION,
    RESIMULATE_MEASUREMENTS,
    RoutingConfig,
    SIMPLE_UNIVARIATE,
    STATE_SPACE_SMOOTHING,
    STRUCTURAL_MISSING,
    TIME_SERIES_INTERP,
    route_column,
)


# Method family mapping (future implementation guidance only; not executed):
# - SIMPLE_UNIVARIATE -> mean/median/mode + missing-flag
# - DEPENDENCY_BASED_MI -> MICE / regression MI (predictive mean matching for continuous;
#   multinomial/logistic for categorical; Poisson/NegBin for counts)
# - LOW_RANK_COMPLETION -> matrix completion / factor model
# - TIME_SERIES_INTERP -> linear/spline for short gaps
# - STATE_SPACE_SMOOTHING -> Kalman/RTS smoother
# - PHYSICS_PROPAGATION -> propagate orbital state with dynamics + covariance sampling
# - RESIMULATE_MEASUREMENTS -> regenerate synthetic observations from sensor forward model
#   (Orekit/TLE) with optional noise
# - STRUCTURAL_MISSING -> do not impute statistically; regenerate via association logic
#   or leave missing
# - LEAVE_AS_MISSING -> keep NA + add missing-flag features


def _as_sim_cfg_dict(simulation_config: Any) -> Dict[str, Any]:
    """Normalize simulation config to a dict."""
    if simulation_config is None:
        return {}
    if isinstance(simulation_config, dict):
        return dict(simulation_config)
    if is_dataclass(simulation_config):
        return asdict(simulation_config)
    return {
        key: getattr(simulation_config, key)
        for key in dir(simulation_config)
        if not key.startswith("_") and not callable(getattr(simulation_config, key))
    }


def _explicit_enabled_state(simulation_config: Any) -> Optional[bool]:
    """Return True/False if explicitly set, else None."""
    if simulation_config is None:
        return None
    if isinstance(simulation_config, dict):
        if "enabled" in simulation_config:
            return bool(simulation_config["enabled"])
        return None
    if hasattr(simulation_config, "enabled"):
        return bool(getattr(simulation_config, "enabled"))
    return None


def _decision(step: str, ran: bool, reason: str, metrics: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return {"step": step, "ran": bool(ran), "reason": reason, "metrics": metrics or {}}


def _estimate_range_from_elevation(elevation_deg: pd.Series, altitude_km: float = 500.0) -> pd.Series:
    """Physics-based slant range approximation."""
    earth_radius_km = 6371.0
    el_rad = np.radians(pd.to_numeric(elevation_deg, errors="coerce"))
    return -earth_radius_km * np.sin(el_rad) + np.sqrt(
        (earth_radius_km * np.sin(el_rad)) ** 2 + altitude_km**2 + 2 * earth_radius_km * altitude_km
    )


def _fill_range_km(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Always attempt range_km physics fill when possible."""
    work = df.copy()
    if "range_km" not in work.columns:
        work["range_km"] = pd.to_numeric(work["range"], errors="coerce") if "range" in work.columns else np.nan
    if "elevation" not in work.columns:
        return work, {"filled": 0, "skipped": True}

    missing_mask = work["range_km"].isna()
    if not missing_mask.any():
        return work, {"filled": 0, "skipped": False}

    estimated = _estimate_range_from_elevation(work.loc[missing_mask, "elevation"])
    work.loc[missing_mask, "range_km"] = estimated
    if "range" in work.columns:
        work.loc[missing_mask, "range"] = work.loc[missing_mask, "range_km"]
    return work, {"filled": int(missing_mask.sum()), "skipped": False}


def _fill_range_rate_stage1(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Always attempt adjacent derivative first."""
    work = df.copy()
    if "range_rate_km_s" not in work.columns:
        work["range_rate_km_s"] = np.nan
    if "satNo" not in work.columns or "obTime" not in work.columns or "range_km" not in work.columns:
        return work, {"physics_filled": 0, "skipped": True}

    work["obTime"] = pd.to_datetime(work["obTime"], errors="coerce")
    physics_filled = 0
    for _, sat_rows in work.groupby("satNo"):
        sat_rows = sat_rows.sort_values("obTime")
        idx = sat_rows.index.tolist()
        times = sat_rows["obTime"].tolist()
        ranges = pd.to_numeric(sat_rows["range_km"], errors="coerce").to_numpy()

        for i in range(1, len(idx)):
            if (
                pd.isna(ranges[i])
                or pd.isna(ranges[i - 1])
                or pd.isna(times[i])
                or pd.isna(times[i - 1])
            ):
                continue
            dt = (times[i] - times[i - 1]).total_seconds()
            if not (0 < dt <= 120):
                continue
            rate = (ranges[i] - ranges[i - 1]) / dt
            if abs(rate) <= 8.0 and pd.isna(work.at[idx[i], "range_rate_km_s"]):
                work.at[idx[i], "range_rate_km_s"] = rate
                physics_filled += 1

    return work, {"physics_filled": physics_filled, "skipped": False}


def _fill_range_rate_stage2(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Mean-fill any remaining range-rate gaps when an observed mean exists."""
    work = df.copy()
    if "range_rate_km_s" not in work.columns:
        work["range_rate_km_s"] = np.nan

    missing_mask = work["range_rate_km_s"].isna()
    rr_mean = pd.to_numeric(work["range_rate_km_s"], errors="coerce").mean()
    mean_fill_count = 0
    if missing_mask.any() and not pd.isna(rr_mean):
        mean_fill_count = int(missing_mask.sum())
        work.loc[missing_mask, "range_rate_km_s"] = rr_mean

    return work, {"mean_filled": mean_fill_count, "mean_available": not pd.isna(rr_mean), "skipped": False}


def _assign_track_ids(df: pd.DataFrame, gap_threshold_seconds: float = 120.0) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Always attempt track grouping fill when missing."""
    work = df.copy()
    if "trackId" not in work.columns:
        work["trackId"] = np.nan
    work["trackId"] = work["trackId"].astype("object")

    if "satNo" not in work.columns or "obTime" not in work.columns:
        return work, {"filled": 0, "skipped": True}

    missing_mask = work["trackId"].isna()
    if not missing_mask.any():
        return work, {"filled": 0, "skipped": False}

    work["obTime"] = pd.to_datetime(work["obTime"], errors="coerce")

    if "idSensor" in work.columns:
        sensor_key = work["idSensor"].fillna("")
    elif "sensorName" in work.columns:
        sensor_key = work["sensorName"].fillna("")
    elif all(col in work.columns for col in ["senlat", "senlon", "senalt"]):
        sensor_key = (
            work["senlat"].astype(str) + "|" + work["senlon"].astype(str) + "|" + work["senalt"].astype(str)
        )
    else:
        sensor_key = pd.Series([""] * len(work), index=work.index)

    temp = work.assign(_group_key=work["satNo"].astype(str) + "|" + sensor_key.astype(str))
    temp = temp.sort_values(["_group_key", "obTime"]).reset_index()
    track_counter = 1
    assigned = pd.Series([None] * len(temp), index=temp.index, dtype="object")

    for _, group in temp.groupby("_group_key", sort=False):
        current_rows: List[int] = []
        previous_time = None
        for temp_idx, row in group.iterrows():
            current_time = row["obTime"]
            if pd.isna(current_time):
                continue
            if previous_time is None:
                current_rows = [temp_idx]
            else:
                gap = (current_time - previous_time).total_seconds()
                if gap > gap_threshold_seconds:
                    for ridx in current_rows:
                        assigned.iloc[ridx] = track_counter
                    track_counter += 1
                    current_rows = [temp_idx]
                else:
                    current_rows.append(temp_idx)
            previous_time = current_time
        if current_rows:
            for ridx in current_rows:
                assigned.iloc[ridx] = track_counter
            track_counter += 1

    assigned.index = temp["index"]
    assigned = assigned.sort_index()
    fill_mask = work["trackId"].isna() & assigned.notna()
    work.loc[fill_mask, "trackId"] = assigned[fill_mask]
    return work, {"filled": int(fill_mask.sum()), "skipped": False}


def _find_sparse_satellites(df: pd.DataFrame, threshold: int = 100) -> List[int]:
    """Find sparse satellites after deterministic preprocessing."""
    if "satNo" not in df.columns or df.empty:
        return []
    counts = df.groupby("satNo").size()
    sparse = counts[counts < threshold]
    return [int(x) for x in sparse.index.tolist()]


def _infer_role(col: str, series: Optional[pd.Series] = None) -> str:
    """Infer a routing role when the inspection report does not specify one."""
    lower = col.lower()
    if lower in {"id", "trackid", "track_id"} or lower.endswith("_id") or lower.endswith("id"):
        if lower in {"trackid", "track_id"}:
            return "relational_id"
        return "id_like"
    if lower in {"x", "y", "z", "vx", "vy", "vz", "x_pos", "y_pos", "z_pos", "x_vel", "y_vel", "z_vel"}:
        return "orbital_state"
    if lower in {"ra", "declination", "azimuth", "elevation", "range", "range_km", "range_rate_km_s"}:
        return "measurement"
    if lower in {"obtime", "timestamp", "time"}:
        return "time_series"
    if series is not None:
        if pd.api.types.is_numeric_dtype(series):
            if "count" in lower:
                return "count"
            return "continuous"
    return "categorical"


def _build_column_diag(
    col: str,
    df: pd.DataFrame,
    inspection_report: Dict[str, Any],
) -> Dict[str, Any]:
    """Merge report-level metrics into a per-column diagnostic payload."""
    per_col_missing = inspection_report.get("per_col_missing_frac", inspection_report.get("missing_fraction", {}))
    temporal_stats = inspection_report.get("temporal_run_stats", inspection_report.get("temporal_runs", {}))
    group_var = inspection_report.get("group_missingness_var", {})
    auc = inspection_report.get("missingness_model_auc", {})
    autocorr = inspection_report.get("autocorr_lag1", {})
    role_map = inspection_report.get("column_roles", {})

    temporal_entry = temporal_stats.get(col, {})
    if temporal_entry and "pct_missing_longest_run" not in temporal_entry and "pct_missing_in_longest_run" in temporal_entry:
        temporal_entry = dict(temporal_entry)
        temporal_entry["pct_missing_longest_run"] = temporal_entry.get("pct_missing_in_longest_run", 0.0)

    diag = {
        "column_name": col,
        "role": role_map.get(col) or _infer_role(col, df[col] if col in df.columns else None),
        "missing_frac": per_col_missing.get(col, float(df[col].isna().mean()) if col in df.columns else 0.0),
        "max_run_len": temporal_entry.get("max_run_len", temporal_entry.get("max_run_length", 0)),
        "pct_missing_longest_run": temporal_entry.get("pct_missing_longest_run", 0.0),
        "missing_indicator_corr_max": inspection_report.get("missing_indicator_corr_max", 0.0),
        "group_missingness_var": group_var,
        "missingness_model_auc": auc,
        "autocorr_lag1": autocorr,
        "low_rank_score": inspection_report.get("low_rank_score", 0.0),
        "tags": inspection_report.get("tags", []) or inspection_report.get("summary_tags", []),
    }
    return diag


def _cap_simulated_ratio(df: pd.DataFrame, max_ratio: float) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Enforce a synthetic ratio cap by trimming simulated rows if needed."""
    if "is_simulated" not in df.columns or df.empty:
        return df, {"capped": False, "simulated_rows_removed": 0}

    sim_mask = df["is_simulated"].fillna(False).astype(bool)
    simulated_count = int(sim_mask.sum())
    original_count = int((~sim_mask).sum())
    if original_count == 0:
        return df, {"capped": False, "simulated_rows_removed": 0}

    max_allowed = int(original_count * max_ratio / max(1e-9, (1.0 - max_ratio)))
    if simulated_count <= max_allowed:
        return df, {"capped": False, "simulated_rows_removed": 0}

    simulated_df = df.loc[sim_mask].copy()
    if "obTime" in simulated_df.columns:
        simulated_df = simulated_df.sort_values("obTime")
    keep_sim = simulated_df.head(max_allowed)
    kept_ids = set(keep_sim.index.tolist())
    keep_mask = (~sim_mask) | df.index.to_series().isin(kept_ids)
    capped_df = df.loc[keep_mask].reset_index(drop=True)
    return capped_df, {"capped": True, "simulated_rows_removed": simulated_count - max_allowed}


def run_pipeline(
    df: pd.DataFrame,
    simulation_config: Any,
    inspection_report: Dict[str, Any],
    cfg: RoutingConfig,
    *,
    apply_simulation_to_gaps: Optional[Callable[..., Any]] = None,
) -> Tuple[pd.DataFrame, List[Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    """
    Run deterministic preprocessing, build routing table, and decide simulation.

    The existing deterministic preprocessing always runs first. For columns other
    than the current hard-coded fields, this controller only emits the planned
    routing family in `routing_table` and does not perform those advanced methods.
    """
    if df is None:
        raise ValueError("df must be a DataFrame")

    decisions: List[Dict[str, Any]] = []
    work = df.copy()

    # 1) Existing deterministic preprocessing first, same order as current pipeline.
    work, range_meta = _fill_range_km(work)
    decisions.append(
        _decision(
            "RANGE_KM_PHYSICS_FILL",
            not range_meta.get("skipped", False),
            "physics-based geometric estimate from elevation" if not range_meta.get("skipped", False) else "missing required columns",
            range_meta,
        )
    )

    work, rr1_meta = _fill_range_rate_stage1(work)
    decisions.append(
        _decision(
            "RANGE_RATE_ADJACENT_DERIVATIVE",
            not rr1_meta.get("skipped", False),
            "adjacent derivative per satNo" if not rr1_meta.get("skipped", False) else "missing required columns",
            rr1_meta,
        )
    )

    work, rr2_meta = _fill_range_rate_stage2(work)
    decisions.append(
        _decision(
            "RANGE_RATE_MEAN_FALLBACK",
            True,
            "mean fallback for remaining range_rate_km_s gaps when an observed mean exists",
            rr2_meta,
        )
    )

    work, track_meta = _assign_track_ids(work)
    decisions.append(
        _decision(
            "TRACK_ID_GROUPING_FILL",
            not track_meta.get("skipped", False),
            "group by satNo and sensor identity; split when gap > 120s" if not track_meta.get("skipped", False) else "missing required columns",
            track_meta,
        )
    )

    sparse_satellites = _find_sparse_satellites(work, threshold=100)
    decisions.append(
        _decision(
            "SPARSE_SATELLITE_CHECK",
            True,
            "post-preprocessing sparse coverage check",
            {"sparse_satellites": sparse_satellites, "threshold": 100},
        )
    )

    # 2) Build routing table for all columns.
    routing_table: Dict[str, Dict[str, Any]] = {}
    for col in work.columns:
        diag = _build_column_diag(col, work, inspection_report or {})
        routing_table[col] = route_column(col, diag, cfg)
    decisions.append(
        _decision(
            "ROUTING_TABLE_BUILT",
            True,
            "diagnostic routing computed for all columns; advanced methods not executed yet",
            {"column_count": len(routing_table)},
        )
    )

    # 3) Simulation decision logic.
    explicit_enabled = _explicit_enabled_state(simulation_config)
    sim_cfg = _as_sim_cfg_dict(simulation_config)
    tags = set((inspection_report or {}).get("tags", []) or (inspection_report or {}).get("summary_tags", []))
    per_col_missing = (inspection_report or {}).get("per_col_missing_frac", (inspection_report or {}).get("missing_fraction", {}))
    temporal_stats = (inspection_report or {}).get("temporal_run_stats", (inspection_report or {}).get("temporal_runs", {}))

    any_long_blocks = False
    for col, stats in temporal_stats.items():
        run_len = int(stats.get("max_run_len", stats.get("max_run_length", 0)))
        if run_len >= cfg.temporal_block_min_run:
            any_long_blocks = True
            break
    any_high_missing = any(float(v) >= cfg.high_missing for v in per_col_missing.values()) if per_col_missing else False

    simulation_requested = explicit_enabled is True
    simulation_auto_sparse = (explicit_enabled is None) and bool(sparse_satellites)
    simulation_auto_tag = "HIGH_MISSINGNESS_REQUIRES_RESIM" in tags
    simulation_auto_temporal = (
        "TEMPORAL_BLOCK_MISSINGNESS" in tags and (any_long_blocks or any_high_missing)
    )
    should_run_simulation = any(
        [simulation_requested, simulation_auto_sparse, simulation_auto_tag, simulation_auto_temporal]
    )

    sim_reason_parts = []
    if simulation_requested:
        sim_reason_parts.append("explicitly_enabled")
    if simulation_auto_sparse:
        sim_reason_parts.append("sparse_satellite_auto_enable")
    if simulation_auto_tag:
        sim_reason_parts.append("high_missingness_tag")
    if simulation_auto_temporal:
        sim_reason_parts.append("temporal_block_rule")

    tle_available = {"line1", "line2"}.issubset(set(work.columns))
    if should_run_simulation and not tle_available:
        decisions.append(
            _decision(
                "SIMULATION_SKIPPED_MISSING_TLE",
                False,
                "simulation requested but TLE/element data missing in dataframe",
                {"reasons": sim_reason_parts},
            )
        )
        should_run_simulation = False

    if should_run_simulation and apply_simulation_to_gaps is None:
        decisions.append(
            _decision(
                "SIMULATION_SKIPPED_NO_CALLBACK",
                False,
                "simulation requested but apply_simulation_to_gaps callback not provided",
                {"reasons": sim_reason_parts},
            )
        )
        should_run_simulation = False

    if should_run_simulation and apply_simulation_to_gaps is not None:
        result = apply_simulation_to_gaps(work.copy(), simulation_config=sim_cfg)
        sim_meta: Dict[str, Any] = {}
        if isinstance(result, tuple) and len(result) == 2:
            sim_df, sim_meta = result
        else:
            sim_df = result
        if not isinstance(sim_df, pd.DataFrame):
            sim_df = work.copy()
            sim_meta = {"warning": "callback did not return DataFrame"}

        capped_df, cap_meta = _cap_simulated_ratio(
            sim_df,
            float(sim_cfg.get("max_synthetic_ratio", 0.5)),
        )
        work = capped_df
        decisions.append(
            _decision(
                "SIMULATION",
                True,
                ",".join(sim_reason_parts) if sim_reason_parts else "requested",
                {"callback_meta": sim_meta, "cap": cap_meta, "config": sim_cfg},
            )
        )
    else:
        decisions.append(
            _decision(
                "SIMULATION_DECISION",
                False,
                "simulation not triggered" if not sim_reason_parts else "simulation requested but skipped",
                {
                    "reasons": sim_reason_parts,
                    "explicit_enabled": explicit_enabled,
                    "sparse_satellites": sparse_satellites,
                },
            )
        )

    return work, decisions, routing_table


def _demo_apply_simulation_to_gaps(df: pd.DataFrame, simulation_config: Optional[Dict[str, Any]] = None):
    """Minimal demo callback that appends one simulated row."""
    if df.empty:
        return df.copy(), {"added": 0}
    demo = df.copy()
    row = demo.iloc[[0]].copy()
    if "is_simulated" not in demo.columns:
        demo["is_simulated"] = False
    row["is_simulated"] = True
    if "obTime" in row.columns:
        row["obTime"] = pd.to_datetime(row["obTime"], errors="coerce") + pd.Timedelta(seconds=30)
    demo = pd.concat([demo, row], ignore_index=True)
    return demo, {"added": 1, "simulation_config": simulation_config or {}}


if __name__ == "__main__":
    base = pd.Timestamp("2026-01-01T00:00:00Z")
    demo_df = pd.DataFrame(
        {
            "satNo": [101] * 4 + [202] * 2,
            "obTime": [base + pd.Timedelta(seconds=s) for s in [0, 30, 60, 400, 0, 30]],
            "idSensor": ["S1"] * 6,
            "elevation": [10.0, 20.0, 25.0, 30.0, 15.0, 18.0],
            "range_km": [np.nan] * 6,
            "range_rate_km_s": [np.nan] * 6,
            "trackId": [np.nan] * 6,
            "flux": [1.0, np.nan, 0.8, np.nan, 0.7, 0.6],
            "category": ["A", "A", None, "B", "B", None],
            "line1": ["1 DEMO"] * 6,
            "line2": ["2 DEMO"] * 6,
        }
    )

    fake_inspection_report = {
        "tags": ["TEMPORAL_BLOCK_MISSINGNESS", "HIGH_MISSINGNESS_REQUIRES_RESIM"],
        "per_col_missing_frac": {
            "range_km": 1.0,
            "range_rate_km_s": 1.0,
            "trackId": 1.0,
            "flux": 0.33,
            "category": 0.33,
        },
        "temporal_run_stats": {
            "range_km": {"max_run_len": 12, "pct_missing_longest_run": 0.50},
            "range_rate_km_s": {"max_run_len": 12, "pct_missing_longest_run": 0.50},
            "flux": {"max_run_len": 1, "pct_missing_longest_run": 0.02},
        },
        "missing_indicator_corr_max": 0.65,
        "group_missingness_var": {"flux": 0.01, "category": 0.03},
        "missingness_model_auc": {"flux": 0.75, "category": 0.60},
        "autocorr_lag1": {"range_km": 0.9, "range_rate_km_s": 0.8, "flux": 0.1},
        "low_rank_score": 0.92,
    }

    out_df, decisions, routing = run_pipeline(
        demo_df,
        simulation_config=None,
        inspection_report=fake_inspection_report,
        cfg=RoutingConfig(),
        apply_simulation_to_gaps=_demo_apply_simulation_to_gaps,
    )

    print("Decisions:")
    for entry in decisions:
        print(f"- {entry['step']}: ran={entry['ran']} reason={entry['reason']} metrics={entry['metrics']}")

    print("\nRouting summary:")
    for col, decision in routing.items():
        print(f"- {col}: {decision['route']} (fallback={decision['fallback']})")

    print("\nOutput preview:")
    print(out_df[["satNo", "range_km", "range_rate_km_s", "trackId"]].head(8).to_string(index=False))
