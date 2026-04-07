"""
Statevector-first pipeline path.

This mode avoids the standard eoobservation-first `generateDataset(...)` flow and
instead starts from `statevector` data.
"""

from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Dict, List, Tuple

import pandas as pd
from loguru import logger

from uct_benchmark.api.apiIntegration import UDLQuery, asyncUDLBatchQuery, datetimeToUDL
from uct_benchmark.data.dataManipulation import apply_downsampling, apply_simulation_to_gaps
from uct_benchmark.data.missingness import (
    apply_missingness_driven_preprocessing,
    inspect_missingness,
    save_artifacts,
    summarize_inspection,
)
from uct_benchmark.database.connection import DatabaseManager
from uct_benchmark.evaluation import (
    evaluate_observation_datasets,
    save_observation_evaluation_artifacts,
)
from uct_benchmark.pipeline.orchestration import (
    _build_tier_configs,
    _parse_time_config,
    _resolve_search_strategy,
    _select_satellites,
)
from uct_benchmark.settings import DownsampleConfig, SimulationConfig


def _resolve_exact_time_window(config: Dict[str, Any]) -> Tuple[datetime, datetime]:
    """Honor explicit start/end dates when provided; otherwise fall back to timeframe."""
    timeframe, timeunit, end_time = _parse_time_config(config)

    start_date_str = config.get("start_date")
    end_date_str = config.get("end_date")
    if start_date_str and end_date_str:
        try:
            if "T" in start_date_str:
                start_dt = datetime.fromisoformat(start_date_str.replace("Z", "+00:00"))
            else:
                start_dt = datetime.fromisoformat(f"{start_date_str}T00:00:00")
            if "T" in end_date_str:
                end_dt = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
            else:
                end_dt = datetime.fromisoformat(f"{end_date_str}T23:59:59")
            return start_dt, end_dt
        except (TypeError, ValueError) as exc:
            logger.warning(f"Failed to parse exact start/end window from config: {exc}")

    if end_time == "now":
        end_dt = datetime.utcnow()
    else:
        end_dt = end_time
    start_dt = end_dt - pd.Timedelta(**{timeunit: timeframe})
    return start_dt, end_dt


def _fetch_statevectors_windowed_or_range(
    token: str,
    sat_ids: List[int],
    start_time: datetime,
    end_time: datetime,
    dt: float,
    search_strategy: str,
    regime: str,
) -> pd.DataFrame:
    """Query statevectors using the dedicated statevector-first request shape."""
    if search_strategy == "windowed":
        window_hours = 24 if regime == "GEO" else 12
        all_windows = []
        current = start_time
        while current < end_time:
            window_end = min(current + pd.Timedelta(hours=window_hours), end_time)
            params = [
                {
                    "satNo": str(sat),
                    "epoch": f"{datetimeToUDL(current)}..{datetimeToUDL(window_end)}",
                }
                for sat in sat_ids
            ]
            try:
                window_df = asyncUDLBatchQuery(token, "statevector", params, dt=dt)
                if window_df is not None and not window_df.empty:
                    all_windows.append(window_df)
            except Exception as exc:
                logger.warning(f"Windowed statevector query failed: {exc}")
            current = window_end
        return pd.concat(all_windows, ignore_index=True) if all_windows else pd.DataFrame()

    params = [
        {
            "satNo": str(sat),
            "epoch": f"{datetimeToUDL(start_time)}..{datetimeToUDL(end_time)}",
        }
        for sat in sat_ids
    ]
    result = asyncUDLBatchQuery(token, "statevector", params, dt=dt)
    if result is not None and not result.empty:
        return result

    # Some tenants behave better with smaller epoch windows. Fall back to the
    # same chunked pattern even when the resolved strategy was fast/hybrid.
    logger.warning(
        "Single-range statevector query returned no data; retrying with windowed chunks."
    )
    return _fetch_statevectors_windowed_or_range(
        token=token,
        sat_ids=sat_ids,
        start_time=start_time,
        end_time=end_time,
        dt=dt,
        search_strategy="windowed",
        regime=regime,
    )


def _dedupe_statevectors(state_df: pd.DataFrame) -> pd.DataFrame:
    if state_df is None or state_df.empty:
        return pd.DataFrame()
    work = state_df.copy()
    if "epoch" in work.columns:
        work["epoch"] = pd.to_datetime(work["epoch"], errors="coerce")
    work = work.dropna(subset=["satNo", "epoch"])
    return work.drop_duplicates(subset=["satNo", "epoch"], keep="first").reset_index(drop=True)


def _build_obs_from_statevectors(state_df: pd.DataFrame) -> pd.DataFrame:
    """Create an observation-like dataframe from statevectors for downstream processing."""
    if state_df is None or state_df.empty:
        return pd.DataFrame()

    import numpy as np

    # Compute geocentric RA/Dec from ECI position (xpos, ypos, zpos in km).
    # This gives the direction from Earth's center to the satellite, which is
    # what orbitCoverage expects. Without this, all observations get ra=0/dec=0
    # and coverage computes as 0 for every satellite.
    x = pd.to_numeric(state_df.get("xpos", 0), errors="coerce").fillna(0).to_numpy()
    y = pd.to_numeric(state_df.get("ypos", 0), errors="coerce").fillna(0).to_numpy()
    z = pd.to_numeric(state_df.get("zpos", 0), errors="coerce").fillna(0).to_numpy()
    r = np.sqrt(x**2 + y**2 + z**2)
    r_safe = np.where(r > 0, r, 1.0)  # avoid division by zero
    ra_deg = np.degrees(np.arctan2(y, x)) % 360
    dec_deg = np.degrees(np.arcsin(np.clip(z / r_safe, -1.0, 1.0)))

    obs_df = pd.DataFrame(
        {
            "satNo": pd.to_numeric(state_df["satNo"], errors="coerce"),
            "obTime": pd.to_datetime(state_df["epoch"], errors="coerce"),
            "idSensor": "STATE_VECTOR",
            "sensorName": "STATE_VECTOR",
            "senlat": 0.0,
            "senlon": 0.0,
            "senalt": 0.0,
            "ra": ra_deg,
            "declination": dec_deg,
            "range": 0.0,
            "range_km": 0.0,
            "range_rate_km_s": pd.NA,
            "trackId": pd.NA,
            "dataMode": "REAL",
            "observationSource": "statevector_first",
        }
    )
    obs_df = obs_df.dropna(subset=["satNo", "obTime"]).reset_index(drop=True)
    obs_df["satNo"] = obs_df["satNo"].astype(int)
    # Add an 'id' column which is expected by downstream operations like downsampling
    obs_df["id"] = obs_df.index.astype(str)
    return obs_df


def _fetch_current_elsets(token: str, sat_ids: List[int], dt: float) -> pd.DataFrame:
    if not sat_ids:
        return pd.DataFrame()

    try:
        current_df = UDLQuery(token, "elset/current", {"satNo": ",".join(map(str, sat_ids))})
        if current_df is not None and not current_df.empty:
            return current_df
    except Exception as exc:
        logger.warning(f"elset/current query failed; falling back to batched elset query: {exc}")

    params = [{"satNo": str(sat), "maxResults": 1} for sat in sat_ids]
    return asyncUDLBatchQuery(token, "elset", params, dt=dt)


def _save_to_database(
    dataset_id: int,
    db_path: str | None,
    obs_df: pd.DataFrame,
    state_df: pd.DataFrame,
    elset_df: pd.DataFrame,
) -> None:
    """Persist the generated dataframes using the existing repositories."""
    db = DatabaseManager(db_path=db_path)
    db.initialize()

    if obs_df is not None and not obs_df.empty:
        obs_insert = obs_df.copy()
        obs_insert["id"] = [
            f"svf-{dataset_id}-{idx}" for idx in range(len(obs_insert))
        ]
        obs_insert["sat_no"] = pd.to_numeric(obs_insert["satNo"], errors="coerce").astype("Int64")
        obs_insert["ob_time"] = pd.to_datetime(obs_insert["obTime"], errors="coerce")
        obs_insert["sensor_name"] = obs_insert.get("sensorName")
        obs_insert["data_mode"] = obs_insert.get("dataMode", "REAL")
        obs_insert["track_id"] = pd.to_numeric(obs_insert.get("trackId"), errors="coerce").astype("Int64")
        db.observations.bulk_insert(obs_insert)

    if state_df is not None and not state_df.empty:
        state_insert = state_df.copy()
        state_insert["sat_no"] = pd.to_numeric(state_insert.get("satNo"), errors="coerce").astype("Int64")
        alias_map = {
            "x_pos": ["x_pos", "xPos", "xpos", "x", "pos_x"],
            "y_pos": ["y_pos", "yPos", "ypos", "y", "pos_y"],
            "z_pos": ["z_pos", "zPos", "zpos", "z", "pos_z"],
            "x_vel": ["x_vel", "xVel", "xvel", "xDot", "xdot", "vx", "vel_x"],
            "y_vel": ["y_vel", "yVel", "yvel", "yDot", "ydot", "vy", "vel_y"],
            "z_vel": ["z_vel", "zVel", "zvel", "zDot", "zdot", "vz", "vel_z"],
            "source": ["source"],
            "data_mode": ["data_mode", "dataMode"],
        }
        for dst, candidates in alias_map.items():
            resolved = None
            for src in candidates:
                if src in state_insert.columns:
                    series = state_insert[src]
                    resolved = series if resolved is None else resolved.fillna(series)
            if resolved is not None:
                if dst in state_insert.columns:
                    state_insert[dst] = state_insert[dst].fillna(resolved)
                else:
                    state_insert[dst] = resolved
        state_insert["epoch"] = pd.to_datetime(state_insert["epoch"], errors="coerce")
        for col in ["x_pos", "y_pos", "z_pos", "x_vel", "y_vel", "z_vel"]:
            if col in state_insert.columns:
                state_insert[col] = pd.to_numeric(state_insert[col], errors="coerce")
        state_insert["source"] = state_insert.get("source", "UDL")
        state_insert["data_mode"] = state_insert.get("data_mode", "REAL")
        required_cols = ["sat_no", "epoch", "x_pos", "y_pos", "z_pos", "x_vel", "y_vel", "z_vel"]
        missing_required = [c for c in required_cols if c not in state_insert.columns]
        if missing_required:
            logger.warning(
                "Skipping state vector persistence; missing required columns after normalization: "
                f"{missing_required}"
            )
        else:
            state_insert = state_insert.dropna(subset=required_cols)
            if state_insert.empty:
                logger.warning(
                    "Skipping state vector persistence; no rows with complete position and velocity data."
                )
            else:
                db.state_vectors.bulk_insert(state_insert)

    if elset_df is not None and not elset_df.empty and {"line1", "line2"}.issubset(elset_df.columns):
        elset_insert = elset_df.copy()
        elset_insert["sat_no"] = pd.to_numeric(elset_insert.get("satNo"), errors="coerce").astype("Int64")
        if "epoch" in elset_insert.columns:
            elset_insert["epoch"] = pd.to_datetime(elset_insert["epoch"], errors="coerce")
        else:
            elset_insert["epoch"] = datetime.utcnow()
        elset_insert["source"] = "UDL"
        db.element_sets.bulk_insert(elset_insert)

    db.close()


def execute_statevector_first_pipeline(
    config: Dict[str, Any],
    dataset_id: int,
    *,
    dt: float = 0.5,
    evaluation_reference_data=None,
    db_path=None,
) -> Tuple[Any, Any, Any, Any, Any, Dict[str, Any], Dict[str, Any]]:
    """Run the statevector-first pipeline."""
    udl_token = os.getenv("UDL_TOKEN")
    if not udl_token:
        raise ValueError("Missing required environment variable: UDL_TOKEN.")

    satellites = _select_satellites(config)
    timeframe, timeunit, _ = _parse_time_config(config)
    tier = str(config.get("tier", "T2")).upper()
    regime = str(config.get("regime", "LEO")).upper()
    search_strategy = _resolve_search_strategy(
        requested=str(config.get("search_strategy", "auto")),
        satellite_count=len(satellites),
        timeframe=timeframe,
        timeunit=timeunit,
    )

    start_dt, end_dt = _resolve_exact_time_window(config)

    state_truth = _fetch_statevectors_windowed_or_range(
        udl_token,
        satellites,
        start_dt,
        end_dt,
        dt=dt,
        search_strategy=search_strategy,
        regime=regime,
    )
    state_truth = _dedupe_statevectors(state_truth)
    if state_truth.empty:
        raise ValueError(
            f"No statevector data returned for satellites {satellites} in {start_dt.isoformat()}..{end_dt.isoformat()}."
        )

    obs_truth = _build_obs_from_statevectors(state_truth)
    reference_obs = evaluation_reference_data.copy() if evaluation_reference_data is not None else obs_truth.copy()

    inspection_report = inspect_missingness(
        obs_truth,
        time_col="obTime" if "obTime" in obs_truth.columns else None,
        group_cols=[c for c in ["satNo", "idSensor"] if c in obs_truth.columns],
        id_cols=[c for c in ["trackId"] if c in obs_truth.columns],
        numeric_cols=[c for c in ["range_km", "range_rate_km_s", "ra", "declination"] if c in obs_truth.columns],
        categorical_cols=[c for c in ["sensorName", "dataMode"] if c in obs_truth.columns],
    )
    summary = summarize_inspection(inspection_report)
    artifact_dir = None
    try:
        report_slug = str(config.get("name", f"dataset-{dataset_id}")).replace(" ", "_")
        artifact_dir = save_artifacts(inspection_report, os.path.join("reports", "missingness", report_slug))
    except Exception as exc:
        logger.warning(f"Failed to save missingness artifacts: {exc}")

    obs_truth, preprocessing_metadata = apply_missingness_driven_preprocessing(
        obs_truth,
        report=inspection_report,
    )

    elset_truth = _fetch_current_elsets(udl_token, sorted(obs_truth["satNo"].unique().tolist()), dt=dt)

    downsample_config, simulation_config = _build_tier_configs(
        tier=tier,
        downsampling=config.get("downsampling"),
        simulation=config.get("simulation"),
    )

    downsampling_metadata = None
    if downsample_config and downsample_config.get("enabled"):
        ds_cfg = DownsampleConfig(
            target_coverage=downsample_config.get("target_coverage", 0.05),
            target_gap=downsample_config.get("target_gap", 2.0),
            max_obs_per_sat=downsample_config.get("max_obs_per_sat", 30),
            preserve_track_boundaries=downsample_config.get("preserve_tracks", True),
            seed=downsample_config.get("seed"),
        )
        obs_truth, downsampling_metadata = apply_downsampling(
            obs_truth,
            sat_params={},
            elset_data=elset_truth,
            config=ds_cfg,
            tier=tier,
        )

    sparse_satellites = preprocessing_metadata.get("sparse_satellites", [])
    tags = set(inspection_report.get("summary_tags", []))
    sim_requested = simulation_config is not None and simulation_config.get("enabled", False)
    sim_auto = False
    if not sim_requested and sparse_satellites:
        simulation_config = {
            "enabled": True,
            "apply_noise": True,
            "sensor_model": "GEODSS",
            "max_synthetic_ratio": 0.5,
            "auto_enabled_reason": "sparse_satellite_detection",
        }
        sim_auto = True
    if not sim_requested and "HIGH_MISSINGNESS_REQUIRES_RESIM" in tags:
        simulation_config = {
            "enabled": True,
            "apply_noise": True,
            "sensor_model": "GEODSS",
            "max_synthetic_ratio": 0.5,
            "auto_enabled_reason": "inspection_tag",
        }
        sim_auto = True

    simulation_metadata = None
    if simulation_config and simulation_config.get("enabled"):
        sensor_df = pd.DataFrame(
            {
                "idSensor": ["SEN001", "SEN002", "SEN003"],
                "name": ["DIEGO_GARCIA", "ASCENSION", "MAUI"],
                "senlat": [-7.3, -7.9, 20.7],
                "senlon": [72.4, -14.4, -156.3],
                "senalt": [0.01, 0.04, 3.1],
                "count": [10, 10, 10],
            }
        )
        sim_cfg = SimulationConfig(
            apply_sensor_noise=simulation_config.get("apply_noise", True),
            sensor_model=simulation_config.get("sensor_model", "GEODSS"),
            max_synthetic_ratio=simulation_config.get("max_synthetic_ratio", 0.5),
            seed=simulation_config.get("seed"),
        )
        obs_truth, simulation_metadata = apply_simulation_to_gaps(
            obs_truth, elset_truth, sensor_df, config=sim_cfg
        )

    evaluation = evaluate_observation_datasets(obs_truth, reference_obs)
    evaluation_artifacts = None
    try:
        report_slug = str(config.get("name", f"dataset-{dataset_id}")).replace(" ", "_")
        evaluation_artifacts = save_observation_evaluation_artifacts(
            evaluation,
            os.path.join("reports", "evaluation", report_slug),
        )
    except Exception as exc:
        logger.warning(f"Failed to save evaluation artifacts: {exc}")

    _save_to_database(dataset_id, db_path, obs_truth, state_truth, elset_truth)

    performance_data = {
        "Pipeline Mode": "statevector_first",
        "Missingness Summary": summary,
        "Missingness Tags": inspection_report.get("summary_tags", []),
        "Missingness Artifacts": artifact_dir,
        "Preprocessing Metadata": preprocessing_metadata,
        "Downsampling Metadata": downsampling_metadata,
        "Simulation Metadata": simulation_metadata,
        "Observation Evaluation Artifacts": evaluation_artifacts,
        "Counts": {
            "state_vectors": len(state_truth),
            "observations": len(obs_truth),
            "elsets": len(elset_truth) if elset_truth is not None else 0,
            "sparse_satellites": sparse_satellites,
        },
    }

    context = {
        "pipeline_mode": "statevector_first",
        "search_strategy_requested": str(config.get("search_strategy", "auto")).lower(),
        "search_strategy_resolved": search_strategy,
        "satellite_count_requested": len(satellites),
        "tier": tier,
        "regime": regime,
        "timeframe": timeframe,
        "timeunit": timeunit,
        "simulation_auto_enabled": sim_auto,
    }
    actual_sats = sorted(obs_truth["satNo"].unique().tolist()) if not obs_truth.empty else []

    return (
        obs_truth.copy(),
        obs_truth,
        state_truth,
        elset_truth,
        actual_sats,
        performance_data,
        context,
    )
