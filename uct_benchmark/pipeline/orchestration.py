"""
Orchestration for UCT dataset generation.

This module adds a thin, reusable orchestration layer around `generateDataset`
to keep backend worker logic small and deterministic.
"""

from __future__ import annotations

import os
import random
from datetime import datetime
from typing import Any, Dict, List, Tuple

from loguru import logger

from uct_benchmark.api.apiIntegration import generateDataset
from uct_benchmark.settings import satIDs as DEFAULT_SATELLITES


def _parse_time_config(config: Dict[str, Any]) -> Tuple[int, str, Any]:
    """Resolve timeframe/timeunit/end_time from optional date range inputs."""
    timeframe = int(config.get("timeframe", 7))
    timeunit = str(config.get("timeunit", "days"))
    end_time: Any = "now"

    start_date_str = config.get("start_date")
    end_date_str = config.get("end_date")
    if not end_date_str:
        return timeframe, timeunit, end_time

    try:
        if "T" in end_date_str:
            end_time = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
        else:
            end_time = datetime.fromisoformat(f"{end_date_str}T23:59:59")

        if start_date_str:
            if "T" in start_date_str:
                start_time = datetime.fromisoformat(start_date_str.replace("Z", "+00:00"))
            else:
                start_time = datetime.fromisoformat(f"{start_date_str}T00:00:00")
            delta_days = max(1, (end_time - start_time).days)
            timeframe = delta_days
            timeunit = "days"
    except (ValueError, TypeError) as exc:
        logger.warning(f"Failed to parse start/end dates from config: {exc}. Falling back to timeframe.")
        end_time = "now"

    return timeframe, timeunit, end_time


def _select_satellites(config: Dict[str, Any]) -> List[int]:
    """Choose explicit satellites or sample from default calibration list."""
    satellites = config.get("satellites", [])
    if satellites:
        return [int(s) for s in satellites]

    object_count = int(config.get("object_count", 5))
    available = list(DEFAULT_SATELLITES)
    random.shuffle(available)
    return available[: min(object_count, len(available))]


def _resolve_search_strategy(
    requested: str,
    satellite_count: int,
    timeframe: int,
    timeunit: str,
) -> str:
    """Resolve search strategy, including automatic selection."""
    requested_norm = (requested or "auto").strip().lower()
    if requested_norm in {"fast", "windowed", "hybrid"}:
        return requested_norm

    # Automatic strategy:
    # - small short pulls -> fast
    # - large/long pulls -> windowed
    # - otherwise -> hybrid
    timeframe_days = timeframe
    if timeunit in {"weeks", "week"}:
        timeframe_days = timeframe * 7
    elif timeunit in {"hours", "hour"}:
        timeframe_days = max(1, timeframe // 24)

    if satellite_count <= 5 and timeframe_days <= 7:
        return "fast"
    if satellite_count >= 10 or timeframe_days >= 30:
        return "windowed"
    return "hybrid"


def _build_tier_configs(
    tier: str,
    downsampling: Dict[str, Any] | None,
    simulation: Dict[str, Any] | None,
) -> Tuple[Dict[str, Any] | None, Dict[str, Any] | None]:
    """
    Apply tier-aware defaults.

    T3 defaults:
    - Require downsampling
    - Require simulation
    """
    tier_norm = (tier or "T2").upper()
    ds_config = downsampling.copy() if downsampling else None
    sim_config = simulation.copy() if simulation else None

    if tier_norm == "T3":
        if ds_config is None:
            ds_config = {}
        ds_config.setdefault("enabled", True)
        ds_config.setdefault("target_coverage", 0.05)
        ds_config.setdefault("target_gap", 2.0)
        ds_config.setdefault("max_obs_per_sat", 30)
        ds_config.setdefault("preserve_tracks", True)
        ds_config.setdefault("seed", None)

        if sim_config is None:
            sim_config = {}
        sim_config.setdefault("enabled", True)
        sim_config.setdefault("apply_noise", True)
        sim_config.setdefault("sensor_model", "GEODSS")
        sim_config.setdefault("max_synthetic_ratio", 0.5)
        sim_config.setdefault("seed", None)

    return ds_config, sim_config


def execute_custom_pipeline(
    config: Dict[str, Any],
    dataset_id: int,
    progress_callback=None,
    dt: float = 0.5,
    evaluation_reference_data=None,
    db_path=None,
) -> Tuple[Any, Any, Any, Any, Any, Dict[str, Any], Dict[str, Any]]:
    """
    Execute dataset generation with orchestration defaults.

    Returns:
        (dataset_obs, obs_truth, state_truth, elset_truth, actual_sats, performance_data, context)
    """
    udl_token = os.getenv("UDL_TOKEN")
    esa_token = os.getenv("ESA_TOKEN")

    if esa_token and esa_token.strip().lower() in {
        "your_esa_api_token_here",
        "changeme",
        "none",
        "null",
    }:
        esa_token = None
    if not udl_token:
        raise ValueError("Missing required environment variable: UDL_TOKEN.")

    satellites = _select_satellites(config)
    timeframe, timeunit, end_time = _parse_time_config(config)
    tier = str(config.get("tier", "T2")).upper()
    regime = str(config.get("regime", "LEO")).upper()

    search_strategy = _resolve_search_strategy(
        requested=str(config.get("search_strategy", "auto")),
        satellite_count=len(satellites),
        timeframe=timeframe,
        timeunit=timeunit,
    )

    window_size_minutes = int(config.get("window_size_minutes", 10) or 10)
    if search_strategy == "windowed" and "window_size_minutes" not in config:
        # Use larger windows for low-density regimes.
        window_size_minutes = 1440 if regime == "GEO" else 720

    downsample_config, simulation_config = _build_tier_configs(
        tier=tier,
        downsampling=config.get("downsampling"),
        simulation=config.get("simulation"),
    )

    context = {
        "search_strategy_requested": str(config.get("search_strategy", "auto")).lower(),
        "search_strategy_resolved": search_strategy,
        "window_size_minutes": window_size_minutes,
        "satellite_count_requested": len(satellites),
        "tier": tier,
        "regime": regime,
        "timeframe": timeframe,
        "timeunit": timeunit,
    }
    logger.info(f"[PIPELINE] Execution context: {context}")

    dataset_obs, obs_truth, state_truth, elset_truth, actual_sats, performance_data = generateDataset(
        UDL_token=udl_token,
        ESA_token=esa_token,
        satIDs=satellites,
        timeframe=timeframe,
        timeunit=timeunit,
        dt=dt,
        max_datapoints=0,
        end_time=end_time,
        use_database=True,
        db_path=db_path,
        dataset_name=config.get("name"),
        downsample_config=downsample_config,
        simulation_config=simulation_config,
        tier=tier,
        dataset_id=dataset_id,
        progress_callback=progress_callback,
        search_strategy=search_strategy,
        window_size_minutes=window_size_minutes,
        regime=regime,
        disable_range_filter=bool(config.get("disable_range_filter", True)),
        allow_satno_fallback=bool(config.get("allow_satno_fallback", True)),
        evaluation_reference_data=evaluation_reference_data,
    )

    return (
        dataset_obs,
        obs_truth,
        state_truth,
        elset_truth,
        actual_sats,
        performance_data,
        context,
    )
