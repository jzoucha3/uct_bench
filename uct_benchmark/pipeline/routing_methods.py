"""Routing method families and deterministic routing rules."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional


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

STRUCTURAL_MISSING = "STRUCTURAL_MISSING"
LEAVE_AS_MISSING = "LEAVE_AS_MISSING"
DROP_COLUMN = "DROP_COLUMN"
SIMPLE_UNIVARIATE = "SIMPLE_UNIVARIATE"
DEPENDENCY_BASED_MI = "DEPENDENCY_BASED_MI"
LOW_RANK_COMPLETION = "LOW_RANK_COMPLETION"
TIME_SERIES_INTERP = "TIME_SERIES_INTERP"
STATE_SPACE_SMOOTHING = "STATE_SPACE_SMOOTHING"
PHYSICS_PROPAGATION = "PHYSICS_PROPAGATION"
RESIMULATE_MEASUREMENTS = "RESIMULATE_MEASUREMENTS"


@dataclass
class RoutingConfig:
    """Thresholds for deterministic diagnose-then-route decisions."""

    high_missing: float = 0.30
    low_missing: float = 0.05
    temporal_block_min_run: int = 10
    temporal_interp_max_run: int = 2
    joint_dropout_corr_warn: float = 0.50
    group_cluster_var_warn: float = 0.02
    mar_auc_warn: float = 0.70
    low_rank_score_warn: float = 0.90


def _metric_for_col(diag: Dict[str, Any], key: str, col: str, default: float = 0.0) -> float:
    """Return a per-column metric whether stored as scalar or mapping."""
    value = diag.get(key, default)
    if isinstance(value, dict):
        col_value = value.get(col, default)
    else:
        col_value = value
    try:
        if col_value is None:
            return default
        return float(col_value)
    except (TypeError, ValueError):
        return default


def _missing_frac(diag: Dict[str, Any]) -> float:
    return _metric_for_col(diag, "missing_frac", diag.get("column_name", ""), 0.0)


def _final_fallback(role: str, missing_frac: float, cfg: RoutingConfig) -> Dict[str, Any]:
    """Default route by role if no specific branch matched."""
    role = (role or "").lower()
    if role in {"id_like", "relational_id"}:
        return {"route": STRUCTURAL_MISSING, "priority": 50, "reasons": ["role_based_fallback"], "fallback": LEAVE_AS_MISSING}
    if role == "orbital_state":
        return {"route": PHYSICS_PROPAGATION, "priority": 50, "reasons": ["role_based_fallback"], "fallback": LEAVE_AS_MISSING}
    if role in {"measurement", "time_series"}:
        return {
            "route": DEPENDENCY_BASED_MI,
            "priority": 50,
            "reasons": ["role_based_fallback"],
            "fallback": SIMPLE_UNIVARIATE,
        }
    route = SIMPLE_UNIVARIATE if missing_frac <= cfg.low_missing else DEPENDENCY_BASED_MI
    fallback = None if route == SIMPLE_UNIVARIATE else SIMPLE_UNIVARIATE
    return {"route": route, "priority": 50, "reasons": ["role_based_fallback"], "fallback": fallback}


def route_column(col: str, diag: Dict[str, Any], cfg: RoutingConfig) -> Dict[str, Any]:
    """
    Route a column into a method family using deterministic rules.

    Expected diag keys:
    - role
    - missing_frac
    - max_run_len
    - pct_missing_longest_run
    - missing_indicator_corr_max
    - group_missingness_var (scalar or dict)
    - missingness_model_auc (scalar or dict)
    - autocorr_lag1 (scalar or dict)
    - low_rank_score
    - tags
    """
    role = str(diag.get("role", "")).lower()
    tags = set(diag.get("tags", []) or [])
    missing_frac = _missing_frac(diag)
    max_run_len = int(_metric_for_col(diag, "max_run_len", col, 0.0))
    pct_missing_longest_run = _metric_for_col(diag, "pct_missing_longest_run", col, 0.0)
    missing_indicator_corr_max = _metric_for_col(diag, "missing_indicator_corr_max", col, 0.0)
    group_cluster_var = _metric_for_col(diag, "group_missingness_var", col, 0.0)
    missingness_model_auc = _metric_for_col(diag, "missingness_model_auc", col, 0.0)
    autocorr_lag1 = _metric_for_col(diag, "autocorr_lag1", col, 0.0)
    low_rank_score = _metric_for_col(diag, "low_rank_score", col, 0.0)

    # a) If role id_like/relational_id -> STRUCTURAL_MISSING (priority 1)
    if role in {"id_like", "relational_id"}:
        return {
            "route": STRUCTURAL_MISSING,
            "priority": 1,
            "reasons": ["id_or_relational_identifier"],
            "fallback": None,
        }

    # b) If missing_frac==1.0 -> DROP_COLUMN (priority 2)
    if missing_frac == 1.0:
        return {
            "route": DROP_COLUMN,
            "priority": 2,
            "reasons": ["fully_missing"],
            "fallback": None,
        }

    # c) If missing_frac>=high_missing ...
    if missing_frac >= cfg.high_missing:
        if role == "orbital_state":
            return {
                "route": PHYSICS_PROPAGATION,
                "priority": 3,
                "reasons": ["high_missingness", "orbital_state"],
                "fallback": LEAVE_AS_MISSING,
            }
        if role in {"measurement", "time_series"}:
            return {
                "route": RESIMULATE_MEASUREMENTS,
                "priority": 3,
                "reasons": ["high_missingness", "measurement_or_time_series"],
                "fallback": STATE_SPACE_SMOOTHING,
            }
        return {
            "route": LEAVE_AS_MISSING,
            "priority": 3,
            "reasons": ["high_missingness", "non_physical_non_temporal"],
            "fallback": DROP_COLUMN,
        }

    # d) For time_series/measurement/orbital_state ...
    if role in {"time_series", "measurement", "orbital_state"}:
        if max_run_len > cfg.temporal_block_min_run or pct_missing_longest_run > 0.10:
            route = PHYSICS_PROPAGATION if role == "orbital_state" else RESIMULATE_MEASUREMENTS
            return {
                "route": route,
                "priority": 4,
                "reasons": ["temporal_block_missingness"],
                "fallback": None,
            }
        if max_run_len <= cfg.temporal_interp_max_run and missing_frac <= cfg.high_missing:
            return {
                "route": TIME_SERIES_INTERP,
                "priority": 5,
                "reasons": ["short_temporal_gaps"],
                "fallback": STATE_SPACE_SMOOTHING,
            }
        if autocorr_lag1 >= 0.6:
            return {
                "route": STATE_SPACE_SMOOTHING,
                "priority": 6,
                "reasons": ["high_lag1_autocorrelation"],
                "fallback": RESIMULATE_MEASUREMENTS,
            }
        return {
            "route": DEPENDENCY_BASED_MI,
            "priority": 7,
            "reasons": ["temporal_non_short_gap_low_autocorr"],
            "fallback": SIMPLE_UNIVARIATE,
        }

    # e) For continuous/count/categorical ...
    if role in {"continuous", "count", "categorical"}:
        JOINT_DROPOUT = missing_indicator_corr_max >= cfg.joint_dropout_corr_warn
        GROUP_CLUSTERED = group_cluster_var >= cfg.group_cluster_var_warn
        LIKELY_MAR = missingness_model_auc >= cfg.mar_auc_warn
        LOW_RANK = (low_rank_score >= cfg.low_rank_score_warn) or ("LIKELY_LOW_RANK_STRUCTURE" in tags)

        if LOW_RANK and role == "continuous" and cfg.low_missing < missing_frac < cfg.high_missing:
            return {
                "route": LOW_RANK_COMPLETION,
                "priority": 8,
                "reasons": ["low_rank_signal"],
                "fallback": DEPENDENCY_BASED_MI,
            }
        if LIKELY_MAR:
            return {
                "route": DEPENDENCY_BASED_MI,
                "priority": 9,
                "reasons": ["likely_mar"],
                "fallback": SIMPLE_UNIVARIATE,
            }
        if JOINT_DROPOUT or GROUP_CLUSTERED:
            reasons = []
            if JOINT_DROPOUT:
                reasons.append("joint_dropout")
            if GROUP_CLUSTERED:
                reasons.append("group_clustered_missingness")
            return {
                "route": LEAVE_AS_MISSING,
                "priority": 10,
                "reasons": reasons,
                "fallback": DEPENDENCY_BASED_MI,
            }
        route = SIMPLE_UNIVARIATE if missing_frac <= cfg.low_missing else DEPENDENCY_BASED_MI
        fallback = None if route == SIMPLE_UNIVARIATE else SIMPLE_UNIVARIATE
        return {
            "route": route,
            "priority": 11,
            "reasons": ["default_non_temporal_statistical_route"],
            "fallback": fallback,
        }

    # f) Final fallback by role if nothing matched.
    return _final_fallback(role, missing_frac, cfg)
