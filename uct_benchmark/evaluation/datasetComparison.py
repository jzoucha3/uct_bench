"""Lightweight observation-dataset comparison utilities."""

from __future__ import annotations

import json
from typing import Any, Dict, Iterable, List, Optional

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from pathlib import Path


DEFAULT_NUMERIC_COLUMNS = [
    "ra",
    "declination",
    "elevation",
    "azimuth",
    "range",
    "range_km",
    "range_rate_km_s",
]


def _normalize_obtime(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    if "obTime" in work.columns:
        work["obTime"] = pd.to_datetime(work["obTime"], errors="coerce")
    return work


def _resolve_join_keys(candidate: pd.DataFrame, reference: pd.DataFrame) -> List[str]:
    if "id" in candidate.columns and "id" in reference.columns:
        return ["id"]
    if "satNo" in candidate.columns and "satNo" in reference.columns and "obTime" in candidate.columns and "obTime" in reference.columns:
        return ["satNo", "obTime"]
    if "satNo" in candidate.columns and "satNo" in reference.columns:
        return ["satNo"]
    return []


def evaluate_observation_datasets(
    candidate_df: Optional[pd.DataFrame],
    reference_df: Optional[pd.DataFrame],
    *,
    numeric_cols: Optional[Iterable[str]] = None,
) -> Dict[str, Any]:
    """
    Compare a transformed observation dataset against a real/reference dataset.

    This is intentionally lightweight and safe for use inside dataset generation.
    It focuses on row-count retention, overlap, synthetic fraction, and simple
    per-column residual metrics on matched rows.
    """
    if candidate_df is None or reference_df is None:
        return {"status": "missing_input"}
    if candidate_df.empty or reference_df.empty:
        return {
            "status": "empty_input",
            "candidate_row_count": int(len(candidate_df)),
            "reference_row_count": int(len(reference_df)),
        }

    candidate = _normalize_obtime(candidate_df)
    reference = _normalize_obtime(reference_df)

    join_keys = _resolve_join_keys(candidate, reference)
    result: Dict[str, Any] = {
        "status": "success",
        "candidate_row_count": int(len(candidate)),
        "reference_row_count": int(len(reference)),
        "join_keys": join_keys,
        "candidate_simulated_count": 0,
        "candidate_simulated_fraction": 0.0,
    }

    if "is_simulated" in candidate.columns:
        sim_count = int(candidate["is_simulated"].fillna(False).astype(bool).sum())
        result["candidate_simulated_count"] = sim_count
        result["candidate_simulated_fraction"] = sim_count / max(1, len(candidate))

    if "satNo" in candidate.columns and "satNo" in reference.columns:
        cand_sats = set(pd.to_numeric(candidate["satNo"], errors="coerce").dropna().astype(int).tolist())
        ref_sats = set(pd.to_numeric(reference["satNo"], errors="coerce").dropna().astype(int).tolist())
        result["shared_satellite_count"] = len(cand_sats & ref_sats)
        result["candidate_satellite_count"] = len(cand_sats)
        result["reference_satellite_count"] = len(ref_sats)

    if not join_keys:
        result["status"] = "no_join_keys"
        return result

    merged = candidate.merge(
        reference,
        on=join_keys,
        how="outer",
        suffixes=("_candidate", "_reference"),
        indicator=True,
    )

    both_mask = merged["_merge"] == "both"
    result["matched_row_count"] = int(both_mask.sum())
    result["candidate_only_row_count"] = int((merged["_merge"] == "left_only").sum())
    result["reference_only_row_count"] = int((merged["_merge"] == "right_only").sum())
    result["retention_ratio_vs_reference"] = result["matched_row_count"] / max(1, len(reference))

    if result["matched_row_count"] == 0:
        result["status"] = "no_comparable_rows"
        return result

    chosen_numeric_cols = list(numeric_cols) if numeric_cols is not None else list(DEFAULT_NUMERIC_COLUMNS)
    per_column: Dict[str, Dict[str, Any]] = {}
    matched = merged.loc[both_mask].copy()

    for col in chosen_numeric_cols:
        cand_col = f"{col}_candidate"
        ref_col = f"{col}_reference"
        if cand_col not in matched.columns or ref_col not in matched.columns:
            continue
        cand_vals = pd.to_numeric(matched[cand_col], errors="coerce")
        ref_vals = pd.to_numeric(matched[ref_col], errors="coerce")
        valid = cand_vals.notna() & ref_vals.notna()
        if not valid.any():
            continue
        delta = cand_vals[valid] - ref_vals[valid]
        per_column[col] = {
            "count": int(valid.sum()),
            "mae": float(np.mean(np.abs(delta))),
            "rmse": float(np.sqrt(np.mean(np.square(delta)))),
            "bias": float(np.mean(delta)),
        }

    result["per_column_metrics"] = per_column
    return result


def save_observation_evaluation_artifacts(report: Dict[str, Any], out_dir: Path | str) -> Dict[str, str]:
    """Persist evaluation stats and simple diagnostic plots."""
    output_dir = Path(out_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    report_path = output_dir / "observation_evaluation.json"
    report_path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")

    counts_plot_path = output_dir / "observation_match_counts.png"
    counts = {
        "matched": int(report.get("matched_row_count", 0) or 0),
        "candidate_only": int(report.get("candidate_only_row_count", 0) or 0),
        "reference_only": int(report.get("reference_only_row_count", 0) or 0),
    }
    fig, ax = plt.subplots(figsize=(6, 4))
    ax.bar(list(counts.keys()), list(counts.values()), color=["#2a9d8f", "#e9c46a", "#e76f51"])
    ax.set_title("Observation Match Counts")
    ax.set_ylabel("Rows")
    fig.tight_layout()
    fig.savefig(counts_plot_path, dpi=150)
    plt.close(fig)

    artifacts = {
        "report_json": str(report_path),
        "match_counts_plot": str(counts_plot_path),
    }

    per_col = report.get("per_column_metrics") or {}
    if per_col:
        rmse_plot_path = output_dir / "observation_rmse.png"
        cols = list(per_col.keys())
        rmse_vals = [float(per_col[col].get("rmse", 0.0)) for col in cols]
        fig, ax = plt.subplots(figsize=(max(6, len(cols) * 1.2), 4))
        ax.bar(cols, rmse_vals, color="#457b9d")
        ax.set_title("Per-Column RMSE")
        ax.set_ylabel("RMSE")
        ax.tick_params(axis="x", rotation=45)
        fig.tight_layout()
        fig.savefig(rmse_plot_path, dpi=150)
        plt.close(fig)
        artifacts["rmse_plot"] = str(rmse_plot_path)

    return artifacts
