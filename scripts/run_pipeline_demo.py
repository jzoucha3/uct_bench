"""
Run the jzoucha_UCTBench orchestrated pipeline with sectioned output.

Features:
- sectioned execution logs for execution context
- Uses `search_strategy=auto|fast|hybrid|windowed`
- Supports credentials via:
  - UDL_TOKEN, or
  - UDL_USERNAME + UDL_PASSWORD (auto-generates token)
- Persists a JSON run report under reports/pipeline_runs/
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Dict


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


REPO_ROOT = _repo_root()
sys.path.insert(0, str(REPO_ROOT))

from dotenv import load_dotenv

from uct_benchmark.api.apiIntegration import UDLTokenGen
from uct_benchmark.database.connection import DatabaseManager
from uct_benchmark.pipeline import execute_custom_pipeline, execute_statevector_first_pipeline


def _print_header(title: str) -> None:
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


def _build_config(args: argparse.Namespace) -> Dict[str, Any]:
    end_date = datetime.now(UTC).date() - timedelta(days=args.end_offset_days)
    start_date = end_date - timedelta(days=args.timeframe)
    name = args.name or f"{args.regime}-{args.tier}-{datetime.now(UTC).strftime('%Y%m%d-%H%M%S')}"

    cfg: Dict[str, Any] = {
        "name": name,
        "regime": args.regime,
        "tier": args.tier,
        "pipeline_mode": args.pipeline_mode,
        "object_count": args.object_count,
        "timeframe": args.timeframe,
        "timeunit": args.timeunit,
        "search_strategy": args.search_strategy,
        "window_size_minutes": args.window_size_minutes,
        "disable_range_filter": args.disable_range_filter,
        "allow_satno_fallback": args.allow_satno_fallback,
        "start_date": f"{start_date.isoformat()}T00:00:00",
        "end_date": f"{end_date.isoformat()}T23:59:59",
    }
    if args.satellites:
        cfg["satellites"] = [int(s.strip()) for s in args.satellites.split(",") if s.strip()]

    if args.enable_downsampling:
        cfg["downsampling"] = {
            "enabled": True,
            "target_coverage": args.target_coverage,
            "target_gap": args.target_gap,
            "max_obs_per_sat": args.max_obs_per_sat,
            "preserve_tracks": True,
        }

    if args.enable_simulation:
        cfg["simulation"] = {
            "enabled": True,
            "apply_noise": True,
            "sensor_model": args.sensor_model,
            "max_synthetic_ratio": args.max_synthetic_ratio,
        }

    return cfg


def _ensure_udl_credentials() -> None:
    token = os.getenv("UDL_TOKEN")
    if token and token.strip():
        return

    username = os.getenv("UDL_USERNAME")
    password = os.getenv("UDL_PASSWORD")
    if username and password:
        os.environ["UDL_TOKEN"] = UDLTokenGen(username, password)
        return

    raise RuntimeError(
        "Missing UDL credentials. Set UDL_TOKEN or set UDL_USERNAME and UDL_PASSWORD in .env."
    )


def _save_report(report: Dict[str, Any]) -> Path:
    out_dir = REPO_ROOT / "reports" / "pipeline_runs"
    out_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    out_path = out_dir / f"run-{timestamp}.json"
    out_path.write_text(json.dumps(report, indent=2, default=str))
    return out_path


def main() -> int:
    parser = argparse.ArgumentParser(description="Run orchestrated UCT pipeline demo.")
    parser.add_argument("--name", default=None)
    parser.add_argument("--regime", default="LEO", choices=["LEO", "MEO", "GEO", "HEO"])
    parser.add_argument("--tier", default="T3", choices=["T1", "T2", "T3", "T4"])
    parser.add_argument("--object-count", type=int, default=4)
    parser.add_argument("--timeframe", type=int, default=3, help="Timeframe in days.")
    parser.add_argument("--timeunit", default="days", choices=["days", "hours", "weeks"])
    parser.add_argument(
        "--search-strategy",
        default="auto",
        choices=["auto", "fast", "hybrid", "windowed"],
    )
    parser.add_argument(
        "--pipeline-mode",
        default="standard",
        choices=["standard", "statevector-first"],
        help="Use the default observation-first pipeline or the statevector-first path.",
    )
    parser.add_argument("--window-size-minutes", type=int, default=10)
    parser.add_argument("--satellites", default=None, help="Comma-separated sat IDs, e.g. 25544,42915")
    parser.add_argument("--end-offset-days", type=int, default=1)
    parser.add_argument("--disable-range-filter", action="store_true", default=True)
    parser.add_argument("--allow-satno-fallback", action="store_true", default=True)
    parser.add_argument("--enable-downsampling", action="store_true", default=True)
    parser.add_argument("--target-coverage", type=float, default=0.05)
    parser.add_argument("--target-gap", type=float, default=2.0)
    parser.add_argument("--max-obs-per-sat", type=int, default=30)
    parser.add_argument("--enable-simulation", action="store_true", default=False)
    parser.add_argument("--sensor-model", default="GEODSS")
    parser.add_argument("--max-synthetic-ratio", type=float, default=0.5)
    parser.add_argument(
        "--db-path",
        default=None,
        help="Optional DuckDB file path for this demo run. Use this to avoid locking the shared default database.",
    )
    parser.add_argument(
        "--evaluation-reference-csv",
        default=None,
        help="Optional CSV of real/reference observations to compare against the transformed output.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Resolve config only, skip UDL calls.")
    args = parser.parse_args()

    load_dotenv(REPO_ROOT / ".env")
    config = _build_config(args)

    _print_header("SECTION 1: PIPELINE CONFIGURATION")
    print(json.dumps(config, indent=2))

    if args.dry_run:
        _print_header("DRY RUN COMPLETE")
        print("No UDL calls were made. Use without --dry-run for live generation.")
        return 0

    _print_header("SECTION 2: CREDENTIAL RESOLUTION")
    _ensure_udl_credentials()
    print("UDL credentials are configured.")

    _print_header("SECTION 3: DATASET RECORD CREATION")
    db = DatabaseManager(db_path=args.db_path)
    db.initialize()
    dataset_id = db.datasets.create_dataset(
        name=config["name"],
        tier=config.get("tier"),
        orbital_regime=config.get("regime"),
        generation_params=config,
    )
    db.execute("UPDATE datasets SET status='generating', updated_at=CURRENT_TIMESTAMP WHERE id = ?", (dataset_id,))
    print(f"Created dataset record: id={dataset_id}, name={config['name']}")
    db.close()

    _print_header("SECTION 4-11: ORCHESTRATED EXECUTION")
    start = datetime.now(UTC)
    evaluation_reference_data = None
    if args.evaluation_reference_csv:
        import pandas as pd

        evaluation_reference_data = pd.read_csv(args.evaluation_reference_csv)
    try:
        dataset_obs, obs_truth, state_truth, elset_truth, actual_sats, performance, context = (
            (
                execute_statevector_first_pipeline(
                    config=config,
                    dataset_id=dataset_id,
                    dt=0.5,
                    evaluation_reference_data=evaluation_reference_data,
                    db_path=args.db_path,
                )
                if args.pipeline_mode == "statevector-first"
                else execute_custom_pipeline(
                    config=config,
                    dataset_id=dataset_id,
                    progress_callback=None,
                    dt=0.5,
                    evaluation_reference_data=evaluation_reference_data,
                    db_path=args.db_path,
                )
            )
        )
    except Exception:
        db = DatabaseManager(db_path=args.db_path)
        db.initialize()
        db.execute(
            "UPDATE datasets SET status='failed', updated_at=CURRENT_TIMESTAMP WHERE id = ?",
            (dataset_id,),
        )
        db.close()
        raise
    end = datetime.now(UTC)

    observation_count = len(dataset_obs) if dataset_obs is not None else 0
    satellite_count = len(actual_sats) if actual_sats is not None else 0
    requested_count = int(context.get("satellite_count_requested", 0))
    avg_coverage = (satellite_count / requested_count) if requested_count > 0 else 0.0

    db = DatabaseManager(db_path=args.db_path)
    db.initialize()
    db.execute(
        """
        UPDATE datasets
        SET status='available',
            observation_count=?,
            satellite_count=?,
            avg_coverage=?,
            updated_at=CURRENT_TIMESTAMP
        WHERE id=?
        """,
        (observation_count, satellite_count, avg_coverage, dataset_id),
    )
    db.close()

    _print_header("SUMMARY")
    print(f"Dataset ID: {dataset_id}")
    print(f"Resolved strategy: {context.get('search_strategy_resolved')}")
    print(f"Observations: {observation_count}")
    print(f"Satellites returned: {satellite_count}")
    
    # Check downsampling
    ds_metadata = performance.get("Downsampling Metadata")
    if ds_metadata:
        status = ds_metadata.get("status")
        orig = ds_metadata.get("original_count", 0)
        final = ds_metadata.get("final_count", 0)
        retention = ds_metadata.get("retention_ratio", 0)
        tier_used = ds_metadata.get("tier", config.get("tier", "?"))
        p_reached = ds_metadata.get("p_reached", (None, None, None))
        ds_cfg = ds_metadata.get("config", {})
        print("Downsampling Summary:")
        print(f"  Status:    {status}")
        print(f"  Tier:      {tier_used}  (obs_max={ds_cfg.get('max_obs_per_sat')}, gap_target={ds_cfg.get('target_gap')}×P, coverage_target={ds_cfg.get('target_coverage')})")
        print(f"  Obs count: {orig} → {final}  ({retention:.1%} retained)")
        if p_reached and p_reached[0] is not None:
            print(f"  p_reached: coverage={p_reached[0]}, gap={p_reached[1]}, obs_count={p_reached[2]}")
        if status == "no_sat_params":
            print("  WARNING: Downsampling was skipped — no satellite parameters available.")
            print("           (statevector-first path: ensure elset_data is populated before calling apply_downsampling)")
    else:
        print("Downsampling: Not enabled or skipped.")

    print(f"Runtime window: {start.isoformat()} -> {end.isoformat()}")

    report = {
        "dataset_id": dataset_id,
        "dataset_name": config["name"],
        "config": config,
        "pipeline_context": context,
        "counts": {
            "dataset_obs": observation_count,
            "obs_truth": len(obs_truth) if obs_truth is not None else 0,
            "state_truth": len(state_truth) if state_truth is not None else 0,
            "elset_truth": len(elset_truth) if elset_truth is not None else 0,
            "actual_satellites": [int(s) for s in actual_sats] if actual_sats is not None else [],
        },
        "performance": performance,
        "started_at": start.isoformat(),
        "ended_at": end.isoformat(),
    }
    report_path = _save_report(report)
    print(f"Report saved: {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
