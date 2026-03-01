"""
Offline smoke checks for jzoucha_UCTBench foundation layers.

Validates:
1) API integration module imports and key callables
2) Data processing module imports
3) Simulation module imports
4) Database initialization and query access
5) FastAPI backend app startup path (without running uvicorn)
"""

from __future__ import annotations

import os
import sys
from pathlib import Path


def _ok(message: str) -> None:
    print(f"[OK] {message}")


def _fail(message: str) -> None:
    print(f"[FAIL] {message}")
    sys.exit(1)


def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    # Force this workspace to resolve imports first, even if the venv has an editable install
    # pointing at another clone.
    sys.path.insert(0, str(repo_root))
    db_path = repo_root / "data" / "database" / "uct_benchmark.duckdb"

    os.environ.setdefault("DATABASE_BACKEND", "duckdb")
    os.environ.setdefault("DATABASE_PATH", str(db_path))

    # API integration checks
    try:
        import uct_benchmark
        from uct_benchmark.api.apiIntegration import UDLQuery, asyncUDLBatchQuery, generateDataset
    except Exception as exc:
        _fail(f"API integration import failed: {exc}")
    package_path = Path(uct_benchmark.__file__).resolve()
    if str(repo_root) not in str(package_path):
        _fail(f"uct_benchmark resolved outside workspace: {package_path}")
    _ok(f"Package path resolved locally: {package_path}")
    _ok("API integration module imports")
    _ok(f"API callables present: {UDLQuery.__name__}, {asyncUDLBatchQuery.__name__}, {generateDataset.__name__}")

    # Data and simulation checks
    try:
        import uct_benchmark.data.dataManipulation as _data_manip
        import uct_benchmark.pipeline.orchestration as _pipeline_orchestration
        import uct_benchmark.simulation.simulateObservations as _sim_obs
    except Exception as exc:
        _fail(f"Data/simulation import failed: {exc}")
    _ok(f"Data module loaded: {_data_manip.__name__}")
    _ok(f"Pipeline orchestration module loaded: {_pipeline_orchestration.__name__}")
    _ok(f"Simulation module loaded: {_sim_obs.__name__}")

    # Database checks through backend singleton path
    try:
        from backend_api.database import close_database, init_database

        db = init_database()
        result = db.execute("SELECT COUNT(*) FROM datasets").fetchone()
        dataset_count = int(result[0]) if result else 0
        close_database()
    except Exception as exc:
        _fail(f"Database init/query failed: {exc}")
    _ok(f"Database initialized and queryable (datasets={dataset_count})")

    # FastAPI checks are optional in slim runtime layout.
    if (repo_root / "backend_api").exists():
        try:
            from backend_api.main import app
        except Exception as exc:
            _fail(f"FastAPI app import failed: {exc}")
        _ok(f"Backend app loaded: {app.title}")

        try:
            from fastapi.testclient import TestClient
        except Exception:
            print(
                "[WARN] fastapi.testclient unavailable (httpx not installed); "
                "skipping endpoint calls."
            )
            print("\nFOUNDATION_SMOKE_CHECK_PASSED")
            return

        try:
            with TestClient(app) as client:
                health_resp = client.get("/health")
                if health_resp.status_code != 200:
                    _fail(f"/health returned {health_resp.status_code}")
                datasets_resp = client.get("/api/v1/datasets/")
                if datasets_resp.status_code != 200:
                    _fail(f"/api/v1/datasets/ returned {datasets_resp.status_code}")
        except Exception as exc:
            _fail(f"FastAPI endpoint checks failed: {exc}")
        _ok("FastAPI endpoint checks passed: /health, /api/v1/datasets/")
    else:
        print("[INFO] backend_api not present in slim runtime layout; skipping API checks.")

    print("\nFOUNDATION_SMOKE_CHECK_PASSED")


if __name__ == "__main__":
    main()
