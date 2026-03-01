import pandas as pd

from uct_benchmark.evaluation.datasetComparison import (
    evaluate_observation_datasets,
    save_observation_evaluation_artifacts,
)


def test_evaluate_observation_datasets_uses_preferred_id_join_and_numeric_metrics():
    reference = pd.DataFrame(
        {
            "id": [1, 2],
            "satNo": [100, 100],
            "obTime": ["2024-01-01T00:00:00", "2024-01-01T00:01:00"],
            "ra": [10.0, 11.0],
            "range_km": [1000.0, 1010.0],
        }
    )
    candidate = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "satNo": [100, 100, 100],
            "obTime": ["2024-01-01T00:00:00", "2024-01-01T00:01:00", "2024-01-01T00:02:00"],
            "ra": [10.5, 11.5, 12.0],
            "range_km": [1001.0, 1012.0, 1020.0],
            "is_simulated": [False, False, True],
        }
    )

    result = evaluate_observation_datasets(candidate, reference)

    assert result["status"] == "success"
    assert result["join_keys"] == ["id"]
    assert result["matched_row_count"] == 2
    assert result["candidate_only_row_count"] == 1
    assert result["reference_only_row_count"] == 0
    assert result["candidate_simulated_count"] == 1
    assert result["per_column_metrics"]["ra"]["count"] == 2
    assert result["per_column_metrics"]["ra"]["mae"] == 0.5


def test_evaluate_observation_datasets_falls_back_to_satno_obtime_join():
    reference = pd.DataFrame(
        {
            "satNo": [5],
            "obTime": ["2024-01-01T00:00:00"],
            "declination": [20.0],
        }
    )
    candidate = pd.DataFrame(
        {
            "satNo": [5],
            "obTime": ["2024-01-01T00:00:00"],
            "declination": [21.0],
        }
    )

    result = evaluate_observation_datasets(candidate, reference)

    assert result["join_keys"] == ["satNo", "obTime"]
    assert result["matched_row_count"] == 1
    assert result["per_column_metrics"]["declination"]["rmse"] == 1.0


def test_save_observation_evaluation_artifacts_writes_files(tmp_path):
    report = {
        "matched_row_count": 2,
        "candidate_only_row_count": 1,
        "reference_only_row_count": 0,
        "per_column_metrics": {
            "ra": {"rmse": 0.5},
            "range_km": {"rmse": 1.5},
        },
    }

    artifacts = save_observation_evaluation_artifacts(report, tmp_path)

    assert (tmp_path / "observation_evaluation.json").exists()
    assert (tmp_path / "observation_match_counts.png").exists()
    assert (tmp_path / "observation_rmse.png").exists()
    assert "report_json" in artifacts
    assert "match_counts_plot" in artifacts
    assert "rmse_plot" in artifacts
