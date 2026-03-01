import pandas as pd

import uct_benchmark.pipeline.pipeline_controller as controller
from uct_benchmark.pipeline.routing_methods import RoutingConfig


def _base_report(tags=None, missing=None, temporal=None):
    return {
        "tags": tags or [],
        "per_col_missing_frac": missing or {},
        "temporal_run_stats": temporal or {},
        "missing_indicator_corr_max": 0.0,
        "group_missingness_var": {},
        "missingness_model_auc": {},
        "autocorr_lag1": {},
        "low_rank_score": 0.0,
    }


def test_run_pipeline_calls_preprocessing_in_order(monkeypatch):
    calls = []

    original_range = controller._fill_range_km
    original_rr1 = controller._fill_range_rate_stage1
    original_rr2 = controller._fill_range_rate_stage2
    original_track = controller._assign_track_ids

    def spy_range(df):
        calls.append("range")
        return original_range(df)

    def spy_rr1(df):
        calls.append("rr1")
        return original_rr1(df)

    def spy_rr2(df):
        calls.append("rr2")
        return original_rr2(df)

    def spy_track(df):
        calls.append("track")
        return original_track(df)

    monkeypatch.setattr(controller, "_fill_range_km", spy_range)
    monkeypatch.setattr(controller, "_fill_range_rate_stage1", spy_rr1)
    monkeypatch.setattr(controller, "_fill_range_rate_stage2", spy_rr2)
    monkeypatch.setattr(controller, "_assign_track_ids", spy_track)

    df = pd.DataFrame(
        {
            "satNo": [1, 1, 1],
            "obTime": ["2024-01-01T00:00:00", "2024-01-01T00:01:00", "2024-01-01T00:02:00"],
            "elevation": [30.0, 31.0, 32.0],
            "range_km": [None, 1000.0, 1060.0],
            "range": [None, 1000.0, 1060.0],
            "range_rate_km_s": [None, None, None],
            "sensorName": ["S1", "S1", "S1"],
            "trackId": [None, None, None],
        }
    )

    out_df, decisions, _ = controller.run_pipeline(df, {}, _base_report(), RoutingConfig())

    assert calls == ["range", "rr1", "rr2", "track"]
    assert out_df["range_km"].isna().sum() == 0
    assert out_df["range"].isna().sum() == 0
    assert out_df["range_rate_km_s"].isna().sum() == 0
    assert out_df["trackId"].isna().sum() == 0
    assert [d["step"] for d in decisions[:4]] == [
        "RANGE_KM_PHYSICS_FILL",
        "RANGE_RATE_ADJACENT_DERIVATIVE",
        "RANGE_RATE_MEAN_FALLBACK",
        "TRACK_ID_GROUPING_FILL",
    ]


def test_range_rate_stage1_and_stage2_apply_under_intended_conditions():
    df = pd.DataFrame(
        {
            "satNo": [42, 42, 42],
            "obTime": ["2024-01-01T00:00:00", "2024-01-01T00:01:00", "2024-01-01T00:02:00"],
            "range_km": [1000.0, 1060.0, 1120.0],
            "range_rate_km_s": [None, None, None],
            "trackId": [1, 1, 1],
        }
    )

    out_df, decisions, _ = controller.run_pipeline(df, {}, _base_report(), RoutingConfig())

    assert out_df.loc[1, "range_rate_km_s"] == 1.0
    assert out_df.loc[2, "range_rate_km_s"] == 1.0
    assert out_df.loc[0, "range_rate_km_s"] == 1.0

    rr1 = next(d for d in decisions if d["step"] == "RANGE_RATE_ADJACENT_DERIVATIVE")
    rr2 = next(d for d in decisions if d["step"] == "RANGE_RATE_MEAN_FALLBACK")
    assert rr1["metrics"]["physics_filled"] == 2
    assert rr2["metrics"]["mean_filled"] == 1
    assert rr2["metrics"]["mean_available"] is True


def test_track_id_fill_uses_sensor_identity_and_time_gap_split():
    df = pd.DataFrame(
        {
            "satNo": [7, 7, 7, 7],
            "obTime": [
                "2024-01-01T00:00:00",
                "2024-01-01T00:01:00",
                "2024-01-01T00:05:00",
                "2024-01-01T00:06:00",
            ],
            "idSensor": ["A", "A", "A", "A"],
            "trackId": [None, None, None, None],
        }
    )

    out_df, decisions, _ = controller.run_pipeline(df, {}, _base_report(), RoutingConfig())

    assert out_df["trackId"].isna().sum() == 0
    assert out_df.loc[0, "trackId"] == out_df.loc[1, "trackId"]
    assert out_df.loc[2, "trackId"] == out_df.loc[3, "trackId"]
    assert out_df.loc[0, "trackId"] != out_df.loc[2, "trackId"]

    track_decision = next(d for d in decisions if d["step"] == "TRACK_ID_GROUPING_FILL")
    assert track_decision["metrics"]["filled"] == 4


def test_simulation_runs_when_explicitly_enabled_and_cap_is_enforced():
    calls = {"count": 0, "config": None}

    def fake_sim(df, simulation_config=None):
        calls["count"] += 1
        calls["config"] = simulation_config
        extra = df.iloc[[0, 1]].copy()
        extra["is_simulated"] = True
        base = df.copy()
        base["is_simulated"] = False
        return pd.concat([base, extra], ignore_index=True), {"simulated_count": len(extra)}

    df = pd.DataFrame(
        {
            "satNo": [1, 1],
            "obTime": ["2024-01-01T00:00:00", "2024-01-01T00:01:00"],
            "line1": ["L1", "L1"],
            "line2": ["L2", "L2"],
            "trackId": [1, 1],
        }
    )

    out_df, decisions, _ = controller.run_pipeline(
        df,
        {"enabled": True, "max_synthetic_ratio": 0.25},
        _base_report(),
        RoutingConfig(),
        apply_simulation_to_gaps=fake_sim,
    )

    assert calls["count"] == 1
    assert calls["config"]["enabled"] is True
    sim_decision = next(d for d in decisions if d["step"] == "SIMULATION")
    assert sim_decision["ran"] is True
    assert sim_decision["metrics"]["cap"]["capped"] is True
    assert out_df["is_simulated"].fillna(False).sum() == 0


def test_simulation_skips_when_tle_missing_even_if_tag_requests_it():
    calls = {"count": 0}

    def fake_sim(df, simulation_config=None):
        calls["count"] += 1
        return df, {}

    df = pd.DataFrame(
        {
            "satNo": [1, 1],
            "obTime": ["2024-01-01T00:00:00", "2024-01-01T00:01:00"],
            "trackId": [1, 1],
        }
    )

    _, decisions, _ = controller.run_pipeline(
        df,
        {},
        _base_report(tags=["HIGH_MISSINGNESS_REQUIRES_RESIM"]),
        RoutingConfig(),
        apply_simulation_to_gaps=fake_sim,
    )

    assert calls["count"] == 0
    skipped = next(d for d in decisions if d["step"] == "SIMULATION_SKIPPED_MISSING_TLE")
    assert skipped["ran"] is False


def test_sparse_satellite_auto_triggers_simulation_when_not_explicitly_set():
    calls = {"count": 0}

    def fake_sim(df, simulation_config=None):
        calls["count"] += 1
        out = df.copy()
        out["is_simulated"] = False
        return out, {"simulated_count": 0}

    df = pd.DataFrame(
        {
            "satNo": [99] * 3,
            "obTime": ["2024-01-01T00:00:00", "2024-01-01T00:01:00", "2024-01-01T00:02:00"],
            "line1": ["L1", "L1", "L1"],
            "line2": ["L2", "L2", "L2"],
            "trackId": [1, 1, 1],
        }
    )

    _, decisions, _ = controller.run_pipeline(
        df,
        {},
        _base_report(),
        RoutingConfig(),
        apply_simulation_to_gaps=fake_sim,
    )

    assert calls["count"] == 1
    sim_decision = next(d for d in decisions if d["step"] == "SIMULATION")
    assert "sparse_satellite_auto_enable" in sim_decision["reason"]


def test_temporal_block_tag_triggers_simulation_when_thresholds_are_met():
    calls = {"count": 0}

    def fake_sim(df, simulation_config=None):
        calls["count"] += 1
        out = df.copy()
        out["is_simulated"] = False
        return out, {}

    df = pd.DataFrame(
        {
            "satNo": [5] * 100,
            "obTime": [f"2024-01-01T00:{i // 60:02d}:{i % 60:02d}" for i in range(100)],
            "line1": ["L1"] * 100,
            "line2": ["L2"] * 100,
            "trackId": [1] * 100,
        }
    )

    report = _base_report(
        tags=["TEMPORAL_BLOCK_MISSINGNESS"],
        missing={"flux": 0.40},
        temporal={"flux": {"max_run_len": 12, "pct_missing_longest_run": 0.20}},
    )

    _, decisions, _ = controller.run_pipeline(
        df,
        {},
        report,
        RoutingConfig(),
        apply_simulation_to_gaps=fake_sim,
    )

    assert calls["count"] == 1
    sim_decision = next(d for d in decisions if d["step"] == "SIMULATION")
    assert "temporal_block_rule" in sim_decision["reason"]


def test_sparse_satellite_does_not_override_explicit_disable():
    calls = {"count": 0}

    def fake_sim(df, simulation_config=None):
        calls["count"] += 1
        return df, {}

    df = pd.DataFrame(
        {
            "satNo": [99] * 3,
            "obTime": ["2024-01-01T00:00:00", "2024-01-01T00:01:00", "2024-01-01T00:02:00"],
            "line1": ["L1", "L1", "L1"],
            "line2": ["L2", "L2", "L2"],
            "trackId": [1, 1, 1],
        }
    )

    _, decisions, _ = controller.run_pipeline(
        df,
        {"enabled": False},
        _base_report(),
        RoutingConfig(),
        apply_simulation_to_gaps=fake_sim,
    )

    assert calls["count"] == 0
    sim_decision = next(d for d in decisions if d["step"] == "SIMULATION_DECISION")
    assert sim_decision["ran"] is False
    assert sim_decision["reason"] == "simulation not triggered"
