"""
Background workers for executing long-running tasks.

Provides worker functions for dataset generation and evaluation
that run in a ThreadPoolExecutor.

Note: Dataset ID is now passed to generateDataset to avoid duplicate creation.
"""

import json
import traceback
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, Optional

from loguru import logger

from . import Job, JobType, get_job_manager
from .progress import DatasetStage, create_job_progress_callback


def _convert_numpy_to_native(obj: Any) -> Any:
    """Recursively convert numpy arrays and types to native Python types for JSON serialization."""
    import numpy as np

    if isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, (np.integer, np.int64, np.int32)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float64, np.float32)):
        return float(obj)
    elif isinstance(obj, np.bool_):
        return bool(obj)
    elif isinstance(obj, dict):
        return {k: _convert_numpy_to_native(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [_convert_numpy_to_native(item) for item in obj]
    return obj


# Global thread pool for background tasks
_executor: Optional[ThreadPoolExecutor] = None


def get_executor() -> ThreadPoolExecutor:
    """Get or create the global thread pool executor."""
    global _executor
    if _executor is None:
        _executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="worker")
    return _executor


def shutdown_executor() -> None:
    """Shutdown the thread pool executor."""
    global _executor
    if _executor is not None:
        _executor.shutdown(wait=False)
        _executor = None


def _load_evaluation_reference_data(db: Any, config: Dict[str, Any]):
    """Load optional evaluation reference observations from a dataset or CSV path."""
    dataset_id = config.get("evaluation_reference_dataset_id")
    csv_path = config.get("evaluation_reference_csv_path")

    if csv_path:
        import pandas as pd

        return pd.read_csv(csv_path)

    if not dataset_id:
        return None

    dataset_id_int = int(dataset_id)
    ref_dataset = db.execute("SELECT id FROM datasets WHERE id = ?", (dataset_id_int,)).fetchone()
    if not ref_dataset:
        raise ValueError(f"Evaluation reference dataset {dataset_id} not found")

    return db.adapter.fetchdf(
        """
        SELECT
            o.id,
            o.sat_no AS satNo,
            o.ob_time AS obTime,
            o.ra,
            o.declination,
            o.sensor_name AS sensorName,
            o.track_id AS trackId,
            o.range_km AS range_km,
            o.range_rate_km_s AS range_rate_km_s,
            o.is_simulated AS is_simulated
        FROM observations o
        JOIN dataset_observations dso ON o.id = dso.observation_id
        WHERE dso.dataset_id = ?
        ORDER BY o.ob_time
        """,
        (dataset_id_int,),
    )


def run_dataset_generation(
    job_id: str,
    dataset_id: int,
    config: Dict[str, Any],
) -> None:
    """
    Worker function for dataset generation.

    Runs in a background thread and updates job status as it progresses.

    Args:
        job_id: The job ID to update progress
        dataset_id: The database ID for the dataset being generated
        config: Dataset generation configuration containing:
            - regime: Orbital regime (LEO, MEO, GEO, HEO)
            - object_count: Number of satellites
            - timeframe: Duration in days
            - satellites: Optional list of specific NORAD IDs
    """
    job_manager = get_job_manager()
    job_manager.start_job(job_id)

    try:
        from backend_api.database import get_db
        from uct_benchmark.pipeline import execute_custom_pipeline

        # Check if downsampling/simulation are enabled for progress weights.
        # T3 enforces both in orchestration defaults.
        tier = str(config.get("tier", "T2")).upper()
        downsampling_enabled = bool(
            config.get("downsampling") and config["downsampling"].get("enabled", False)
        ) or tier == "T3"
        simulation_enabled = bool(
            config.get("simulation") and config["simulation"].get("enabled", False)
        ) or tier == "T3"
        logger.info(
            f"[WORKER] job={job_id} dataset_id={dataset_id} "
            f"received config keys={sorted(config.keys())} "
            f"downsampling_enabled={downsampling_enabled} simulation_enabled={simulation_enabled}"
        )

        # Create progress callback for granular progress updates
        progress_callback = create_job_progress_callback(
            job_id,
            job_manager,
            downsampling_enabled=downsampling_enabled,
            simulation_enabled=simulation_enabled,
        )

        # Update progress - initializing
        progress_callback(DatasetStage.INITIALIZING, 0.0)

        # Mark initialization complete
        progress_callback(DatasetStage.INITIALIZING, 1.0)

        db = get_db()
        evaluation_reference_data = _load_evaluation_reference_data(db, config)

        # Execute the orchestrated pipeline
        (
            dataset_obs,
            obs_truth,
            state_truth,
            elset_truth,
            actual_sats,
            performance_data,
            pipeline_context,
        ) = execute_custom_pipeline(
            config=config,
            dataset_id=dataset_id,
            progress_callback=progress_callback,
            dt=0.5,
            evaluation_reference_data=evaluation_reference_data,
        )
        obs_truth_count = len(obs_truth) if obs_truth is not None else 0
        dataset_obs_count = len(dataset_obs) if dataset_obs is not None else 0
        simulated_obs_count = 0
        if obs_truth is not None and not getattr(obs_truth, "empty", True) and "is_simulated" in obs_truth.columns:
            try:
                simulated_obs_count = int(obs_truth["is_simulated"].fillna(False).sum())
            except Exception:
                simulated_obs_count = 0
        logger.info(
            f"[WORKER] generateDataset returned dataset_obs={dataset_obs_count}, "
            f"obs_truth={obs_truth_count}, simulated_in_obs_truth={simulated_obs_count}, "
            f"state_truth={len(state_truth) if state_truth is not None else 0}, "
            f"elset_truth={len(elset_truth) if elset_truth is not None else 0}"
        )

        # Update progress - persisting to database
        progress_callback(DatasetStage.PERSISTING_DATABASE, 0.0)

        # Update dataset record in database
        observation_count = len(dataset_obs) if dataset_obs is not None else 0
        satellite_count = len(actual_sats) if actual_sats is not None else 0

        # Calculate coverage as ratio of satellites with full data vs requested
        requested_count = int(pipeline_context.get("satellite_count_requested", 0))
        avg_coverage = (satellite_count / requested_count) if requested_count > 0 else 0.0

        # Estimate size in bytes (approx 500 bytes per observation as JSON)
        estimated_size_bytes = observation_count * 500

        # Update the dataset status with all metrics
        db.execute(
            """
            UPDATE datasets
            SET status = 'available',
                observation_count = ?,
                satellite_count = ?,
                avg_coverage = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (observation_count, satellite_count, avg_coverage, dataset_id),
        )

        # Link observations to dataset if we have observation data
        # NOTE: This is a CRITICAL step - if linking fails, the dataset is unusable
        progress_callback(DatasetStage.PERSISTING_DATABASE, 0.5)
        logger.info(f"[WORKER] About to link observations for dataset {dataset_id}")
        if obs_truth is not None and not obs_truth.empty and "id" in obs_truth.columns:
            obs_ids = obs_truth["id"].tolist()
            track_assignments = {}
            if "trackId" in obs_truth.columns:
                import pandas as pd

                INT32_MAX = 2147483647  # Max value for INT32
                for _, row in obs_truth.iterrows():
                    track_id = row.get("trackId")
                    # Convert NaN/NaT to None (DuckDB can't handle NaN in INT columns)
                    if pd.isna(track_id):
                        track_id = None
                    elif track_id is not None:
                        # Convert to int if it's a string or float
                        try:
                            track_id = int(track_id)
                            # Check if value fits in INT32 (database schema limitation)
                            if track_id > INT32_MAX or track_id < -INT32_MAX:
                                track_id = None  # Too large for INT32, store as NULL
                        except (ValueError, TypeError):
                            track_id = None
                    track_assignments[row["id"]] = track_id
            # Don't catch exceptions here - linking failure should fail the entire job
            # A dataset without linked observations is corrupted and unusable
            db.datasets.add_observations_to_dataset(dataset_id, obs_ids, track_assignments)
            logger.info(f"Linked {len(obs_ids)} observations to dataset {dataset_id}")
        else:
            # If we have no observations to link, this is also an error
            if observation_count > 0:
                raise ValueError(
                    f"Dataset has {observation_count} observations in count but no observation IDs to link. "
                    "This indicates a data consistency issue."
                )

        # Finalize
        progress_callback(DatasetStage.PERSISTING_DATABASE, 1.0)
        progress_callback(DatasetStage.FINALIZING, 0.5)

        # Complete the job
        result = {
            "dataset_id": dataset_id,
            "observation_count": observation_count,
            "satellite_count": satellite_count,
            "actual_satellites": [int(s) for s in actual_sats] if actual_sats is not None else [],
            "pipeline_context": pipeline_context,
            "performance": performance_data,
        }

        # Convert numpy arrays to native Python types for JSON serialization
        result = _convert_numpy_to_native(result)
        job_manager.complete_job(job_id, result)
        logger.info(f"Dataset generation completed for job {job_id}")

    except Exception as e:
        error_msg = f"{type(e).__name__}: {str(e)}"
        logger.error(f"Dataset generation failed for job {job_id}: {error_msg}")
        logger.debug(traceback.format_exc())

        # Update dataset status to failed
        try:
            from backend_api.database import get_db

            db = get_db()
            # Rollback any failed transaction state before executing update
            try:
                db._connection.rollback()
            except Exception as rollback_error:
                logger.debug(f"Rollback not needed or failed: {rollback_error}")
            db.execute(
                "UPDATE datasets SET status = 'failed', updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (dataset_id,),
            )
        except Exception as db_error:
            # Log the secondary failure - this is critical as the dataset will be stuck in 'generating' state
            logger.error(
                f"CRITICAL: Failed to mark dataset {dataset_id} as failed: {db_error}. "
                "Dataset may be stuck in 'generating' state."
            )
            # Include in error message so it's visible in job status
            error_msg = f"{error_msg} [DB update also failed: {db_error}]"

        job_manager.fail_job(job_id, error_msg)


def run_evaluation_pipeline(
    job_id: str,
    submission_id: int,
    dataset_id: int,
    file_path: str,
) -> None:
    """
    Worker function for running evaluation on a submission.

    Runs in a background thread and updates job status as it progresses.

    Args:
        job_id: The job ID to update progress
        submission_id: The database ID for the submission
        dataset_id: The dataset ID to evaluate against
        file_path: Path to the uploaded UCTP output file
    """
    job_manager = get_job_manager()
    job_manager.start_job(job_id)

    try:
        from backend_api.database import get_db
        from uct_benchmark.evaluation.binaryMetrics import binaryMetrics
        from uct_benchmark.evaluation.orbitAssociation import orbitAssociation
        from uct_benchmark.evaluation.stateMetrics import stateMetrics

        job_manager.update_job(job_id, progress=10)

        db = get_db()

        # Load dataset from database
        dataset_row = db.execute("SELECT * FROM datasets WHERE id = ?", (dataset_id,)).fetchone()

        if not dataset_row:
            raise ValueError(f"Dataset {dataset_id} not found")

        job_manager.update_job(job_id, progress=20)

        # Load the submission file (UCTP output)
        with open(file_path, "r") as f:
            submission_data = json.load(f)

        job_manager.update_job(job_id, progress=30)

        # Get reference data from database
        # This would load the truth observations and states for comparison
        observations = db.adapter.fetchdf(
            """
            SELECT o.* FROM observations o
            JOIN dataset_observations dso ON o.id = dso.observation_id
            WHERE dso.dataset_id = ?
            """,
            (dataset_id,),
        )

        job_manager.update_job(job_id, progress=40)

        # Run orbit association
        # The submission_data should contain predicted track/object assignments
        # compared against the truth from the dataset
        associations = (
            orbitAssociation(
                submission_data.get("predictions", []),
                observations,
            )
            if "predictions" in submission_data
            else {}
        )

        job_manager.update_job(job_id, progress=60)

        # Compute binary metrics (TP, FP, FN, precision, recall, F1)
        binary_results = (
            binaryMetrics(associations)
            if associations
            else {
                "true_positives": 0,
                "false_positives": 0,
                "false_negatives": 0,
                "precision": 0.0,
                "recall": 0.0,
                "f1_score": 0.0,
            }
        )

        job_manager.update_job(job_id, progress=80)

        # Compute state metrics (position/velocity RMS for true positives)
        state_results = {
            "position_rms_km": 0.0,
            "velocity_rms_km_s": 0.0,
        }

        if associations:
            state_results = stateMetrics(associations) or state_results

        job_manager.update_job(job_id, progress=90)

        # Store results in database
        db.execute(
            """
            INSERT INTO submission_results (
                submission_id,
                true_positives,
                false_positives,
                false_negatives,
                precision,
                recall,
                f1_score,
                position_rms_km,
                velocity_rms_km_s,
                raw_results
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                submission_id,
                binary_results.get("true_positives", 0),
                binary_results.get("false_positives", 0),
                binary_results.get("false_negatives", 0),
                binary_results.get("precision", 0.0),
                binary_results.get("recall", 0.0),
                binary_results.get("f1_score", 0.0),
                state_results.get("position_rms_km", 0.0),
                state_results.get("velocity_rms_km_s", 0.0),
                json.dumps({"binary": binary_results, "state": state_results}),
            ),
        )

        # Update submission status
        db.execute(
            """
            UPDATE submissions
            SET status = 'completed', completed_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (submission_id,),
        )

        # Complete job
        result = {
            "submission_id": submission_id,
            "binary_metrics": binary_results,
            "state_metrics": state_results,
        }

        # Convert numpy arrays to native Python types for JSON serialization
        result = _convert_numpy_to_native(result)
        job_manager.complete_job(job_id, result)
        logger.info(f"Evaluation completed for job {job_id}")

    except Exception as e:
        error_msg = f"{type(e).__name__}: {str(e)}"
        logger.error(f"Evaluation failed for job {job_id}: {error_msg}")
        logger.debug(traceback.format_exc())

        # Update submission status to failed
        try:
            from backend_api.database import get_db

            db = get_db()
            db.execute(
                "UPDATE submissions SET status = 'failed' WHERE id = ?",
                (submission_id,),
            )
        except Exception as db_error:
            # Log the secondary failure - this is critical as the submission will be stuck
            logger.error(
                f"CRITICAL: Failed to mark submission {submission_id} as failed: {db_error}. "
                "Submission may be stuck in 'processing' state."
            )
            # Include in error message so it's visible in job status
            error_msg = f"{error_msg} [DB update also failed: {db_error}]"

        job_manager.fail_job(job_id, error_msg)


def submit_dataset_generation(
    dataset_id: int,
    config: Dict[str, Any],
) -> Job:
    """
    Submit a dataset generation job to run in the background.

    Args:
        dataset_id: The database ID for the dataset
        config: Dataset generation configuration

    Returns:
        The created Job instance
    """
    job_manager = get_job_manager()
    job = job_manager.create_job(
        JobType.DATASET_GENERATION,
        metadata={"dataset_id": dataset_id, "config": config},
    )

    executor = get_executor()
    executor.submit(run_dataset_generation, job.id, dataset_id, config)

    return job


def submit_evaluation(
    submission_id: int,
    dataset_id: int,
    file_path: str,
) -> Job:
    """
    Submit an evaluation job to run in the background.

    Args:
        submission_id: The database ID for the submission
        dataset_id: The dataset ID to evaluate against
        file_path: Path to the uploaded results file

    Returns:
        The created Job instance
    """
    job_manager = get_job_manager()
    job = job_manager.create_job(
        JobType.EVALUATION,
        metadata={
            "submission_id": submission_id,
            "dataset_id": dataset_id,
            "file_path": file_path,
        },
    )

    executor = get_executor()
    executor.submit(run_evaluation_pipeline, job.id, submission_id, dataset_id, file_path)

    return job
