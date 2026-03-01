import pandas as pd

from backend_api.jobs.workers import _load_evaluation_reference_data


class _FakeResult:
    def __init__(self, row):
        self._row = row

    def fetchone(self):
        return self._row


class _FakeAdapter:
    def __init__(self, frame):
        self._frame = frame
        self.last_query = None
        self.last_params = None

    def fetchdf(self, query, params):
        self.last_query = query
        self.last_params = params
        return self._frame


class _FakeDb:
    def __init__(self, exists_row, frame):
        self._exists_row = exists_row
        self.adapter = _FakeAdapter(frame)

    def execute(self, query, params):
        return _FakeResult(self._exists_row)


def test_load_evaluation_reference_data_from_dataset_returns_dataframe():
    frame = {"id": [1], "satNo": [25544]}
    db = _FakeDb((1,), frame)

    result = _load_evaluation_reference_data(db, {"evaluation_reference_dataset_id": "1"})

    assert result == frame
    assert db.adapter.last_params == (1,)


def test_load_evaluation_reference_data_returns_none_when_not_configured():
    db = _FakeDb(None, {})

    result = _load_evaluation_reference_data(db, {})

    assert result is None


def test_load_evaluation_reference_data_from_csv_path(tmp_path):
    csv_path = tmp_path / "reference.csv"
    pd.DataFrame({"id": [1], "satNo": [25544]}).to_csv(csv_path, index=False)
    db = _FakeDb(None, {})

    result = _load_evaluation_reference_data(
        db,
        {"evaluation_reference_csv_path": str(csv_path)},
    )

    assert list(result.columns) == ["id", "satNo"]
    assert int(result.iloc[0]["satNo"]) == 25544
