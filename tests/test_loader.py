import sqlite3

import pandas as pd

from src.loader import load_twice


def test_sqlite_double_load_is_idempotent(tmp_path):
    frame = pd.read_csv("data/processed_data.csv").head(12)
    csv_path = tmp_path / "processed.csv"
    db_path = tmp_path / "test.db"
    frame.to_csv(csv_path, index=False)
    summary = load_twice(csv_path, db_path, reset=True, summary_path=tmp_path / "verification.json")
    assert summary["inserted_first_load"] == 12
    assert summary["inserted_second_load"] == 0
    assert summary["stored_rows"] == 12
    assert summary["idempotency_passed"] is True
    with sqlite3.connect(db_path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM releases").fetchone()[0] == 12
