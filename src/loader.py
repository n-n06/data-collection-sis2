from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

import pandas as pd

from .config import DATA_DIR

TABLE_COLUMNS = [
    "release_id", "artist_mbid", "artist_name", "artist_credit_display", "release_group_id",
    "release_group_title", "title", "status", "format_detail", "format_category", "media_count",
    "track_count", "country_code", "release_date", "release_year", "release_decade", "date_precision",
    "label", "catalog_number", "barcode", "primary_type", "release_age_years", "track_count_zscore",
    "is_multi_disc", "collected_at", "source_endpoint",
]

CREATE_SQL = """
CREATE TABLE IF NOT EXISTS releases (
    release_id TEXT PRIMARY KEY,
    artist_mbid TEXT NOT NULL,
    artist_name TEXT NOT NULL CHECK (artist_name = 'Pink Floyd'),
    artist_credit_display TEXT NOT NULL,
    release_group_id TEXT,
    release_group_title TEXT NOT NULL,
    title TEXT NOT NULL,
    status TEXT NOT NULL,
    format_detail TEXT NOT NULL,
    format_category TEXT NOT NULL CHECK (format_category <> 'Digital'),
    media_count INTEGER NOT NULL CHECK (media_count >= 1),
    track_count INTEGER NOT NULL CHECK (track_count >= 1),
    country_code TEXT,
    release_date TEXT NOT NULL,
    release_year INTEGER NOT NULL CHECK (release_year BETWEEN 1960 AND 2026),
    release_decade INTEGER NOT NULL,
    date_precision TEXT NOT NULL,
    label TEXT,
    catalog_number TEXT,
    barcode TEXT,
    primary_type TEXT,
    release_age_years INTEGER NOT NULL,
    track_count_zscore REAL NOT NULL,
    is_multi_disc INTEGER NOT NULL CHECK (is_multi_disc IN (0, 1)),
    collected_at TEXT NOT NULL,
    source_endpoint TEXT NOT NULL,
    loaded_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_releases_year ON releases(release_year);
CREATE INDEX IF NOT EXISTS idx_releases_format ON releases(format_category);
CREATE INDEX IF NOT EXISTS idx_releases_country ON releases(country_code);
"""


def _clean_value(value):
    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        return value.item()
    return value


def insert_frame(connection: sqlite3.Connection, frame: pd.DataFrame) -> int:
    placeholders = ",".join("?" for _ in TABLE_COLUMNS)
    columns = ",".join(TABLE_COLUMNS)
    sql = f"INSERT INTO releases ({columns}) VALUES ({placeholders}) ON CONFLICT(release_id) DO NOTHING"
    rows = [tuple(_clean_value(row[column]) for column in TABLE_COLUMNS) for _, row in frame.iterrows()]
    before = connection.total_changes
    connection.executemany(sql, rows)
    connection.commit()
    return connection.total_changes - before


def verification_results(connection: sqlite3.Connection) -> dict:
    count_row = connection.execute(
        "SELECT COUNT(*), COUNT(DISTINCT release_id) FROM releases"
    ).fetchone()
    duplicates = connection.execute(
        "SELECT COUNT(*) FROM (SELECT release_id FROM releases GROUP BY release_id HAVING COUNT(*) > 1)"
    ).fetchone()[0]
    by_format = [dict(row) for row in connection.execute(
        "SELECT format_category, COUNT(*) AS release_count FROM releases GROUP BY format_category ORDER BY release_count DESC"
    )]
    by_decade = [dict(row) for row in connection.execute(
        "SELECT release_decade, COUNT(*) AS release_count, ROUND(AVG(track_count), 2) AS average_tracks FROM releases GROUP BY release_decade ORDER BY release_decade"
    )]
    sample = [dict(row) for row in connection.execute(
        "SELECT release_id, title, release_date, format_category, country_code, track_count FROM releases ORDER BY release_date, release_id LIMIT 5"
    )]
    return {
        "stored_rows": int(count_row[0]),
        "distinct_release_ids": int(count_row[1]),
        "duplicate_primary_keys": int(duplicates),
        "counts_by_format": by_format,
        "counts_by_decade": by_decade,
        "sample_rows": sample,
    }


def load_twice(input_path: Path, database_path: Path, reset: bool = False, summary_path: Path | None = None) -> dict:
    frame = pd.read_csv(input_path, dtype={"release_id": "string", "barcode": "string", "catalog_number": "string"})
    missing = [column for column in TABLE_COLUMNS if column not in frame.columns]
    if missing:
        raise ValueError(f"Processed data is missing database columns: {missing}")
    if reset and database_path.exists():
        database_path.unlink()
    database_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(database_path)
    connection.row_factory = sqlite3.Row
    try:
        connection.executescript(CREATE_SQL)
        inserted_first = insert_frame(connection, frame)
        count_after_first = connection.execute("SELECT COUNT(*) FROM releases").fetchone()[0]
        inserted_second = insert_frame(connection, frame)
        count_after_second = connection.execute("SELECT COUNT(*) FROM releases").fetchone()[0]
        checks = verification_results(connection)
    finally:
        connection.close()

    summary = {
        "processed_records": int(len(frame)),
        "inserted_first_load": int(inserted_first),
        "stored_after_first_load": int(count_after_first),
        "inserted_second_load": int(inserted_second),
        "stored_after_second_load": int(count_after_second),
        "idempotency_passed": bool(count_after_first == count_after_second == checks["stored_rows"] and inserted_second == 0),
        **checks,
    }
    summary_path = summary_path or DATA_DIR / "database_verification.json"
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    print(
        f"SQLite: first insert={inserted_first}, second insert={inserted_second}, "
        f"stored={count_after_second}, idempotent={summary['idempotency_passed']}"
    )
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Load cleaned releases into SQLite and demonstrate idempotency.")
    parser.add_argument("--input", type=Path, default=DATA_DIR / "processed_data.csv")
    parser.add_argument("--database", type=Path, default=DATA_DIR / "output.db")
    parser.add_argument("--reset", action="store_true")
    args = parser.parse_args()
    load_twice(args.input, args.database, args.reset)


if __name__ == "__main__":
    main()
