from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

import pandas as pd
import pandera as pa
from pandera import Check, Column, DataFrameSchema
from pandera.errors import SchemaErrors

from .config import CURRENT_YEAR, DATA_DIR, PHYSICAL_FORMATS

UUID_PATTERN = r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
DATE_PATTERN = r"^\d{4}(?:-\d{2})?(?:-\d{2})?$"
ALLOWED_STATUSES = ["Official", "Promotion", "Bootleg", "Pseudo-Release", "Withdrawn", "Unknown"]


def build_schema() -> DataFrameSchema:
    """Pandera schema with type, required, range, category, date, UUID and uniqueness rules."""
    return DataFrameSchema(
        {
            "release_id": Column(str, checks=Check.str_matches(UUID_PATTERN), unique=True, nullable=False),
            "artist_mbid": Column(str, checks=Check.str_matches(UUID_PATTERN), nullable=False),
            "artist_name": Column(str, checks=Check.eq("Pink Floyd"), nullable=False),
            "title": Column(str, checks=Check.str_length(min_value=1, max_value=300), nullable=False),
            "status": Column(str, checks=Check.isin(ALLOWED_STATUSES), nullable=False),
            "format_detail": Column(str, checks=Check.str_length(min_value=1), nullable=False),
            "format_category": Column(str, checks=Check.isin(sorted(PHYSICAL_FORMATS)), nullable=False),
            "media_count": Column(int, checks=Check.in_range(1, 20), nullable=False, coerce=True),
            "track_count": Column(int, checks=Check.in_range(1, 500), nullable=False, coerce=True),
            "country_code": Column(str, checks=Check.str_matches(r"^[A-Z]{2}$"), nullable=True),
            "release_date": Column(str, checks=Check.str_matches(DATE_PATTERN), nullable=False),
            "release_year": Column(int, checks=Check.in_range(1960, CURRENT_YEAR), nullable=False, coerce=True),
            "date_precision": Column(str, checks=Check.isin(["year", "month", "day", "partial"]), nullable=False),
            "is_test_invalid": Column(bool, nullable=False, coerce=True),
        },
        strict=False,
        coerce=False,
        ordered=False,
        name="physical_pink_floyd_releases",
    )


def _partition_invalid(frame: pd.DataFrame) -> pd.Series:
    """Return a row mask mirroring schema rules so individual bad records can be quarantined."""
    release_ids = frame["release_id"].astype(str)
    duplicate_ids = release_ids.duplicated(keep=False)
    return (
        ~release_ids.str.match(UUID_PATTERN, na=False)
        | duplicate_ids
        | frame["artist_mbid"].astype(str).str.match(UUID_PATTERN, na=False).eq(False)
        | frame["artist_name"].ne("Pink Floyd")
        | frame["title"].fillna("").astype(str).str.len().between(1, 300).eq(False)
        | frame["status"].isin(ALLOWED_STATUSES).eq(False)
        | frame["format_category"].isin(PHYSICAL_FORMATS).eq(False)
        | pd.to_numeric(frame["media_count"], errors="coerce").between(1, 20).eq(False)
        | pd.to_numeric(frame["track_count"], errors="coerce").between(1, 500).eq(False)
        | frame["release_date"].fillna("").astype(str).str.match(DATE_PATTERN).eq(False)
        | pd.to_numeric(frame["release_year"], errors="coerce").between(1960, CURRENT_YEAR).eq(False)
        | frame["date_precision"].isin(["year", "month", "day", "partial"]).eq(False)
    )


def validate(input_path: Path, valid_output: Path | None = None) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    valid_output = valid_output or DATA_DIR / "valid_records.csv"
    records = [json.loads(line) for line in input_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    frame = pd.DataFrame(records)
    schema = build_schema()
    failure_cases = pd.DataFrame(columns=["schema_context", "column", "check", "check_number", "failure_case", "index"])
    try:
        schema.validate(frame, lazy=True)
    except SchemaErrors as exc:
        failure_cases = exc.failure_cases.copy()

    invalid_mask = _partition_invalid(frame)
    invalid = frame.loc[invalid_mask].copy()
    valid = frame.loc[~invalid_mask].copy()

    if not failure_cases.empty:
        failure_cases.to_csv(DATA_DIR / "validation_errors.csv", index=False)
    else:
        (DATA_DIR / "validation_errors.csv").write_text("schema_context,column,check,failure_case,index\n", encoding="utf-8")
    valid.to_csv(valid_output, index=False)
    invalid.to_csv(DATA_DIR / "invalid_records.csv", index=False)

    summary = {
        "input_records": int(len(frame)),
        "valid_records": int(len(valid)),
        "invalid_records": int(len(invalid)),
        "pandera_failure_cases": int(len(failure_cases)),
        "deliberate_invalid_detected": int(invalid.get("is_test_invalid", pd.Series(dtype=bool)).fillna(False).astype(bool).sum()),
        "handling_strategy": "quarantine invalid rows; exclude them from cleaning and database loading",
        "schema_rule_count": 14,
    }
    (DATA_DIR / "validation_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"Pandera validation: {len(valid)} valid, {len(invalid)} invalid")
    return valid, invalid, summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate consumed Kafka records with Pandera.")
    parser.add_argument("--input", type=Path, default=DATA_DIR / "consumed_messages.jsonl")
    parser.add_argument("--valid-output", type=Path, default=DATA_DIR / "valid_records.csv")
    args = parser.parse_args()
    validate(args.input, args.valid_output)


if __name__ == "__main__":
    main()
