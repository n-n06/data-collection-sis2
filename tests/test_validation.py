import json
from pathlib import Path

import pandas as pd

from src.test_records import deliberate_invalid_records
from src.validator import _partition_invalid, build_schema


def test_schema_has_at_least_five_meaningful_rules():
    schema = build_schema()
    assert len(schema.columns) >= 10
    for required in ["release_id", "title", "status", "format_category", "track_count", "release_year"]:
        assert required in schema.columns


def test_three_deliberate_invalid_records_are_detected():
    sample = json.loads(Path("data/raw_data.jsonl").read_text(encoding="utf-8").splitlines()[0])
    bad = deliberate_invalid_records(sample)
    mask = _partition_invalid(pd.DataFrame(bad))
    assert len(bad) == 3
    assert mask.all()
