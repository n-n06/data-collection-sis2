import json
from pathlib import Path


def test_reference_pipeline_counts_are_consistent():
    summary = json.loads(Path("data/pipeline_summary.json").read_text(encoding="utf-8"))
    assert summary["collected_api_records"] >= 200
    assert summary["published_kafka_messages"] == summary["consumed_kafka_messages"]
    assert summary["valid_records"] + summary["invalid_records"] == summary["consumed_kafka_messages"]
    assert summary["records_after_cleaning"] == summary["stored_sqlite_records"]
    assert summary["second_load_inserted"] == 0
    assert summary["all_checks_passed"] is True
