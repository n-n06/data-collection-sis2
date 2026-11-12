from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
import uuid
from pathlib import Path

from src.analysis import analyze
from src.api_client import collect
from src.cleaner import clean_and_transform
from src.config import DATA_DIR, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC
from src.loader import load_twice
from src.validator import validate


def process_pipeline(reset_database: bool = True) -> dict:
    _, _, validation = validate(DATA_DIR / "consumed_messages.jsonl")
    _, cleaning = clean_and_transform(DATA_DIR / "valid_records.csv")
    database = load_twice(DATA_DIR / "processed_data.csv", DATA_DIR / "output.db", reset=reset_database)
    eda = analyze(DATA_DIR / "processed_data.csv")

    collection = json.loads((DATA_DIR / "collection_summary.json").read_text(encoding="utf-8"))
    producer = json.loads((DATA_DIR / "producer_summary.json").read_text(encoding="utf-8"))
    consumer = json.loads((DATA_DIR / "consumer_summary.json").read_text(encoding="utf-8"))
    summary = {
        "collected_api_records": collection["records_collected"],
        "published_kafka_messages": producer["published_messages"],
        "consumed_kafka_messages": consumer["consumed_messages"],
        "valid_records": validation["valid_records"],
        "invalid_records": validation["invalid_records"],
        "records_after_cleaning": cleaning["records_after_cleaning"],
        "stored_sqlite_records": database["stored_rows"],
        "second_load_inserted": database["inserted_second_load"],
        "idempotency_passed": database["idempotency_passed"],
        "figures_generated": len(eda["figures"]),
    }
    expected = summary["collected_api_records"] + validation["invalid_records"]
    checks = {
        "published_equals_expected": summary["published_kafka_messages"] == expected,
        "consumed_equals_published": summary["consumed_kafka_messages"] == summary["published_kafka_messages"],
        "valid_plus_invalid_equals_consumed": summary["valid_records"] + summary["invalid_records"] == summary["consumed_kafka_messages"],
        "cleaned_equals_stored": summary["records_after_cleaning"] == summary["stored_sqlite_records"],
        "idempotency": summary["idempotency_passed"],
    }
    summary["verification_checks"] = checks
    summary["all_checks_passed"] = all(checks.values())
    (DATA_DIR / "pipeline_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    if not summary["all_checks_passed"]:
        raise RuntimeError(f"Pipeline verification failed: {checks}")
    print(json.dumps(summary, indent=2))
    return summary


def ensure_topic(bootstrap_servers: str, topic: str) -> None:
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError

    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers.split(","), client_id="pink-floyd-topic-admin")
    try:
        if topic not in admin.list_topics():
            try:
                admin.create_topics([NewTopic(name=topic, num_partitions=1, replication_factor=1)], validate_only=False)
            except TopicAlreadyExistsError:
                pass
    finally:
        admin.close()


def run_full(source: str, target: int, inject_invalid: int, bootstrap_servers: str, topic: str) -> dict:
    records = collect(source=source, target=target)
    expected = len(records) + inject_invalid
    run_id = str(uuid.uuid4())
    ensure_topic(bootstrap_servers, topic)

    consumer_command = [
        sys.executable, "-m", "src.consumer", "--run-id", run_id, "--expected", str(expected),
        "--bootstrap-servers", bootstrap_servers, "--topic", topic,
    ]
    consumer_process = subprocess.Popen(consumer_command, cwd=Path(__file__).resolve().parent)
    try:
        time.sleep(4)  # consumer must subscribe with auto_offset_reset=latest before this run is published
        producer_command = [
            sys.executable, "-m", "src.producer", "--run-id", run_id,
            "--inject-invalid", str(inject_invalid), "--bootstrap-servers", bootstrap_servers, "--topic", topic,
        ]
        subprocess.run(producer_command, cwd=Path(__file__).resolve().parent, check=True)
        return_code = consumer_process.wait(timeout=120)
        if return_code != 0:
            raise RuntimeError(f"Consumer process exited with status {return_code}")
    finally:
        if consumer_process.poll() is None:
            consumer_process.terminate()
            consumer_process.wait(timeout=10)
    return process_pipeline(reset_database=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="End-to-end MusicBrainz -> Kafka -> Pandera -> SQLite -> EDA pipeline")
    subparsers = parser.add_subparsers(dest="command", required=True)

    collect_parser = subparsers.add_parser("collect", help="collect and preserve raw data")
    collect_parser.add_argument("--source", choices=["api", "snapshot"], default="api")
    collect_parser.add_argument("--target", type=int, default=250)

    process_parser = subparsers.add_parser("process", help="validate, clean, load twice, verify and analyze consumed messages")
    process_parser.add_argument("--keep-database", action="store_true", help="do not recreate output.db before the idempotency test")

    full_parser = subparsers.add_parser("full", help="run the complete pipeline with a real Kafka broker")
    full_parser.add_argument("--source", choices=["api", "snapshot"], default="api")
    full_parser.add_argument("--target", type=int, default=250)
    full_parser.add_argument("--inject-invalid", type=int, choices=[0, 3], default=3)
    full_parser.add_argument("--bootstrap-servers", default=KAFKA_BOOTSTRAP_SERVERS)
    full_parser.add_argument("--topic", default=KAFKA_TOPIC)

    args = parser.parse_args()
    if args.command == "collect":
        collect(args.source, args.target)
    elif args.command == "process":
        process_pipeline(reset_database=not args.keep_database)
    elif args.command == "full":
        run_full(args.source, args.target, args.inject_invalid, args.bootstrap_servers, args.topic)


if __name__ == "__main__":
    main()
