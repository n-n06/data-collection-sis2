from __future__ import annotations

import argparse
import json
import uuid
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

from .config import DATA_DIR, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC
from .test_records import deliberate_invalid_records


def load_records(path: Path) -> list[dict]:
    records = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    if len(records) < 200:
        raise ValueError(f"At least 200 records are required; found {len(records)} in {path}.")
    return records


def publish(
    input_path: Path,
    run_id: str,
    inject_invalid: int = 3,
    bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
    topic: str = KAFKA_TOPIC,
) -> dict:
    records = load_records(input_path)
    if inject_invalid not in {0, 3}:
        raise ValueError("inject_invalid must be 0 or 3")
    if inject_invalid:
        records += deliberate_invalid_records(records[0])

    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers.split(","),
            key_serializer=lambda value: value.encode("utf-8"),
            value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
            acks="all",
            retries=5,
            request_timeout_ms=30000,
            max_block_ms=30000,
        )
    except NoBrokersAvailable as exc:
        raise RuntimeError(f"No Kafka broker is available at {bootstrap_servers}") from exc

    published = 0
    try:
        futures = []
        for sequence, record in enumerate(records, start=1):
            envelope = {"run_id": run_id, "sequence": sequence, "record": record}
            futures.append(producer.send(topic, key=record["release_id"], value=envelope))
        for future in futures:
            future.get(timeout=30)
            published += 1
        producer.flush(timeout=30)
    except KafkaError as exc:
        raise RuntimeError(f"Kafka transmission failed after {published} acknowledgements: {exc}") from exc
    finally:
        producer.close(timeout=10)

    summary = {
        "run_id": run_id,
        "topic": topic,
        "bootstrap_servers": bootstrap_servers,
        "source_records": len(records) - inject_invalid,
        "deliberate_invalid_records": inject_invalid,
        "published_messages": published,
    }
    (DATA_DIR / "producer_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"Published {published} JSON messages to topic '{topic}' for run_id={run_id}")
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish physical release records to Kafka as JSON.")
    parser.add_argument("--input", type=Path, default=DATA_DIR / "raw_data.jsonl")
    parser.add_argument("--run-id", default=str(uuid.uuid4()))
    parser.add_argument("--inject-invalid", type=int, choices=[0, 3], default=3)
    parser.add_argument("--bootstrap-servers", default=KAFKA_BOOTSTRAP_SERVERS)
    parser.add_argument("--topic", default=KAFKA_TOPIC)
    args = parser.parse_args()
    publish(args.input, args.run_id, args.inject_invalid, args.bootstrap_servers, args.topic)


if __name__ == "__main__":
    main()
