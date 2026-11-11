import argparse
import json
import time
from pathlib import Path
from __future__ import annotations

from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable

from .config import DATA_DIR, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC


def consume(
    output_path: Path,
    run_id: str,
    expected: int,
    idle_timeout: int = 45,
    bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
    topic: str = KAFKA_TOPIC,
) -> dict:
    
    if expected < 200:
        raise ValueError("expected message count must be at least 200")

    # make dir for output
    output_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers.split(","),
            group_id=f"sis2-pf-{run_id}",
            auto_offset_reset="latest",
            enable_auto_commit=False,
            value_deserializer=lambda value: json.loads(value.decode("utf-8")),
            consumer_timeout_ms=1000,
            request_timeout_ms=30000,
        )
    except NoBrokersAvailable as exc:
        raise RuntimeError(f"No Kafka broker is available at {bootstrap_servers}") from exc

    consumed = 0
    skipped_other_runs = 0
    last_matching_message = time.monotonic()

    try:
        with output_path.open("w", encoding="utf-8") as handle:
            while consumed < expected:
                received_in_poll = False

                # pull batches from Kafka
                batches = consumer.poll(timeout_ms=1000, max_records=min(500, expected - consumed))

                for messages in batches.values():
                    for message in messages:
                        received_in_poll = True

                        envelope = message.value
                        if envelope.get("run_id") != run_id:
                            skipped_other_runs += 1
                            continue

                        handle.write(json.dumps(envelope["record"], ensure_ascii=False) + "\n")
                        consumed += 1
                        last_matching_message = time.monotonic()

                        if consumed >= expected:
                            break

                    if consumed >= expected:
                        break

                if not received_in_poll and time.monotonic() - last_matching_message > idle_timeout:
                    raise TimeoutError(
                        f"Consumer stopped after {idle_timeout}s idle: received {consumed}/{expected} messages for run_id={run_id}."
                    ) # handle cases when kafka would lag

        # commit offsets
        consumer.commit()

    except (KafkaError, json.JSONDecodeError, KeyError) as exc:
        raise RuntimeError(f"Kafka message processing failed after {consumed} records: {exc}") from exc
    
    finally:
        # close connectoin
        consumer.close()

    summary = {
        "run_id": run_id,
        "topic": topic,
        "bootstrap_servers": bootstrap_servers,
        "expected_messages": expected,
        "consumed_messages": consumed,
        "skipped_messages_from_other_runs": skipped_other_runs,
        "termination_condition": "consumed == expected",
        "output": str(output_path.relative_to(DATA_DIR.parent)),
    }

    # write the json summary log
    (DATA_DIR / "consumer_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"Consumed {consumed}/{expected} JSON messages from topic '{topic}' for run_id={run_id}")

    # return the json log
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Consume one finite, run-scoped batch from Kafka.")
    
    parser.add_argument("--output", type=Path, default=DATA_DIR / "consumed_messages.jsonl")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--expected", type=int, required=True)
    parser.add_argument("--idle-timeout", type=int, default=45)
    parser.add_argument("--bootstrap-servers", default=KAFKA_BOOTSTRAP_SERVERS)
    parser.add_argument("--topic", default=KAFKA_TOPIC)
    args = parser.parse_args()

    consume(args.output, args.run_id, args.expected, args.idle_timeout, args.bootstrap_servers, args.topic)


if __name__ == "__main__":
    main()
