#!/usr/bin/env python3
"""Output consumer — reads the transformed/normalized topic and surfaces records.

Confirms the end-to-end round-trip: publisher_sim -> raw topic -> Flink ETL ->
output topic -> this consumer. Connection + topic come from the environment so
it is swappable (local PLAINTEXT -> Aiven SASL_SSL) with no code change.

Usage:
    pip install kafka-python
    python .local/app.py

Env (all optional, defaulted for the local PoC):
    KAFKA_BOOTSTRAP_SERVERS   default localhost:9092
    OUTPUT_TOPIC              default ambyint-io-normalized
"""
import json
import os

try:
    from kafka import KafkaConsumer
except ImportError:
    raise SystemExit("kafka-python is required: pip install kafka-python")

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
OUTPUT_TOPIC = os.environ.get("OUTPUT_TOPIC", "ambyint-io-normalized")


def main() -> None:
    consumer = KafkaConsumer(
        OUTPUT_TOPIC,
        bootstrap_servers=BOOTSTRAP,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="poc-output-consumer",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )

    print(f"[app] <- {BOOTSTRAP} topic={OUTPUT_TOPIC} (Ctrl-C to stop)")
    seen = 0
    try:
        for msg in consumer:
            seen += 1
            rec = msg.value
            # Surface the normalized shape so the transform is observable.
            print(f"[app] #{seen} key={msg.key} "
                  f"tenant={rec.get('tenant')} device={rec.get('device')} "
                  f"reading={rec.get('reading')} unit={rec.get('unit')} "
                  f"event_time={rec.get('event_time')}")
    except KeyboardInterrupt:
        print(f"\n[app] interrupted, total consumed={seen}")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
