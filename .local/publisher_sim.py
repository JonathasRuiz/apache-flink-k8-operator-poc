#!/usr/bin/env python3
"""Publisher simulator — produces synthetic records to the raw source topic.

Reads Kafka connection + topic from the environment so it stays swappable
(local PLAINTEXT -> Aiven SASL_SSL) with no code change, mirroring how the
Flink job is wired.

Usage:
    pip install kafka-python
    python .local/publisher_sim.py --rate 50 --count 0   # 50 msg/s, forever

Env (all optional, defaulted for the local PoC):
    KAFKA_BOOTSTRAP_SERVERS   default localhost:9092
    SOURCE_TOPIC              default ambyint-io-raw
"""
import argparse
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

try:
    from kafka import KafkaProducer
except ImportError:
    raise SystemExit("kafka-python is required: pip install kafka-python")

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SOURCE_TOPIC = os.environ.get("SOURCE_TOPIC", "ambyint-io-raw")

# A few synthetic tenants so downstream can observe per-key behavior / skew.
TENANTS = ["tenant-a", "tenant-b", "tenant-c"]
DEVICES = [f"device-{i:03d}" for i in range(20)]


def make_record() -> dict:
    """A deliberately 'raw' record: mixed case, loose types, needs normalizing."""
    return {
        "EventId": str(uuid.uuid4()),
        "tenant": random.choice(TENANTS),
        "Device": random.choice(DEVICES),
        # raw reading as a string with trailing unit — the ETL normalizes this
        "reading": f"{random.uniform(0, 100):.3f} psi",
        "Ts": datetime.now(timezone.utc).isoformat(),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish synthetic records to Kafka.")
    parser.add_argument("--rate", type=float, default=20.0,
                        help="messages per second (default 20)")
    parser.add_argument("--count", type=int, default=0,
                        help="total messages to send; 0 = run forever (default 0)")
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
    )

    print(f"[publisher_sim] -> {BOOTSTRAP} topic={SOURCE_TOPIC} "
          f"rate={args.rate}/s count={'inf' if args.count == 0 else args.count}")

    interval = 1.0 / args.rate if args.rate > 0 else 0
    sent = 0
    try:
        while args.count == 0 or sent < args.count:
            rec = make_record()
            # key by tenant so partitioning / keyspace behavior is observable
            producer.send(SOURCE_TOPIC, key=rec["tenant"], value=rec)
            sent += 1
            if sent % 100 == 0:
                print(f"[publisher_sim] sent={sent}")
            if interval:
                time.sleep(interval)
    except KeyboardInterrupt:
        print("\n[publisher_sim] interrupted")
    finally:
        producer.flush()
        producer.close()
        print(f"[publisher_sim] done, total sent={sent}")


if __name__ == "__main__":
    main()
