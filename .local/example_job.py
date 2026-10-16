#!/usr/bin/env python3
"""PyFlink ETL — raw -> transform/normalize -> output topic.

Consumes the raw source topic, normalizes each record (clean field names,
split the "<value> <unit>" reading into typed fields, canonical timestamps),
and produces to a distinct output topic.

Design contract (see openspec change add-flink-operator-poc):
  * pipeline.max-parallelism = 720   (irreversible key-group count, set here)
  * state backend = RocksDB with durable checkpoints
  * Kafka connection comes from the ENVIRONMENT, never hardcoded, so the same
    file runs unchanged against local PLAINTEXT Kafka and Aiven SASL_SSL+mTLS.

Run locally for a quick check (needs a local PyFlink install + local Kafka):
    pip install apache-flink
    python .local/example_job.py

In-cluster it is submitted by deploy/flink/application-job.yaml (application
mode). The flinkConfiguration there sets the same state/checkpoint/maxParallelism
values; this file sets them too so a standalone local run behaves identically.
"""
import os

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment


# --- Connection + topics from the environment (swappable, never hardcoded) ---
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SECURITY_PROTOCOL = os.environ.get("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
SASL_MECHANISM = os.environ.get("KAFKA_SASL_MECHANISM", "")
SASL_USERNAME = os.environ.get("KAFKA_SASL_USERNAME", "")
SASL_PASSWORD = os.environ.get("KAFKA_SASL_PASSWORD", "")
SOURCE_TOPIC = os.environ.get("SOURCE_TOPIC", "ambyint-io-raw")
OUTPUT_TOPIC = os.environ.get("OUTPUT_TOPIC", "ambyint-io-normalized")

# Platform-wide, irreversible. Key-group count fixed at first checkpoint.
MAX_PARALLELISM = 720


def _kafka_security_properties() -> str:
    """Render Flink SQL connector security properties for the active protocol.

    PLAINTEXT (local): no extra properties.
    SASL_SSL (Aiven):  inject sasl mechanism + JAAS from env-provided creds.
    This is the swap point — values come from the Secret, the code is identical.
    """
    if SECURITY_PROTOCOL == "PLAINTEXT":
        return f"'properties.security.protocol' = 'PLAINTEXT',"
    jaas = (
        "org.apache.kafka.common.security.scram.ScramLoginModule required "
        f'username="{SASL_USERNAME}" password="{SASL_PASSWORD}";'
    )
    return (
        f"'properties.security.protocol' = '{SECURITY_PROTOCOL}',"
        f"'properties.sasl.mechanism' = '{SASL_MECHANISM}',"
        f"'properties.sasl.jaas.config' = '{jaas}',"
    )


def main() -> None:
    env = StreamExecutionEnvironment.get_execution_environment()

    # State backend + durable checkpointing. In-cluster these are also set via
    # flinkConfiguration; setting them here keeps a standalone local run honest.
    env.set_max_parallelism(MAX_PARALLELISM)
    env.enable_checkpointing(30_000)  # 30s
    env.get_config().set_global_job_parameter("state.backend", "rocksdb")

    t_env = StreamTableEnvironment.create(env)
    sec = _kafka_security_properties()

    # --- Source: raw topic -----------------------------------------------
    t_env.execute_sql(f"""
        CREATE TABLE raw_src (
            EventId   STRING,
            tenant    STRING,
            Device    STRING,
            reading   STRING,
            Ts        STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{SOURCE_TOPIC}',
            'properties.bootstrap.servers' = '{BOOTSTRAP}',
            {sec}
            'properties.group.id' = 'flink-etl-normalize',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true'
        )
    """)

    # --- Sink: normalized output topic -----------------------------------
    t_env.execute_sql(f"""
        CREATE TABLE normalized_out (
            event_id   STRING,
            tenant     STRING,
            device     STRING,
            reading    DOUBLE,
            unit       STRING,
            event_time STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{OUTPUT_TOPIC}',
            'properties.bootstrap.servers' = '{BOOTSTRAP}',
            {sec}
            'format' = 'json'
        )
    """)

    # --- Transform / normalize -------------------------------------------
    # - canonical lower_snake field names
    # - split "<value> <unit>" reading into a typed DOUBLE + unit STRING
    # - pass through tenant/device/event_id, canonical event_time
    t_env.execute_sql(f"""
        INSERT INTO normalized_out
        SELECT
            EventId                                             AS event_id,
            LOWER(tenant)                                       AS tenant,
            LOWER(Device)                                       AS device,
            CAST(SPLIT_INDEX(reading, ' ', 0) AS DOUBLE)        AS reading,
            SPLIT_INDEX(reading, ' ', 1)                        AS unit,
            Ts                                                  AS event_time
        FROM raw_src
    """)


if __name__ == "__main__":
    main()
