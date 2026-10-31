"""Where change records come from: a Kafka topic, or files with the same key/value shape."""

from __future__ import annotations

import json
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from cdcstream.config import TOPIC
from cdcstream.generate import Record

FILE_SCHEMA = "key string, value string"


def write_files(records: list[Record], folder: Path, per_file: int = 250) -> int:
    """Split records into numbered JSON-lines files. Order inside the stream is preserved."""
    folder.mkdir(parents=True, exist_ok=True)
    chunks = [records[i : i + per_file] for i in range(0, len(records), per_file)]
    for n, chunk in enumerate(chunks):
        lines = (json.dumps({"key": r.key, "value": r.value}) for r in chunk)
        (folder / f"part-{n:05d}.json").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return len(chunks)


def produce_to_kafka(records: list[Record], bootstrap: str, topic: str = TOPIC) -> int:
    """Send records keyed by order_id, so every change to one order lands in one partition."""
    from confluent_kafka import Producer

    producer = Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True})
    for r in records:
        producer.produce(topic, key=r.key.encode(), value=None if r.value is None else r.value.encode())
        producer.poll(0)
    remaining = producer.flush(30)
    if remaining:
        raise RuntimeError(f"{remaining} messages were not delivered to {topic}")
    return len(records)


def file_stream(spark: SparkSession, folder: Path, files_per_trigger: int = 1) -> DataFrame:
    return (
        spark.readStream.schema(FILE_SCHEMA)
        .option("maxFilesPerTrigger", files_per_trigger)
        .json(str(folder))
        .select("key", "value", F.lit(None).cast("int").alias("partition"), F.lit(None).cast("long").alias("offset"))
    )


def kafka_stream(spark: SparkSession, bootstrap: str, topic: str = TOPIC, max_offsets: int = 400) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", max_offsets)
        .load()
        .select(
            F.col("key").cast("string").alias("key"),
            F.col("value").cast("string").alias("value"),
            "partition",
            "offset",
        )
    )
