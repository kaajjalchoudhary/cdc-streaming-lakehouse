"""End to end through a real Kafka broker. Runs when KAFKA_BOOTSTRAP is set (CI starts Redpanda)."""

from __future__ import annotations

import os

import pytest

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP")
pytestmark = pytest.mark.skipif(not BOOTSTRAP, reason="set KAFKA_BOOTSTRAP to run against a broker")


def test_kafka_to_delta_reproduces_the_source_table(spark, lake) -> None:
    from cdcstream import sources
    from cdcstream.generate import GenerateConfig, generate
    from cdcstream.stream import run
    from tests.test_streaming import silver_state

    stream = generate(GenerateConfig(events=600, max_orders=120, seed=99))
    topic = f"orders.cdc.test.{os.getpid()}"
    sources.produce_to_kafka(stream.records, BOOTSTRAP, topic)

    batches = run(spark, lake, sources.kafka_stream(spark, BOOTSTRAP, topic, max_offsets=200))

    assert len(batches) >= 3
    live, deleted = silver_state(spark, lake)
    assert live == stream.final_orders
    assert deleted == stream.deleted_ids
    bronze = spark.read.format("delta").load(lake.table("bronze_cdc_events"))
    offsets = bronze.select("partition", "offset").distinct().count()
    assert offsets == len(stream.records), "every Kafka message landed in bronze exactly once"
