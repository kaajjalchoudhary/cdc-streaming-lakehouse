"""Run the streaming query: source -> foreachBatch(apply_batch) -> Delta, with a checkpoint."""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession

from cdcstream.apply import BatchStats, apply_batch, ensure_tables
from cdcstream.config import Lake

log = logging.getLogger(__name__)


def run(spark: SparkSession, lake: Lake, source: DataFrame, available_now: bool = True) -> list[BatchStats]:
    """Process everything currently available (or run continuously) and return per-batch stats.

    The checkpoint records which source offsets each batch id covered. After a crash, Spark
    re-runs the last batch id with exactly the same offsets; apply_batch is idempotent for a
    given batch id, so nothing is lost or applied twice.
    """
    ensure_tables(spark, lake)
    results: list[BatchStats] = []

    def handle(batch: DataFrame, batch_id: int) -> None:
        results.append(apply_batch(batch.sparkSession, lake, batch, batch_id))

    writer = source.writeStream.foreachBatch(handle).option("checkpointLocation", lake.checkpoint)
    writer = writer.trigger(availableNow=True) if available_now else writer.trigger(processingTime="10 seconds")
    query = writer.queryName("orders_cdc").start()
    query.awaitTermination()
    if query.exception():
        raise RuntimeError(query.exception())
    return results
