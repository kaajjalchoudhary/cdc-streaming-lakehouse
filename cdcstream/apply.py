"""Apply one micro-batch of change records to the lakehouse (the foreachBatch function).

For every micro-batch:
1. bronze.cdc_events  every record as received, plus Kafka partition/offset, appended
                      idempotently with Delta's txnAppId/txnVersion (= the batch id);
2. bronze.cdc_dead_letters  records that are not valid change envelopes;
3. silver.orders      MERGE of the latest change per order. A change is applied only if its
                      log sequence number (LSN) is newer than the row's, so duplicates and
                      replayed old events are no-ops. Deletes are kept as soft-deleted rows
                      so a stale update can never resurrect a deleted order;
4. gold.daily_orders  rebuilt for the order dates the batch touched.

Re-running a batch id (after a crash between the write and the checkpoint commit) changes
nothing, which is what makes the pipeline exactly-once end to end.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

from cdcstream.config import Lake

log = logging.getLogger(__name__)

ROW = StructType(
    [
        StructField("order_id", LongType()),
        StructField("customer_id", LongType()),
        StructField("status", StringType()),
        StructField("amount", DoubleType()),
        StructField("created_at", StringType()),
        StructField("updated_at", StringType()),
    ]
)
ENVELOPE = StructType(
    [
        StructField("before", ROW),
        StructField("after", ROW),
        StructField("op", StringType()),
        StructField("ts_ms", LongType()),
        StructField("source", StructType([StructField("lsn", LongType()), StructField("table", StringType())])),
    ]
)
SILVER_DDL = (
    "order_id BIGINT, customer_id BIGINT, status STRING, amount DOUBLE, created_at TIMESTAMP, "
    "updated_at TIMESTAMP, is_deleted BOOLEAN, _lsn BIGINT, _op STRING, _applied_at TIMESTAMP"
)
GOLD_DDL = "order_date DATE, status STRING, orders BIGINT, revenue DOUBLE, _built_at TIMESTAMP"


@dataclass
class BatchStats:
    batch_id: int
    records: int = 0
    tombstones: int = 0
    dead_letters: int = 0
    changes: int = 0
    keys: int = 0
    dates: list[date] = field(default_factory=list)


def ensure_tables(spark: SparkSession, lake: Lake) -> None:
    for name, ddl, parts in (
        ("silver_orders", SILVER_DDL, []),
        ("gold_daily_orders", GOLD_DDL, []),
    ):
        path = lake.table(name)
        if not DeltaTable.isDeltaTable(spark, path):
            writer = spark.createDataFrame([], ddl).write.format("delta")
            if parts:
                writer = writer.partitionBy(*parts)
            writer.save(path)


def _append_once(df: DataFrame, path: str, app_id: str, batch_id: int) -> None:
    """Delta idempotent write: a (txnAppId, txnVersion) pair is committed at most once."""
    (
        df.write.format("delta").mode("append")
        .option("txnAppId", app_id)
        .option("txnVersion", batch_id)
        .save(path)
    )


def apply_batch(spark: SparkSession, lake: Lake, batch: DataFrame, batch_id: int) -> BatchStats:
    stats = BatchStats(batch_id=batch_id)
    records = (
        batch.withColumn("e", F.from_json("value", ENVELOPE))
        .withColumn("_batch_id", F.lit(batch_id).cast("long"))
        .withColumn("_ingested_at", F.current_timestamp())
        .cache()
    )
    stats.records = records.count()
    if stats.records == 0:
        records.unpersist()
        return stats

    # 1. bronze: everything, exactly once per batch id
    _append_once(
        records.select("key", "value", "partition", "offset", "_batch_id", "_ingested_at"),
        lake.table("bronze_cdc_events"),
        "bronze_cdc_events",
        batch_id,
    )

    is_tombstone = F.col("value").isNull()
    is_valid = F.col("e.op").isin("c", "r", "u", "d") & F.col("e.source.lsn").isNotNull()
    stats.tombstones = records.filter(is_tombstone).count()

    # 2. dead letters: not a tombstone, not a valid envelope
    dead = records.filter(~is_tombstone & ~F.coalesce(is_valid, F.lit(False)))
    stats.dead_letters = dead.count()
    if stats.dead_letters:
        _append_once(
            dead.select("key", "value", "partition", "offset", "_batch_id", "_ingested_at"),
            lake.table("bronze_cdc_dead_letters"),
            "bronze_cdc_dead_letters",
            batch_id,
        )

    # 3. silver: latest change per key, applied only if newer than what silver holds
    row = F.coalesce(F.col("e.after"), F.col("e.before"))  # deletes carry the old row in `before`
    changes = records.filter(~is_tombstone & F.coalesce(is_valid, F.lit(False))).select(
        row["order_id"].alias("order_id"),
        row["customer_id"].alias("customer_id"),
        row["status"].alias("status"),
        row["amount"].alias("amount"),
        F.to_timestamp(row["created_at"]).alias("created_at"),
        F.to_timestamp(row["updated_at"]).alias("updated_at"),
        (F.col("e.op") == "d").alias("is_deleted"),
        F.col("e.source.lsn").alias("_lsn"),
        F.col("e.op").alias("_op"),
        F.current_timestamp().alias("_applied_at"),
    )
    stats.changes = changes.count()
    newest_first = Window.partitionBy("order_id").orderBy(F.col("_lsn").desc())
    latest = changes.withColumn("_rn", F.row_number().over(newest_first)).filter("_rn = 1").drop("_rn").cache()
    stats.keys = latest.count()

    if stats.keys:
        (
            DeltaTable.forPath(spark, lake.table("silver_orders")).alias("t")
            .merge(latest.alias("s"), "t.order_id = s.order_id")
            .whenMatchedUpdateAll(condition="s._lsn > t._lsn")
            .whenNotMatchedInsertAll()
            .execute()
        )
        stats.dates = sorted(r.d for r in latest.select(F.to_date("created_at").alias("d")).distinct().collect())
        refresh_gold(spark, lake, stats.dates)

    latest.unpersist()
    records.unpersist()
    log.info(
        "batch %d: %d records, %d changes for %d orders, %d tombstones, %d dead letters",
        batch_id, stats.records, stats.changes, stats.keys, stats.tombstones, stats.dead_letters,
    )
    return stats


def refresh_gold(spark: SparkSession, lake: Lake, dates: list[date]) -> None:
    if not dates:
        return
    in_list = ", ".join(f"DATE '{d.isoformat()}'" for d in dates)
    daily = (
        spark.read.format("delta").load(lake.table("silver_orders"))
        .filter(~F.col("is_deleted") & F.to_date("created_at").isin(dates))
        .groupBy(F.to_date("created_at").alias("order_date"), "status")
        .agg(F.count("*").alias("orders"), F.round(F.sum("amount"), 2).alias("revenue"))
        .withColumn("_built_at", F.current_timestamp())
    )
    (
        daily.write.format("delta").mode("overwrite")
        .option("replaceWhere", f"order_date IN ({in_list})")
        .save(lake.table("gold_daily_orders"))
    )
