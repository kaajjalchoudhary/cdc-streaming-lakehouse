"""Summary of the lake after a run."""

from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from cdcstream.config import Lake


def build_report(spark: SparkSession, lake: Lake) -> str:
    read = lambda name: spark.read.format("delta").load(lake.table(name))  # noqa: E731
    silver = read("silver_orders")
    bronze = read("bronze_cdc_events")
    lines = ["", "silver_orders"]
    live, deleted = silver.filter(~F.col("is_deleted")).count(), silver.filter("is_deleted").count()
    lines.append(f"  live orders {live}, soft-deleted {deleted}, highest LSN applied {silver.agg(F.max('_lsn')).first()[0]}")
    for r in silver.filter(~F.col("is_deleted")).groupBy("status").count().orderBy("status").collect():
        lines.append(f"  {r.status:<10} {r['count']:>5}")

    lines += ["", "bronze_cdc_events"]
    lines.append(f"  {bronze.count()} records in {bronze.select('_batch_id').distinct().count()} micro-batches")
    if DeltaTable.isDeltaTable(spark, lake.table("bronze_cdc_dead_letters")):
        lines.append(f"  dead letters: {read('bronze_cdc_dead_letters').count()}")

    gold = read("gold_daily_orders")
    lines += ["", "gold_daily_orders (revenue of live orders by day)"]
    for r in gold.groupBy("order_date").agg(F.sum("orders").alias("o"), F.round(F.sum("revenue"), 2).alias("rev")) \
            .orderBy("order_date").collect():
        lines.append(f"  {r.order_date}  {r.o:>4} orders  {r.rev:>10,.2f}")
    return "\n".join(lines)
