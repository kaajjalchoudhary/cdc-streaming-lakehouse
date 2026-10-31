"""Streaming job tests with the file source (same key/value records as Kafka)."""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import functions as F  # noqa: E402

from cdcstream import sources  # noqa: E402
from cdcstream.apply import apply_batch, ensure_tables  # noqa: E402
from cdcstream.config import Lake  # noqa: E402
from cdcstream.generate import GenerateConfig, Record, generate  # noqa: E402
from cdcstream.stream import run  # noqa: E402

SMALL = GenerateConfig(events=400, max_orders=80)
SCHEMA = "key string, value string, partition int, offset long"


def silver_state(spark, lake: Lake) -> tuple[dict, set]:
    rows = spark.read.format("delta").load(lake.table("silver_orders")).collect()
    live = {
        r.order_id: {
            "order_id": r.order_id,
            "customer_id": r.customer_id,
            "status": r.status,
            "amount": r.amount,
            "created_at": r.created_at.strftime("%Y-%m-%dT%H:%M:%S"),
            "updated_at": r.updated_at.strftime("%Y-%m-%dT%H:%M:%S"),
        }
        for r in rows
        if not r.is_deleted
    }
    return live, {r.order_id for r in rows if r.is_deleted}


def test_stream_reproduces_the_source_table_exactly(spark, lake: Lake) -> None:
    stream = generate(SMALL)
    sources.write_files(stream.records, lake.landing, per_file=60)
    batches = run(spark, lake, sources.file_stream(spark, lake.landing))

    assert len(batches) > 1, "several micro-batches"
    live, deleted = silver_state(spark, lake)
    assert live == stream.final_orders
    assert deleted == stream.deleted_ids
    assert sum(b.dead_letters for b in batches) == stream.poison_pills
    assert sum(b.tombstones for b in batches) == stream.tombstones
    bronze = spark.read.format("delta").load(lake.table("bronze_cdc_events")).count()
    assert bronze == len(stream.records)


def test_restart_from_checkpoint_continues_without_loss_or_duplicates(spark, lake: Lake, tmp_path: Path) -> None:
    stream = generate(SMALL)
    staged = tmp_path / "staged"
    n_files = sources.write_files(stream.records, staged, per_file=60)
    lake.landing.mkdir(parents=True)

    files = sorted(staged.iterdir())
    for f in files[: n_files // 2]:
        shutil.copy(f, lake.landing / f.name)
    run(spark, lake, sources.file_stream(spark, lake.landing))  # first run stops when drained

    for f in files[n_files // 2 :]:
        shutil.copy(f, lake.landing / f.name)
    run(spark, lake, sources.file_stream(spark, lake.landing))  # restart picks up only new files

    live, _ = silver_state(spark, lake)
    assert live == stream.final_orders
    assert spark.read.format("delta").load(lake.table("bronze_cdc_events")).count() == len(stream.records)


def test_replaying_a_batch_id_changes_nothing(spark, lake: Lake) -> None:
    """Simulates a crash after writing but before the checkpoint commit: the batch runs again."""
    ensure_tables(spark, lake)
    records = generate(SMALL).records[:150]
    batch = spark.createDataFrame([(r.key, r.value, None, None) for r in records], SCHEMA)
    apply_batch(spark, lake, batch, 0)
    def snapshot() -> list[tuple]:
        silver = spark.read.format("delta").load(lake.table("silver_orders")).drop("_applied_at")
        return sorted(map(tuple, silver.collect()))

    before = snapshot()
    apply_batch(spark, lake, batch, 0)

    after = snapshot()
    assert after == before
    assert spark.read.format("delta").load(lake.table("bronze_cdc_events")).count() == len(records)


def test_stale_update_cannot_resurrect_a_deleted_order(spark, lake: Lake) -> None:
    ensure_tables(spark, lake)

    def event(op: str, lsn: int, status: str) -> Record:
        row = {"order_id": 1, "customer_id": 7, "status": status, "amount": 10.0,
               "created_at": "2026-03-01T08:00:00", "updated_at": "2026-03-01T08:00:00"}
        body = {"before": row if op == "d" else None, "after": None if op == "d" else row,
                "op": op, "ts_ms": lsn, "source": {"lsn": lsn, "table": "orders"}}
        return Record(json.dumps({"order_id": 1}), json.dumps(body))

    def apply(recs: list[Record], batch_id: int) -> None:
        df = spark.createDataFrame([(r.key, r.value, None, None) for r in recs], SCHEMA)
        apply_batch(spark, lake, df, batch_id)

    apply([event("c", 1, "placed"), event("u", 2, "paid"), event("d", 3, "paid")], 0)
    apply([event("u", 2, "paid")], 1)  # replayed update older than the delete

    row = spark.read.format("delta").load(lake.table("silver_orders")).filter("order_id = 1").first()
    assert row.is_deleted and row._lsn == 3
    gold = spark.read.format("delta").load(lake.table("gold_daily_orders"))
    assert gold.filter(F.col("orders") > 0).count() == 0
