"""Generate a deterministic Debezium-style change stream for an `orders` table.

Each record mirrors a Kafka message: a JSON key ({"order_id": n}) and a JSON value holding
the change envelope ({"before", "after", "op", "ts_ms", "source": {"lsn", "table"}}), or a
null value (a tombstone, sent after every delete so log compaction can drop the key).

Delivery is at-least-once and messy on purpose:
- ~2% of events are delivered twice;
- a connector restart re-sends an older range of events, which arrive after newer ones;
- a few messages are not valid JSON ("poison pills").

The generator also returns the ground truth: the table as it really is at the end. The
streaming job must reproduce it exactly, whatever the delivery order.
"""

from __future__ import annotations

import json
import random
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

STATUS_FLOW = {"placed": "paid", "paid": "shipped", "shipped": "delivered"}
POISON_PILLS = ["{not json", "\u0000\u0001binary-garbage", '{"op": "u", "after": {']


@dataclass(frozen=True)
class GenerateConfig:
    events: int = 1500
    max_orders: int = 300
    seed: int = 21
    start: datetime = datetime(2026, 3, 1, 8, 0, tzinfo=timezone.utc)


@dataclass
class Record:
    key: str
    value: str | None  # None = tombstone


@dataclass
class ChangeStream:
    records: list[Record]
    final_orders: dict[int, dict]  # ground truth: live rows by order_id
    deleted_ids: set[int] = field(default_factory=set)
    duplicates: int = 0
    replayed: int = 0
    poison_pills: int = 0
    tombstones: int = 0


def _iso(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%dT%H:%M:%S")


def generate(cfg: GenerateConfig | None = None) -> ChangeStream:
    cfg = cfg or GenerateConfig()
    rng = random.Random(cfg.seed)
    table: dict[int, dict] = {}
    deleted: set[int] = set()
    emitted: list[Record] = []  # events in commit (lsn) order, before delivery problems
    next_id, clock = 1, cfg.start

    for lsn in range(1, cfg.events + 1):
        clock += timedelta(seconds=rng.randint(5, 120))
        live = sorted(table)
        roll = rng.random()
        if next_id <= cfg.max_orders and (not live or roll < 0.35):
            after = {
                "order_id": next_id,
                "customer_id": rng.randint(1, 80),
                "status": "placed",
                "amount": round(rng.uniform(5, 400), 2),
                "created_at": _iso(clock),
                "updated_at": _iso(clock),
            }
            before, op = None, "c"
            table[next_id] = after
            next_id += 1
        elif roll < 0.98 or not live:
            order_id = rng.choice(live)
            before = dict(table[order_id])
            after = dict(before, updated_at=_iso(clock))
            if before["status"] in STATUS_FLOW and rng.random() < 0.8:
                after["status"] = STATUS_FLOW[before["status"]]
            else:
                after["amount"] = round(before["amount"] * rng.uniform(0.8, 1.2), 2)  # price correction
            op = "u"
            table[order_id] = after
        else:
            order_id = rng.choice(live)
            before, after, op = table.pop(order_id), None, "d"
            deleted.add(order_id)

        key = json.dumps({"order_id": (after or before)["order_id"]})
        envelope = {
            "before": before,
            "after": after,
            "op": op,
            "ts_ms": int(clock.timestamp() * 1000),
            "source": {"lsn": lsn, "table": "orders"},
        }
        emitted.append(Record(key, json.dumps(envelope)))
        if op == "d":
            emitted.append(Record(key, None))

    stream = ChangeStream(records=[], final_orders=table, deleted_ids=deleted)
    replay_at = int(len(emitted) * 0.7)
    replay = emitted[int(len(emitted) * 0.5) : int(len(emitted) * 0.6)]
    for i, record in enumerate(emitted):
        stream.records.append(record)
        if record.value is None:
            stream.tombstones += 1
        elif rng.random() < 0.02:
            stream.records.append(Record(record.key, record.value))  # redelivered
            stream.duplicates += 1
        if i == replay_at:
            # Connector restart: an older range is sent again, after newer events.
            stream.records.extend(Record(r.key, r.value) for r in replay)
            stream.replayed = len(replay)
        if i in (100, 700, 1300):
            stream.records.append(Record(json.dumps({"order_id": -1}), POISON_PILLS[(i // 600) % 3]))
            stream.poison_pills += 1
    return stream
