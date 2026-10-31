"""Generator tests. Pure Python, no Spark needed."""

from __future__ import annotations

import json

from cdcstream.generate import GenerateConfig, generate


def _replay(records) -> dict[int, dict]:
    """Reference implementation: apply valid events in LSN order, skipping anything older."""
    table, seen = {}, {}
    for r in records:
        if r.value is None:
            continue
        try:
            e = json.loads(r.value)
        except json.JSONDecodeError:
            continue
        if not isinstance(e, dict) or "source" not in e:
            continue
        row = e["after"] or e["before"]
        oid, lsn = row["order_id"], e["source"]["lsn"]
        if lsn <= seen.get(oid, 0):
            continue
        seen[oid] = lsn
        if e["op"] == "d":
            table.pop(oid, None)
        else:
            table[oid] = e["after"]
    return table


def test_deterministic() -> None:
    a, b = generate(), generate()
    assert [(r.key, r.value) for r in a.records] == [(r.key, r.value) for r in b.records]


def test_lsn_rule_reproduces_ground_truth_despite_messy_delivery() -> None:
    stream = generate()
    assert _replay(stream.records) == stream.final_orders


def test_naive_last_write_wins_would_be_wrong() -> None:
    """Applying events in arrival order (ignoring LSN) resurrects old values. That's the bug LSNs prevent."""
    stream = generate()
    table = {}
    for r in stream.records:
        if r.value is None:
            continue
        try:
            e = json.loads(r.value)
            row = e["after"] or e["before"]
        except (json.JSONDecodeError, KeyError, TypeError):
            continue
        if e["op"] == "d":
            table.pop(row["order_id"], None)
        else:
            table[row["order_id"]] = e["after"]
    assert table != stream.final_orders


def test_stream_contains_every_delivery_problem() -> None:
    s = generate(GenerateConfig())
    assert s.duplicates > 0 and s.replayed > 0 and s.poison_pills == 3
    assert s.tombstones == len(s.deleted_ids) > 0
