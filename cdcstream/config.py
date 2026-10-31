"""Lake locations: Delta tables and the streaming checkpoint live under one root."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

TOPIC = "orders.cdc"


@dataclass(frozen=True)
class Lake:
    root: Path

    @classmethod
    def from_env(cls) -> Lake:
        return cls(Path(os.getenv("LAKE_ROOT", "lake")).resolve())

    def table(self, name: str) -> str:
        return str(self.root / "tables" / name)

    @property
    def checkpoint(self) -> str:
        return str(self.root / "checkpoints" / "orders_cdc")

    @property
    def landing(self) -> Path:
        """Folder used by the file source (same record shape as Kafka: key + value)."""
        return self.root / "landing"
