from __future__ import annotations

import os
from pathlib import Path

import pytest

from cdcstream.config import Lake


@pytest.fixture(scope="session")
def spark():
    pytest.importorskip("pyspark")
    pytest.importorskip("delta")
    os.environ.setdefault("SPARK_SHUFFLE_PARTITIONS", "2")
    from cdcstream.spark import get_spark

    session = get_spark("cdcstream-tests")
    yield session
    session.stop()


@pytest.fixture
def lake(tmp_path: Path) -> Lake:
    return Lake(tmp_path / "lake")
