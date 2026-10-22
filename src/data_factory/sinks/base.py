"""
Abstract Base Sink definition.
Enforces the Adapter Pattern across all destination types (Parquet, CSV, PostgreSQL).
"""

from abc import ABC, abstractmethod
from typing import Any
import pandas as pd


class BaseSink(ABC):
    """
    Abstract contract for pipeline data persistence.
    Isolates data transformation logic from storage implementations,
    satisfying the Open/Closed Principle (OCP) and enabling zero-refactor database loading in Stage 2.
    """

    @abstractmethod
    def write_batch(self, records: list[dict[str, Any]]) -> None:
        """Persist a chunk/batch of raw or validated dictionary records."""
        raise NotImplementedError

    @abstractmethod
    def write_dataframe(self, df: pd.DataFrame) -> None:
        """Persist a consolidated analytical pandas DataFrame."""
        raise NotImplementedError

    @abstractmethod
    def finalize(self) -> dict[str, Any]:
        """
        Close any open buffers, flush pending I/O, compute integrity hashes,
        and return summary metadata for the execution manifest.
        """
        raise NotImplementedError
