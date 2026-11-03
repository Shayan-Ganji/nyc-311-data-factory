"""
Abstract Base Source interface.
Enforces Inversion of Control across data ingestion sources.
"""

from abc import ABC, abstractmethod
from typing import Any, Generator


class BaseSource(ABC):
    """Abstract contract for data ingestion sources."""

    @abstractmethod
    def stream_batches(
        self, batch_size: int | None = None, **kwargs: Any
    ) -> Generator[list[dict[str, Any]], None, None]:
        """Yield chunks of raw dictionary records."""
        raise NotImplementedError

    @abstractmethod
    def get_metadata(self) -> dict[str, Any]:
        """Return source metadata (format, size, hash, path/URI)."""
        raise NotImplementedError
