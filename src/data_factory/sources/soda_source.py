"""
SODA API Ingestion Source.
Wraps SodaClient to satisfy the BaseSource interface.
"""

from typing import Any, Generator
from ..client import SodaClient
from .base import BaseSource


class SodaSource(BaseSource):
    """Adapter wrapping SodaClient as a BaseSource."""

    def __init__(self, client: SodaClient) -> None:
        self.client = client

    def stream_batches(
        self, batch_size: int | None = None, **kwargs: Any
    ) -> Generator[list[dict[str, Any]], None, None]:
        resume_created = kwargs.get("resume_created_date")
        resume_key = kwargs.get("resume_unique_key")
        return self.client.stream_batches(
            resume_created_date=resume_created,
            resume_unique_key=resume_key,
        )

    def get_metadata(self) -> dict[str, Any]:
        return {
            "source_type": "soda_api",
            "endpoint": self.client.config.soda_endpoint,
            "agency": self.client.config.target_agency,
        }
