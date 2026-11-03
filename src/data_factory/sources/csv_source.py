"""
CSV File Ingestion Source.
Streams records from local CSV files in memory-budgeted chunks.
"""

import hashlib
from pathlib import Path
from typing import Any, Generator
import pandas as pd

from .base import BaseSource


class CsvFileSource(BaseSource):
    """
    Concrete BaseSource implementation for local CSV files.
    Implements memory-safe streaming using pandas chunksize.
    """

    def __init__(self, file_path: Path | str, chunk_size: int = 10000) -> None:
        self.file_path = Path(file_path)
        self.chunk_size = chunk_size
        if not self.file_path.exists():
            raise FileNotFoundError(f"Source CSV file not found: {self.file_path}")

    def stream_batches(
        self, batch_size: int | None = None, **kwargs: Any
    ) -> Generator[list[dict[str, Any]], None, None]:
        c_size = batch_size or self.chunk_size
        for chunk in pd.read_csv(self.file_path, chunksize=c_size):
            if "Unnamed: 0" in chunk.columns:
                chunk = chunk.drop(columns=["Unnamed: 0"])
            # Convert NaN to None for clean database insertion
            records = chunk.where(pd.notnull(chunk), None).to_dict(orient="records")
            yield records

    def get_metadata(self) -> dict[str, Any]:
        hasher = hashlib.sha256()
        with open(self.file_path, "rb") as f:
            while chunk := f.read(65536):
                hasher.update(chunk)
        return {
            "source_type": "csv_file",
            "file_path": str(self.file_path.resolve()),
            "file_name": self.file_path.name,
            "file_size_bytes": self.file_path.stat().st_size,
            "sha256": hasher.hexdigest(),
        }
