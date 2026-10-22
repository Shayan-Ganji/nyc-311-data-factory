"""
Concrete File Sink implementations: Parquet, CSV, and JSON Lines.
Ensures deterministic ordering, cryptographic SHA-256 verification, and robust file I/O.
"""

import hashlib
import json
from pathlib import Path
from typing import Any
import pandas as pd

from ..exceptions import StorageError
from .base import BaseSink


def compute_file_sha256(file_path: Path) -> str:
    """Compute cryptographic SHA-256 hex digest of a local file in chunks."""
    if not file_path.exists():
        raise StorageError(f"Target file for hashing does not exist: {file_path}")

    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        while chunk := f.read(64 * 1024):
            sha256.update(chunk)
    return sha256.hexdigest()


class ParquetSink(BaseSink):
    """
    Columnar storage sink using Apache Parquet.
    Applies snappy compression and deterministic sorting for high read throughput and idempotency.
    """

    def __init__(self, target_path: Path, sort_column: str = "unique_key") -> None:
        self.target_path = Path(target_path)
        self.sort_column = sort_column
        self.record_count = 0
        self._buffers: list[pd.DataFrame] = []

    def write_batch(self, records: list[dict[str, Any]]) -> None:
        if not records:
            return
        df = pd.DataFrame(records)
        self.write_dataframe(df)

    def write_dataframe(self, df: pd.DataFrame) -> None:
        if df.empty:
            return
        self.record_count += len(df)
        self._buffers.append(df)

    def finalize(self) -> dict[str, Any]:
        self.target_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            if self._buffers:
                consolidated = pd.concat(self._buffers, ignore_index=True)
                if self.sort_column in consolidated.columns:
                    consolidated = consolidated.sort_values(by=self.sort_column).reset_index(drop=True)
            else:
                consolidated = pd.DataFrame()

            # Write deterministic parquet
            consolidated.to_parquet(self.target_path, index=False, engine="pyarrow", compression="snappy")
            sha256_hash = compute_file_sha256(self.target_path)
            size_bytes = self.target_path.stat().st_size

            return {
                "sink_type": "ParquetSink",
                "path": str(self.target_path),
                "record_count": len(consolidated),
                "size_bytes": size_bytes,
                "sha256": sha256_hash,
            }
        except Exception as e:
            raise StorageError(f"Failed to finalize ParquetSink to {self.target_path}: {e}") from e


class CsvSink(BaseSink):
    """
    Row-oriented CSV sink with standard RFC 4180 escaping and deterministic sorting.
    """

    def __init__(self, target_path: Path, sort_column: str = "unique_key") -> None:
        self.target_path = Path(target_path)
        self.sort_column = sort_column
        self.record_count = 0
        self._buffers: list[pd.DataFrame] = []

    def write_batch(self, records: list[dict[str, Any]]) -> None:
        if not records:
            return
        df = pd.DataFrame(records)
        self.write_dataframe(df)

    def write_dataframe(self, df: pd.DataFrame) -> None:
        if df.empty:
            return
        self.record_count += len(df)
        self._buffers.append(df)

    def finalize(self) -> dict[str, Any]:
        self.target_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            if self._buffers:
                consolidated = pd.concat(self._buffers, ignore_index=True)
                if self.sort_column in consolidated.columns:
                    consolidated = consolidated.sort_values(by=self.sort_column).reset_index(drop=True)
            else:
                consolidated = pd.DataFrame()

            # Write deterministic CSV with explicit newline
            consolidated.to_csv(self.target_path, index=False, encoding="utf-8", lineterminator="\n")
            sha256_hash = compute_file_sha256(self.target_path)
            size_bytes = self.target_path.stat().st_size

            return {
                "sink_type": "CsvSink",
                "path": str(self.target_path),
                "record_count": len(consolidated),
                "size_bytes": size_bytes,
                "sha256": sha256_hash,
            }
        except Exception as e:
            raise StorageError(f"Failed to finalize CsvSink to {self.target_path}: {e}") from e


class JsonlSink(BaseSink):
    """
    Stream-friendly JSON Lines sink, ideal for raw checkpoints and quarantine logging.
    Appends line-by-line without high memory overhead.
    """

    def __init__(self, target_path: Path, append: bool = False) -> None:
        self.target_path = Path(target_path)
        self.append = append
        self.record_count = 0
        self.target_path.parent.mkdir(parents=True, exist_ok=True)
        self._mode = "a" if append else "w"
        self._file = open(self.target_path, self._mode, encoding="utf-8")

    def write_batch(self, records: list[dict[str, Any]]) -> None:
        for rec in records:
            line = json.dumps(rec, ensure_ascii=False)
            self._file.write(f"{line}\n")
            self.record_count += 1
        self._file.flush()

    def write_dataframe(self, df: pd.DataFrame) -> None:
        records = df.to_dict(orient="records")
        self.write_batch(records)

    def finalize(self) -> dict[str, Any]:
        try:
            if not self._file.closed:
                self._file.close()

            sha256_hash = compute_file_sha256(self.target_path)
            size_bytes = self.target_path.stat().st_size

            return {
                "sink_type": "JsonlSink",
                "path": str(self.target_path),
                "record_count": self.record_count,
                "size_bytes": size_bytes,
                "sha256": sha256_hash,
            }
        except Exception as e:
            raise StorageError(f"Failed to finalize JsonlSink to {self.target_path}: {e}") from e
