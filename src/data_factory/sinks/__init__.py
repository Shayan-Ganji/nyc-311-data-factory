"""
Sinks subpackage: Adapter pattern implementations for analytical data persistence.
Decouples pipeline execution from concrete storage backends (File system vs Database).
"""

from .base import BaseSink
from .file_sinks import CsvSink, JsonlSink, ParquetSink

__all__ = ["BaseSink", "ParquetSink", "CsvSink", "JsonlSink"]
