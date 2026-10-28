"""
Sinks subpackage: Adapter pattern implementations for analytical data persistence.
Decouples pipeline execution from concrete storage backends (File system vs Database).
"""

from .base import BaseSink
from .file_sinks import CsvSink, JsonlSink, ParquetSink
from .postgres import PostgresSink

__all__ = ["BaseSink", "ParquetSink", "CsvSink", "JsonlSink", "PostgresSink"]

