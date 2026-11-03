"""
NYC 311 & Credit Data Factory Package
====================================
A robust, production-grade ingestion and curation engine.
Designed for resilience, determinism, schema evolution, and storage decoupling.
Supports multiple sources (SODA API, CSV File) and multiple sinks (Parquet, CSV, PostgreSQL).
"""

from .credit_pipeline import CreditPipelineEngine
from .pipeline import PipelineEngine
from .sinks import BaseSink, CsvSink, JsonlSink, ParquetSink, PostgresSink
from .sources import BaseSource, CsvFileSource, SodaSource

__version__ = "0.2.0"
__author__ = "Shayan Ganji"

__all__ = [
    "PipelineEngine",
    "CreditPipelineEngine",
    "BaseSource",
    "CsvFileSource",
    "SodaSource",
    "BaseSink",
    "CsvSink",
    "JsonlSink",
    "ParquetSink",
    "PostgresSink",
]
