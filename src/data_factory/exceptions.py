"""
Exception hierarchy and process exit codes for NYC 311 Data Factory.
"""

import sys


class DataFactoryError(Exception):
    """Base exception for all domain errors within the data factory pipeline."""

    exit_code: int = 1

    def __init__(self, message: str, exit_code: int | None = None) -> None:
        super().__init__(message)
        if exit_code is not None:
            self.exit_code = exit_code


class ConfigurationError(DataFactoryError):
    """Raised when pipeline configuration or CLI arguments are invalid (Exit Code 1)."""

    exit_code: int = 1


class NetworkError(DataFactoryError):
    """Raised when network requests or SODA API fail after exhausting retry budget (Exit Code 2)."""

    exit_code: int = 2


class SchemaDriftError(DataFactoryError):
    """Raised when upstream API schema deviates from contract or mandatory columns are missing (Exit Code 3)."""

    exit_code: int = 3


class QuarantineThresholdExceededError(DataFactoryError):
    """Raised when the ratio of quarantined records exceeds acceptable budget (Exit Code 4)."""

    exit_code: int = 4


class StorageError(DataFactoryError):
    """Raised when persistence layer (disk I/O, parquet serialization, DB sink) fails."""

    exit_code: int = 1


def handle_pipeline_exception(err: Exception) -> int:
    """
    Standard top-level CLI error handler.
    Prints a structured diagnostic message to stderr and returns the corresponding exit code.
    """
    if isinstance(err, DataFactoryError):
        sys.stderr.write(f"\n[DATA_FACTORY_ERROR] (Exit {err.exit_code}): {err}\n")
        return err.exit_code
    sys.stderr.write(f"\n[UNEXPECTED_SYSTEM_ERROR] (Exit 1): {err}\n")
    return 1
