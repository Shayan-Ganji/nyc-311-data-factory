"""
Configuration module for the NYC 311 Data Factory pipeline.
Encapsulates all ingestion parameters, API endpoints, time windows, and file paths.
"""

from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class PipelineConfig:
    """Immutable configuration container for pipeline execution."""

    # Socrata SODA API
    soda_endpoint: str = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
    app_token: str | None = None  # Optional SODA app token to avoid throttling

    # Target Scope: NYC Department of Transportation (DOT)
    target_agency: str = "DOT"
    start_date: str = "2023-01-01T00:00:00.000"
    end_date: str = "2024-06-30T23:59:59.999"

    # Ingestion & Pagination parameters
    batch_size: int = 50000
    timeout_seconds: int = 45
    max_retries: int = 3
    backoff_factor: float = 2.0
    jitter: bool = True

    # Data Quality & Quarantine
    quarantine_threshold: float = 0.10  # Maximum 10% allowed quarantine rate
    source_timezone: str = "America/New_York"
    target_timezone: str = "UTC"

    # Storage Paths (all strictly relative to project root)
    project_root: Path = field(default_factory=lambda: Path("."))
    raw_dir: Path = field(default_factory=lambda: Path("data/raw"))
    curated_dir: Path = field(default_factory=lambda: Path("data/curated"))
    reference_dir: Path = field(default_factory=lambda: Path("data"))

    # Specific File Paths
    checkpoint_file: Path = field(default_factory=lambda: Path("data/raw/checkpoint.json"))
    raw_storage_file: Path = field(default_factory=lambda: Path("data/raw/raw_ingested_records.jsonl"))
    reference_population_file: Path = field(default_factory=lambda: Path("data/nyc_modzcta_population.json"))
    curated_case_level_parquet: Path = field(default_factory=lambda: Path("data/curated/nyc_311_dot_curated.parquet"))
    curated_case_level_csv: Path = field(default_factory=lambda: Path("data/curated/nyc_311_dot_curated.csv"))
    curated_summary_parquet: Path = field(default_factory=lambda: Path("data/curated/borough_complaint_monthly_summary.parquet"))
    curated_summary_csv: Path = field(default_factory=lambda: Path("data/curated/borough_complaint_monthly_summary.csv"))
    quarantine_file: Path = field(default_factory=lambda: Path("data/curated/quarantine.jsonl"))
    manifest_file: Path = field(default_factory=lambda: Path("data/curated/run_manifest.json"))

    def ensure_directories(self) -> None:
        """Create necessary data directory structure if it does not exist."""
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.curated_dir.mkdir(parents=True, exist_ok=True)
        self.reference_dir.mkdir(parents=True, exist_ok=True)


DEFAULT_CONFIG = PipelineConfig()
