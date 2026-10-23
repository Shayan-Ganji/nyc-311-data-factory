"""
Configuration module for the NYC 311 Data Factory pipeline.
Encapsulates all ingestion parameters, API endpoints, time windows, and file paths.
"""

from dataclasses import dataclass, field
from pathlib import Path


def find_project_root() -> Path:
    """
    Dynamically locate repository root by walking up ancestors
    until requirements.txt or pyproject.toml is found.
    Defaults to current working directory if not found.
    """
    for p in [Path.cwd(), *Path.cwd().parents]:
        if (p / "requirements.txt").exists() and (p / "src").exists():
            return p
    # Fallback checking file location
    file_parent = Path(__file__).resolve().parent.parent.parent
    if (file_parent / "requirements.txt").exists():
        return file_parent
    return Path.cwd()


@dataclass(frozen=True)
class PipelineConfig:
    """Immutable configuration container for pipeline execution."""

    # Project Root
    project_root: Path = field(default_factory=find_project_root)

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

    # Directory Paths
    @property
    def raw_dir(self) -> Path:
        return self.project_root / "data" / "raw"

    @property
    def curated_dir(self) -> Path:
        return self.project_root / "data" / "curated"

    @property
    def reference_dir(self) -> Path:
        return self.project_root / "data"

    # Specific File Paths
    @property
    def checkpoint_file(self) -> Path:
        return self.raw_dir / "checkpoint.json"

    @property
    def raw_storage_file(self) -> Path:
        return self.raw_dir / "raw_ingested_records.jsonl"

    @property
    def reference_population_file(self) -> Path:
        return self.reference_dir / "nyc_modzcta_population.json"

    @property
    def curated_case_level_parquet(self) -> Path:
        return self.curated_dir / "nyc_311_dot_curated.parquet"

    @property
    def curated_case_level_csv(self) -> Path:
        return self.curated_dir / "nyc_311_dot_curated.csv"

    @property
    def curated_summary_parquet(self) -> Path:
        return self.curated_dir / "borough_complaint_monthly_summary.parquet"

    @property
    def curated_summary_csv(self) -> Path:
        return self.curated_dir / "borough_complaint_monthly_summary.csv"

    @property
    def quarantine_file(self) -> Path:
        return self.curated_dir / "quarantine.jsonl"

    @property
    def manifest_file(self) -> Path:
        return self.curated_dir / "run_manifest.json"

    def ensure_directories(self) -> None:
        """Create necessary data directory structure if it does not exist."""
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.curated_dir.mkdir(parents=True, exist_ok=True)
        self.reference_dir.mkdir(parents=True, exist_ok=True)


DEFAULT_CONFIG = PipelineConfig()
