"""
Enrichment module for NYC 311 Data Factory.
Integrates NYC Department of Health MODZCTA Census population data to:
1. Reconstruct 'Unspecified' boroughs using incident ZIP codes.
2. Provide official population denominators for per-capita rate calculations (complaints per 10k residents).
"""

import json
from pathlib import Path
from typing import Any
import numpy as np
import pandas as pd

# Standard NYC Postal Code mapping to Boroughs
def map_zip_to_borough(zip_code: Any) -> str | None:
    """
    Map a 5-digit US postal ZIP code to its corresponding NYC Borough.
    Returns uppercase borough name or None if outside NYC boundaries.
    """
    if not zip_code:
        return None
    try:
        z = int(str(zip_code)[:5])
    except (ValueError, TypeError):
        return None

    if 10001 <= z <= 10292:
        return "MANHATTAN"
    if 10301 <= z <= 10314:
        return "STATEN ISLAND"
    if 10451 <= z <= 10475:
        return "BRONX"
    if 11201 <= z <= 11256:
        return "BROOKLYN"
    if (11001 <= z <= 11109) or (11351 <= z <= 11697):
        return "QUEENS"
    return None


class PopulationEnricher:
    """
    Loads NYC MODZCTA Census population table and computes borough aggregations.
    """

    def __init__(self, population_file: Path) -> None:
        self.population_file = Path(population_file)
        self.borough_populations: dict[str, int] = {}
        self.zip_to_population: dict[str, int] = {}
        self._load_population_data()

    def _load_population_data(self) -> None:
        """Parse MODZCTA JSON table and aggregate populations by borough."""
        if not self.population_file.exists():
            raise FileNotFoundError(f"Population reference file not found at: {self.population_file}")

        with open(self.population_file, "r", encoding="utf-8") as f:
            records = json.load(f)

        for rec in records:
            modzcta = str(rec.get("modzcta", "")).strip()
            pop = int(rec.get("population", 0))
            if modzcta:
                self.zip_to_population[modzcta] = pop

            borough = map_zip_to_borough(modzcta)
            if borough:
                self.borough_populations[borough] = self.borough_populations.get(borough, 0) + pop

    def get_borough_population(self, borough: str) -> int:
        """Get official population for a given NYC borough."""
        normalized = borough.strip().upper()
        return self.borough_populations.get(normalized, 0)

    def recover_borough(self, current_borough: str | None, incident_zip: str | None) -> str:
        """
        Recover 'Unspecified' or empty borough using incident_zip if available.
        """
        if current_borough and current_borough.upper() not in {"UNSPECIFIED", "UNKNOWN", "NONE", ""}:
            return current_borough.upper()

        if incident_zip:
            recovered = map_zip_to_borough(incident_zip)
            if recovered:
                return recovered

        return "Unspecified"

    def enrich_case_level_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply borough recovery across an entire case-level DataFrame.
        """
        if df.empty:
            return df

        # Recover unspecified boroughs where valid ZIP exists
        unspecified_mask = df["borough"].astype(str).str.upper().isin(["UNSPECIFIED", "UNKNOWN", "NONE", "NAN"])
        if unspecified_mask.any():
            recovered_boroughs = df.loc[unspecified_mask, "incident_zip"].apply(map_zip_to_borough)
            df.loc[unspecified_mask & recovered_boroughs.notna(), "borough"] = recovered_boroughs

        return df

    def build_wide_summary_table(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Build the canonical Wide-Format Summary Table:
        Dimensions: Borough × Complaint Type × Month
        Metrics:
            - total_complaints
            - closed_complaints
            - median_resolution_hours
            - mean_resolution_hours
            - borough_population
            - complaints_per_10k_residents
        """
        if df.empty:
            return pd.DataFrame()

        work_df = df.copy()

        # Extract Month period (YYYY-MM)
        work_df["month"] = pd.to_datetime(work_df["created_date_utc"]).dt.to_period("M").astype(str)

        # Flag closed complaints
        work_df["is_closed"] = work_df["status"].astype(str).str.title() == "Closed"

        # Group by (borough, complaint_type, month)
        grouped = work_df.groupby(["borough", "complaint_type", "month"], observed=False)

        summary = grouped.agg(
            total_complaints=("unique_key", "count"),
            closed_complaints=("is_closed", "sum"),
            median_resolution_hours=("resolution_time_hours", "median"),
            mean_resolution_hours=("resolution_time_hours", "mean"),
        ).reset_index()

        # Attach borough populations
        summary["borough_population"] = summary["borough"].astype(str).map(self.borough_populations).fillna(0).astype(int)

        # Compute per-capita rate per 10k residents
        summary["complaints_per_10k_residents"] = np.where(
            summary["borough_population"] > 0,
            np.round((summary["total_complaints"] / summary["borough_population"]) * 10000.0, 4),
            np.nan,
        )

        # Round metrics for determinism
        summary["median_resolution_hours"] = summary["median_resolution_hours"].round(2)
        summary["mean_resolution_hours"] = summary["mean_resolution_hours"].round(2)

        # Deterministic sort
        summary = summary.sort_values(
            by=["borough", "complaint_type", "month"]
        ).reset_index(drop=True)

        return summary
