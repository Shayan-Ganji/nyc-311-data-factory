"""
Schema specification, data validation, and transformation rules for NYC 311 Data Factory.
Handles explicit column contracts, schema drift detection, UTC normalization, and derived metrics.
"""

from datetime import datetime
import re
from typing import Any
import numpy as np
import pandas as pd
from zoneinfo import ZoneInfo

from .exceptions import SchemaDriftError

# Mandatory columns that must exist in any raw payload from SODA
MANDATORY_RAW_COLUMNS = {
    "unique_key",
    "created_date",
    "agency",
    "complaint_type",
    "status",
}

# Target canonical schema columns
CANONICAL_COLUMNS = [
    "unique_key",
    "created_date_utc",
    "closed_date_utc",
    "agency",
    "agency_name",
    "complaint_type",
    "descriptor",
    "location_type",
    "incident_zip",
    "incident_address",
    "street_name",
    "city",
    "status",
    "resolution_description",
    "borough",
    "latitude",
    "longitude",
    "resolution_time_hours",
]

# Timezones
NYC_TZ = ZoneInfo("America/New_York")
UTC_TZ = ZoneInfo("UTC")

ZIP_REGEX = re.compile(r"^\d{5}$")


def check_schema_drift(records: list[dict[str, Any]]) -> None:
    """
    Validate that incoming raw payload satisfies the foundational contract.
    Raises SchemaDriftError if any mandatory column is completely absent across the batch.
    """
    if not records:
        return

    # Check union of keys across the batch
    present_keys = set().union(*(r.keys() for r in records))
    missing_mandatory = MANDATORY_RAW_COLUMNS - present_keys

    if missing_mandatory:
        raise SchemaDriftError(
            f"Upstream SODA schema drift detected! Mandatory columns missing: {sorted(missing_mandatory)}"
        )


def parse_nyc_timestamp(ts_str: str | None) -> datetime | None:
    """
    Parse an ISO timestamp from SODA (originating in America/New_York local time)
    and normalize it to an aware UTC datetime object.
    Returns None if timestamp is absent or unparseable.
    """
    if not ts_str or not isinstance(ts_str, str):
        return None

    try:
        # SODA formats typically: '2023-05-14T11:20:00.000' or with timezone
        dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            # Socrata NYC 311 strings are recorded in America/New_York time
            dt = dt.replace(tzinfo=NYC_TZ)
        return dt.astimezone(UTC_TZ)
    except Exception:
        return None


def clean_zip_code(zip_val: Any) -> str | None:
    """Validate and clean 5-digit US ZIP code. Returns clean 5-digit string or None."""
    if zip_val is None:
        return None
    zip_str = str(zip_val).strip()
    # Handle floats like 10001.0
    if zip_str.endswith(".0"):
        zip_str = zip_str[:-2]
    # Check if exact 5 digits
    if ZIP_REGEX.match(zip_str):
        return zip_str
    # Extract first 5 digits if standard 9-digit format (10001-1234)
    if len(zip_str) >= 5 and zip_str[:5].isdigit():
        return zip_str[:5]
    return None


def transform_record_to_canonical(raw: dict[str, Any]) -> dict[str, Any]:
    """
    Transform a single valid raw dictionary record into the canonical schema dictionary.
    Computes UTC datetimes and resolution_time_hours.
    """
    created_utc = parse_nyc_timestamp(raw.get("created_date"))
    closed_utc = parse_nyc_timestamp(raw.get("closed_date"))

    # Compute resolution time in hours
    res_time_hours: float | None = None
    status = str(raw.get("status", "")).strip().title()

    if created_utc and closed_utc:
        diff_seconds = (closed_utc - created_utc).total_seconds()
        if diff_seconds >= 0:
            res_time_hours = round(diff_seconds / 3600.0, 4)

    # Clean coordinates
    lat = raw.get("latitude")
    lon = raw.get("longitude")
    try:
        lat = float(lat) if lat is not None else None
    except (ValueError, TypeError):
        lat = None
    try:
        lon = float(lon) if lon is not None else None
    except (ValueError, TypeError):
        lon = None

    # Clean borough
    borough = str(raw.get("borough", "Unspecified")).strip().upper()
    if not borough or borough in {"UNSPECIFIED", "UNKNOWN", "NONE"}:
        borough = "Unspecified"

    return {
        "unique_key": str(raw.get("unique_key", "")).strip(),
        "created_date_utc": created_utc,
        "closed_date_utc": closed_utc,
        "agency": str(raw.get("agency", "")).strip().upper(),
        "agency_name": str(raw.get("agency_name", "")).strip(),
        "complaint_type": str(raw.get("complaint_type", "")).strip(),
        "descriptor": str(raw.get("descriptor", "")).strip() or None,
        "location_type": str(raw.get("location_type", "")).strip() or None,
        "incident_zip": clean_zip_code(raw.get("incident_zip")),
        "incident_address": str(raw.get("incident_address", "")).strip() or None,
        "street_name": str(raw.get("street_name", "")).strip() or None,
        "city": str(raw.get("city", "")).strip() or None,
        "status": status,
        "resolution_description": str(raw.get("resolution_description", "")).strip() or None,
        "borough": borough,
        "latitude": lat,
        "longitude": lon,
        "resolution_time_hours": res_time_hours,
    }


def to_canonical_dataframe(canonical_records: list[dict[str, Any]]) -> pd.DataFrame:
    """
    Convert a list of canonical dictionaries into a strongly-typed pandas DataFrame.
    """
    if not canonical_records:
        return pd.DataFrame(columns=CANONICAL_COLUMNS)

    df = pd.DataFrame(canonical_records)

    # Ensure all canonical columns exist
    for col in CANONICAL_COLUMNS:
        if col not in df.columns:
            df[col] = None

    # Cast data types explicitly
    df["unique_key"] = df["unique_key"].astype(str)
    df["created_date_utc"] = pd.to_datetime(df["created_date_utc"], utc=True)
    df["closed_date_utc"] = pd.to_datetime(df["closed_date_utc"], utc=True)
    df["agency"] = df["agency"].astype("category")
    df["complaint_type"] = df["complaint_type"].astype(str)
    df["status"] = df["status"].astype("category")
    df["borough"] = df["borough"].astype("category")
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df["resolution_time_hours"] = pd.to_numeric(df["resolution_time_hours"], errors="coerce")

    return df[CANONICAL_COLUMNS]
