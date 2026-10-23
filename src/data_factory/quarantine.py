"""
Quarantine router and data quality filter for NYC 311 Data Factory.
Isolates anomalous, corrupted, or non-compliant records into a Dead-Letter Queue (quarantine.jsonl)
with precise error codes, preserving pipeline continuity and preventing silent data corruption.
"""

from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from .config import PipelineConfig
from .exceptions import QuarantineThresholdExceededError
from .schema import clean_zip_code, parse_nyc_timestamp, transform_record_to_canonical
from .sinks.file_sinks import JsonlSink

# Standardized Error Codes for Data Quality Quarantine
ERR_CLOSED_BEFORE_CREATED = "ERR_CLOSED_BEFORE_CREATED"
ERR_MISSING_MANDATORY_FIELD = "ERR_MISSING_MANDATORY_FIELD"
ERR_CLOSED_WITHOUT_CLOSED_DATE = "ERR_CLOSED_WITHOUT_CLOSED_DATE"
ERR_INVALID_ZIP = "ERR_INVALID_ZIP"
ERR_INVALID_COORDINATES = "ERR_INVALID_COORDINATES"


class QuarantineRouter:
    """
    Routes incoming raw records through deterministic validation rules.
    Directs valid records to canonical transformation and diverts defective records
    to a streamable quarantine sink.
    """

    def __init__(self, config: PipelineConfig, quarantine_sink: JsonlSink | None = None) -> None:
        self.config = config
        self.sink = quarantine_sink or JsonlSink(self.config.quarantine_file)

        # Statistical counters
        self.total_processed = 0
        self.total_valid = 0
        self.total_quarantined = 0
        self.error_code_counts: dict[str, int] = {
            ERR_CLOSED_BEFORE_CREATED: 0,
            ERR_MISSING_MANDATORY_FIELD: 0,
            ERR_CLOSED_WITHOUT_CLOSED_DATE: 0,
            ERR_INVALID_ZIP: 0,
            ERR_INVALID_COORDINATES: 0,
        }

    def validate_record(self, raw: dict[str, Any]) -> tuple[bool, str | None, str | None]:
        """
        Evaluate a single raw record against business logic rules.
        Returns:
            (True, None, None) if record is valid.
            (False, error_code, error_description) if record violates quality rules.
        """
        # Rule 1: Mandatory Fields
        unique_key = raw.get("unique_key")
        created_date_str = raw.get("created_date")
        if not unique_key or not created_date_str:
            return False, ERR_MISSING_MANDATORY_FIELD, "Missing mandatory field: unique_key or created_date"

        created_dt = parse_nyc_timestamp(created_date_str)
        if not created_dt:
            return False, ERR_MISSING_MANDATORY_FIELD, f"Unparseable created_date timestamp: {created_date_str}"

        # Rule 2: Closed Date and Inverted Timestamps
        closed_date_str = raw.get("closed_date")
        status = str(raw.get("status", "")).strip().title()

        if closed_date_str:
            closed_dt = parse_nyc_timestamp(closed_date_str)
            if not closed_dt:
                return False, ERR_CLOSED_BEFORE_CREATED, f"Unparseable closed_date timestamp: {closed_date_str}"

            # Paradox Trap: closed_date strictly before created_date
            if closed_dt < created_dt:
                return (
                    False,
                    ERR_CLOSED_BEFORE_CREATED,
                    f"Temporal paradox: closed_date ({closed_dt}) < created_date ({created_dt})",
                )
        elif status == "Closed":
            # Status indicates closed, but closed_date is missing entirely
            return (
                False,
                ERR_CLOSED_WITHOUT_CLOSED_DATE,
                "Record status is 'Closed' but closed_date is null/empty",
            )

        # Rule 3: Coordinate boundaries (if present, must be within plausible Greater NYC bounds)
        lat_val = raw.get("latitude")
        lon_val = raw.get("longitude")
        if lat_val is not None and lon_val is not None:
            try:
                lat = float(lat_val)
                lon = float(lon_val)
                # NYC bounding box approx: Lat [40.45, 41.05], Lon [-74.30, -73.65]
                # Filter out obvious 0,0 or flipped coordinates
                if lat != 0.0 or lon != 0.0:
                    if not (40.4 <= lat <= 41.2 and -74.4 <= lon <= -73.5):
                        return (
                            False,
                            ERR_INVALID_COORDINATES,
                            f"Coordinates out of bounds for NYC: lat={lat}, lon={lon}",
                        )
            except (ValueError, TypeError):
                return False, ERR_INVALID_COORDINATES, f"Non-numeric coordinates: lat={lat_val}, lon={lon_val}"

        # All validation passed
        return True, None, None

    def route_batch(
        self,
        raw_records: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """
        Filter a batch of raw records.
        Routes valid records to canonical transformation and invalid records to the quarantine sink.
        Returns:
            (valid_canonical_records, quarantined_envelopes)
        """
        valid_canonical_records: list[dict[str, Any]] = []
        quarantined_envelopes: list[dict[str, Any]] = []

        now_iso = datetime.now(timezone.utc).isoformat()

        for raw in raw_records:
            self.total_processed += 1
            is_valid, err_code, err_msg = self.validate_record(raw)

            if is_valid:
                canonical = transform_record_to_canonical(raw)
                valid_canonical_records.append(canonical)
                self.total_valid += 1
            else:
                self.total_quarantined += 1
                if err_code in self.error_code_counts:
                    self.error_code_counts[err_code] += 1
                else:
                    self.error_code_counts[err_code] = 1

                envelope = {
                    "unique_key": str(raw.get("unique_key", "UNKNOWN")),
                    "error_code": err_code,
                    "error_message": err_msg,
                    "quarantined_at_utc": now_iso,
                    "raw_record": raw,
                }
                quarantined_envelopes.append(envelope)

        # Write quarantined envelopes immediately to sink
        if quarantined_envelopes:
            self.sink.write_batch(quarantined_envelopes)

        return valid_canonical_records, quarantined_envelopes

    @property
    def quarantine_rate(self) -> float:
        """Calculate the proportion of processed records routed to quarantine."""
        if self.total_processed == 0:
            return 0.0
        return self.total_quarantined / self.total_processed

    def verify_quarantine_budget(self) -> None:
        """
        Verify that quarantine rate does not exceed configured tolerance.
        Raises QuarantineThresholdExceededError (Exit Code 4) if threshold breached.
        """
        rate = self.quarantine_rate
        threshold = self.config.quarantine_threshold
        if rate > threshold:
            raise QuarantineThresholdExceededError(
                f"Quarantine rate {rate:.4%} exceeded tolerance threshold {threshold:.4%}! "
                f"({self.total_quarantined} defective records out of {self.total_processed} processed)"
            )

    def finalize(self) -> dict[str, Any]:
        """Finalize quarantine sink and return structured metrics."""
        sink_metadata = self.sink.finalize()
        return {
            "total_processed": self.total_processed,
            "total_valid": self.total_valid,
            "total_quarantined": self.total_quarantined,
            "quarantine_rate": round(self.quarantine_rate, 5),
            "error_breakdown": dict(self.error_code_counts),
            "quarantine_sink_metadata": sink_metadata,
        }
