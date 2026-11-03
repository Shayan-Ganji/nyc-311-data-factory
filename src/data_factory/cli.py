"""
Command-Line Interface (CLI) for the NYC 311 & Credit Data Factory package.
Provides an executable entry point with structured argument parsing and POSIX exit codes.
"""

import argparse
import logging
import sys
from pathlib import Path

from .client import SodaClient
from .config import DEFAULT_CONFIG, PipelineConfig
from .credit_pipeline import CreditPipelineEngine
from .exceptions import handle_pipeline_exception
from .pipeline import PipelineEngine
from .schema import check_schema_drift
from .sinks.postgres import PostgresSink
from .sources.csv_source import CsvFileSource


def configure_logging(verbose: bool = False) -> None:
    """Setup structured console logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def main() -> int:
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Data Factory - Multi-Source Ingestion & Curation Engine (NYC 311 & Credit Scoring)"
    )
    parser.add_argument(
        "--full-run",
        action="store_true",
        help="Execute the complete end-to-end extraction, curation, and export workflow for NYC 311.",
    )
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Resume pipeline execution from saved checkpoint if available.",
    )
    parser.add_argument(
        "--check-schema",
        action="store_true",
        help="Verify upstream SODA 2.0 endpoint connectivity and validate schema contract.",
    )
    parser.add_argument(
        "--ingest-credit",
        type=str,
        default=None,
        help="Ingest Kaggle Give Me Some Credit CSV dataset through the Data Factory into PostgreSQL.",
    )
    parser.add_argument(
        "--db-url",
        type=str,
        default="postgresql+psycopg2://postgres@127.0.0.1:5432/olist_db",
        help="PostgreSQL connection string.",
    )
    parser.add_argument(
        "--max-records",
        type=int,
        default=None,
        help="Optional limit on total ingested records (useful for smoke tests).",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable debug logging output.",
    )

    args = parser.parse_args()
    configure_logging(args.verbose)

    config = DEFAULT_CONFIG

    try:
        if args.check_schema:
            logging.info("Checking upstream SODA API connectivity and schema contract...")
            client = SodaClient(config)
            test_batch = client.execute_request(params={"$limit": 5, "agency": config.target_agency})
            check_schema_drift(test_batch)
            logging.info("Schema contract check passed! SODA API is compliant and responsive.")
            return 0

        if args.ingest_credit:
            csv_path = Path(args.ingest_credit)
            logging.info(f"Starting Credit Data Factory ingestion from: {csv_path}")
            source = CsvFileSource(csv_path, chunk_size=25000)
            sink = PostgresSink(
                connection_url=args.db_url,
                schema="credit",
                table_name="applications_raw",
                if_exists="replace",
            )
            pipeline = CreditPipelineEngine(
                source=source,
                sink=sink,
                quarantine_file="data/curated/credit_quarantine.jsonl",
                manifest_file="data/curated/credit_run_manifest.json",
            )
            manifest = pipeline.run()
            print("\n" + "=" * 60)
            print("CREDIT DATASET INGESTION SUCCESSFUL")
            print("=" * 60)
            print(f"Total Processed:    {manifest['records_summary']['total_processed']:,}")
            print(f"Total Valid Loaded: {manifest['records_summary']['total_valid']:,}")
            print(f"Total Quarantined:  {manifest['records_summary']['total_quarantined']:,}")
            print(f"Destination Table:  credit.applications_raw")
            print(f"Duration:           {manifest['duration_seconds']}s")
            print("=" * 60 + "\n")
            return 0

        if args.full_run or args.incremental:
            engine = PipelineEngine(config=config)
            manifest = engine.run(resume=args.incremental, max_records=args.max_records)
            print("\n" + "=" * 60)
            print("PIPELINE EXECUTION SUCCESSFUL")
            print("=" * 60)
            print(f"Total Raw Ingested:    {manifest['record_counts']['total_raw_ingested']:,}")
            print(f"Total Valid Curated:   {manifest['record_counts']['total_valid_curated']:,}")
            print(f"Total Quarantined:     {manifest['record_counts']['total_quarantined']:,}")
            print(f"Quarantine Rate:       {manifest['record_counts']['quarantine_rate']:.4%}")
            print(f"Summary Table Rows:    {manifest['record_counts']['summary_table_rows']:,}")
            print(f"Manifest Path:         {config.manifest_file}")
            print("=" * 60 + "\n")
            return 0

        parser.print_help()
        return 0

    except Exception as err:
        return handle_pipeline_exception(err)


if __name__ == "__main__":
    sys.exit(main())
