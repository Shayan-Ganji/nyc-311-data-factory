"""
PostgreSQL Database Sink implementation.
Provides enterprise bulk persistence using PostgreSQL's native COPY protocol.
Enables seamless architectural linkage between Stage 1 data pipeline and Stage 2 database performance.
"""

import io
from typing import Any
import pandas as pd
from sqlalchemy import create_engine, text

from ..exceptions import StorageError
from .base import BaseSink


class PostgresSink(BaseSink):
    """
    High-performance relational database sink for PostgreSQL.
    Utilizes PostgreSQL COPY FROM STDIN streaming buffer for ultra-fast bulk loading.
    Satisfies the Open/Closed Principle by extending BaseSink without modifying pipeline core.
    """

    def __init__(
        self,
        connection_url: str = "postgresql+psycopg2://postgres@127.0.0.1:5432/olist_db",
        schema: str = "nyc_311",
        table_name: str = "service_requests",
        if_exists: str = "replace",
    ) -> None:
        self.connection_url = connection_url
        self.schema = schema
        self.table_name = table_name
        self.if_exists = if_exists
        self.record_count = 0
        self.engine = create_engine(self.connection_url)

    def write_batch(self, records: list[dict[str, Any]]) -> None:
        """Persist a chunk of records by converting to DataFrame and streaming."""
        if not records:
            return
        df = pd.DataFrame(records)
        self.write_dataframe(df)

    def write_dataframe(self, df: pd.DataFrame) -> None:
        """
        Persist a DataFrame directly into PostgreSQL using memory-streamed COPY protocol.
        Achieves ~100x speedup over standard row-by-row INSERT statements.
        """
        if df.empty:
            return

        # Ensure target schema exists
        with self.engine.connect() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {self.schema};"))
            conn.commit()

        # Initialize table structure using SQLAlchemy type reflection on empty schema
        df.head(0).to_sql(
            name=self.table_name,
            con=self.engine,
            schema=self.schema,
            if_exists=self.if_exists,
            index=False,
        )

        raw_conn = self.engine.raw_connection()
        try:
            cursor = raw_conn.cursor()
            buffer = io.StringIO()
            df.to_csv(buffer, index=False, header=False, na_rep="\\N")
            buffer.seek(0)

            copy_sql = (
                f"COPY {self.schema}.{self.table_name} "
                f"FROM STDIN WITH (FORMAT CSV, NULL '\\N')"
            )
            cursor.copy_expert(sql=copy_sql, file=buffer)
            raw_conn.commit()
            cursor.close()
            self.record_count += len(df)
        except Exception as exc:
            raw_conn.rollback()
            raise StorageError(f"PostgresSink bulk insert failed: {exc}") from exc
        finally:
            raw_conn.close()

    def finalize(self) -> dict[str, Any]:
        """
        Flush operations, verify physical table count, and return metadata manifest.
        """
        with self.engine.connect() as conn:
            query = text(f"SELECT COUNT(*) FROM {self.schema}.{self.table_name};")
            persisted_count = conn.execute(query).scalar()

        return {
            "format": "postgresql",
            "schema": self.schema,
            "table": self.table_name,
            "record_count": int(persisted_count) if persisted_count else 0,
        }
