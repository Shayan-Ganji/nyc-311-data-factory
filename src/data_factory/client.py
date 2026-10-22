"""
SODA API HTTP Client module.
Handles resilient ingestion from NYC Open Data (Socrata SODA 2.0 API).
Implements Keyset/Cursor Pagination, Session pooling, Exponential Backoff with Jitter,
and dual GET/POST query execution.
"""

import logging
import random
import time
from typing import Any, Generator
import requests

from .config import PipelineConfig
from .exceptions import NetworkError

logger = logging.getLogger(__name__)


class SodaClient:
    """
    Resilient HTTP Client for Socrata Open Data API (SODA 2.0).
    Decoupled from execution environment and fully mockable for offline unit testing.
    """

    def __init__(self, config: PipelineConfig, session: requests.Session | None = None) -> None:
        self.config = config
        self.session = session or requests.Session()

        # Set default headers
        self.session.headers.update({
            "User-Agent": "NYC-311-Data-Factory/0.1.0 (Academic Research; Shayan Ganji)",
            "Accept": "application/json",
        })
        if self.config.app_token:
            self.session.headers["X-App-Token"] = self.config.app_token

    def execute_request(
        self,
        params: dict[str, Any],
        method: str = "GET",
        timeout: int | None = None,
    ) -> list[dict[str, Any]]:
        """
        Execute an HTTP request against the SODA endpoint with exponential backoff and jitter.

        Supported HTTP methods:
        - GET: Passes SoQL parameters in query string
        - POST: Passes SoQL parameters in form body (application/x-www-form-urlencoded)

        Retries on transient network errors (HTTP 429, 500, 502, 503, 504, ConnectionError, Timeout).
        """
        timeout = timeout or self.config.timeout_seconds
        method = method.upper()
        url = self.config.soda_endpoint

        attempt = 0
        last_exception: Exception | None = None

        while attempt <= self.config.max_retries:
            try:
                if method == "GET":
                    response = self.session.get(url, params=params, timeout=timeout)
                elif method == "POST":
                    response = self.session.post(url, data=params, timeout=timeout)
                else:
                    raise ValueError(f"Unsupported HTTP method for SODA client: {method}")

                # Check for rate limiting or server errors
                if response.status_code == 200:
                    data = response.json()
                    if isinstance(data, list):
                        return data
                    if isinstance(data, dict) and "error" in data:
                        raise NetworkError(f"SODA API returned error payload: {data.get('message', data)}")
                    return [data]

                # Retryable status codes
                if response.status_code in {429, 500, 502, 503, 504}:
                    logger.warning(
                        f"Transient HTTP {response.status_code} on attempt {attempt + 1}/{self.config.max_retries + 1}. Retrying..."
                    )
                    attempt += 1
                    if attempt > self.config.max_retries:
                        raise NetworkError(
                            f"HTTP {response.status_code} error from SODA API after {self.config.max_retries} retries: {response.text[:200]}"
                        )
                    self._sleep_backoff(attempt)
                    continue

                # Non-retryable client error (400, 403, 404, etc.)
                raise NetworkError(
                    f"Fatal SODA API error HTTP {response.status_code}: {response.text[:300]}"
                )

            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as net_err:
                last_exception = net_err
                logger.warning(
                    f"Network connectivity failure on attempt {attempt + 1}/{self.config.max_retries + 1}: {net_err}"
                )
                attempt += 1
                if attempt > self.config.max_retries:
                    raise NetworkError(
                        f"Network failure after {self.config.max_retries} retries: {last_exception}"
                    ) from last_exception
                self._sleep_backoff(attempt)

        raise NetworkError(f"Failed to fetch data from SODA API after {self.config.max_retries} retries.")

    def _sleep_backoff(self, attempt: int) -> None:
        """Calculate and execute exponential backoff with full jitter."""
        max_backoff = 30.0
        backoff = min(max_backoff, self.config.backoff_factor * (2 ** (attempt - 1)))
        sleep_duration = random.uniform(0.5, backoff) if self.config.jitter else backoff
        time.sleep(sleep_duration)

    def build_keyset_where_clause(
        self,
        base_where: str,
        last_created_date: str | None = None,
        last_unique_key: str | None = None,
    ) -> str:
        """
        Construct a deterministic Keyset (cursor) pagination $where clause.
        Avoids O(N) offset scanning degradation on Socrata backends.
        """
        if not last_created_date or not last_unique_key:
            return base_where

        cursor_condition = (
            f"(created_date > '{last_created_date}' OR "
            f"(created_date = '{last_created_date}' AND unique_key > '{last_unique_key}'))"
        )
        return f"{base_where} AND {cursor_condition}"

    def stream_batches(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
        agency: str | None = None,
        resume_created_date: str | None = None,
        resume_unique_key: str | None = None,
        method: str = "GET",
    ) -> Generator[list[dict[str, Any]], None, None]:
        """
        Stream full dataset in batches using keyset pagination.
        Yields list of raw record dictionaries per batch until dataset exhausted.
        Supports resume-on-failure from exact cursor position.
        """
        start = start_date or self.config.start_date
        end = end_date or self.config.end_date
        target_agency = agency or self.config.target_agency

        base_where = (
            f"agency = '{target_agency}' AND "
            f"created_date >= '{start}' AND "
            f"created_date <= '{end}'"
        )

        current_created_date = resume_created_date
        current_unique_key = resume_unique_key

        while True:
            where_clause = self.build_keyset_where_clause(
                base_where=base_where,
                last_created_date=current_created_date,
                last_unique_key=current_unique_key,
            )

            params = {
                "$where": where_clause,
                "$order": "created_date ASC, unique_key ASC",
                "$limit": self.config.batch_size,
            }

            batch = self.execute_request(params=params, method=method)
            if not batch:
                break

            yield batch

            # Update cursor to last item of the batch
            last_record = batch[-1]
            current_created_date = last_record.get("created_date")
            current_unique_key = str(last_record.get("unique_key"))

            # If received fewer records than batch_size, we reached the end of the query
            if len(batch) < self.config.batch_size:
                break
