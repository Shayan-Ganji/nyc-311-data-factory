"""
Offline Unit Tests for SodaClient:
Validates GET/POST execution, transient retry backoff, exhaustion error raising,
and deterministic keyset pagination logic.
"""

from pathlib import Path
import sys
import unittest
from unittest.mock import MagicMock, patch
import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from data_factory.client import SodaClient
from data_factory.config import PipelineConfig
from data_factory.exceptions import NetworkError


class TestSodaClientOffline(unittest.TestCase):
    """Unit tests for SodaClient running 100% offline."""

    def setUp(self) -> None:
        self.config = PipelineConfig(
            max_retries=2,
            backoff_factor=0.01,  # Fast backoff for tests
            jitter=False,
        )
        self.mock_session = MagicMock(spec=requests.Session)
        self.mock_session.headers = {}
        self.client = SodaClient(self.config, session=self.mock_session)

    def test_execute_request_get_success(self) -> None:
        """Test standard HTTP GET returning 200 OK with record list."""
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = [{"unique_key": "123", "agency": "DOT"}]
        self.mock_session.get.return_value = mock_resp

        result = self.client.execute_request(params={"$limit": 1}, method="GET")

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["unique_key"], "123")
        self.mock_session.get.assert_called_once()

    def test_execute_request_post_support(self) -> None:
        """Test HTTP POST method execution passes parameters in data body."""
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = [{"unique_key": "456", "agency": "DOT"}]
        self.mock_session.post.return_value = mock_resp

        result = self.client.execute_request(params={"$limit": 1}, method="POST")

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["unique_key"], "456")
        self.mock_session.post.assert_called_once()

    def test_retry_on_429_then_succeed(self) -> None:
        """Test transient HTTP 429 Too Many Requests triggers retry and succeeds on 2nd attempt."""
        resp_429 = MagicMock()
        resp_429.status_code = 429
        resp_429.text = "Too Many Requests"

        resp_200 = MagicMock()
        resp_200.status_code = 200
        resp_200.json.return_value = [{"unique_key": "789"}]

        self.mock_session.get.side_effect = [resp_429, resp_200]

        result = self.client.execute_request(params={}, method="GET")

        self.assertEqual(len(result), 1)
        self.assertEqual(self.mock_session.get.call_count, 2)

    def test_exhaust_retries_raises_network_error(self) -> None:
        """Test continuous server errors (HTTP 500) exhaust retry budget and raise NetworkError (Exit Code 2)."""
        resp_500 = MagicMock()
        resp_500.status_code = 500
        resp_500.text = "Internal Server Error"
        self.mock_session.get.return_value = resp_500

        with self.assertRaises(NetworkError) as ctx:
            self.client.execute_request(params={}, method="GET")

        self.assertIn("500", str(ctx.exception))
        # initial + 2 retries = 3 calls
        self.assertEqual(self.mock_session.get.call_count, 3)

    def test_keyset_where_clause_construction(self) -> None:
        """Test cursor pagination generates correct SoQL where clause with tie-breaking."""
        base = "agency = 'DOT'"
        # Without cursor
        clause1 = self.client.build_keyset_where_clause(base)
        self.assertEqual(clause1, base)

        # With cursor
        clause2 = self.client.build_keyset_where_clause(
            base,
            last_created_date="2023-05-01T12:00:00.000",
            last_unique_key="10050",
        )
        self.assertIn("created_date > '2023-05-01T12:00:00.000'", clause2)
        self.assertIn("unique_key > '10050'", clause2)


if __name__ == "__main__":
    unittest.main()
