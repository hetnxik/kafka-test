import csv
import io
import logging
import os
import subprocess
from typing import List, Dict, Any

from .config import Config

logger = logging.getLogger(__name__)

# Columns inserted into ClickHouse, in order.
# Must match the schema of the target table (see schema.sql).
COLUMNS = [
    "date", "hour_of_day", "event_name", "package_name", "os",
    "event_count", "session_count", "user_count",
    "overflow_session_count", "non_overflow_session_count",
    "overflow_user_count", "non_overflow_user_count",
]


class ClickHouseClient:
    """
    Inserts aggregated event records into ClickHouse via clickhouse-client.

    Uses FORMAT CSVWithNames so the column order is explicit.
    The target table uses SummingMergeTree, so multiple inserts for the same
    key (e.g. from multiple hourly flushes) are merged automatically by ClickHouse.

    For local testing without ClickHouse, set OUTPUT_PATH env var and the client
    will fall back to writing CSV instead (see _insert_csv).
    """

    def __init__(self, config: Config):
        self.host = config.clickhouse_host
        self.user = config.clickhouse_user
        self.password = config.clickhouse_password
        self.database = config.clickhouse_database
        self.table = config.clickhouse_table
        self.output_path = config.output_path  # used for CSV fallback

    def _base_cmd(self) -> List[str]:
        return [
            "clickhouse-client",
            "--host", self.host,
            "--user", self.user,
            "--password", self.password,
            "--database", self.database,
        ]

    def test_connection(self) -> bool:
        """Ping ClickHouse with SELECT 1. Returns True if reachable."""
        try:
            result = subprocess.run(
                self._base_cmd() + ["--query", "SELECT 1"],
                capture_output=True, text=True, timeout=10
            )
            if result.returncode == 0:
                return True
            logger.error("ClickHouse connection test failed: %s", result.stderr)
            return False
        except Exception as e:
            logger.error("ClickHouse connection test error: %s", e)
            return False

    def batch_insert(self, records: List[Dict[str, Any]]) -> bool:
        """
        Insert a batch of aggregated records into ClickHouse.
        Serialises records as CSVWithNames and pipes to clickhouse-client.
        Returns True on success, False on failure.
        """
        if not records:
            return True

        # Serialise to CSV in memory
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=COLUMNS, quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()
        for record in records:
            writer.writerow({col: record.get(col, "") for col in COLUMNS})

        cmd = self._base_cmd() + [
            "--query", f"INSERT INTO {self.table} FORMAT CSVWithNames"
        ]

        try:
            result = subprocess.run(
                cmd,
                input=output.getvalue(),
                capture_output=True,
                text=True,
                timeout=120
            )
            if result.returncode != 0:
                logger.error("ClickHouse insert failed: %s", result.stderr)
                return False
            logger.info("Inserted %d records into %s.%s", len(records), self.database, self.table)
            return True
        except subprocess.TimeoutExpired:
            logger.error("ClickHouse insert timed out (120s)")
            return False
        except Exception as e:
            logger.error("ClickHouse insert error: %s", e)
            return False
