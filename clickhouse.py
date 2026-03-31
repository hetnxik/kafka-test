import csv
import io
import subprocess
import logging
from typing import List, Dict, Any

from config import Config

logger = logging.getLogger(__name__)


class ClickHouseClient:
    COLUMNS = [
        "date", "hour_of_day", "event_name", "package_name",
        "os", "event_count", "session_count", "user_count",
    ]

    def __init__(self, config: Config):
        self.host = config.clickhouse_host
        self.user = config.clickhouse_user
        self.password = config.clickhouse_password
        self.database = config.clickhouse_database
        self.table = config.clickhouse_table

    def _base_cmd(self) -> List[str]:
        return [
            "clickhouse-client",
            "--host", self.host,
            "--user", self.user,
            "--password", self.password,
            "--database", self.database,
        ]

    def batch_insert(self, records: List[Dict[str, Any]]) -> bool:
        if not records:
            return True

        logger.info(f"Inserting {len(records)} records into {self.database}.{self.table}")

        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=self.COLUMNS, quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()
        for record in records:
            writer.writerow({col: record.get(col, "") for col in self.COLUMNS})

        cmd = self._base_cmd() + ["--query", f"INSERT INTO {self.table} FORMAT CSVWithNames"]

        try:
            result = subprocess.run(cmd, input=output.getvalue(), capture_output=True, text=True, timeout=120)
            if result.returncode != 0:
                logger.error(f"Insert failed: {result.stderr}")
                return False
            logger.info(f"Inserted {len(records)} records successfully")
            return True
        except subprocess.TimeoutExpired:
            logger.error("Insert timed out (120s)")
            return False
        except Exception as e:
            logger.error(f"Insert error: {e}")
            return False

    def test_connection(self) -> bool:
        try:
            result = subprocess.run(
                self._base_cmd() + ["--query", "SELECT 1"],
                capture_output=True, text=True, timeout=10
            )
            return result.returncode == 0
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False
