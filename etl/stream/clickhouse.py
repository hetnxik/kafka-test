import csv
import logging
import os
from typing import List, Dict, Any

from .config import Config

logger = logging.getLogger(__name__)

# Columns written to the output CSV, in order.
# These match the schema of the ClickHouse table event_counts_hourly.
COLUMNS = [
    "date", "hour_of_day", "event_name", "package_name", "os",
    "event_count", "session_count", "user_count",
    "overflow_session_count", "non_overflow_session_count",
    "overflow_user_count", "non_overflow_user_count",
]


class ClickHouseClient:
    """
    Output sink for aggregated event records.

    Currently writes to a local CSV file (append mode). Originally designed
    to insert into ClickHouse via clickhouse-client — the class name and column
    list reflect that schema. To switch back to ClickHouse, replace batch_insert
    with a subprocess call to clickhouse-client with FORMAT CSVWithNames.
    """

    def __init__(self, config: Config):
        self.output_path = config.output_path

    def test_connection(self) -> bool:
        """Verify the output directory can be created/written to."""
        try:
            os.makedirs(os.path.dirname(self.output_path) or ".", exist_ok=True)
            return True
        except Exception as e:
            logger.error(f"Cannot create output directory: {e}")
            return False

    def batch_insert(self, records: List[Dict[str, Any]]) -> bool:
        """
        Append a batch of aggregated records to the CSV file.
        Writes the header row only if the file doesn't exist yet.
        Returns True on success, False on failure.
        """
        if not records:
            return True

        write_header = not os.path.exists(self.output_path)
        try:
            with open(self.output_path, "a", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=COLUMNS)
                if write_header:
                    writer.writeheader()
                for record in records:
                    writer.writerow({col: record.get(col, "") for col in COLUMNS})
            logger.info(f"Appended {len(records)} records to {self.output_path}")
            return True
        except Exception as e:
            logger.error(f"CSV write error: {e}")
            return False
