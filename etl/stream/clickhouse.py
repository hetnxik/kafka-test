import csv
import logging
import os
from typing import List, Dict, Any

from .config import Config

logger = logging.getLogger(__name__)

COLUMNS = [
    "date", "hour_of_day", "event_name", "package_name", "os",
    "event_count", "session_count", "user_count",
    "overflow_session_count", "non_overflow_session_count",
    "overflow_user_count", "non_overflow_user_count",
]


class ClickHouseClient:
    def __init__(self, config: Config):
        self.output_path = config.output_path

    def test_connection(self) -> bool:
        try:
            os.makedirs(os.path.dirname(self.output_path) or ".", exist_ok=True)
            return True
        except Exception as e:
            logger.error(f"Cannot create output directory: {e}")
            return False

    def batch_insert(self, records: List[Dict[str, Any]]) -> bool:
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
