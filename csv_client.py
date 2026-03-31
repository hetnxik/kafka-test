import csv
import logging
import os
from typing import List, Dict, Any

logger = logging.getLogger(__name__)

OUTPUT_FILE = "output.csv"
KEY_COLS = ["date", "hour_of_day", "event_name", "package_name", "os"]
SUM_COLS = ["event_count", "session_count", "user_count"]
COLUMNS = KEY_COLS + SUM_COLS


class CsvClient:
    def __init__(self):
        if not os.path.exists(OUTPUT_FILE):
            with open(OUTPUT_FILE, "w", newline="") as f:
                csv.DictWriter(f, fieldnames=COLUMNS).writeheader()

    def test_connection(self) -> bool:
        logger.info(f"[DRY RUN] CSV output will be written to {OUTPUT_FILE}")
        return True

    def batch_insert(self, records: List[Dict[str, Any]]) -> bool:
        try:
            # Read existing rows
            existing: Dict[tuple, Dict] = {}
            if os.path.exists(OUTPUT_FILE):
                with open(OUTPUT_FILE, "r", newline="") as f:
                    for row in csv.DictReader(f):
                        key = tuple(row[c] for c in KEY_COLS)
                        existing[key] = {
                            **{c: row[c] for c in KEY_COLS},
                            **{c: int(row[c]) for c in SUM_COLS},
                        }

            # Merge new records (SummingMergeTree: sum on same key)
            for record in records:
                key = tuple(str(record.get(c, "")) for c in KEY_COLS)
                if key in existing:
                    for c in SUM_COLS:
                        existing[key][c] += int(record.get(c, 0))
                else:
                    existing[key] = {
                        **{c: str(record.get(c, "")) for c in KEY_COLS},
                        **{c: int(record.get(c, 0)) for c in SUM_COLS},
                    }

            # Write back merged result sorted by key (mirrors ORDER BY)
            with open(OUTPUT_FILE, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=COLUMNS)
                writer.writeheader()
                for row in sorted(existing.values(), key=lambda r: tuple(str(r[c]) for c in KEY_COLS)):
                    writer.writerow(row)

            logger.info(f"[DRY RUN] Merged {len(records)} records → {len(existing)} rows in {OUTPUT_FILE}")
            return True
        except Exception as e:
            logger.error(f"[DRY RUN] CSV write error: {e}")
            return False
