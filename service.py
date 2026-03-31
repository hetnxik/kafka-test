import json
import logging
import sys
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

from kafka import KafkaConsumer
from kafka.errors import KafkaError

from config import Config
from clickhouse import ClickHouseClient
from csv_client import CsvClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class WindowBuffer:
    """Accumulates event counts for the current 5-min window."""

    def __init__(self, os_values: List[str]):
        self.os_values = os_values
        self.counts: Dict = defaultdict(int)
        self.sessions: Dict = defaultdict(set)
        self.users: Dict = defaultdict(set)

    def add(self, event_name: str, package_name: str, os: str,
            session_id: str, user_id: str, timestamp: str) -> None:
        try:
            dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        except Exception:
            dt = datetime.now(timezone.utc)

        date = str(dt.date())
        hour_of_day = dt.hour
        key = (date, hour_of_day, event_name, package_name, os)

        self.counts[key] += 1
        self.sessions[key].add(session_id)
        self.users[key].add(user_id)

    def flush(self) -> List[Dict[str, Any]]:
        """Returns records with os zero-fill, then resets buffer."""
        seen = set()
        for (date, hour_of_day, event_name, package_name, os) in self.counts:
            seen.add((date, hour_of_day, event_name, package_name))

        records = []
        for (date, hour_of_day, event_name, package_name) in seen:
            for os in self.os_values:
                key = (date, hour_of_day, event_name, package_name, os)
                records.append({
                    "date": date,
                    "hour_of_day": hour_of_day,
                    "event_name": event_name,
                    "package_name": package_name,
                    "os": os,
                    "event_count": self.counts.get(key, 0),
                    "session_count": len(self.sessions.get(key, set())),
                    "user_count": len(self.users.get(key, set())),
                })

        self.counts.clear()
        self.sessions.clear()
        self.users.clear()

        return records

    def is_empty(self) -> bool:
        return len(self.counts) == 0


class EventStreamService:
    """Kafka consumer that aggregates events into 5-min windows and flushes to ClickHouse."""

    def __init__(self, config: Config):
        self.config = config
        self.running = True
        self.buffer = WindowBuffer(config.os_values)
        self.buffer_lock = threading.Lock()
        self.flush_thread: Optional[threading.Thread] = None

        self.clickhouse = CsvClient() if config.dry_run else ClickHouseClient(config)

        self.consumer = KafkaConsumer(
            config.kafka_topic,
            bootstrap_servers=config.kafka_brokers,
            group_id=config.kafka_group_id,
            enable_auto_commit=False,
            auto_offset_reset="latest",
            consumer_timeout_ms=1000,
        )

    def _calculate_next_flush_time(self) -> float:
        now = time.time()
        interval = self.config.flush_interval_seconds
        return interval - (now % interval)

    def _process_message(self, msg) -> None:
        try:
            value = msg.value
            if not value:
                return

            data = json.loads(value.decode("utf-8"))

            event_name = data.get("event_name", "")
            package_name = data.get("package_name", "")
            os = data.get("os", "")
            session_id = data.get("session_id", "")
            user_id = data.get("user_id", "")
            timestamp = data.get("timestamp", "")

            # Skip empty or debug packages
            if not package_name or package_name.endswith(".debug"):
                return

            with self.buffer_lock:
                self.buffer.add(event_name, package_name, os, session_id, user_id, timestamp)

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse message: {e}")
        except Exception as e:
            logger.error(f"Error processing message: {e}")

    def _flush_to_clickhouse(self) -> bool:
        with self.buffer_lock:
            if self.buffer.is_empty():
                logger.info("Buffer empty, nothing to flush")
                return True
            records = self.buffer.flush()

        logger.info(f"Flushing {len(records)} records to ClickHouse")

        max_retries = 3
        for attempt in range(max_retries):
            if self.clickhouse.batch_insert(records):
                try:
                    self.consumer.commit()
                    logger.info(f"Flushed {len(records)} records and committed offsets")
                    return True
                except Exception as e:
                    logger.error(f"Failed to commit offsets: {e}")
                    return False

            if attempt < max_retries - 1:
                delay = 2 ** attempt
                logger.warning(f"Insert failed, retrying in {delay}s ({attempt + 2}/{max_retries})")
                time.sleep(delay)

        logger.error(f"Failed to insert after {max_retries} attempts")
        return False

    def _flush_loop(self) -> None:
        logger.info("Flush thread started")
        while self.running:
            sleep_time = self._calculate_next_flush_time()
            logger.debug(f"Next flush in {sleep_time:.1f}s")

            while sleep_time > 0 and self.running:
                time.sleep(min(sleep_time, 1.0))
                sleep_time -= 1.0

            if not self.running:
                break

            try:
                self._flush_to_clickhouse()
            except Exception as e:
                logger.error(f"Error in flush loop: {e}")

        logger.info("Flush thread stopped")

    def start(self) -> None:
        logger.info("Starting Event Stream Service")

        if not self.clickhouse.test_connection():
            logger.error("ClickHouse connection failed, exiting")
            sys.exit(1)

        logger.info(f"Subscribed to topic: {self.config.kafka_topic}")

        self.flush_thread = threading.Thread(target=self._flush_loop, daemon=True)
        self.flush_thread.start()

        try:
            while self.running:
                try:
                    for msg in self.consumer:
                        if not self.running:
                            break
                        self._process_message(msg)
                except StopIteration:
                    pass
                except KafkaError as e:
                    logger.error(f"Kafka error: {e}, retrying...")
                    time.sleep(1)

        except KeyboardInterrupt:
            logger.info("Interrupted")
        except Exception as e:
            logger.error(f"Fatal error: {e}")
        finally:
            self.shutdown()

    def shutdown(self) -> None:
        logger.info("Shutting down...")
        self.running = False

        logger.info("Final flush...")
        try:
            self._flush_to_clickhouse()
        except Exception as e:
            logger.error(f"Error during final flush: {e}")

        if self.flush_thread and self.flush_thread.is_alive():
            self.flush_thread.join(timeout=10)

        self.consumer.close()
        logger.info("Shutdown complete")
