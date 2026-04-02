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

from .config import Config
from .clickhouse import ClickHouseClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class WindowBuffer:
    """Accumulates event counts for the current flush window."""

    def __init__(self, os_values: List[str]):
        self.os_values = os_values
        self.counts: Dict = defaultdict(int)
        self.sessions: Dict = defaultdict(set)
        self.users: Dict = defaultdict(set)
        self.prev_hour_sessions: set = set()
        self.prev_hour_users: set = set()

    def add(self, event_name: str, package_name: str, os: str,
            session_id: str, user_id: str, timestamp: str) -> None:
        try:
            ts = timestamp.strip()
            if ts.isdigit():
                epoch = int(ts)
                # Milliseconds if 13 digits, seconds otherwise
                dt = datetime.fromtimestamp(epoch / 1000 if epoch > 1e10 else epoch, tz=timezone.utc)
            else:
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except Exception:
            logger.warning("Unparseable timestamp %r, using current UTC time", timestamp)
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

        curr_all_sessions: set = set()
        curr_all_users: set = set()

        records = []
        for (date, hour_of_day, event_name, package_name) in seen:
            for os in self.os_values:
                key = (date, hour_of_day, event_name, package_name, os)
                curr_sess = self.sessions.get(key, set())
                curr_user = self.users.get(key, set())

                curr_all_sessions.update(curr_sess)
                curr_all_users.update(curr_user)

                records.append({
                    "date": date,
                    "hour_of_day": hour_of_day,
                    "event_name": event_name,
                    "package_name": package_name,
                    "os": os,
                    "event_count": self.counts.get(key, 0),
                    "session_count": len(curr_sess),
                    "user_count": len(curr_user),
                    "overflow_session_count": len(curr_sess & self.prev_hour_sessions),
                    "non_overflow_session_count": len(curr_sess - self.prev_hour_sessions),
                    "overflow_user_count": len(curr_user & self.prev_hour_users),
                    "non_overflow_user_count": len(curr_user - self.prev_hour_users),
                })

        # Rotate: current hour becomes previous hour for next flush
        self.prev_hour_sessions = curr_all_sessions
        self.prev_hour_users = curr_all_users

        self.counts.clear()
        self.sessions.clear()
        self.users.clear()

        return records

    def stats(self) -> Dict[str, int]:
        unique_events = len({k[2] for k in self.counts})
        unique_packages = len({k[3] for k in self.counts})
        total_events = sum(self.counts.values())
        unique_sessions = len({s for ss in self.sessions.values() for s in ss})
        unique_users = len({u for us in self.users.values() for u in us})
        return {
            "buffered_events": total_events,
            "unique_event_types": unique_events,
            "unique_packages": unique_packages,
            "unique_sessions": unique_sessions,
            "unique_users": unique_users,
        }

    def is_empty(self) -> bool:
        return len(self.counts) == 0


class EventStreamService:
    """Kafka consumer that aggregates events into time windows and flushes to CSV."""

    # Log a throughput summary every N messages
    _LOG_EVERY = 1000

    def __init__(self, config: Config):
        self.config = config
        self.running = True
        self.buffer = WindowBuffer(config.os_values)
        self.buffer_lock = threading.Lock()
        self.flush_thread: Optional[threading.Thread] = None

        self._msg_count = 0          # total messages processed
        self._skip_count = 0         # messages skipped (debug/empty package)
        self._error_count = 0        # parse errors

        self.sink = ClickHouseClient(config)

        logger.info("Connecting to Kafka — brokers=%s  topic=%s  group=%s",
                    config.kafka_brokers, config.kafka_topic, config.kafka_group_id)
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
                self._skip_count += 1
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
                self._skip_count += 1
                return

            if not event_name:
                logger.warning("Message missing event_name — partition=%d offset=%d",
                               msg.partition, msg.offset)

            if not session_id or not user_id:
                logger.warning("Message missing session_id/user_id — event=%s package=%s "
                               "partition=%d offset=%d",
                               event_name, package_name, msg.partition, msg.offset)

            with self.buffer_lock:
                self.buffer.add(event_name, package_name, os, session_id, user_id, timestamp)

            self._msg_count += 1
            if self._msg_count % self._LOG_EVERY == 0:
                with self.buffer_lock:
                    stats = self.buffer.stats()
                logger.info(
                    "Throughput — processed=%d  skipped=%d  errors=%d  |  "
                    "buffer: events=%d types=%d packages=%d sessions=%d users=%d",
                    self._msg_count, self._skip_count, self._error_count,
                    stats["buffered_events"], stats["unique_event_types"],
                    stats["unique_packages"], stats["unique_sessions"], stats["unique_users"],
                )

        except json.JSONDecodeError as e:
            self._error_count += 1
            logger.error("JSON parse error — partition=%d offset=%d error=%s",
                         msg.partition, msg.offset, e)
        except Exception as e:
            self._error_count += 1
            logger.error("Error processing message — partition=%d offset=%d error=%s",
                         msg.partition, msg.offset, e)

    def _flush(self) -> bool:
        with self.buffer_lock:
            if self.buffer.is_empty():
                logger.info("Flush triggered — buffer is empty, nothing to write")
                return True
            stats = self.buffer.stats()
            records = self.buffer.flush()

        logger.info(
            "Flushing — records=%d  event_types=%d  packages=%d  sessions=%d  users=%d",
            len(records), stats["unique_event_types"], stats["unique_packages"],
            stats["unique_sessions"], stats["unique_users"],
        )

        max_retries = 3
        for attempt in range(max_retries):
            if self.sink.batch_insert(records):
                try:
                    self.consumer.commit()
                    logger.info("Flush complete — wrote %d records, offsets committed", len(records))
                    return True
                except Exception as e:
                    logger.error("Flush succeeded but offset commit failed: %s", e)
                    return False

            if attempt < max_retries - 1:
                delay = 2 ** attempt
                logger.warning("Write failed — retrying in %ds (%d/%d)", delay, attempt + 2, max_retries)
                time.sleep(delay)

        logger.error("Flush failed after %d attempts — %d records lost", max_retries, len(records))
        return False

    def _flush_loop(self) -> None:
        logger.info("Flush thread started — interval=%ds", self.config.flush_interval_seconds)
        while self.running:
            sleep_time = self._calculate_next_flush_time()
            next_flush = datetime.now(timezone.utc).strftime("%H:%M:%S")
            logger.info("Next flush in %.0fs (at ~%s UTC)", sleep_time, next_flush)

            while sleep_time > 0 and self.running:
                time.sleep(min(sleep_time, 1.0))
                sleep_time -= 1.0

            if not self.running:
                break

            try:
                self._flush()
            except Exception as e:
                logger.error("Unexpected error in flush loop: %s", e)

        logger.info("Flush thread stopped")

    def start(self) -> None:
        logger.info(
            "Starting Event Stream Service — topic=%s  flush_interval=%ds  output=%s",
            self.config.kafka_topic, self.config.flush_interval_seconds, self.config.output_path,
        )

        if not self.sink.test_connection():
            logger.error("Output sink unavailable, exiting")
            sys.exit(1)

        logger.info("Output sink ready — writing to %s", self.config.output_path)
        logger.info("Subscribed to Kafka topic: %s", self.config.kafka_topic)

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
                    # consumer_timeout_ms reached — normal, keep looping
                    pass
                except KafkaError as e:
                    logger.error("Kafka error: %s — retrying in 1s", e)
                    time.sleep(1)

        except KeyboardInterrupt:
            logger.info("Interrupted by user")
        except Exception as e:
            logger.error("Fatal error: %s", e)
        finally:
            self.shutdown()

    def shutdown(self) -> None:
        logger.info("Shutting down — processed=%d  skipped=%d  errors=%d",
                    self._msg_count, self._skip_count, self._error_count)
        self.running = False

        logger.info("Running final flush...")
        try:
            self._flush()
        except Exception as e:
            logger.error("Error during final flush: %s", e)

        if self.flush_thread and self.flush_thread.is_alive():
            self.flush_thread.join(timeout=10)

        self.consumer.close()
        logger.info("Shutdown complete")
