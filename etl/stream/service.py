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
    """
    Accumulates raw Kafka events into hourly aggregates in memory.

    Aggregation key: (date, hour_of_day, event_name, package_name, os)
    For each key we track:
      - event_count:   total number of events
      - session_count: unique session IDs seen this hour
      - user_count:    unique user IDs seen this hour

    Overflow vs non-overflow
    ------------------------
    A session is "overflow" if it was already active in the previous clock hour.
    This matters because a single session can span an hour boundary — e.g., a user
    starts a session at 10:55 and is still active at 11:10. Without overflow tracking,
    that session would be double-counted as a "new" session in hour 11.

    - overflow_session_count:     sessions active in both this hour and the previous hour
    - non_overflow_session_count: sessions active only in this hour (genuinely new)
    - overflow + non_overflow == session_count (always)

    The same logic applies to users.

    Implementation note: session/user sets are tracked per clock hour (hour_sessions /
    hour_users dicts) and persist across flushes so that overflow can be computed
    correctly even when the hour boundary falls mid-flush-window.
    """

    def __init__(self, os_values: List[str]):
        self.os_values = os_values

        # Per (date, hour, event, package, os) aggregates for the current flush window
        self.counts: Dict = defaultdict(int)
        self.sessions: Dict = defaultdict(set)
        self.users: Dict = defaultdict(set)

        # Per clock-hour session/user sets — persisted across flushes for overflow calculation.
        # Key: (date, hour_of_day)
        self.hour_sessions: Dict = defaultdict(set)
        self.hour_users: Dict = defaultdict(set)

    def add(self, event_name: str, package_name: str, os: str,
            session_id: str, user_id: str, timestamp) -> None:
        """Parse the event timestamp and accumulate counts into the buffer."""
        dt = self._parse_timestamp(timestamp)
        date = str(dt.date())
        hour_of_day = dt.hour
        key = (date, hour_of_day, event_name, package_name, os)

        self.counts[key] += 1
        self.sessions[key].add(session_id)
        self.users[key].add(user_id)

        # Also accumulate into the clock-hour sets (used for overflow at flush time)
        self.hour_sessions[(date, hour_of_day)].add(session_id)
        self.hour_users[(date, hour_of_day)].add(user_id)

    def flush(self) -> List[Dict[str, Any]]:
        """
        Convert buffered counts into records and reset the window buffer.

        For each (date, hour, event, package) seen on any OS, produces one row
        per OS (zero-filled for OSes with no events). Overflow is computed by
        comparing against the previous clock hour's session/user sets.
        """
        # Collect unique (date, hour, event, package) combos seen this window
        seen = set()
        for (date, hour_of_day, event_name, package_name, os) in self.counts:
            seen.add((date, hour_of_day, event_name, package_name))

        records = []
        for (date, hour_of_day, event_name, package_name) in seen:
            # Look up previous clock hour for overflow comparison
            prev_date, prev_hour = self._prev_clock_hour(date, hour_of_day)
            prev_sess = self.hour_sessions.get((prev_date, prev_hour), set())
            prev_user = self.hour_users.get((prev_date, prev_hour), set())

            # Produce one row per OS (zero-filled if no events for that OS)
            for os in self.os_values:
                key = (date, hour_of_day, event_name, package_name, os)
                curr_sess = self.sessions.get(key, set())
                curr_user = self.users.get(key, set())

                records.append({
                    "date": date,
                    "hour_of_day": hour_of_day,
                    "event_name": event_name,
                    "package_name": package_name,
                    "os": os,
                    "event_count": self.counts.get(key, 0),
                    "session_count": len(curr_sess),
                    "user_count": len(curr_user),
                    # Intersection with previous hour = carried-over sessions/users
                    "overflow_session_count": len(curr_sess & prev_sess),
                    "non_overflow_session_count": len(curr_sess - prev_sess),
                    "overflow_user_count": len(curr_user & prev_user),
                    "non_overflow_user_count": len(curr_user - prev_user),
                })

        # Reset per-window state; clock-hour sets are kept for future overflow lookups
        self.counts.clear()
        self.sessions.clear()
        self.users.clear()

        # Prune clock-hour sets older than 2 hours to cap memory usage
        self._prune_old_hours()

        return records

    def stats(self) -> Dict[str, int]:
        """Return a snapshot of the current buffer for logging."""
        return {
            "buffered_events": sum(self.counts.values()),
            "unique_event_types": len({k[2] for k in self.counts}),
            "unique_packages": len({k[3] for k in self.counts}),
            "unique_sessions": len({s for ss in self.sessions.values() for s in ss}),
            "unique_users": len({u for us in self.users.values() for u in us}),
        }

    def is_empty(self) -> bool:
        return len(self.counts) == 0

    @staticmethod
    def _parse_timestamp(timestamp) -> datetime:
        """
        Parse a timestamp that may be:
          - int/float: Unix epoch in milliseconds (>1e10) or seconds
          - str digits: same as above but as a string
          - str ISO 8601: e.g. "2026-04-02T11:35:00Z"
        Falls back to current UTC time if parsing fails.
        """
        try:
            if isinstance(timestamp, (int, float)):
                epoch = float(timestamp)
                return datetime.fromtimestamp(epoch / 1000 if epoch > 1e10 else epoch, tz=timezone.utc)
            ts = timestamp.strip()
            if ts.isdigit():
                epoch = int(ts)
                return datetime.fromtimestamp(epoch / 1000 if epoch > 1e10 else epoch, tz=timezone.utc)
            return datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except Exception:
            logger.warning("Unparseable timestamp %r, using current UTC time", timestamp)
            return datetime.now(timezone.utc)

    @staticmethod
    def _prev_clock_hour(date: str, hour: int):
        """Return (date, hour) for the clock hour immediately before the given one."""
        if hour > 0:
            return date, hour - 1
        from datetime import date as date_type, timedelta
        prev = str(date_type.fromisoformat(date) - timedelta(days=1))
        return prev, 23

    def _prune_old_hours(self) -> None:
        """Drop clock-hour entries older than 2 hours to prevent unbounded memory growth."""
        now = datetime.now(timezone.utc)
        for key in list(self.hour_sessions.keys()):
            d, h = key
            try:
                bucket_dt = datetime.fromisoformat(f"{d}T{h:02d}:00:00+00:00")
                if (now - bucket_dt).total_seconds() > 7200:
                    self.hour_sessions.pop(key, None)
                    self.hour_users.pop(key, None)
            except Exception:
                pass


class EventStreamService:
    """
    Main service: consumes from Kafka, aggregates into hourly windows, flushes to output sink.

    Flow
    ----
    1. KafkaConsumer polls messages continuously (main thread).
    2. Each message is parsed and added to WindowBuffer (in-memory).
    3. A background flush thread wakes up every flush_interval_seconds,
       calls buffer.flush() to get aggregated records, and writes them to the sink.
    4. Kafka offsets are committed only after a successful flush — guarantees
       at-least-once delivery (no data loss on crash, possible duplicate rows on retry).

    Threading
    ---------
    The main thread and flush thread share the buffer via buffer_lock.
    The flush thread holds the lock only during flush(), so message processing
    is not blocked for long.
    """

    # Log a throughput summary every N messages
    _LOG_EVERY = 1000

    def __init__(self, config: Config):
        self.config = config
        self.running = True
        self.buffer = WindowBuffer(config.os_values)
        self.buffer_lock = threading.Lock()
        self.flush_thread: Optional[threading.Thread] = None

        self._msg_count = 0       # total messages processed
        self._skip_count = 0      # messages skipped (missing fields, debug packages)
        self._error_count = 0     # parse/processing errors

        self.sink = ClickHouseClient(config)

        logger.info("Connecting to Kafka — brokers=%s  topic=%s  group=%s",
                    config.kafka_brokers, config.kafka_topic, config.kafka_group_id)
        self.consumer = KafkaConsumer(
            config.kafka_topic,
            bootstrap_servers=config.kafka_brokers,
            group_id=config.kafka_group_id,
            enable_auto_commit=False,   # manual commit after flush
            auto_offset_reset="latest", # start from live messages, not beginning of topic
            consumer_timeout_ms=1000,   # raises StopIteration after 1s of no messages
        )

    def _calculate_next_flush_time(self) -> float:
        """Seconds until the next flush interval boundary (clock-aligned)."""
        now = time.time()
        interval = self.config.flush_interval_seconds
        return interval - (now % interval)

    def _process_message(self, msg) -> None:
        """Validate and add a single Kafka message to the buffer."""
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

            # Skip messages with missing required fields
            if not package_name or package_name.endswith(".debug"):
                self._skip_count += 1
                return
            if not event_name or not os or not timestamp:
                self._skip_count += 1
                return
            if not session_id or not user_id:
                # Empty session/user IDs cause user_count > session_count anomalies
                self._skip_count += 1
                return

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
        """
        Flush buffered records to the output sink and commit Kafka offsets.
        Retries up to 3 times on sink failure. Offsets are NOT committed if
        the sink write fails (so the same records will be reprocessed on restart).
        """
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
        """Background thread: wakes up at each flush interval and calls _flush()."""
        logger.info("Flush thread started — interval=%ds", self.config.flush_interval_seconds)
        while self.running:
            sleep_time = self._calculate_next_flush_time()
            logger.info("Next flush in %.0fs", sleep_time)

            # Sleep in 1s increments so we can respond to shutdown quickly
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
        """Start the flush thread and begin consuming messages from Kafka."""
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
                    # consumer_timeout_ms reached with no messages — normal, keep looping
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
        """Graceful shutdown: final flush, wait for flush thread, close consumer."""
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
