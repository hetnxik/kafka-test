import json
import logging
import sys
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, Tuple

from kafka import KafkaConsumer
from kafka.errors import KafkaError

from .config import Config
from .clickhouse import ClickHouseClient

logger = logging.getLogger(__name__)

COLUMNS = [
    "date_hour", "minute_bucket", "event_name", "package_name", "os",
    "event_count", "session_count", "user_count",
    "overflow_session_count", "non_overflow_session_count",
    "overflow_user_count", "non_overflow_user_count",
]


def _parse_timestamp(timestamp) -> datetime:
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


def _bucket_key(dt: datetime) -> Tuple[str, int]:
    """Returns (date_hour, minute_bucket) for a given datetime."""
    date_hour = dt.strftime("%Y-%m-%d %H")
    minute_bucket = (dt.minute // 5) * 5
    return date_hour, minute_bucket


def _prev_bucket(date_hour: str, minute_bucket: int) -> Tuple[str, int]:
    """Returns the (date_hour, minute_bucket) for the bucket immediately before this one."""
    if minute_bucket >= 5:
        return date_hour, minute_bucket - 5
    # Roll back to previous hour
    dt = datetime.strptime(date_hour, "%Y-%m-%d %H").replace(tzinfo=timezone.utc)
    prev_hour = dt.replace(hour=dt.hour - 1) if dt.hour > 0 else dt.replace(
        day=dt.day - 1, hour=23
    )
    return prev_hour.strftime("%Y-%m-%d %H"), 55


class FineGrainBuffer:
    """Accumulates event counts at 5-minute bucket granularity."""

    def __init__(self, os_values: List[str]):
        self.os_values = os_values
        self.counts: Dict = defaultdict(int)
        self.sessions: Dict = defaultdict(set)   # per (date_hour, minute_bucket, event, package, os)
        self.users: Dict = defaultdict(set)
        # Per-bucket session/user sets for overflow calculation, persisted across flushes
        self.bucket_sessions: Dict[Tuple[str, int], set] = defaultdict(set)
        self.bucket_users: Dict[Tuple[str, int], set] = defaultdict(set)

    def add(self, event_name: str, package_name: str, os: str,
            session_id: str, user_id: str, timestamp) -> None:
        dt = _parse_timestamp(timestamp)
        date_hour, minute_bucket = _bucket_key(dt)
        key = (date_hour, minute_bucket, event_name, package_name, os)

        self.counts[key] += 1
        self.sessions[key].add(session_id)
        self.users[key].add(user_id)
        self.bucket_sessions[(date_hour, minute_bucket)].add(session_id)
        self.bucket_users[(date_hour, minute_bucket)].add(user_id)

    def flush(self) -> List[Dict[str, Any]]:
        seen = set()
        for (date_hour, minute_bucket, event_name, package_name, os) in self.counts:
            seen.add((date_hour, minute_bucket, event_name, package_name))

        records = []
        for (date_hour, minute_bucket, event_name, package_name) in seen:
            prev_dh, prev_mb = _prev_bucket(date_hour, minute_bucket)
            prev_sess = self.bucket_sessions.get((prev_dh, prev_mb), set())
            prev_user = self.bucket_users.get((prev_dh, prev_mb), set())

            for os in self.os_values:
                key = (date_hour, minute_bucket, event_name, package_name, os)
                curr_sess = self.sessions.get(key, set())
                curr_user = self.users.get(key, set())

                records.append({
                    "date_hour": date_hour,
                    "minute_bucket": minute_bucket,
                    "event_name": event_name,
                    "package_name": package_name,
                    "os": os,
                    "event_count": self.counts.get(key, 0),
                    "session_count": len(curr_sess),
                    "user_count": len(curr_user),
                    "overflow_session_count": len(curr_sess & prev_sess),
                    "non_overflow_session_count": len(curr_sess - prev_sess),
                    "overflow_user_count": len(curr_user & prev_user),
                    "non_overflow_user_count": len(curr_user - prev_user),
                })

        self.counts.clear()
        self.sessions.clear()
        self.users.clear()
        self._prune_old_buckets()
        return records

    def _prune_old_buckets(self) -> None:
        """Keep only the last 2 hours of bucket data to cap memory."""
        now = datetime.now(timezone.utc)
        for key in list(self.bucket_sessions.keys()):
            date_hour, _ = key
            try:
                bucket_dt = datetime.strptime(date_hour, "%Y-%m-%d %H").replace(tzinfo=timezone.utc)
                if (now - bucket_dt).total_seconds() > 7200:
                    self.bucket_sessions.pop(key, None)
                    self.bucket_users.pop(key, None)
            except Exception:
                pass

    def stats(self) -> Dict[str, int]:
        return {
            "buffered_events": sum(self.counts.values()),
            "unique_event_types": len({k[2] for k in self.counts}),
            "unique_packages": len({k[3] for k in self.counts}),
            "unique_sessions": len({s for ss in self.sessions.values() for s in ss}),
            "unique_users": len({u for us in self.users.values() for u in us}),
        }

    def is_empty(self) -> bool:
        return len(self.counts) == 0


class TestStreamService:
    """5-minute granularity Kafka consumer for testing. Writes to CSV."""

    _LOG_EVERY = 1000

    def __init__(self, config: Config):
        self.config = config
        self.running = True
        self.buffer = FineGrainBuffer(config.os_values)
        self.buffer_lock = threading.Lock()
        self.flush_thread: Optional[threading.Thread] = None

        self._msg_count = 0
        self._skip_count = 0
        self._error_count = 0

        # Override output path for test output
        test_config = Config(
            kafka_brokers=config.kafka_brokers,
            kafka_topic=config.kafka_topic,
            kafka_group_id=config.kafka_group_id + "-test",
            clickhouse_host=config.clickhouse_host,
            clickhouse_user=config.clickhouse_user,
            clickhouse_password=config.clickhouse_password,
            clickhouse_database=config.clickhouse_database,
            clickhouse_table=config.clickhouse_table,
            output_path=config.output_path.replace(".csv", "_5min.csv"),
            flush_interval_seconds=config.flush_interval_seconds,
        )
        self.sink = ClickHouseClient(test_config)
        self.output_path = test_config.output_path

        logger.info("Connecting to Kafka — brokers=%s  topic=%s  group=%s",
                    config.kafka_brokers, config.kafka_topic, test_config.kafka_group_id)
        self.consumer = KafkaConsumer(
            config.kafka_topic,
            bootstrap_servers=config.kafka_brokers,
            group_id=test_config.kafka_group_id,
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

            if not package_name or package_name.endswith(".debug"):
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

        # Patch column names for CSV sink
        import csv, os as _os
        write_header = not _os.path.exists(self.output_path)
        try:
            with open(self.output_path, "a", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=COLUMNS)
                if write_header:
                    writer.writeheader()
                for record in records:
                    writer.writerow({col: record.get(col, "") for col in COLUMNS})
            self.consumer.commit()
            logger.info("Flush complete — wrote %d records to %s", len(records), self.output_path)
            return True
        except Exception as e:
            logger.error("Flush failed: %s", e)
            return False

    def _flush_loop(self) -> None:
        logger.info("Flush thread started — interval=%ds", self.config.flush_interval_seconds)
        while self.running:
            sleep_time = self._calculate_next_flush_time()
            logger.info("Next flush in %.0fs", sleep_time)

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
            "Starting Test Stream Service (5-min granularity) — topic=%s  output=%s",
            self.config.kafka_topic, self.output_path,
        )

        if not self.sink.test_connection():
            logger.error("Output sink unavailable, exiting")
            sys.exit(1)

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
