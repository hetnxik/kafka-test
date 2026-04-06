import os
from dataclasses import dataclass, field
from typing import List


@dataclass
class Config:
    # --- Kafka ---
    # Broker address inside the cluster
    kafka_brokers: str = "logs-cluster-kafka-bootstrap.kafka-cluster.svc:9092"
    # Topic to consume events from
    kafka_topic: str = "events"
    # Consumer group ID — Kafka uses this to track which offsets this service has consumed.
    # Offsets are only committed after a successful flush, so no data is lost if the service crashes.
    kafka_group_id: str = "journey-aggregate"

    # --- ClickHouse (used when writing directly to ClickHouse instead of CSV) ---
    clickhouse_host: str = "10.6.155.14"
    clickhouse_user: str = ""
    clickhouse_password: str = ""
    clickhouse_database: str = "atlas_kafka"
    clickhouse_table: str = "event_counts_hourly"

    # --- Output ---
    # Path for CSV output. Directory is created automatically if it doesn't exist.
    output_path: str = "output/event_counts.csv"

    # --- Window ---
    # How often (in seconds) the in-memory buffer is flushed to the output sink.
    # Default is 5 minutes (300s). Aggregation keys are by clock hour, not flush window,
    # so multiple flushes within the same hour append rows for the same hour.
    flush_interval_seconds: int = 300

    # OS values used for zero-filling: every (event, package) combo will have a row
    # for each OS even if no events were seen for that OS in the window.
    os_values: List[str] = field(default_factory=lambda: ["ANDROID", "IOS"])

    @classmethod
    def from_env(cls) -> "Config":
        """Load config from environment variables, falling back to defaults."""
        return cls(
            kafka_brokers=os.getenv("KAFKA_BROKERS", cls.kafka_brokers),
            kafka_topic=os.getenv("KAFKA_TOPIC", cls.kafka_topic),
            kafka_group_id=os.getenv("KAFKA_GROUP_ID", cls.kafka_group_id),
            clickhouse_host=os.getenv("CKH_HOST", cls.clickhouse_host),
            clickhouse_user=os.getenv("CKH_USER", cls.clickhouse_user),
            clickhouse_password=os.getenv("CKH_PASSWORD", cls.clickhouse_password),
            clickhouse_database=os.getenv("CLICKHOUSE_DATABASE", cls.clickhouse_database),
            clickhouse_table=os.getenv("CKH_TABLE", cls.clickhouse_table),
            output_path=os.getenv("OUTPUT_PATH", cls.output_path),
            flush_interval_seconds=int(os.getenv("FLUSH_INTERVAL_SECONDS", str(cls.flush_interval_seconds))),
        )
