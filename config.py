import os
from dataclasses import dataclass, field
from typing import List


@dataclass
class Config:
    # Kafka
    kafka_brokers: str = "logs-cluster-kafka-bootstrap.kafka-cluster.svc:9092"
    kafka_topic: str = "events"
    kafka_group_id: str = "journey-aggregate"

    # ClickHouse
    clickhouse_host: str = "10.6.155.14"
    clickhouse_user: str = ""
    clickhouse_password: str = ""
    clickhouse_database: str = "atlas_kafka"
    clickhouse_table: str = "event_counts_hourly"

    # Window
    flush_interval_seconds: int = 300  # 5 minutes

    # Dry run
    dry_run: bool = False

    # OS values to zero-fill
    os_values: List[str] = field(default_factory=lambda: ["ANDROID", "IOS"])

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            kafka_brokers=os.getenv("KAFKA_BROKERS", cls.kafka_brokers),
            kafka_topic=os.getenv("KAFKA_TOPIC", cls.kafka_topic),
            kafka_group_id=os.getenv("KAFKA_GROUP_ID", cls.kafka_group_id),
            clickhouse_host=os.getenv("CKH_HOST", cls.clickhouse_host),
            clickhouse_user=os.getenv("CKH_USER", cls.clickhouse_user),
            clickhouse_password=os.getenv("CKH_PASSWORD", cls.clickhouse_password),
            clickhouse_database=os.getenv("CKH_DATABASE", cls.clickhouse_database),
            clickhouse_table=os.getenv("CKH_TABLE", cls.clickhouse_table),
            flush_interval_seconds=int(os.getenv("FLUSH_INTERVAL_SECONDS", str(cls.flush_interval_seconds))),
            dry_run=os.getenv("DRY_RUN", "").lower() in ("1", "true", "yes"),
        )
