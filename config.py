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

    # Allowlisted event names (case-insensitive match)
    allowed_events: List[str] = field(default_factory=lambda: [
        "ny_app_started",
        "ny_user_onboarded",
        "ny_user_first_ride_completed",
        "ny_rider_ride_completed",
        "driver_assigned",
        "app_remove",
        "ny_user_source_and_destination",
        "ny_user_request_quotes",
        "notification_recieve",
        "serviceTab_homeScreen",
        "notification_open",
        "ny_user_app_version",
        "Metro_ticket_payment_successful",
        "user_session_start",
        "NEW_SIGNUP",
        "end_ride_success",
        "ny_driver_status_change",
        "ride_cancelled",
        "FIRST_RIDE_COMPLETED",
        "btp_new_signup_del",
        "btp_new_signup_ahm",
        "btp_new_signup_noi",
        "btp_new_signup_gur",
        "btp_new_signup_sur",
        "btp_new_signup_vad",
        "mt_home_bus",
        "mt_home_metro",
        "mt_home_train",
        "mt_home_search",
        "NY_BUS_OTP_TYPED",
        "NY_BUS_OTP_SCANNED",
        "MT_JOURNEY_INFO_PAY",
        "METRO_TICKET_PAYMENT_FAILED",
        "ny_user_ride_started",
        "ny_user_ride_assigned",
        "ny_user_ride_completed",
        "first_open",
        "Odishauser_first_ride_completed",
        "Yatriuser_first_ride_completed",
        "keralaSavaariuser_first_ride_completed",
        "ny_user_twostar_rating",
        "ny_cab_firstride",
        "ys_bike_firstride",
    ])

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
