CREATE TABLE IF NOT EXISTS event_counts_hourly
(
    date                        Date,
    hour_of_day                 UInt8,
    event_name                  String,
    package_name                String,
    os                          String,
    event_count                 UInt64,
    session_count               UInt64,
    user_count                  UInt64,
    overflow_session_count      UInt64,
    non_overflow_session_count  UInt64,
    overflow_user_count         UInt64,
    non_overflow_user_count     UInt64
)
ENGINE = SummingMergeTree()
ORDER BY (date, hour_of_day, event_name, package_name, os);
