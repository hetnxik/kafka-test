# Event Stream Aggregator

Consumes raw app events from a Kafka topic, aggregates them into hourly counts per `(event_name, package_name, os)`, and inserts the results into ClickHouse every hour.

---

## How it works

### Flow

```
Kafka topic (events)
        │
        ▼
 EventStreamService          ← main thread: polls Kafka continuously
        │
        ▼
  WindowBuffer               ← accumulates counts in memory, keyed by clock hour
        │
        ▼  every 1 hour (flush thread)
  ClickHouseClient           ← inserts records via clickhouse-client
        │
        ▼
 ClickHouse: event_counts_hourly (SummingMergeTree)
```

### Aggregation

Each Kafka message must be a JSON object with these fields:

| Field | Description |
|---|---|
| `event_name` | Name of the event (e.g. `ny_user_ride_completed`) |
| `package_name` | App package (e.g. `in.juspay.nammayatri`) |
| `os` | `ANDROID` or `IOS` |
| `session_id` | Session identifier |
| `user_id` | User identifier |
| `timestamp` | Unix ms/s epoch or ISO 8601 string |

Messages missing any of these fields are silently skipped and counted in the `skipped` log counter.

Events are grouped by `(date, hour_of_day, event_name, package_name, os)`. For each group:
- **event_count** — total number of events
- **session_count** — unique session IDs
- **user_count** — unique user IDs

Every `(event, package)` combination seen on any OS gets a zero-filled row for the other OS, so the output always has symmetric ANDROID/IOS pairs.

### Overflow vs non-overflow

A session that starts at 10:55 and is still active at 11:10 spans an hour boundary. Without overflow tracking it would be double-counted as a new session in hour 11.

- **overflow_session_count** — sessions active in *both* this hour and the previous hour
- **non_overflow_session_count** — sessions active *only* in this hour (genuinely new)
- `overflow + non_overflow == session_count` always holds

Same logic applies to `overflow_user_count` / `non_overflow_user_count`.

### Flush & offset commit

The flush thread wakes up every hour, flushes the buffer to ClickHouse, and only then commits Kafka offsets. This guarantees **at-least-once delivery** — if the pod crashes mid-flush, the same messages are reprocessed on restart. Since the ClickHouse table uses `SummingMergeTree`, duplicate inserts for the same key are merged automatically.

---

## ClickHouse table

```sql
CREATE TABLE event_counts_hourly (
    date                        Date,
    hour_of_day                 UInt8,
    event_name                  String,
    package_name                String,
    os                          String,
    event_count                 UInt64,
    overflow_session_count      UInt64,
    non_overflow_session_count  UInt64,
    overflow_user_count         UInt64,
    non_overflow_user_count     UInt64
) ENGINE = SummingMergeTree()
ORDER BY (date, hour_of_day, event_name, package_name, os);
```

See `schema.sql` for the full DDL. Always query with `sum()`:

```sql
SELECT date, hour_of_day, event_name, package_name, os,
       sum(event_count), sum(session_count), sum(user_count)
FROM event_counts_hourly
GROUP BY date, hour_of_day, event_name, package_name, os
```

---

## Running

```bash
cd etl
pip install -r requirements.txt
python -m stream.run
```

Stop with `Ctrl+C` — triggers a final flush before exit.

### Test service (5-minute granularity, CSV output)

For local testing without ClickHouse. Keys are `(date_hour, minute_bucket)` instead of `(date, hour_of_day)`. Output goes to `output/event_counts_5min.csv`.

```bash
python -m stream.test_run
```

---

## Configuration

| Env var | Default | Description |
|---|---|---|
| `KAFKA_BROKERS` | `logs-cluster-kafka-bootstrap.kafka-cluster.svc:9092` | Kafka broker address |
| `KAFKA_TOPIC` | `events` | Topic to consume |
| `KAFKA_GROUP_ID` | `journey-aggregate` | Consumer group ID |
| `CKH_HOST` | `10.6.155.14` | ClickHouse host |
| `CKH_USER` | *(empty)* | ClickHouse user |
| `CKH_PASSWORD` | *(empty)* | ClickHouse password |
| `CKH_TABLE` | `event_counts_hourly` | ClickHouse table |
| `FLUSH_INTERVAL_SECONDS` | `3600` | Flush interval in seconds |

Example:

```bash
CKH_USER=myuser CKH_PASSWORD=mypass python -m stream.run
```

---

## File structure

```
etl/stream/
├── config.py         # Config dataclass, env var loading
├── service.py        # WindowBuffer + EventStreamService (hourly aggregation)
├── clickhouse.py     # ClickHouse sink (inserts via clickhouse-client)
├── run.py            # Entry point for main service
├── test_service.py   # 5-minute granularity variant for local testing
└── test_run.py       # Entry point for test service
```
