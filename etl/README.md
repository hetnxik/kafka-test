# Event Stream Aggregator

Consumes raw app events from a Kafka topic, aggregates them into hourly counts per `(event_name, package_name, os)`, and writes the results to CSV (or ClickHouse).

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
  WindowBuffer               ← accumulates counts in memory
        │
        ▼  every 5 minutes (flush thread)
  ClickHouseClient           ← appends records to CSV (or inserts into ClickHouse)
        │
        ▼
 output/event_counts.csv
```

### Aggregation

Each Kafka message is expected to be a JSON object with these fields:

| Field | Description |
|---|---|
| `event_name` | Name of the event (e.g. `ny_user_ride_completed`) |
| `package_name` | App package (e.g. `in.juspay.nammayatri`) |
| `os` | `ANDROID` or `IOS` |
| `session_id` | Session identifier |
| `user_id` | User identifier |
| `timestamp` | Unix ms/s epoch or ISO 8601 string |

Messages missing any of these fields (or with empty `session_id`/`user_id`) are silently skipped.

Events are grouped by `(date, hour_of_day, event_name, package_name, os)`. For each group the buffer tracks:
- **event_count** — total number of events
- **session_count** — unique session IDs
- **user_count** — unique user IDs

Every `(event, package)` combination seen on any OS gets a zero-filled row for the other OS as well, so the output always has symmetric ANDROID/IOS pairs.

### Overflow vs non-overflow

A session that starts in hour 10 and is still active in hour 11 would be double-counted as a "new" session without carry-over tracking. The buffer handles this:

- **overflow_session_count** — sessions active in *both* this hour and the previous hour
- **non_overflow_session_count** — sessions active *only* in this hour (genuinely new)
- `overflow + non_overflow == session_count` always holds

The same split is computed for users (`overflow_user_count`, `non_overflow_user_count`).

### Flush & offset commit

The flush thread wakes up every `flush_interval_seconds` (default 5 min), flushes the buffer to the output sink, and only then commits Kafka offsets. This guarantees **at-least-once delivery** — if the service crashes mid-flush, the same messages will be reprocessed on restart (producing duplicate rows in the CSV, which can be deduped by grouping on the key columns and summing counts).

---

## Output format

### `output/event_counts.csv`

| Column | Type | Description |
|---|---|---|
| `date` | string | e.g. `2026-04-02` |
| `hour_of_day` | int | 0–23 |
| `event_name` | string | |
| `package_name` | string | |
| `os` | string | `ANDROID` or `IOS` |
| `event_count` | int | total events in this hour |
| `session_count` | int | unique sessions in this hour |
| `user_count` | int | unique users in this hour |
| `overflow_session_count` | int | sessions carried over from previous hour |
| `non_overflow_session_count` | int | new sessions this hour |
| `overflow_user_count` | int | users carried over from previous hour |
| `non_overflow_user_count` | int | new users this hour |

Since the same clock hour is written across multiple 5-min flushes, **always aggregate before analysis**:

```python
df_agg = df.group_by(["date", "hour_of_day", "event_name", "package_name", "os"]).agg(
    pl.col("event_count").sum(),
    pl.col("session_count").sum(),
    pl.col("user_count").sum(),
    pl.col("overflow_session_count").sum(),
    pl.col("non_overflow_session_count").sum(),
    pl.col("overflow_user_count").sum(),
    pl.col("non_overflow_user_count").sum(),
)
```

---

## Running

### Main service (hourly aggregation)

```bash
cd etl
pip install -r requirements.txt
python -m stream.run
```

### Test service (5-minute granularity)

Same logic but keys are `(date_hour, minute_bucket)` instead of `(date, hour_of_day)`. Useful for validating counts over short runs. Output goes to `output/event_counts_5min.csv`.

```bash
python -m stream.test_run
```

Stop either service with `Ctrl+C` — it will do a final flush before exiting.

---

## Configuration

All settings can be overridden via environment variables:

| Env var | Default | Description |
|---|---|---|
| `KAFKA_BROKERS` | `logs-cluster-kafka-bootstrap.kafka-cluster.svc:9092` | Kafka broker address |
| `KAFKA_TOPIC` | `events` | Topic to consume |
| `KAFKA_GROUP_ID` | `journey-aggregate` | Consumer group ID |
| `CKH_HOST` | `10.6.155.14` | ClickHouse host (if using ClickHouse sink) |
| `CKH_USER` | *(empty)* | ClickHouse user |
| `CKH_PASSWORD` | *(empty)* | ClickHouse password |
| `CKH_TABLE` | `event_counts_hourly` | ClickHouse table |
| `OUTPUT_PATH` | `output/event_counts.csv` | CSV output path |
| `FLUSH_INTERVAL_SECONDS` | `300` | How often to flush (seconds) |

Example:

```bash
CKH_USER=myuser CKH_PASSWORD=mypass OUTPUT_PATH=/data/events.csv python -m stream.run
```

---

## File structure

```
etl/
├── stream/
│   ├── config.py         # Config dataclass, env var loading
│   ├── service.py        # WindowBuffer + EventStreamService (hourly aggregation)
│   ├── clickhouse.py     # Output sink (currently CSV, designed for ClickHouse)
│   ├── run.py            # Entry point for main service
│   ├── test_service.py   # 5-minute granularity variant of service.py
│   └── test_run.py       # Entry point for test service
├── aggregate.py          # Batch CSV ETL aggregator (separate from stream pipeline)
├── read.py               # Chunked CSV reader for batch ETL
├── run.py                # Entry point for batch ETL pipeline
├── config.py             # Config for batch ETL
├── schema.sql            # ClickHouse table schema
└── requirements.txt      # pandas, kafka-python
```

---

## Switching back to ClickHouse

`clickhouse.py` currently writes to CSV. To write directly to ClickHouse instead, replace `batch_insert` with:

```python
import subprocess, io, csv

def batch_insert(self, records):
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=COLUMNS)
    writer.writeheader()
    for record in records:
        writer.writerow({col: record.get(col, "") for col in COLUMNS})

    cmd = [
        "clickhouse-client",
        "--host", self.host, "--user", self.user, "--password", self.password,
        "--database", self.database,
        "--query", f"INSERT INTO {self.table} FORMAT CSVWithNames"
    ]
    result = subprocess.run(cmd, input=output.getvalue(), capture_output=True, text=True, timeout=120)
    return result.returncode == 0
```
