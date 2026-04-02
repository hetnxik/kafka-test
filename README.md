# ETL — Event Journey Aggregate

Processes Firebase event data into hourly aggregates. Two modes: **batch** (CSV files) and **stream** (Kafka).

---

## Folder Structure

```
etl/
├── read.py           # Streams a CSV file in chunks
├── aggregate.py      # Builds hourly counts and event transitions
├── run.py            # Batch entry point — single file
└── stream/
    ├── config.py     # Kafka + ClickHouse config (env-driven)
    ├── clickhouse.py # ClickHouse writer (CLI-based batch insert)
    ├── service.py    # Kafka consumer + 5-min window buffer + flush thread
    └── run.py        # Stream entry point
```

---

## Batch Pipeline

Reads a CSV file, builds hourly aggregates, writes to `output/`.

### Input

CSV files under `data/` with columns:
```
session_id, user_id, timestamp, event_type, created_at,
event_name, event_payload, metadata, os, package_name
```

- `timestamp` is used for all bucketing. `created_at` is ignored.

### Output

| File | Description |
|---|---|
| `output/hourly_event_counts.csv` | Hourly counts per event, package, OS |
| `output/event_transitions.csv` | Session-level event transition counts |

#### `hourly_event_counts.csv` columns
```
date, hour_of_day, event_name, package_name, os,
event_count, session_count, user_count
```

#### `event_transitions.csv` columns
```
date, hour_of_day, from_event, to_event, transition_count
```

### Running — single file

```bash
python -m etl.run --file data/events_1day.csv
```

Each run **merges** into existing output — running multiple files one by one accumulates results without overwriting.

### How it works

1. **`read.py`** — streams the CSV in chunks of `CHUNK_SIZE` rows (default 5M) so the full file is never loaded into memory.
2. **`aggregate.py`** — the `Aggregator` class accumulates partial counts across chunks:
   - Groups by `(hour, event_name, package_name, os)`
   - Tracks `session_id` and `user_id` as sets for distinct counts
   - Carries session state across chunks (`session_tails`) to detect cross-chunk event transitions
   - On `to_dataframes()`, zero-fills the `os` dimension for every `(event_name, package_name)` combo seen
3. **`run.py`** — ties it together: reads → aggregates → merges with existing output → writes.

### Event filtering

`config.py` defines `EVENT_PACKAGE_MAPPING` — a dict of `event_name → [package_names]`. Only events from this mapping are counted, and zero-fill uses it to ensure known combos always appear in output even with 0 counts.

```python
EVENT_PACKAGE_MAPPING = {
    "ny_rider_ride_completed": ["in.juspay.nammayatri", ...],
    "end_ride_success":        ["in.juspay.nammayatripartner", ...],
    ...
}
```

---

## Stream Pipeline

Continuously consumes from Kafka, aggregates into 5-minute windows, and pushes to ClickHouse every 5 minutes.

### Architecture

```
Kafka topic
    ↓
Consumer thread — polls messages, filters, adds to WindowBuffer
    ↓  (every 5 min)
Flush thread — floors to hour, groups by (date, hour_of_day, event_name, package_name)
    ↓           zero-fills OS dimension
INSERT into ClickHouse (SummingMergeTree)
    ↓
ClickHouse merges parts, sums counts for same key across flushes
```

### ClickHouse table

```sql
CREATE TABLE event_counts_hourly (
    date          Date,
    hour_of_day   UInt8,
    event_name    String,
    package_name  String,
    os            String,
    event_count   UInt64,
    session_count UInt64,
    user_count    UInt64
) ENGINE = SummingMergeTree()
ORDER BY (date, hour_of_day, event_name, package_name, os);
```

See `schema.sql` in the project root to create the table.

Always query with `sum()`:
```sql
SELECT date, hour_of_day, event_name, package_name, os,
       sum(event_count), sum(session_count), sum(user_count)
FROM event_counts_hourly
GROUP BY date, hour_of_day, event_name, package_name, os
```

### Running

```bash
pip install -r requirements_stream.txt
python -m etl.stream.run
```

### Environment variables

| Variable | Description | Default |
|---|---|---|
| `KAFKA_BROKERS` | Kafka bootstrap servers | `logs-cluster-kafka-bootstrap.kafka-cluster.svc:9092` |
| `KAFKA_TOPIC` | Topic to consume | `events` |
| `KAFKA_GROUP_ID` | Consumer group ID | `journey-aggregate` |
| `CKH_HOST` | ClickHouse host | `localhost` |
| `CKH_USER` | ClickHouse user | `` |
| `CKH_PASSWORD` | ClickHouse password | `` |
| `CKH_DATABASE` | ClickHouse database | `atlas_kafka` |
| `CKH_TABLE` | ClickHouse table | `event_counts_hourly` |
| `FLUSH_INTERVAL_SECONDS` | Window size in seconds | `300` |

### Key behaviours

- **No event filtering** — all `(event_name, package_name)` combos from Kafka are counted. No mapping required.
- **Skipped messages** — empty `package_name` or `.debug` packages are dropped silently.
- **OS zero-fill** — for every `(event_name, package_name)` combo seen in a window, both `ANDROID` and `IOS` rows are written. Missing OS gets `event_count = 0`.
- **Additive inserts** — `SummingMergeTree` sums counts for the same key across multiple flushes. Multiple 5-min windows within the same hour accumulate correctly.
- **At-least-once delivery** — Kafka offsets are committed only after a successful ClickHouse insert. On failure, retries up to 3 times with exponential backoff. On pod restart, messages are re-consumed from last committed offset.
- **Graceful shutdown** — `SIGTERM`/`SIGINT` triggers a final flush before exit.

### Expected Kafka message format

```json
{
  "session_id": "abc-123",
  "user_id": "user-456",
  "timestamp": "2026-03-30 10:23:45.123",
  "event_name": "ny_app_started",
  "package_name": "in.juspay.nammayatri",
  "os": "ANDROID"
}
```

---

## Config reference (`config.py`)

| Variable | Description |
|---|---|
| `DUMP_DIR` | Source data directory |
| `OUTPUT_DIR` | Batch output directory |
| `CHUNK_SIZE` | Rows per chunk for batch processing |
| `EVENT_PACKAGE_MAPPING` | Batch-only: valid `(event_name, package_name)` combos + zero-fill list |
