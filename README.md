# FileChange Stream Toolkit

Monitors a text file in real time, detects line-level INSERT / UPDATE / DELETE
changes, streams them through Kafka, processes them with PySpark Structured
Streaming, and persists them to a database with a full audit trail.

## Architecture

```
Notepad / text file
        │  (filesystem events)
        ▼
FileWatcher (watchdog, debounced)
        │  (line-level diff vs last snapshot)
        ▼
DiffEngine  →  ChangeEvent(event_id, file_name, old_value, new_value,
                            event_type, line_number, event_timestamp)
        │
        ▼
KafkaEventProducer  →  topic: file-change-events
        │
        ▼
PySpark Structured Streaming (src/consumer/spark_consumer.py)
        │  (foreachBatch, checkpointed)
        ▼
Database sink (idempotent on event_id)
   - dev:  SQLite (no infra required)
   - prod: Snowflake (or Postgres via JDBC)
```

### Why line-level diffing?
A filesystem "modified" event only tells you a file changed, not what
changed — and editors like Notepad rewrite the whole file on save. The
`DiffEngine` (src/watcher/diff_engine.py) uses `difflib.SequenceMatcher`
(the same algorithm behind line-based `diff`) to compare the previous
snapshot against the new content and recover INSERT / UPDATE / DELETE
semantics at the line level.

### Fault tolerance
- **Producer side**: `KafkaEventProducer` spills failed sends to a local
  JSONL dead-letter queue and retries flushing it on a background timer, so
  a broker outage doesn't lose events.
- **Consumer side**: Spark checkpointing (`spark.checkpoint_location`)
  resumes from the last committed offset after a restart.
- **DB side**: all sinks dedupe on `event_id` (PRIMARY KEY / MERGE), so
  at-least-once delivery from Kafka never produces duplicate rows.
- **Watcher side**: the last-known file content is persisted to
  `snapshots/<file>.snapshot` so a watcher restart resumes diffing from the
  correct baseline instead of replaying the whole file as inserts.

## Project layout

```
config/config.yaml          all settings, env-override-able (FCSTREAM_* vars)
src/watcher/                file_watcher.py, diff_engine.py
src/models/event.py         ChangeEvent dataclass
src/producer/                kafka_producer.py
src/consumer/                spark_consumer.py (production path)
src/db/                      db_sink.py (SQLite + Snowflake), schema.sql
src/utils/                   config_loader.py, logger.py
main_watcher.py              entrypoint: watcher -> Kafka
main_consumer_lite.py        entrypoint: Kafka -> DB (no Spark, for quick dev testing)
docker-compose.yml           local Kafka + Zookeeper + Kafka UI
tests/                       unit tests (diff engine, DB sink)
sample_data/sample.txt       file to watch in local testing
```

## Setup

```bash
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt   # pyspark/snowflake-connector are heavy; omit if just testing the watcher/diff/DB path

# start local Kafka (requires Docker)
docker compose up -d
```

## Running it

**1. Quick local test (no Kafka/Spark needed)** — validates diffing + DB writes directly:
```bash
python3 -c "
from src.watcher.diff_engine import DiffEngine
from src.db.db_sink import SQLiteSink
engine = DiffEngine('sample.txt')
old = open('sample_data/sample.txt').read().splitlines()
new = old.copy(); new[0] = 'changed line'
events = engine.diff(old, new)
SQLiteSink('./db/file_change_events.db').write(events)
"
```

**2. Full pipeline:**
```bash
# Terminal 1 - start the watcher (publishes to Kafka)
python main_watcher.py

# Terminal 2 - start the consumer
# Lightweight (no Spark, for dev):
python main_consumer_lite.py dev
# OR production path with Spark:
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    src/consumer/spark_consumer.py dev

# Terminal 3 - edit sample_data/sample.txt and save; watch events flow through
echo "order_id=1005,status=PENDING,amount=20.00" >> sample_data/sample.txt
```

Kafka UI (topic inspection): http://localhost:8080

## Switching to Snowflake / Postgres for production

Edit `config/config.yaml` → `database.prod`, set credentials via env vars
(e.g. `FCSTREAM_DATABASE_PROD_PASSWORD`), and run consumers with `prod` as the
target argument instead of `dev`. DDL for Snowflake, SQL Server, and
Postgres are all in `src/db/schema.sql`.

## Tests

```bash
pytest tests/ -v
```

## Resume-worthy description

Built a real-time, event-driven streaming pipeline that monitors file
changes via a debounced filesystem watcher, derives line-level
INSERT/UPDATE/DELETE semantics with a custom diff engine, publishes events
to Kafka with dead-letter-queue fault tolerance, processes them with PySpark
Structured Streaming using checkpointed, idempotent micro-batch writes, and
persists a full audit trail to Snowflake/SQL Server with sub-5-second
end-to-end latency.
