# streaming_pipeline

A lightweight, production-aware file change detection system that watches local CSV/text files, extracts line-level diffs, and streams structured change events to Apache Kafka — where they are consumed, transformed, and persisted to a database sink.

Built to handle real-world edge cases: editors that rewrite entire files on save, watcher restarts that would otherwise replay the whole file, and Kafka delivery failures that would silently drop data.

---

## What it does

1. **Watches** a directory for file modifications using `watchdog`
2. **Diffs** each changed file against a persisted snapshot using `difflib.SequenceMatcher` to identify which lines were inserted, updated, or deleted — not just that the file changed
3. **Publishes** structured `ChangeEvent` messages to a Kafka topic
4. **Consumes** those events via a Spark Structured Streaming job (full path) or a lightweight `confluent-kafka` consumer (dev path)
5. **Writes** change records to PostgreSQL (dev) or Snowflake (production), idempotently on `event_id`

---

## Why line-level diffing matters

Most file watchers tell you *that* a file changed. This system tells you *what* changed and *how*.

Editors like Notepad write a completely new file to disk on every save. A naive watcher would emit every line as an insert on each save. `SequenceMatcher` diffs the new content against the last known snapshot, so only the lines that actually changed produce events — regardless of how the editor writes the file.

Snapshots are persisted to disk so the watcher can resume correctly after a restart without replaying the entire file history.

---

## Architecture

```
 ┌─────────────────────────────────────────────────────────┐
 │                        Watcher                          │
 │                                                         │
 │   watchdog (FileSystemEventHandler)                     │
 │       │  debounced on_modified                          │
 │       ▼                                                 │
 │   DiffEngine (difflib.SequenceMatcher)                  │
 │       │  compare new content vs. persisted snapshot     │
 │       ▼                                                 │
 │   ChangeEvent dataclass                                 │
 │   { event_id, timestamp, op, line_no, old, new }        │
 │       │                                                 │
 │       ▼                                                 │
 │   KafkaProducer  ──► dead-letter JSONL on failure       │
 └────────────┬────────────────────────────────────────────┘
              │  Kafka topic: file_change_events
 ┌────────────▼────────────────────────────────────────────┐
 │                      Consumer                           │
 │                                                         │
 │   [Full]  Spark Structured Streaming                    │
 │           with checkpoint for at-least-once delivery    │
 │                                                         │
 │   [Lite]  confluent-kafka consumer                      │
 │           for local dev / unit testing                  │
 │                                                         │
 │       ▼                                                 │
 │   DB Sink (idempotent on event_id)                      │
 │   ├── PostgreSQL  (dev)                                 │
 │   └── Snowflake   (production)                          │
 └─────────────────────────────────────────────────────────┘
```

---

## Project layout

```
streaming_pipeline/
├── config/
│   └── config.yaml          # all tunables; overridable via FCSTREAM_* env vars
├── src/
│   ├── watcher/
│   │   ├── file_watcher.py  # watchdog handler + debounce logic
│   │   └── diff_engine.py   # SequenceMatcher diff → ChangeEvent list
│   ├── producer/
│   │   └── kafka_producer.py
│   ├── consumer/
│   │   ├── spark_consumer.py
│   │   └── lite_consumer.py
│   ├── db/
│   │   ├── postgres_sink.py
│   │   └── snowflake_sink.py
│   ├── models/
│   │   └── change_event.py  # ChangeEvent dataclass
│   └── utils/
│       ├── config_loader.py
│       └── logger.py
├── tests/
│   ├── test_diff_engine.py
│   └── test_db_sink.py
├── sample_data/             # example CSVs for local testing
├── snapshots/               # persisted file baselines (gitignored)
├── logs/                    # rotating log output (gitignored)
├── main_watcher.py          # entrypoint: start the file watcher
├── main_consumer_lite.py    # entrypoint: start the lite consumer
├── docker-compose.yml       # Kafka + Zookeeper + Kafka UI
└── requirements.txt
```

---

## Quickstart

### 1. Start Kafka locally

```bash
docker-compose up -d
```

Kafka UI is available at `http://localhost:8080` once the stack is up.

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure

Edit `config/config.yaml` or export environment variables. All settings are prefixed `FCSTREAM_`:

```bash
export FCSTREAM_WATCH_DIR=/path/to/your/files
export FCSTREAM_KAFKA_BOOTSTRAP=localhost:9092
export FCSTREAM_KAFKA_TOPIC=file_change_events
export FCSTREAM_DB_TARGET=postgres   # or snowflake
```

### 4. Start the watcher

```bash
python main_watcher.py
```

### 5. Start the consumer

Dev (lite) path:

```bash
python main_consumer_lite.py --db dev
```

Full Spark path:

```bash
spark-submit src/consumer/spark_consumer.py --db production
```

---

## Configuration reference

| Key | Env override | Default | Description |
|-----|-------------|---------|-------------|
| `watch_dir` | `FCSTREAM_WATCH_DIR` | `./sample_data` | Directory to watch |
| `kafka.bootstrap` | `FCSTREAM_KAFKA_BOOTSTRAP` | `localhost:9092` | Kafka broker address |
| `kafka.topic` | `FCSTREAM_KAFKA_TOPIC` | `file_change_events` | Topic name |
| `kafka.dlq_path` | `FCSTREAM_DLQ_PATH` | `./logs/dlq.jsonl` | Dead-letter queue file |
| `db.target` | `FCSTREAM_DB_TARGET` | `postgres` | `postgres` or `snowflake` |
| `watcher.debounce_ms` | `FCSTREAM_DEBOUNCE_MS` | `300` | Debounce delay in ms |
| `watcher.snapshot_dir` | `FCSTREAM_SNAPSHOT_DIR` | `./snapshots` | Snapshot persistence path |

---

## Fault tolerance

| Failure scenario | How it is handled |
|-----------------|-------------------|
| Kafka broker unreachable | Failed events written to `dlq.jsonl`; retried on a background timer |
| Watcher process restart | Snapshots on disk used as baseline; no replay of unchanged lines |
| Kafka redelivery / duplicate messages | DB writes are idempotent on `event_id` (UUID per change) |
| Consumer crash mid-batch | Spark checkpoint stores committed offsets; resumes from last good position |

---

## Running tests

```bash
pytest tests/ -v
```

Tests cover the diff engine (insert, update, delete, no-op, full-rewrite scenarios) and the DB sink (idempotency, schema validation).

---

## Migrating to Snowflake

1. Set `FCSTREAM_DB_TARGET=snowflake`
2. Add Snowflake credentials to `config/config.yaml` under the `snowflake:` key (account, user, password, warehouse, database, schema)
3. Run the Spark consumer path — the Snowflake sink uses the Snowflake Spark connector and writes via `MERGE INTO` on `event_id`

---

## Requirements

- Python 3.9+
- Docker (for local Kafka)
- Java 11+ (for Spark consumer path only)
- Snowflake account (for production sink only)

---

## License

MIT
