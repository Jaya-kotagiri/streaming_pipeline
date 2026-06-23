"""
DBSink - writes a micro-batch of change-event rows to the configured database.

Two backends are supported:
- sqlite (dev): direct sqlite3 writes, used for local development/testing
  without standing up Kafka/Spark infrastructure.
- snowflake (prod): uses the Snowflake Connector for Python. Called from
  within the Spark foreachBatch sink (see src/consumer/spark_consumer.py),
  where each batch is collected and written via executemany for throughput.

Idempotency: writes are keyed on event_id (PRIMARY KEY / UNIQUE), so
re-delivery from Kafka's at-least-once semantics will not duplicate rows.
"""

import sqlite3
from pathlib import Path
from typing import List

from src.models.event import ChangeEvent

SQLITE_DDL = """
CREATE TABLE IF NOT EXISTS file_change_events (
    event_id TEXT PRIMARY KEY,
    file_name TEXT NOT NULL,
    old_value TEXT,
    new_value TEXT,
    event_type TEXT NOT NULL,
    line_number INTEGER,
    event_timestamp TEXT NOT NULL,
    loaded_at TEXT DEFAULT (datetime('now'))
);
"""


class SQLiteSink:
    def __init__(self, db_path: str, logger=None):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.logger = logger
        self._init_schema()

    def _init_schema(self):
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute(SQLITE_DDL)
            conn.commit()
        finally:
            conn.close()

    def write(self, events: List[ChangeEvent]):
        if not events:
            return
        rows = [
            (
                e.event_id,
                e.file_name,
                e.old_value,
                e.new_value,
                e.event_type,
                e.line_number,
                e.event_timestamp,
            )
            for e in events
        ]
        conn = sqlite3.connect(self.db_path)
        try:
            conn.executemany(
                """
                INSERT OR IGNORE INTO file_change_events
                    (event_id, file_name, old_value, new_value, event_type, line_number, event_timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            conn.commit()
        finally:
            conn.close()
        if self.logger:
            self.logger.info(f"Wrote {len(rows)} event(s) to SQLite sink.")

    def fetch_recent(self, limit: int = 20):
        conn = sqlite3.connect(self.db_path)
        try:
            conn.row_factory = sqlite3.Row
            cur = conn.execute(
                "SELECT * FROM file_change_events ORDER BY loaded_at DESC LIMIT ?",
                (limit,),
            )
            return [dict(r) for r in cur.fetchall()]
        finally:
            conn.close()


class SnowflakeSink:
    """Production sink. Requires `snowflake-connector-python`.
    Instantiated and called from within Spark's foreachBatch (driver side)."""

    def __init__(self, snowflake_config: dict, logger=None):
        self.config = snowflake_config
        self.logger = logger
        self._conn = None

    def _connect(self):
        import snowflake.connector

        if self._conn is None or self._conn.is_closed():
            self._conn = snowflake.connector.connect(
                account=self.config["account"],
                user=self.config["user"],
                password=self.config["password"],
                warehouse=self.config["warehouse"],
                database=self.config["database"],
                schema=self.config["schema"],
            )
        return self._conn

    def write(self, events: List[ChangeEvent]):
        if not events:
            return
        conn = self._connect()
        table = self.config.get("table", "file_change_events")
        rows = [
            (
                e.event_id,
                e.file_name,
                e.old_value,
                e.new_value,
                e.event_type,
                e.line_number,
                e.event_timestamp,
            )
            for e in events
        ]
        with conn.cursor() as cur:
            cur.executemany(
                f"""
                MERGE INTO {table} t
                USING (SELECT %s AS event_id) s
                ON t.event_id = s.event_id
                WHEN NOT MATCHED THEN INSERT
                    (event_id, file_name, old_value, new_value, event_type, line_number, event_timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                [(r[0],) + r for r in rows],
            )
        conn.commit()
        if self.logger:
            self.logger.info(f"Wrote {len(rows)} event(s) to Snowflake sink.")

    def close(self):
        if self._conn and not self._conn.is_closed():
            self._conn.close()


def get_sink(database_config: dict, target: str = "dev", logger=None):
    """Factory: target is 'dev' or 'prod', matching keys in config.yaml's
    `database` section."""
    cfg = database_config[target]
    dialect = cfg["dialect"]
    if dialect == "sqlite":
        return SQLiteSink(cfg["path"], logger=logger)
    if dialect == "snowflake":
        return SnowflakeSink(cfg, logger=logger)
    raise ValueError(f"Unsupported dialect: {dialect}")
