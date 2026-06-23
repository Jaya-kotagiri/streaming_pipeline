"""
Spark Structured Streaming consumer.

Reads ChangeEvent JSON records from the Kafka topic, parses them against a
strict schema, and writes each micro-batch to the configured database sink
(SQLite for dev, Snowflake for prod) via foreachBatch.

Fault tolerance:
- Checkpointing (spark.checkpoint_location) gives exactly-once offset
  tracking across restarts.
- The DB sink writes are idempotent (event_id is the dedup key), so Spark's
  at-least-once delivery on retry does not produce duplicate rows.

Run with:
    spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
        src/consumer/spark_consumer.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from src.db.db_sink import get_sink
from src.models.event import ChangeEvent
from src.utils.config_loader import load_config
from src.utils.logger import get_logger

EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("file_name", StringType(), False),
        StructField("old_value", StringType(), True),
        StructField("new_value", StringType(), True),
        StructField("event_type", StringType(), False),
        StructField("line_number", IntegerType(), True),
        StructField("event_timestamp", StringType(), False),
    ]
)


def build_spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def make_foreach_batch_writer(db_config: dict, target: str, logger):
    """Returns a function with signature (df, epoch_id) for foreachBatch,
    closing over a lazily-initialized DB sink (one per executor process is
    fine here since writes happen on the driver after collect())."""

    sink_holder = {}

    def _get_sink():
        if "sink" not in sink_holder:
            sink_holder["sink"] = get_sink(db_config, target=target, logger=logger)
        return sink_holder["sink"]

    def write_batch(batch_df, epoch_id):
        rows = batch_df.collect()
        if not rows:
            return
        events = [
            ChangeEvent(
                event_id=r["event_id"],
                file_name=r["file_name"],
                old_value=r["old_value"],
                new_value=r["new_value"],
                event_type=r["event_type"],
                line_number=r["line_number"],
                event_timestamp=r["event_timestamp"],
            )
            for r in rows
        ]
        sink = _get_sink()
        sink.write(events)
        logger.info(f"[epoch {epoch_id}] wrote {len(events)} event(s) to DB.")

    return write_batch


def main(target: str = "dev"):
    config = load_config()
    logger = get_logger("spark_consumer", config.get("logging"))

    spark = build_spark_session(config["spark"]["app_name"])
    spark.sparkContext.setLogLevel("WARN")

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config["kafka"]["bootstrap_servers"])
        .option("subscribe", config["kafka"]["topic"])
        .option("startingOffsets", config["kafka"].get("auto_offset_reset", "earliest"))
        .option("maxOffsetsPerTrigger", config["spark"].get("max_offsets_per_trigger", 1000))
        .load()
    )

    parsed = raw_stream.select(
        from_json(col("value").cast("string"), EVENT_SCHEMA).alias("data")
    ).select("data.*")

    writer = make_foreach_batch_writer(config["database"], target, logger)

    query = (
        parsed.writeStream.foreachBatch(writer)
        .option("checkpointLocation", config["spark"]["checkpoint_location"])
        .trigger(processingTime=config["spark"]["trigger_interval"])
        .start()
    )

    logger.info("Spark Structured Streaming consumer started. Awaiting termination...")
    query.awaitTermination()


if __name__ == "__main__":
    db_target = sys.argv[1] if len(sys.argv) > 1 else "dev"
    main(target=db_target)
