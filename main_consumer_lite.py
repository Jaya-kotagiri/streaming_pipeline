"""
Lightweight consumer for local development/testing.

This is NOT the production path (that's src/consumer/spark_consumer.py +
PySpark Structured Streaming) but lets you validate the watcher -> Kafka ->
DB flow end-to-end quickly with plain kafka-python, before standing up a
Spark cluster.

Run:
    python main_consumer_lite.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from kafka import KafkaConsumer

from src.db.db_sink import get_sink
from src.models.event import ChangeEvent
from src.utils.config_loader import load_config
from src.utils.logger import get_logger


def main(target: str = "dev"):
    config = load_config()
    logger = get_logger("consumer_lite", config.get("logging"))

    consumer = KafkaConsumer(
        config["kafka"]["topic"],
        bootstrap_servers=config["kafka"]["bootstrap_servers"].split(","),
        group_id=config["kafka"]["group_id"],
        auto_offset_reset=config["kafka"].get("auto_offset_reset", "earliest"),
        value_deserializer=lambda v: v.decode("utf-8"),
        enable_auto_commit=True,
    )

    sink = get_sink(config["database"], target=target, logger=logger)

    logger.info(f"Listening on topic '{config['kafka']['topic']}'...")
    for message in consumer:
        try:
            event = ChangeEvent.from_json(message.value)
            sink.write([event])
            logger.info(f"Persisted {event.event_type} event {event.event_id}")
        except Exception as e:
            logger.error(f"Failed to process message: {e}")


if __name__ == "__main__":
    db_target = sys.argv[1] if len(sys.argv) > 1 else "dev"
    main(target=db_target)
