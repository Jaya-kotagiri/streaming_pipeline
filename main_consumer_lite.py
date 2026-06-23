"""
main_consumer_lite.py — streaming_pipeline
Jaya Kotagiri

Lightweight Kafka consumer for local development and unit testing.
Uses confluent-kafka directly (no Spark dependency) and commits offsets
manually only after a successful DB write — avoiding the auto-commit
data-loss window.

For production use the Spark consumer path:
    spark-submit src/consumer/spark_consumer.py --db production

Usage:
    python main_consumer_lite.py
    python main_consumer_lite.py --db dev
    python main_consumer_lite.py --db production
    python main_consumer_lite.py --config config/config.yaml --db dev
"""

import argparse
import signal
import sys

from src.utils.config_loader import load_config
from src.utils.logger import get_logger
from src.consumer.lite_consumer import JayaLiteConsumer


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Jaya streaming_pipeline — lite Kafka consumer"
    )
    parser.add_argument(
        "--config",
        default="config/config.yaml",
        help="Path to config YAML (default: config/config.yaml)",
    )
    parser.add_argument(
        "--db",
        choices=["dev", "production"],
        default="dev",
        help="DB target: dev (PostgreSQL) or production (Snowflake)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    cfg = load_config(args.config, env_prefix="JAYA_STREAM_")
    logger = get_logger("jaya.consumer.lite", cfg["logging"])

    db_target = "postgres" if args.db == "dev" else "snowflake"
    cfg["db"]["target"] = db_target

    logger.info("=== Jaya streaming_pipeline — lite consumer starting ===")
    logger.info("Kafka topic  : %s", cfg["kafka"]["topic"])
    logger.info("Consumer group: %s", cfg["consumer"]["group_id"])
    logger.info("DB target    : %s", db_target)
    logger.info("Offset commit: manual (after confirmed DB write)")

    consumer = JayaLiteConsumer(cfg, logger)

    # Graceful shutdown on SIGTERM (e.g. from docker-compose down)
    def _shutdown(signum, frame):
        logger.info("SIGTERM received — shutting down consumer.")
        consumer.stop()
        sys.exit(0)

    signal.signal(signal.SIGTERM, _shutdown)

    try:
        consumer.start()
    except KeyboardInterrupt:
        logger.info("Shutdown signal received.")
    finally:
        consumer.stop()
        logger.info("Consumer stopped cleanly.")
        sys.exit(0)


if __name__ == "__main__":
    main()
