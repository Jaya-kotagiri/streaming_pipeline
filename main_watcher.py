"""
main_watcher.py — streaming_pipeline
Jaya Kotagiri

Entrypoint for the file watcher process.
Loads config, initialises the logger, and starts the watchdog observer
on the configured watch directory.

Usage:
    python main_watcher.py
    python main_watcher.py --config config/config.yaml

Environment overrides (prefix: JAYA_STREAM_):
    JAYA_STREAM_WATCH_DIR       directory to monitor
    JAYA_STREAM_KAFKA_BOOTSTRAP kafka broker address
    JAYA_STREAM_KAFKA_TOPIC     kafka topic name
"""

import argparse
import sys
import time

from src.utils.config_loader import load_config
from src.utils.logger import get_logger
from src.watcher.file_watcher import JayaFileWatcher


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Jaya streaming_pipeline — file watcher"
    )
    parser.add_argument(
        "--config",
        default="config/config.yaml",
        help="Path to config YAML (default: config/config.yaml)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    cfg = load_config(args.config, env_prefix="JAYA_STREAM_")
    logger = get_logger("jaya.watcher", cfg["logging"])

    logger.info("=== Jaya streaming_pipeline — watcher starting ===")
    logger.info("Watch dir : %s", cfg["watcher"]["watch_dir"])
    logger.info("Kafka     : %s → %s", cfg["kafka"]["bootstrap_servers"], cfg["kafka"]["topic"])
    logger.info("Snapshots : %s", cfg["watcher"]["snapshot_dir"])

    watcher = JayaFileWatcher(cfg, logger)

    try:
        watcher.start()
        logger.info("Watcher running. Press Ctrl+C to stop.")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutdown signal received.")
    finally:
        watcher.stop()
        logger.info("Watcher stopped cleanly.")
        sys.exit(0)


if __name__ == "__main__":
    main()
