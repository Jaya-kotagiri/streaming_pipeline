"""
Entrypoint: starts the file watcher, diffing detected changes and publishing
them to Kafka as they occur.

Run:
    python main_watcher.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from src.producer.kafka_producer import KafkaEventProducer
from src.utils.config_loader import load_config
from src.utils.logger import get_logger
from src.watcher.file_watcher import FileWatcher


def main():
    config = load_config()
    logger = get_logger("file_watcher", config.get("logging"))

    producer = KafkaEventProducer(config["kafka"], logger=logger)

    def on_events(events):
        producer.publish(events)

    watcher = FileWatcher(
        watch_path=config["watcher"]["watch_path"],
        snapshot_dir=config["watcher"]["snapshot_dir"],
        on_events=on_events,
        poll_interval_seconds=config["watcher"]["poll_interval_seconds"],
        encoding=config["watcher"]["encoding"],
        logger=logger,
    )

    try:
        watcher.start(blocking=True)
    finally:
        producer.close()


if __name__ == "__main__":
    main()
