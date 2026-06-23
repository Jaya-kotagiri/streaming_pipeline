"""
FileWatcher - monitors a single text file for changes using `watchdog`,
debounces rapid-fire OS events (editors often fire multiple events per save),
diffs against the last known snapshot, and hands resulting ChangeEvents to a
publisher callback (e.g. the Kafka producer).

A snapshot of the file's last-known content is persisted to disk so the
watcher can resume correctly after a restart without re-emitting stale events.
"""

import os
import threading
import time
from pathlib import Path
from typing import Callable, List

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from src.models.event import ChangeEvent
from src.watcher.diff_engine import DiffEngine


class _DebouncedHandler(FileSystemEventHandler):
    """Coalesces bursts of filesystem events for the watched file into a
    single callback, fired after `debounce_seconds` of quiet."""

    def __init__(self, watch_path: Path, debounce_seconds: float, on_change: Callable[[], None]):
        super().__init__()
        self.watch_path = watch_path.resolve()
        self.debounce_seconds = debounce_seconds
        self.on_change = on_change
        self._timer = None
        self._lock = threading.Lock()

    def _matches(self, event) -> bool:
        if event.is_directory:
            return False
        try:
            return Path(event.src_path).resolve() == self.watch_path
        except OSError:
            return False

    def _schedule(self):
        with self._lock:
            if self._timer is not None:
                self._timer.cancel()
            self._timer = threading.Timer(self.debounce_seconds, self.on_change)
            self._timer.daemon = True
            self._timer.start()

    def on_modified(self, event):
        if self._matches(event):
            self._schedule()

    def on_created(self, event):
        if self._matches(event):
            self._schedule()


class FileWatcher:
    def __init__(
        self,
        watch_path: str,
        snapshot_dir: str,
        on_events: Callable[[List[ChangeEvent]], None],
        poll_interval_seconds: float = 0.5,
        encoding: str = "utf-8",
        logger=None,
    ):
        self.watch_path = Path(watch_path)
        self.snapshot_path = Path(snapshot_dir) / (self.watch_path.name + ".snapshot")
        self.on_events = on_events
        self.poll_interval_seconds = poll_interval_seconds
        self.encoding = encoding
        self.logger = logger
        self.diff_engine = DiffEngine(file_name=self.watch_path.name)

        os.makedirs(self.watch_path.parent, exist_ok=True)
        os.makedirs(Path(snapshot_dir), exist_ok=True)
        if not self.watch_path.exists():
            self.watch_path.touch()

        self._last_lines = self._load_snapshot()
        self._observer = Observer()

    # ---- snapshot persistence -------------------------------------------------

    def _load_snapshot(self) -> List[str]:
        if self.snapshot_path.exists():
            with open(self.snapshot_path, "r", encoding=self.encoding) as f:
                return f.read().splitlines()
        # No snapshot yet: treat current file content as the baseline so we
        # don't replay the whole file as INSERT events on first run.
        return self._read_current()

    def _save_snapshot(self, lines: List[str]):
        with open(self.snapshot_path, "w", encoding=self.encoding) as f:
            f.write("\n".join(lines))

    def _read_current(self) -> List[str]:
        try:
            with open(self.watch_path, "r", encoding=self.encoding) as f:
                return f.read().splitlines()
        except FileNotFoundError:
            return []

    # ---- change handling --------------------------------------------------

    def _handle_change(self):
        new_lines = self._read_current()
        if new_lines == self._last_lines:
            return  # no semantic change (e.g. touch with same content)

        events = self.diff_engine.diff(self._last_lines, new_lines)
        if events:
            if self.logger:
                self.logger.info(
                    f"Detected {len(events)} change(s) in {self.watch_path.name}"
                )
            self.on_events(events)

        self._last_lines = new_lines
        self._save_snapshot(new_lines)

    # ---- lifecycle ----------------------------------------------------------

    def start(self, blocking: bool = True):
        handler = _DebouncedHandler(
            self.watch_path, self.poll_interval_seconds, self._handle_change
        )
        self._observer.schedule(handler, str(self.watch_path.parent), recursive=False)
        self._observer.start()
        if self.logger:
            self.logger.info(f"Watching {self.watch_path} for changes...")

        if blocking:
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                self.stop()

    def stop(self):
        self._observer.stop()
        self._observer.join()
        if self.logger:
            self.logger.info("File watcher stopped.")
