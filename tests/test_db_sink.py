import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.db.db_sink import SQLiteSink
from src.models.event import ChangeEvent, EventType


def make_event(event_id="evt-1", **overrides):
    defaults = dict(
        event_id=event_id,
        file_name="sample.txt",
        event_type=EventType.UPDATE.value,
        old_value="old",
        new_value="new",
        line_number=1,
    )
    defaults.update(overrides)
    return ChangeEvent(**defaults)


def test_write_and_fetch():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"
        sink = SQLiteSink(str(db_path))
        sink.write([make_event()])
        rows = sink.fetch_recent()
        assert len(rows) == 1
        assert rows[0]["event_id"] == "evt-1"
        assert rows[0]["old_value"] == "old"
        assert rows[0]["new_value"] == "new"


def test_duplicate_event_id_is_ignored():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"
        sink = SQLiteSink(str(db_path))
        sink.write([make_event(event_id="dup-1")])
        sink.write([make_event(event_id="dup-1", new_value="should-not-overwrite")])
        rows = sink.fetch_recent()
        assert len(rows) == 1
        assert rows[0]["new_value"] == "new"  # original value preserved


def test_empty_batch_is_noop():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"
        sink = SQLiteSink(str(db_path))
        sink.write([])
        assert sink.fetch_recent() == []
