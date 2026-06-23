import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.models.event import EventType
from src.watcher.diff_engine import DiffEngine


def make_engine():
    return DiffEngine(file_name="sample.txt")


def test_no_change_produces_no_events():
    engine = make_engine()
    lines = ["a", "b", "c"]
    events = engine.diff(lines, lines)
    assert events == []


def test_pure_insert():
    engine = make_engine()
    old = ["a", "b"]
    new = ["a", "b", "c"]
    events = engine.diff(old, new)
    assert len(events) == 1
    assert events[0].event_type == EventType.INSERT.value
    assert events[0].new_value == "c"
    assert events[0].old_value is None
    assert events[0].line_number == 3


def test_pure_delete():
    engine = make_engine()
    old = ["a", "b", "c"]
    new = ["a", "c"]
    events = engine.diff(old, new)
    assert len(events) == 1
    assert events[0].event_type == EventType.DELETE.value
    assert events[0].old_value == "b"
    assert events[0].new_value is None


def test_pure_update():
    engine = make_engine()
    old = ["order_id=1,status=PENDING"]
    new = ["order_id=1,status=SHIPPED"]
    events = engine.diff(old, new)
    assert len(events) == 1
    assert events[0].event_type == EventType.UPDATE.value
    assert events[0].old_value == "order_id=1,status=PENDING"
    assert events[0].new_value == "order_id=1,status=SHIPPED"


def test_mixed_replace_unequal_length_emits_update_and_insert():
    engine = make_engine()
    old = ["x=1"]
    new = ["x=1-changed", "y=new-line"]
    events = engine.diff(old, new)
    types = sorted(e.event_type for e in events)
    assert types == sorted([EventType.UPDATE.value, EventType.INSERT.value])


def test_mixed_replace_unequal_length_emits_update_and_delete():
    engine = make_engine()
    old = ["x=1", "extra-line"]
    new = ["x=1-changed"]
    events = engine.diff(old, new)
    types = sorted(e.event_type for e in events)
    assert types == sorted([EventType.UPDATE.value, EventType.DELETE.value])


def test_multiple_independent_changes():
    engine = make_engine()
    old = ["a", "b", "c", "d"]
    new = ["a", "B", "c", "d", "e"]
    events = engine.diff(old, new)
    event_types = {e.event_type for e in events}
    assert EventType.UPDATE.value in event_types
    assert EventType.INSERT.value in event_types
    update_events = [e for e in events if e.event_type == EventType.UPDATE.value]
    assert update_events[0].old_value == "b"
    assert update_events[0].new_value == "B"
