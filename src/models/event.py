"""
ChangeEvent - the canonical record published to Kafka and persisted to the DB.

event_type is one of: INSERT, UPDATE, DELETE
line_number is 1-indexed and refers to position in the file at detection time.
"""

import json
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum


class EventType(str, Enum):
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"


@dataclass
class ChangeEvent:
    file_name: str
    event_type: str  # EventType value
    old_value: str = None
    new_value: str = None
    line_number: int = None
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    event_timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False)

    def to_dict(self) -> dict:
        return asdict(self)

    @staticmethod
    def from_json(payload: str) -> "ChangeEvent":
        data = json.loads(payload)
        return ChangeEvent(**data)
