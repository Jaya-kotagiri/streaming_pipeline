"""
DiffEngine - compares the previous snapshot of a file against its current
content and emits a list of ChangeEvent objects describing what changed.

Why line-level diffing instead of just "file changed"?
A text editor like Notepad rewrites the entire file on save, so a raw
filesystem event only tells you THAT something changed, not WHAT. We diff
the old and new line lists with difflib.SequenceMatcher (same algorithm
backing `diff`/git's line diff) to recover INSERT / UPDATE / DELETE
semantics at the line level.

Matching strategy:
- 'replace' opcode of equal-length spans -> paired as UPDATE events
  (line N changed from old text to new text)
- 'replace' opcode of unequal length, 'insert' opcode -> INSERT events
- 'delete' opcode -> DELETE events
"""

from difflib import SequenceMatcher
from typing import List

from src.models.event import ChangeEvent, EventType


class DiffEngine:
    def __init__(self, file_name: str):
        self.file_name = file_name

    def diff(self, old_lines: List[str], new_lines: List[str]) -> List[ChangeEvent]:
        """Return ChangeEvents transforming old_lines -> new_lines."""
        events: List[ChangeEvent] = []
        matcher = SequenceMatcher(a=old_lines, b=new_lines, autojunk=False)

        for tag, i1, i2, j1, j2 in matcher.get_opcodes():
            if tag == "equal":
                continue

            old_chunk = old_lines[i1:i2]
            new_chunk = new_lines[j1:j2]

            if tag == "replace":
                # Pair off line-by-line as updates; leftover lines become
                # inserts (new longer) or deletes (old longer)
                paired = min(len(old_chunk), len(new_chunk))
                for k in range(paired):
                    events.append(
                        ChangeEvent(
                            file_name=self.file_name,
                            event_type=EventType.UPDATE.value,
                            old_value=old_chunk[k],
                            new_value=new_chunk[k],
                            line_number=j1 + k + 1,
                        )
                    )
                for k in range(paired, len(old_chunk)):
                    events.append(
                        ChangeEvent(
                            file_name=self.file_name,
                            event_type=EventType.DELETE.value,
                            old_value=old_chunk[k],
                            new_value=None,
                            line_number=i1 + k + 1,
                        )
                    )
                for k in range(paired, len(new_chunk)):
                    events.append(
                        ChangeEvent(
                            file_name=self.file_name,
                            event_type=EventType.INSERT.value,
                            old_value=None,
                            new_value=new_chunk[k],
                            line_number=j1 + k + 1,
                        )
                    )

            elif tag == "insert":
                for k, line in enumerate(new_chunk):
                    events.append(
                        ChangeEvent(
                            file_name=self.file_name,
                            event_type=EventType.INSERT.value,
                            old_value=None,
                            new_value=line,
                            line_number=j1 + k + 1,
                        )
                    )

            elif tag == "delete":
                for k, line in enumerate(old_chunk):
                    events.append(
                        ChangeEvent(
                            file_name=self.file_name,
                            event_type=EventType.DELETE.value,
                            old_value=line,
                            new_value=None,
                            line_number=i1 + k + 1,
                        )
                    )

        return events
