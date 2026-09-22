"""A minimal Server-Sent Events parser.

Written by hand rather than pulled from a library for two reasons: the wire
format is small enough that a dependency is not worth it, and resumption
depends on tracking the `id:` field, which is the part that actually matters
here. A dropped connection is normal on a stream that runs for days - the
point is to come back at the right place, not to avoid disconnecting.

The parser is deliberately decoupled from the network so it can be tested
against byte strings without touching Wikimedia.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field


@dataclass
class Event:
    event: str = "message"
    data: str = ""
    id: str | None = None
    retry_ms: int | None = None

    @property
    def is_empty(self) -> bool:
        return not self.data


@dataclass
class SSEParser:
    """Feed it lines, get complete events back.

    Holds the last seen id so a reconnect can send `Last-Event-ID`.
    """

    last_event_id: str | None = None
    _buffer: Event = field(default_factory=Event)
    _data_lines: list[str] = field(default_factory=list)

    def feed(self, lines: Iterable[str]) -> Iterator[Event]:
        for raw in lines:
            line = raw.rstrip("\n").rstrip("\r")

            # A blank line dispatches whatever has accumulated.
            if line == "":
                event = self._flush()
                if event is not None:
                    yield event
                continue

            # Comment lines (the stream sends ":ok" as a keepalive).
            if line.startswith(":"):
                continue

            name, _, value = line.partition(":")
            value = value.removeprefix(" ")

            if name == "data":
                self._data_lines.append(value)
            elif name == "event":
                self._buffer.event = value
            elif name == "id":
                self._buffer.id = value
            elif name == "retry":
                try:
                    self._buffer.retry_ms = int(value)
                except ValueError:
                    pass
            # Unknown fields are ignored, per the SSE spec.

    def _flush(self) -> Event | None:
        if not self._data_lines and self._buffer.id is None:
            self._buffer = Event()
            return None

        event = Event(
            event=self._buffer.event,
            data="\n".join(self._data_lines),
            id=self._buffer.id,
            retry_ms=self._buffer.retry_ms,
        )
        if event.id:
            self.last_event_id = event.id

        self._buffer = Event()
        self._data_lines = []
        return None if event.is_empty else event
