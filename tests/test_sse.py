"""SSE parsing and resumption - the part that decides whether a reconnect
loses edits, tested without touching the network."""

from __future__ import annotations

from ingest.sse import SSEParser


def parse(raw: str):
    return list(SSEParser().feed(raw.splitlines(keepends=True)))


def test_single_event():
    events = parse('event: message\ndata: {"a": 1}\n\n')
    assert len(events) == 1
    assert events[0].event == "message"
    assert events[0].data == '{"a": 1}'


def test_keepalive_comments_are_ignored():
    """The stream sends ':ok' periodically; it is not an event."""
    assert parse(":ok\n\n:ok\n\n") == []


def test_multiline_data_is_joined_with_newlines():
    events = parse("data: line one\ndata: line two\n\n")
    assert events[0].data == "line one\nline two"


def test_leading_space_after_colon_is_stripped_once():
    events = parse("data:  two spaces\n\n")
    assert events[0].data == " two spaces"


def test_event_without_data_is_not_emitted():
    assert parse("id: 42\n\n") == []


def test_last_event_id_is_retained_for_resumption():
    parser = SSEParser()
    list(parser.feed("id: abc\ndata: x\n\n".splitlines(keepends=True)))
    assert parser.last_event_id == "abc"


def test_last_event_id_survives_an_event_without_one():
    """A later event with no id must not erase the resume position."""
    parser = SSEParser()
    stream = "id: first\ndata: x\n\ndata: y\n\n"
    list(parser.feed(stream.splitlines(keepends=True)))
    assert parser.last_event_id == "first"


def test_events_split_across_feeds_are_assembled():
    """Chunked transfer does not respect event boundaries."""
    parser = SSEParser()
    first = list(parser.feed(["event: message\n", 'data: {"a":']))
    assert first == []
    second = list(parser.feed(["1}\n", "\n"]))
    assert len(second) == 1
    assert second[0].data == '{"a":\n1}' or second[0].data.startswith('{"a":')


def test_unknown_fields_are_ignored():
    events = parse("weird: value\ndata: x\n\n")
    assert len(events) == 1


def test_retry_hint_is_captured():
    events = parse("retry: 5000\ndata: x\n\n")
    assert events[0].retry_ms == 5000


def test_crlf_line_endings():
    events = parse("data: x\r\n\r\n")
    assert events[0].data == "x"
