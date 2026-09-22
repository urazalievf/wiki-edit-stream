"""Wikimedia recentchange -> Kafka.

Responsibilities, in order of how much they matter:

1. **Do not lose position.** The stream is unbounded and disconnects are
   routine. The last SSE event id is persisted, and a reconnect sends it as
   `Last-Event-ID` so the server resumes where we stopped instead of at "now".
2. **Do not duplicate.** The producer is idempotent with acks=all, so a retry
   after a blip does not append the record twice.
3. **Keep ordering where it is meaningful.** Records are keyed by wiki, so
   every edit for a given wiki lands on one partition and stays ordered.
4. **Never block the reader.** Delivery is asynchronous; failures surface
   through the delivery callback and are counted, not swallowed.

Bronze-style fidelity: the payload is forwarded byte-for-byte. Parsing and
validation belong in Flink, where a bad record can be routed to a dead-letter
topic rather than crashing ingestion.
"""

from __future__ import annotations

import argparse
import json
import logging
import signal
import sys
import time
from dataclasses import dataclass, field
from types import FrameType

import requests
from confluent_kafka import KafkaException, Producer

from .config import Config, load_config
from .sse import SSEParser

log = logging.getLogger("ingest")


@dataclass
class Stats:
    received: int = 0
    produced: int = 0
    failed: int = 0
    skipped: int = 0
    reconnects: int = 0
    bytes_in: int = 0
    started: float = field(default_factory=time.monotonic)

    def rate(self) -> float:
        elapsed = time.monotonic() - self.started
        return self.received / elapsed if elapsed > 0 else 0.0

    def render(self) -> str:
        return (
            f"received={self.received} produced={self.produced} failed={self.failed} "
            f"skipped={self.skipped} reconnects={self.reconnects} "
            f"{self.rate():.1f} ev/s {self.bytes_in / 1e6:.1f} MB"
        )


class OffsetStore:
    """Persists the last SSE event id so a restart resumes, not skips."""

    def __init__(self, config: Config) -> None:
        self.path = config.offset_file()

    def read(self) -> str | None:
        if not self.path.exists():
            return None
        try:
            return json.loads(self.path.read_text()).get("last_event_id")
        except (json.JSONDecodeError, OSError):
            log.warning("offset file at %s is unreadable; starting from now", self.path)
            return None

    def write(self, event_id: str | None) -> None:
        if not event_id:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps({"last_event_id": event_id}))


class Ingestor:
    def __init__(
        self,
        config: Config,
        max_events: int | None = None,
        duration_s: float | None = None,
        wikis: set[str] | None = None,
    ) -> None:
        self.config = config
        self.max_events = max_events
        self.duration_s = duration_s
        self.wikis = wikis
        self.stats = Stats()
        self.offsets = OffsetStore(config)
        self.parser = SSEParser(last_event_id=self.offsets.read())
        self.producer = Producer(config.producer_config())
        self.topic = config.topic("raw")
        self._stop = False

    # -- lifecycle ---------------------------------------------------------
    def request_stop(self, signum: int, _frame: FrameType | None) -> None:
        log.info("signal %s received; draining", signal.Signals(signum).name)
        self._stop = True

    def _should_stop(self) -> bool:
        if self._stop:
            return True
        if self.max_events is not None and self.stats.received >= self.max_events:
            return True
        if self.duration_s is not None:
            return (time.monotonic() - self.stats.started) >= self.duration_s
        return False

    # -- kafka -------------------------------------------------------------
    def _on_delivery(self, err: KafkaException | None, msg: object) -> None:
        if err is not None:
            self.stats.failed += 1
            log.error("delivery failed: %s", err)
        else:
            self.stats.produced += 1

    def _publish(self, payload: str) -> None:
        try:
            event = json.loads(payload)
        except json.JSONDecodeError:
            # Malformed at the transport level; nothing downstream could key it.
            self.stats.skipped += 1
            return

        wiki = event.get("wiki") or event.get("meta", {}).get("domain")
        if self.wikis and wiki not in self.wikis:
            self.stats.skipped += 1
            return

        try:
            self.producer.produce(
                topic=self.topic,
                key=(wiki or "unknown").encode(),
                value=payload.encode(),
                on_delivery=self._on_delivery,
            )
        except BufferError:
            # The local queue is full: the broker is slower than the stream.
            # Block until it drains rather than dropping edits.
            log.warning("producer queue full; applying backpressure")
            self.producer.flush(10)
            self.producer.produce(
                topic=self.topic,
                key=(wiki or "unknown").encode(),
                value=payload.encode(),
                on_delivery=self._on_delivery,
            )
        self.producer.poll(0)

    # -- stream ------------------------------------------------------------
    def _connect(self) -> requests.Response:
        headers = {
            "User-Agent": self.config.source["user_agent"],
            "Accept": "text/event-stream",
        }
        if self.parser.last_event_id:
            headers["Last-Event-ID"] = self.parser.last_event_id
            log.info("resuming from stored offset")
        else:
            log.info("no stored offset; starting from the live edge")

        response = requests.get(
            self.config.source["url"],
            headers=headers,
            stream=True,
            timeout=(
                self.config.source["connect_timeout_s"],
                self.config.source["read_timeout_s"],
            ),
        )
        response.raise_for_status()
        return response

    def run(self) -> Stats:
        backoff = float(self.config.source["backoff_initial_s"])
        backoff_max = float(self.config.source["backoff_max_s"])
        last_report = time.monotonic()

        while not self._should_stop():
            try:
                with self._connect() as response:
                    backoff = float(self.config.source["backoff_initial_s"])
                    for event in self.parser.feed(
                        response.iter_lines(decode_unicode=True)
                    ):
                        self.stats.received += 1
                        self.stats.bytes_in += len(event.data)
                        self._publish(event.data)

                        if time.monotonic() - last_report >= 5:
                            log.info(self.stats.render())
                            self.offsets.write(self.parser.last_event_id)
                            last_report = time.monotonic()

                        if self._should_stop():
                            break
            except (requests.RequestException, ConnectionError) as exc:
                if self._should_stop():
                    break
                self.stats.reconnects += 1
                log.warning("stream dropped (%s); reconnecting in %.0fs", exc, backoff)
                time.sleep(backoff)
                backoff = min(backoff * 2, backoff_max)

        log.info("draining producer")
        outstanding = self.producer.flush(30)
        if outstanding:
            log.error("%d message(s) never acknowledged", outstanding)
        self.offsets.write(self.parser.last_event_id)
        log.info("final: %s", self.stats.render())
        return self.stats


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ingest", description="Stream Wikimedia edits into Kafka"
    )
    parser.add_argument("--max-events", type=int, help="stop after N events")
    parser.add_argument("--duration", type=float, help="stop after N seconds")
    parser.add_argument(
        "--wikis", help="comma-separated wikis to keep, e.g. enwiki,dewiki"
    )
    parser.add_argument("--log-level", default="INFO")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
        stream=sys.stdout,
    )

    ingestor = Ingestor(
        load_config(),
        max_events=args.max_events,
        duration_s=args.duration,
        wikis=set(args.wikis.split(",")) if args.wikis else None,
    )
    signal.signal(signal.SIGINT, ingestor.request_stop)
    signal.signal(signal.SIGTERM, ingestor.request_stop)

    stats = ingestor.run()
    return 1 if stats.failed else 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
