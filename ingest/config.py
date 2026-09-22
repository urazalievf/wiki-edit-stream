"""Configuration loading, shared by the ingest service and the Flink jobs."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = REPO_ROOT / "conf" / "pipeline.yaml"


@dataclass(frozen=True)
class Config:
    source: dict[str, Any]
    kafka: dict[str, Any]
    topics: dict[str, Any]
    flink: dict[str, Any]
    raw: dict[str, Any] = field(default_factory=dict, repr=False)

    @property
    def bootstrap(self) -> str:
        return self.kafka["bootstrap_servers"]

    def topic(self, name: str) -> str:
        return self.topics[name]

    def offset_file(self) -> Path:
        value = Path(self.source["offset_file"])
        return value if value.is_absolute() else REPO_ROOT / value

    def producer_config(self) -> dict[str, Any]:
        return {
            "bootstrap.servers": self.bootstrap,
            **{key: value for key, value in self.kafka.get("producer", {}).items()},
        }


def load_config(path: str | os.PathLike[str] | None = None) -> Config:
    with open(Path(path) if path else DEFAULT_CONFIG) as handle:
        raw = yaml.safe_load(handle)

    kafka = dict(raw["kafka"])
    if override := os.getenv("WES_BOOTSTRAP_SERVERS"):
        kafka["bootstrap_servers"] = override

    source = dict(raw["source"])
    if override := os.getenv("WES_SOURCE_URL"):
        source["url"] = override

    topics = dict(raw["topics"])
    if prefix := os.getenv("WES_TOPIC_PREFIX"):
        # Lets a test or a second environment run against isolated topics.
        topics = {
            key: (f"{prefix}{value}" if isinstance(value, str) else value)
            for key, value in topics.items()
        }

    return Config(
        source=source, kafka=kafka, topics=topics, flink=raw["flink"], raw=raw
    )
