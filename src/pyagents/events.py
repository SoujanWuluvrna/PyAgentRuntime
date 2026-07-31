"""Typed events and pluggable, scheduler-owned sinks."""

from __future__ import annotations

import threading
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter


class _Event(BaseModel):
    model_config = ConfigDict(frozen=True)
    sequence: int = Field(ge=0)
    run_id: str
    agent: str
    attempt: int = Field(ge=1)


class RunStarted(_Event):
    type: Literal["run_started"] = "run_started"


class RunFinished(_Event):
    type: Literal["run_finished"] = "run_finished"


class RunFailed(_Event):
    type: Literal["run_failed"] = "run_failed"
    error_type: str
    message: str


class RunRetried(_Event):
    type: Literal["run_retried"] = "run_retried"
    backoff_s: float


class RunTimedOut(_Event):
    type: Literal["run_timed_out"] = "run_timed_out"
    timeout_s: float


RunEvent = Annotated[
    RunStarted | RunFinished | RunFailed | RunRetried | RunTimedOut,
    Field(discriminator="type"),
]
EVENT_ADAPTER: TypeAdapter[RunEvent] = TypeAdapter(RunEvent)


class EventSink(ABC):
    @abstractmethod
    def emit(self, event: RunEvent) -> None: ...


class MemoryEventSink(EventSink):
    def __init__(self) -> None:
        self.events: list[RunEvent] = []

    def emit(self, event: RunEvent) -> None:
        self.events.append(EVENT_ADAPTER.validate_python(event))


class JsonlEventSink(EventSink):
    """Append-only JSONL. A lock protects callers in the same process."""

    def __init__(self, path: str | Path = "events.jsonl") -> None:
        self.path = Path(path)
        self._lock = threading.Lock()

    def emit(self, event: RunEvent) -> None:
        line = EVENT_ADAPTER.dump_json(event).decode() + "\n"
        with self._lock, self.path.open("a", encoding="utf-8") as handle:
            handle.write(line)
