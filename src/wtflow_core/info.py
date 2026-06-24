from __future__ import annotations

import datetime
from dataclasses import dataclass, field
from typing import TYPE_CHECKING
from uuid import UUID, uuid4

if TYPE_CHECKING:
    from wtflow_core.graph import Graph
    from wtflow_core.node import Node


def _utcnow() -> datetime.datetime:
    return datetime.datetime.now(tz=datetime.timezone.utc)


@dataclass(kw_only=True)
class Info:
    start_time: datetime.datetime | None = None
    end_time: datetime.datetime | None = None

    def start(self) -> None:
        self.start_time = _utcnow()

    def end(self) -> None:
        self.end_time = _utcnow()


@dataclass(kw_only=True)
class RunInfo(Info):
    graph: Graph
    run_id: UUID = field(default_factory=uuid4)
    created_at: datetime.datetime = field(default_factory=_utcnow)


@dataclass(kw_only=True)
class ExecutionInfo(Info):
    graph: Graph
    node: Node
    execution_id: UUID = field(default_factory=uuid4)
