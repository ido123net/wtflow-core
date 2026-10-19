from __future__ import annotations

import datetime
import os
import platform
import socket
from dataclasses import dataclass, field
from types import TracebackType
from typing import TYPE_CHECKING, Self
from uuid import UUID, uuid4

if TYPE_CHECKING:
    from wtflow_core.engine import NodeResult
    from wtflow_core.graph import Graph
    from wtflow_core.node import Node


@dataclass(frozen=True)
class SystemInfo:
    hostname: str = field(default_factory=socket.gethostname)
    os_name: str = field(default_factory=platform.system)
    os_release: str = field(default_factory=platform.release)
    os_version: str = field(default_factory=platform.version)
    machine: str = field(default_factory=platform.machine)
    cpu_count: int | None = field(default_factory=os.cpu_count)


def _utcnow() -> datetime.datetime:
    return datetime.datetime.now(tz=datetime.UTC)


@dataclass(kw_only=True)
class Info:
    start_time: datetime.datetime | None = None
    end_time: datetime.datetime | None = None

    def start(self) -> None:
        self.start_time = _utcnow()

    def end(self) -> None:
        self.end_time = _utcnow()

    def __enter__(self) -> Self:
        self.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.end()


@dataclass(kw_only=True)
class RunInfo(Info):
    graph: Graph
    run_id: UUID = field(default_factory=uuid4)
    created_at: datetime.datetime = field(default_factory=_utcnow)
    system_info: SystemInfo = field(default_factory=SystemInfo)


@dataclass(kw_only=True)
class ExecutionInfo(Info):
    run_info: RunInfo
    node: Node
    retcode: int | None = None
    result: NodeResult | None = None
    execution_id: UUID = field(default_factory=uuid4)
