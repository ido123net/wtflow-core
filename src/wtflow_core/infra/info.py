from __future__ import annotations

import datetime
import os
import platform
import socket
from dataclasses import dataclass, field
from typing import TYPE_CHECKING
from uuid import UUID, uuid4

if TYPE_CHECKING:
    from wtflow_core.infra.nodes import Node
    from wtflow_core.infra.workflow import Graph


@dataclass(frozen=True)
class SystemInfo:
    hostname: str = field(default_factory=socket.gethostname)
    os_name: str = field(default_factory=platform.system)
    os_release: str = field(default_factory=platform.release)
    os_version: str = field(default_factory=platform.version)
    machine: str = field(default_factory=platform.machine)
    cpu_count: int | None = field(default_factory=os.cpu_count)


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
    system_info: SystemInfo = field(default_factory=SystemInfo)


@dataclass(kw_only=True)
class ExecutionInfo(Info):
    graph: Graph
    node: Node
    execution_id: UUID = field(default_factory=uuid4)
