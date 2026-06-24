from __future__ import annotations

from dataclasses import dataclass, field

from wtflow_core.artifact import Artifact


@dataclass(frozen=True)
class Node:
    name: str
    command: str | None = None
    timeout: float | None = None
    artifacts: tuple[Artifact, ...] = field(default_factory=tuple)
