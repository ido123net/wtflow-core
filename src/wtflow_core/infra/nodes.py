from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable

from wtflow_core.infra.artifact import Artifact


@dataclass(frozen=True)
class Node:
    name: str
    command: str | None = None
    timeout: float | None = None
    artifacts: tuple[Artifact, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class TreeNode(Node):
    children: Iterable[TreeNode] = field(default_factory=tuple, hash=False)
