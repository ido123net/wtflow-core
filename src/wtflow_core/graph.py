from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field

from wtflow_core.node import Node


@dataclass(frozen=True)
class Graph:
    name: str
    nodes: tuple[Node, ...] = field(default_factory=tuple)
    edges: tuple[tuple[Node, Node], ...] = field(default_factory=tuple)

    def items(self) -> set[tuple[Node, tuple[Node, ...]]]:
        predecessors = defaultdict[Node, list[Node]](list)
        for child, parent in self.edges:
            predecessors[child].append(parent)
        return {(node, tuple(predecessors[node])) for node in self.nodes}
