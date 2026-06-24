from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field

from wtflow_core.infra.nodes import Node, TreeNode


@dataclass(frozen=True)
class Graph:
    name: str
    nodes: tuple[Node, ...] = field(default_factory=tuple)
    edges: tuple[tuple[Node, Node], ...] = field(default_factory=tuple)

    def items(self) -> set[tuple[Node, tuple[Node, ...]]]:
        predecessors = defaultdict[Node, set[Node]](set)
        for parent, child in self.edges:
            predecessors[child].add(parent)
        return {(node, tuple(predecessors[node])) for node in self.nodes}


@dataclass(frozen=True)
class Tree:
    name: str
    root: TreeNode

    def as_graph(self) -> Graph:
        nodes = set()
        edges = set()

        def _add_node(node: TreeNode, parent: TreeNode | None = None) -> None:
            nodes.add(node)
            if parent is not None:
                edges.add((node, parent))
            for child in node.children:
                _add_node(child, node)

        _add_node(self.root)
        return Graph(self.name, nodes=tuple(nodes), edges=tuple(edges))
