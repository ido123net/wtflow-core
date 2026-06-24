from __future__ import annotations

from typing import Iterable

from wtflow_core.artifact import Artifact
from wtflow_core.graph import Graph
from wtflow_core.node import Node


class TreeNode:
    def __init__(
        self,
        name: str,
        command: str | None = None,
        timeout: float | None = None,
        artifacts: tuple[Artifact, ...] | None = None,
        children: Iterable[TreeNode] | None = None,
    ) -> None:
        self.name = name
        self.command = command
        self.timeout = timeout
        self.artifacts = artifacts or ()
        self.children = children or []

    def as_node(self) -> Node:
        return Node(
            name=self.name,
            command=self.command,
            timeout=self.timeout,
            artifacts=self.artifacts,
        )


class Tree:
    def __init__(self, name: str, root: TreeNode) -> None:
        self.name = name
        self.root = root

    def as_graph(self) -> Graph:
        nodes = []
        edges = []

        def _add_node(node: TreeNode, parent: TreeNode | None = None) -> None:
            nodes.append(node.as_node())
            if parent is not None:
                edges.append((node.as_node(), parent.as_node()))
            for child in node.children:
                _add_node(child, node)

        _add_node(self.root)
        return Graph(self.name, nodes=tuple(nodes), edges=tuple(edges))
