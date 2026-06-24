from __future__ import annotations

from abc import abstractmethod
from typing import Any

from wtflow_core.graph import Graph
from wtflow_core.node import Node
from wtflow_core.services.base_service import BaseService


class DBService(BaseService):
    @abstractmethod
    async def save_run(self, graph: Graph) -> Any: ...

    @abstractmethod
    async def start_run(self, run_id: Any) -> None: ...

    @abstractmethod
    async def finish_run(self, run_id: Any) -> None: ...

    @abstractmethod
    async def start_execution(self, run_id: Any, node: Node) -> Any: ...

    @abstractmethod
    async def finish_execution(self, run_id: Any, execution_id: Any) -> None: ...


class NoDBService(DBService):
    async def save_run(self, graph: Graph) -> Any:
        return graph.name

    async def start_run(self, run_id: Any) -> None:
        pass

    async def finish_run(self, run_id: Any) -> None:
        pass

    async def start_execution(self, run_id: Any, node: Node) -> Any:
        return node.name

    async def finish_execution(self, run_id: Any, execution_id: Any) -> None:
        pass
