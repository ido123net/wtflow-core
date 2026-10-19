from __future__ import annotations

from abc import abstractmethod

from wtflow_core.graph import Graph
from wtflow_core.info import ExecutionInfo, RunInfo
from wtflow_core.services.base_service import BaseService


class DBService(BaseService):
    @abstractmethod
    async def save_graph(self, graph: Graph) -> None:
        raise NotImplementedError

    @abstractmethod
    async def start_run(self, run_info: RunInfo) -> None:
        raise NotImplementedError

    @abstractmethod
    async def finish_run(self, run_info: RunInfo) -> None:
        raise NotImplementedError

    @abstractmethod
    async def start_execution(self, execution_info: ExecutionInfo) -> None:
        raise NotImplementedError

    @abstractmethod
    async def finish_execution(self, execution_info: ExecutionInfo) -> None:
        raise NotImplementedError


class NoDBService(DBService):
    async def save_graph(self, graph: Graph) -> None:
        pass

    async def start_run(self, run_info: RunInfo) -> None:
        pass

    async def finish_run(self, run_info: RunInfo) -> None:
        pass

    async def start_execution(self, execution_info: ExecutionInfo) -> None:
        pass

    async def finish_execution(self, execution_info: ExecutionInfo) -> None:
        pass
