from __future__ import annotations

import asyncio
import logging
from enum import IntEnum
from graphlib import TopologicalSorter
from typing import Any, Generator

from wtflow_core.config import Config
from wtflow_core.graph import Graph
from wtflow_core.node import Node, NodeExecutor, NodeResult, NodeState
from wtflow_core.services.servicer import Servicer

logger = logging.getLogger(__name__)


class ExitCode(IntEnum):
    SUCCESS = 0
    FAIL = 1


class Executor:
    def __init__(self, graph: Graph, servicer: Servicer) -> None:
        self.graph = graph
        self.db_service = servicer.db_service
        self.storage_service = servicer.storage_service
        self.ts = TopologicalSorter(self.graph)
        self.ts.prepare()

        self.failed_flag = False
        self._node_executors: dict[Node, NodeExecutor] = {}

    async def execute(self, run_id: Any) -> ExitCode:
        while self.ts.is_active() or not self.all_done():
            if not self.ts.is_active():
                logger.info(f"Still running: {[exe.node.name for exe in self.running_executors()]}")
            ready_nodes = self.ts.get_ready()
            for node in ready_nodes:
                self._node_executors[node] = NodeExecutor(node, run_id, self.db_service, self.storage_service)
            for executor in self.get_executors(NodeState.INIT):
                asyncio.create_task(self.execute_node(executor))
                executor.state = NodeState.START
            if self.failed_flag:
                for executor in self.running_executors():
                    executor.cancel()
            await asyncio.sleep(0.1)
        return ExitCode.SUCCESS if not self.failed_flag else ExitCode.FAIL

    async def execute_node(self, node_executor: NodeExecutor) -> None:
        logger.info(f"Execute node {node_executor.node.name}")
        executor, result = await node_executor.main()
        logger.info(f"Node: {node_executor.node.name} {result = }")
        self.ts.done(executor.node)
        if result.result is not NodeResult.SUCCESS:
            self.failed_flag = True

    def get_executors(self, state: NodeState) -> Generator[NodeExecutor, None, None]:
        return (exe for exe in self._node_executors.values() if exe.state is state)

    def running_executors(self) -> Generator[NodeExecutor, None, None]:
        return (exe for exe in self._node_executors.values() if exe.state is not NodeState.DONE)

    def all_done(self) -> bool:
        return set(self._node_executors.values()) == set(self.get_executors(NodeState.DONE))


class Engine:
    def __init__(self, config: Config | None = None) -> None:
        self.config = config or Config()
        self.servicer = Servicer.from_config(self.config)
        self.db_service = self.servicer.db_service

    async def save_run(self, graph: Graph) -> Any:
        return await self.db_service.save_run(graph)

    async def execute(self, graph: Graph, run_id: Any) -> int:
        executor = Executor(graph, self.servicer)
        await self.db_service.start_run(run_id)
        result = await executor.execute(run_id)
        await self.db_service.finish_run(run_id)
        return result

    async def run_graph(self, graph: Graph) -> int:
        run_id = await self.save_run(graph)
        result = await self.execute(graph, run_id)
        return result
