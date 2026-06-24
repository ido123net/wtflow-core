from __future__ import annotations

import asyncio
import logging
import os
import signal
from enum import IntEnum
from graphlib import TopologicalSorter
from typing import TYPE_CHECKING

from wtflow_core.config import Config
from wtflow_core.infra.artifact import Artifact
from wtflow_core.infra.info import ExecutionInfo, RunInfo
from wtflow_core.services.servicer import Servicer

if TYPE_CHECKING:
    from wtflow_core.infra.nodes import Node
    from wtflow_core.infra.workflow import Graph
    from wtflow_core.services.storage.storage_service import StorageService

logger = logging.getLogger(__name__)


class ExitCode(IntEnum):
    SUCCESS = 0
    FAIL = 1


class NodeResult(IntEnum):
    SUCCESS = 0
    FAIL = 1
    TIMEOUT = 2
    CANCEL = 3


async def _wait_process(process: asyncio.subprocess.Process, timeout: float | None) -> NodeResult:
    try:
        result = await asyncio.wait_for(process.wait(), timeout)
        return NodeResult.FAIL if result else NodeResult.SUCCESS
    except asyncio.TimeoutError:
        return NodeResult.TIMEOUT
    except asyncio.CancelledError:
        return NodeResult.CANCEL
    finally:
        if process.returncode is None:
            os.killpg(os.getpgid(process.pid), signal.SIGTERM)
        await process.wait()


async def _start_process(command: str) -> asyncio.subprocess.Process:
    return await asyncio.create_subprocess_shell(
        command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        start_new_session=True,
    )


async def _read_stream(
    storage_service: StorageService,
    workflow: Graph,
    node: Node,
    stream: asyncio.StreamReader,
    artifact: Artifact,
) -> None:
    with storage_service.open_artifact(workflow, node, artifact) as f:
        while data := await stream.readline():
            f.write(data)


class Executor:
    def __init__(self, graph: Graph, servicer: Servicer) -> None:
        self.graph = graph
        self.servicer = servicer
        self.db_service = servicer.db_service
        self.run_info = RunInfo(graph=graph)

    async def execute(self) -> ExitCode:
        self.run_info.start()
        await self.db_service.start_run(self.run_info)
        ts = TopologicalSorter(self.graph)
        ts.prepare()
        while ts.is_active():
            ready_nodes = ts.get_ready()
            tasks = [asyncio.create_task(self.execute_node(node, ts)) for node in ready_nodes]
            for future_result in asyncio.as_completed(tasks):
                result = await future_result
                if result:
                    _cancel_tasks(tasks)
                    return ExitCode.FAIL
        self.run_info.end()
        await self.db_service.finish_run(self.run_info)
        return ExitCode.SUCCESS

    async def execute_node(self, node: Node, ts: TopologicalSorter[Node]) -> NodeResult:
        execution_info = ExecutionInfo(graph=self.graph, node=node)
        execution_info.start()
        await self.db_service.start_execution(self.run_info, execution_info)
        try:
            result = await self._execute_node(node)
            execution_info.end()
            await self.db_service.finish_execution(self.run_info, execution_info)
            return result
        finally:
            ts.done(node)

    async def _execute_node(self, node: Node) -> NodeResult:
        if not node.command:
            return NodeResult.SUCCESS

        process = await _start_process(node.command)
        stream_tasks = [self._stream_task(node, process, artifact_name) for artifact_name in ("stdout", "stderr")]
        result = await _wait_process(process, node.timeout)
        await asyncio.gather(*stream_tasks)
        return result

    def _stream_task(
        self,
        node: Node,
        process: asyncio.subprocess.Process,
        artifact_name: str,
    ) -> asyncio.Task[None]:
        artifact = Artifact(artifact_name)
        stream: asyncio.StreamReader = getattr(process, artifact_name)
        return asyncio.create_task(_read_stream(self.servicer.storage_service, self.graph, node, stream, artifact))


class Engine:
    def __init__(self, config: Config | None = None) -> None:
        self.config = config or Config()
        self.servicer = Servicer.from_config(self.config)

    async def run_graph(self, graph: Graph) -> int:
        await self.servicer.db_service.save_graph(graph)
        executor = Executor(graph, self.servicer)
        result = await executor.execute()
        return result


def _cancel_tasks(tasks: list[asyncio.Task[NodeResult]]) -> None:
    for task in tasks:
        task.cancel()
