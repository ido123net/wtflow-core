from __future__ import annotations

import asyncio
import logging
import os
import signal
from enum import IntEnum
from graphlib import TopologicalSorter
from typing import TYPE_CHECKING, Any

from wtflow_core.artifact import Artifact
from wtflow_core.config import Config
from wtflow_core.services.servicer import Servicer

if TYPE_CHECKING:
    from wtflow_core.graph import Graph
    from wtflow_core.node import Node
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
    run_id: Any,
    execution_id: Any,
    stream: asyncio.StreamReader,
    artifact: Artifact,
) -> None:
    with storage_service.open_artifact(run_id, execution_id, artifact) as f:
        while data := await stream.readline():
            f.write(data)


class Executor:
    def __init__(self, graph: Graph, servicer: Servicer) -> None:
        self.graph = graph
        self.servicer = servicer
        self.db_service = servicer.db_service

    async def execute(self, run_id: Any) -> ExitCode:
        ts = TopologicalSorter(self.graph)
        ts.prepare()
        while ts.is_active():
            ready_nodes = ts.get_ready()
            tasks = [asyncio.create_task(self.execute_node(run_id, node, ts)) for node in ready_nodes]
            try:
                await asyncio.gather(*tasks)
            except Exception:
                return ExitCode.FAIL
        return ExitCode.SUCCESS

    async def execute_node(self, run_id: Any, node: Node, ts: TopologicalSorter[Node]) -> NodeResult:
        execution_id = await self.db_service.start_execution(run_id, node)
        try:
            result = await self._execute_node(node, run_id, execution_id)
            await self.db_service.finish_execution(run_id, execution_id)
            if result:
                raise Exception
            return result
        finally:
            ts.done(node)

    async def _execute_node(self, node: Node, run_id: Any, execution_id: Any) -> NodeResult:
        if not node.command:
            return NodeResult.SUCCESS

        process = await _start_process(node.command)
        stream_tasks = [
            self._stream_task(process, artifact_name, run_id, execution_id) for artifact_name in ("stdout", "stderr")
        ]
        result = await _wait_process(process, node.timeout)
        await asyncio.gather(*stream_tasks)
        return result

    def _stream_task(
        self,
        process: asyncio.subprocess.Process,
        artifact_name: str,
        run_id: Any,
        execution_id: Any,
    ) -> asyncio.Task[None]:
        artifact = Artifact(artifact_name)
        stream: asyncio.StreamReader = getattr(process, artifact_name)
        return asyncio.create_task(_read_stream(self.servicer.storage_service, run_id, execution_id, stream, artifact))


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
