from __future__ import annotations

import asyncio
import logging
import os
import signal
from enum import IntEnum
from graphlib import TopologicalSorter
from types import TracebackType
from typing import Self

from wtflow_core.artifact import Artifact
from wtflow_core.config import Config
from wtflow_core.graph import Graph
from wtflow_core.info import ExecutionInfo, RunInfo
from wtflow_core.node import Node
from wtflow_core.services.db.db_service import DBService
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


async def _wait_process(process: asyncio.subprocess.Process) -> int:
    try:
        return await process.wait()
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
    def __init__(self, graph: Graph, db_service: DBService, storage_service: StorageService) -> None:
        self.graph = graph
        self.db_service = db_service
        self.storage_service = storage_service
        self.ts = TopologicalSorter(self.graph)
        self._failing_tasks = 0
        self._running_tasks: set[asyncio.Task[tuple[Node, NodeResult]]] = set()

    def _start_ready_nodes(self, run_info: RunInfo) -> None:
        for ready_node in self.ts.get_ready():
            self._running_tasks.add(asyncio.create_task(self.execute_node(ready_node, run_info)))

    async def _is_done(self) -> bool:
        done_tasks = {task for task in self._running_tasks if task.done()}
        for done_task in done_tasks:
            node, result = await done_task
            self.ts.done(node)
            if result is not NodeResult.SUCCESS:
                self._failing_tasks += 1

        self._running_tasks = self._running_tasks - done_tasks

        return self._failing_tasks > 0 or not (self.ts.is_active() or self._running_tasks)

    async def _cancel_running(self) -> None:
        for task in self._running_tasks:
            task.cancel()
        await asyncio.gather(*self._running_tasks, return_exceptions=True)
        self._running_tasks.clear()

    async def execute(self) -> ExitCode:
        self.ts.prepare()

        with RunInfo(graph=self.graph) as run_info:
            await self.db_service.start_run(run_info)
            while True:
                if await self._is_done():
                    await self._cancel_running()
                    break
                self._start_ready_nodes(run_info)
                await asyncio.sleep(0.001)
        await self.db_service.finish_run(run_info)

        return ExitCode.FAIL if self._failing_tasks > 0 else ExitCode.SUCCESS

    async def execute_node(self, node: Node, run_info: RunInfo) -> tuple[Node, NodeResult]:
        with ExecutionInfo(run_info=run_info, node=node) as execution_info:
            await self.db_service.start_execution(execution_info)
            retcode = None
            try:
                retcode = await asyncio.wait_for(self._execute_node(node), node.timeout)
                result = NodeResult.SUCCESS if retcode == 0 else NodeResult.FAIL
            except asyncio.CancelledError:
                result = NodeResult.CANCEL
            except TimeoutError:
                result = NodeResult.TIMEOUT
            execution_info.retcode = retcode
            execution_info.result = result

        await self.db_service.finish_execution(execution_info)
        return node, result

    async def _execute_node(self, node: Node) -> int:
        if not node.command:
            return NodeResult.SUCCESS

        process = await _start_process(node.command)
        async with asyncio.TaskGroup() as tg:
            for artifact_name in ("stdout", "stderr"):
                artifact = Artifact(artifact_name)
                stream: asyncio.StreamReader = getattr(process, artifact_name)
                tg.create_task(_read_stream(self.storage_service, self.graph, node, stream, artifact))
            result = tg.create_task(_wait_process(process))
        return await result


class Engine:
    def __init__(self, config: Config | None = None) -> None:
        self.config = config or Config()

    async def run_graph(self, graph: Graph) -> int:
        await self.db_service.save_graph(graph)
        executor = Executor(graph, self.db_service, self.storage_service)
        result = await executor.execute()
        return result

    def start(self) -> None:
        self.db_service = self.config.database.create_db_service()
        self.storage_service = self.config.storage.create_storage_service()

    def close(self) -> None:
        self.db_service.close()
        self.storage_service.close()

    def __enter__(self) -> Self:
        self.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.close()
