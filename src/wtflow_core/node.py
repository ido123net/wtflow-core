from __future__ import annotations

import asyncio
import os
import signal
from dataclasses import dataclass, field
from enum import IntEnum, auto
from typing import TYPE_CHECKING, Any, Literal

from wtflow_core.artifact import Artifact
from wtflow_core.services.storage.storage_service import ArtifactWriter, StorageService

if TYPE_CHECKING:
    from wtflow_core.services.db.db_service import DBService


class NodeResult(IntEnum):
    SUCCESS = auto()
    FAIL = auto()
    TIMEOUT = auto()
    CANCEL = auto()


@dataclass
class NodeOutcome:
    result: NodeResult | None = None
    retcode: int | None = None


class NodeState(IntEnum):
    INIT = auto()
    START = auto()
    WAIT = auto()
    DONE = auto()


async def kill_process(process: asyncio.subprocess.Process) -> None:
    if process.returncode is None:
        os.killpg(os.getpgid(process.pid), signal.SIGTERM)
    await process.wait()


class NodeExecutor:
    def __init__(
        self,
        node: Node,
        run_id: Any,
        db_service: DBService,
        storage_service: StorageService,
    ) -> None:
        self.node = node
        self.canceled = False
        self.state = NodeState.INIT
        self.run_id = run_id
        self.db_service = db_service
        self.storage_service = storage_service
        self.outcome = NodeOutcome()

        self._open_artifacts: dict[Artifact, ArtifactWriter] = {}

    @property
    def done(self) -> bool:
        return self.state is NodeState.DONE

    async def _start_process(self, command: str) -> asyncio.subprocess.Process:
        process = await asyncio.create_subprocess_shell(
            command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=True,
        )
        self.state = NodeState.WAIT
        return process

    async def _process_stream(self, data: bytes, artifact: Artifact) -> None:
        if artifact not in self._open_artifacts:
            artifact_writer = self.storage_service.open_artifact(self.run_id, self.execution_id, artifact)
            self._open_artifacts[artifact] = artifact_writer
        self._open_artifacts[artifact].write(data)

    def _close_artifacts(self) -> None:
        for artifact in self._open_artifacts.values():
            artifact.close()

    async def _wait_process(self, process: asyncio.subprocess.Process) -> None:
        retcode = process.returncode
        assert process.stdout
        assert process.stderr
        stdout, stderr = await asyncio.gather(process.stdout.read(), process.stderr.read())
        if stdout:
            await self._process_stream(stdout, Artifact("stdout"))
        if stderr:
            await self._process_stream(stderr, Artifact("stderr"))
        if retcode is not None:
            self.outcome.retcode = retcode
            self.outcome.result = NodeResult.FAIL if retcode else NodeResult.SUCCESS
            self.state = NodeState.DONE

    def cancel(self, result: Literal[NodeResult.CANCEL, NodeResult.TIMEOUT] = NodeResult.CANCEL) -> None:
        self.canceled = True
        self.outcome.result = result

    async def main(self) -> tuple[NodeExecutor, NodeOutcome]:
        self.execution_id = await self.db_service.start_execution(self.run_id, self.node)
        if self.node.command is None:
            self.outcome.result = NodeResult.SUCCESS
            self.state = NodeState.DONE
        process = None
        while not self.done:
            if self.state is NodeState.START:
                assert self.node.command is not None
                process = await self._start_process(self.node.command)
            elif self.state is NodeState.WAIT:
                assert process
                if self.canceled:
                    await kill_process(process)
                    self.state = NodeState.DONE
                await self._wait_process(process)
        self._close_artifacts()
        await self.db_service.finish_execution(self.run_id, self.execution_id, self.outcome)
        return self, self.outcome


@dataclass(frozen=True)
class Node:
    name: str
    command: str | None = None
    timeout: float | None = None
    artifacts: tuple[Artifact, ...] = field(default_factory=tuple)
