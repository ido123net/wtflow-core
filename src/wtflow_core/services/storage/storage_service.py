from __future__ import annotations

import sys
from abc import ABC, abstractmethod
from contextlib import AbstractContextManager, contextmanager
from typing import TYPE_CHECKING, BinaryIO, Generator

from wtflow_core.services.base_service import BaseService

if TYPE_CHECKING:
    from wtflow_core.artifact import Artifact
    from wtflow_core.graph import Graph
    from wtflow_core.node import Node


class ArtifactWriter(ABC):
    @abstractmethod
    def write(self, data: bytes) -> int: ...

    @abstractmethod
    def close(self) -> None: ...


class StorageService(BaseService):
    @abstractmethod
    def open_artifact(
        self,
        workflow: Graph,
        node: Node,
        artifact: Artifact,
    ) -> AbstractContextManager[ArtifactWriter]: ...


class StreamArtifactWriter(ArtifactWriter):
    def __init__(self, stream: BinaryIO) -> None:
        self.stream = stream

    def write(self, data: bytes) -> int:
        return self.stream.write(data)

    def close(self) -> None:
        pass


class NoStorageService(StorageService):
    @contextmanager
    def open_artifact(
        self,
        workflow: Graph,
        node: Node,
        artifact: Artifact,
    ) -> Generator[StreamArtifactWriter, None, None]:
        if artifact.name == "stdout":
            writer = StreamArtifactWriter(sys.stdout.buffer)
        elif artifact.name == "stderr":
            writer = StreamArtifactWriter(sys.stderr.buffer)
        else:  # pragma: no cover
            raise NotImplementedError(f"Artifact {artifact.name} is not supported in {self.__class__.__name__}")
        try:
            yield writer
        finally:
            writer.close()
