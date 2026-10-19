from __future__ import annotations

import sys
from abc import ABC, abstractmethod
from collections.abc import Generator
from contextlib import AbstractContextManager, contextmanager
from typing import BinaryIO

from wtflow_core.artifact import Artifact
from wtflow_core.graph import Graph
from wtflow_core.node import Node
from wtflow_core.services.base_service import BaseService


class ArtifactWriter(ABC):
    @abstractmethod
    def write(self, data: bytes) -> int:
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError


class StorageService(BaseService):
    @abstractmethod
    def open_artifact(
        self,
        workflow: Graph,
        node: Node,
        artifact: Artifact,
    ) -> AbstractContextManager[ArtifactWriter]:
        raise NotImplementedError


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
        else:
            raise NotImplementedError(f"Artifact {artifact.name} is not supported in {self.__class__.__name__}")
        try:
            yield writer
        finally:
            writer.close()
