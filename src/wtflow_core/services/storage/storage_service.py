from __future__ import annotations

import sys
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, BinaryIO

from wtflow_core.services.base_service import BaseService

if TYPE_CHECKING:
    from wtflow_core.artifact import Artifact


class ArtifactWriter(ABC):
    @abstractmethod
    def write(self, data: bytes) -> int: ...

    @abstractmethod
    def close(self) -> None: ...


class StorageService(BaseService):
    @abstractmethod
    def open_artifact(
        self,
        run_id: Any,
        execution_id: Any,
        artifact: Artifact,
    ) -> ArtifactWriter: ...


class StreamArtifactWriter(ArtifactWriter):
    def __init__(self, stream: BinaryIO) -> None:
        self.stream = stream

    def write(self, data: bytes) -> int:
        return self.stream.write(data)

    def close(self) -> None:
        pass


class NoStorageService(StorageService):
    def open_artifact(
        self,
        run_id: Any,
        execution_id: Any,
        artifact: Artifact,
    ) -> StreamArtifactWriter:
        if artifact.name == "stdout":
            writer = StreamArtifactWriter(sys.stdout.buffer)
        elif artifact.name == "stderr":
            writer = StreamArtifactWriter(sys.stderr.buffer)
        else:  # pragma: no cover
            raise NotImplementedError(f"Artifact {artifact.name} is not supported in {self.__class__.__name__}")
        return writer
