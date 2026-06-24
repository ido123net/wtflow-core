from __future__ import annotations

import pathlib
from contextlib import closing, contextmanager
from io import BufferedWriter
from typing import TYPE_CHECKING, Any, Generator

from wtflow_core.services.storage.storage_service import ArtifactWriter, StorageService

if TYPE_CHECKING:
    from wtflow_core.artifact import Artifact


class LocalArtifactWriter(ArtifactWriter):
    def __init__(self, path: pathlib.Path) -> None:
        self.path = path
        self._handle: BufferedWriter | None = None

    def write(self, data: bytes) -> int:
        if self._handle is None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self._handle = self.path.open("ab")
        return self._handle.write(data)

    def close(self) -> None:
        if self._handle is not None:
            self._handle.close()
            self._handle = None


class LocalStorageService(StorageService):
    def __init__(self, base_path: pathlib.Path | str) -> None:
        super().__init__()
        self.base_path = pathlib.Path(base_path)

    def _get_path(
        self,
        run_id: Any,
        execution_id: Any,
        name: str,
        file_type: str,
    ) -> pathlib.Path:
        return self.base_path / str(run_id) / str(execution_id) / f"{name}.{file_type}"

    @contextmanager
    def open_artifact(
        self,
        run_id: Any,
        execution_id: Any,
        artifact: Artifact,
    ) -> Generator[LocalArtifactWriter, None, None]:
        path = self._get_path(run_id, execution_id, artifact.name, artifact.file_type)
        with closing(LocalArtifactWriter(path)) as writer:
            yield writer
