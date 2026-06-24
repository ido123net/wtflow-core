from __future__ import annotations

from dataclasses import dataclass

from wtflow_core.config import Config
from wtflow_core.services.db.db_service import DBService
from wtflow_core.services.storage.storage_service import StorageService


@dataclass
class Servicer:
    db_service: DBService
    storage_service: StorageService

    @classmethod
    def from_config(cls, config: Config) -> Servicer:
        db_service = config.database.create_db_service()
        storage_service = config.storage.create_storage_service()
        return Servicer(
            db_service=db_service,
            storage_service=storage_service,
        )
