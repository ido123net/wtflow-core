import pytest

from wtflow_core.config import LocalStorageConfig, Sqlite3Config
from wtflow_core.services.db.sqlite.sqlite_db_service import Sqlite3DBService


@pytest.fixture()
def data_dir(tmp_path_factory):
    data_dir = tmp_path_factory.mktemp("data")
    return data_dir


@pytest.fixture()
def local_storage_config(data_dir):
    return LocalStorageConfig(base_path=data_dir)


@pytest.fixture()
def db_config(data_dir):
    database_path = f"{data_dir}/test.db"
    config = Sqlite3Config(database_path=database_path)
    db_service = config.create_db_service()
    assert isinstance(db_service, Sqlite3DBService)
    return config
