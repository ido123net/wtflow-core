import sqlite3
from contextlib import closing

import pytest

from wtflow_core.config import Config
from wtflow_core.engine import Engine, ExitCode, NodeResult
from wtflow_core.graph import Graph
from wtflow_core.node import Node
from wtflow_core.services.db.sqlite.sqlite_db_service import digest


@pytest.mark.asyncio
async def test_with_db_config(db_config):
    config = Config(database=db_config)
    root_node = Node(name="RootNode")
    n1 = Node(name="n1", command="echo 'Hello'")
    graph = Graph(
        name="test_with_db_config",
        nodes=(root_node, n1),
        edges=((root_node, n1),),
    )
    with Engine(config=config) as engine:
        assert await engine.run_graph(graph) == 0


@pytest.mark.asyncio
async def test_with_db_and_storage_config(db_config, local_storage_config):
    config = Config(database=db_config, storage=local_storage_config)
    root_node = Node(name="Root Node")
    child1 = Node(name="Node 1 (success)", command="echo 'Hello'")
    child2 = Node(name="Node 2 (failure)", command="sleep 0.1 && exit 1")
    child3 = Node(name="Node 3 (cancel)", command="sleep 1 && echo 'World'")
    graph = Graph(
        name="test_with_db",
        nodes=(root_node, child1, child2, child3),
        edges=(
            (root_node, child1),
            (root_node, child2),
            (root_node, child3),
        ),
    )
    with Engine(config=config) as engine:
        assert await engine.run_graph(graph) == ExitCode.FAIL

    with closing(sqlite3.connect(db_config.database_path)) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT node_digest, result FROM executions")
        res = dict(cursor.fetchall())

    assert res[digest(root_node)] == NodeResult.SUCCESS
    assert res[digest(child1)] == NodeResult.SUCCESS
    assert res[digest(child2)] == NodeResult.FAIL
    assert res[digest(child3)] == NodeResult.CANCEL
