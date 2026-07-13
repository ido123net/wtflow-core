import sqlite3
from contextlib import closing

import pytest

from wtflow_core.config import Config
from wtflow_core.engine import Engine, ExitCode
from wtflow_core.graph import Graph
from wtflow_core.node import Node
from wtflow_core.tree import Tree, TreeNode

NODE1 = Node(name="Node1", command="echo 'NODE1'")
NODE2 = Node(name="Node2", command="echo 'NODE2'")


@pytest.mark.asyncio
async def test_with_db_config(db_config):
    config = Config(database=db_config)
    graph1 = Graph(name="graph1", nodes=(NODE1, NODE2), edges=((NODE1, NODE2),))
    graph2 = Graph(name="graph2", nodes=(NODE1,))
    engine = Engine(config=config)
    assert await engine.run_graph(graph1) == 0
    assert await engine.run_graph(graph1) == 0
    assert await engine.run_graph(graph2) == 0


@pytest.mark.asyncio
async def test_with_db_and_storage_config(db_config, local_storage_config):
    config = Config(database=db_config, storage=local_storage_config)
    graph = Graph(name="graph", nodes=(NODE1,))
    engine = Engine(config=config)
    assert await engine.run_graph(graph) == 0


@pytest.mark.asyncio
async def test_with_db_timeout(db_config):
    config = Config(database=db_config)
    wf = Tree(
        name="test timeout node",
        root=TreeNode(
            name="Root Node",
            command="echo 'Hello' && sleep 1 && echo 'World'",
            timeout=0.1,
        ),
    )
    engine = Engine(config)
    result = await engine.run_graph(wf.as_graph())
    assert result == ExitCode.FAIL
    with closing(sqlite3.connect(db_config.database_path)) as conn:
        res = conn.execute("SELECT * FROM executions")
        print(res.fetchall())


@pytest.mark.asyncio
async def test_with_db_parallel_fail(db_config):
    config = Config(database=db_config)
    wf = Tree(
        name="test timeout node",
        root=TreeNode(
            name="Root Node",
            children=[
                TreeNode(
                    name="node1",
                    command="echo node1 && exit 1",
                ),
                TreeNode(
                    name="node2",
                    command="echo node2 && sleep 5",
                ),
            ],
        ),
    )
    engine = Engine(config)
    result = await engine.run_graph(wf.as_graph())
    assert result == ExitCode.FAIL
    with closing(sqlite3.connect(db_config.database_path)) as conn:
        res = conn.execute("SELECT * FROM executions")
        print(res.fetchall())
