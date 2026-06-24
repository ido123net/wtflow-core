import pytest

from wtflow_core.config import Config
from wtflow_core.engine import Engine
from wtflow_core.graph import Graph
from wtflow_core.node import Node

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
