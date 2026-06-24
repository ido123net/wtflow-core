import pytest

from wtflow_core.config import Config
from wtflow_core.infra.engine import Engine
from wtflow_core.infra.nodes import TreeNode
from wtflow_core.infra.workflow import Tree


@pytest.mark.asyncio
async def test_with_db_config(db_config):
    config = Config(database=db_config)
    wf = Tree(
        name="test with db config",
        root=TreeNode(
            name="Root Node",
            children=[
                TreeNode(name="Node 1", command="echo 'Hello'"),
            ],
        ),
    )
    engine = Engine(config=config)
    assert await engine.run_graph(wf.as_graph()) == 0


@pytest.mark.asyncio
async def test_with_db_and_storage_config(db_config, local_storage_config):
    config = Config(database=db_config, storage=local_storage_config)
    wf = Tree(
        name="test with db",
        root=TreeNode(
            name="Root Node",
            children=[
                TreeNode(name="Node 1", command="echo 'Hello'"),
            ],
        ),
    )
    engine = Engine(config=config)
    assert await engine.run_graph(wf.as_graph()) == 0
