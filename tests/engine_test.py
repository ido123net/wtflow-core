import time

import pytest

from wtflow_core.config import Config
from wtflow_core.engine import Engine, ExitCode
from wtflow_core.tree import Tree, TreeNode


@pytest.mark.asyncio
async def test_run():
    wf = Tree(
        name="test run",
        root=TreeNode(
            name="Root Node",
            children=[
                TreeNode(name="Node 1", command='echo "Hello 1"'),
                TreeNode(
                    name="Node 2",
                    children=[
                        TreeNode(name="Node 2.1", command='echo "World 2.1"'),
                        TreeNode(name="Node 2.2", command='echo "World 2.2"'),
                    ],
                ),
            ],
        ),
    )
    engine = Engine()
    await engine.run_graph(wf.as_graph())


@pytest.mark.asyncio
async def test_fail_run(capfd):
    wf = Tree(
        name="test fail run",
        root=TreeNode(
            name="fail node",
            command="command-not-exist",
        ),
    )
    engine = Engine()
    assert await engine.run_graph(wf.as_graph()) == ExitCode.FAIL
    _, err = capfd.readouterr()
    assert "not found" in err


@pytest.mark.asyncio
async def test_stop_on_failure(capfdbinary):
    wf = Tree(
        name="test stop on failure",
        root=TreeNode(
            name="Root Node",
            children=[
                TreeNode(name="Node 1", command='echo "Hello 1"'),
                TreeNode(
                    name="Node 2",
                    children=[
                        TreeNode(name="Node 2.1", command='echo "World 2.1"'),
                        TreeNode(name="Node 2.2", command="command-not-exist"),
                        TreeNode(name="Node 2.3", command='echo "EXISTS" && sleep 1 && echo "NOPE"'),
                    ],
                ),
                TreeNode(name="Node 3", command='echo "Hello 3"'),
            ],
        ),
    )
    engine = Engine(config=Config())
    assert await engine.run_graph(wf.as_graph()) == ExitCode.FAIL
    out, _ = capfdbinary.readouterr()
    assert b"EXISTS" in out
    assert b"NOPE" not in out


@pytest.mark.asyncio
async def test_timeout_node(capfdbinary):
    wf = Tree(
        name="test timeout node",
        root=TreeNode(
            name="Root Node",
            command="echo 'Hello' && sleep 1 && echo 'World'",
            timeout=0.1,
        ),
    )
    engine = Engine(Config())
    start_time = time.perf_counter()
    assert await engine.run_graph(wf.as_graph()) == ExitCode.FAIL
    elapsed = time.perf_counter() - start_time
    assert elapsed < 0.2
    stdout, stderr = capfdbinary.readouterr()
    assert stdout == b"Hello\n"
    assert stderr == b""


@pytest.mark.asyncio
async def test_with_storage_config(local_storage_config, data_dir):
    config = Config(storage=local_storage_config)
    wf = Tree(
        name="test no db",
        root=TreeNode(
            name="Root Node",
            children=[
                TreeNode(name="Node 1", command="echo 'Hello' && echo 'world'"),
            ],
        ),
    )
    engine = Engine(config=config)
    assert await engine.run_graph(wf.as_graph()) == 0
    log_path = data_dir / "test no db" / "Node 1" / "stdout.txt"
    assert log_path.exists()
    assert log_path.read_text() == "Hello\nworld\n"
