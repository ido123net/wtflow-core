import inspect
import time

import pytest

from wtflow_core.config import Config
from wtflow_core.engine import Engine, ExitCode
from wtflow_core.graph import Graph
from wtflow_core.node import Node


def _callername():
    frame = inspect.currentframe()
    return inspect.getouterframes(frame)[1].function


@pytest.mark.asyncio
async def test_run():
    root_node = Node(name="RootNode")
    n1 = Node(name="n1", command='echo "Hello 1"')
    n2 = Node(name="n2")
    n2_1 = Node(name="n2.1", command='echo "World 2.1"')
    n2_2 = Node(name="n2.2", command='echo "World 2.2"')
    graph = Graph(
        name=_callername(),
        nodes=(root_node, n1, n2, n2_1, n2_2),
        edges=(
            (root_node, n1),
            (root_node, n2),
            (n2, n2_1),
            (n2, n2_2),
        ),
    )
    with Engine() as engine:
        await engine.run_graph(graph)


@pytest.mark.asyncio
async def test_fail_run(capfd):
    graph = Graph(
        name=_callername(),
        nodes=(
            Node(
                name="fail_node",
                command="command-not-exist",
            ),
        ),
    )
    with Engine() as engine:
        assert await engine.run_graph(graph) == ExitCode.FAIL
    _, err = capfd.readouterr()
    assert "not found" in err


@pytest.mark.asyncio
async def test_stop_on_failure(capfdbinary):
    root_node = Node(name="RootNode")
    n1 = Node(name="n1", command='echo "Hello 1"')
    n2 = Node(name="n2")
    n2_1 = Node(name="n2.1", command='echo "World 2.1"')
    n2_2 = Node(name="n2.2", command="sleep 0.01 && command-not-exist")
    n2_3 = Node(name="n2.3", command='echo "EXISTS" && sleep 1 && echo "NOPE"')
    n3 = Node(name="n3", command='echo "Hello 3"')
    graph = Graph(
        name=_callername(),
        nodes=(root_node, n1, n2, n2_1, n2_2, n2_3, n3),
        edges=((root_node, n1), (root_node, n2), (root_node, n3), (n2, n2_1), (n2, n2_2), (n2, n2_3)),
    )
    with Engine(config=Config()) as engine:
        assert await engine.run_graph(graph) == ExitCode.FAIL
    out, _ = capfdbinary.readouterr()
    assert b"EXISTS" in out
    assert b"NOPE" not in out


@pytest.mark.asyncio
async def test_timeout_node(capfdbinary):
    graph = Graph(
        name=_callername(),
        nodes=(
            Node(
                name="RootNode",
                command="echo 'Hello' && sleep 1 && echo 'World'",
                timeout=0.1,
            ),
        ),
    )
    start_time = time.perf_counter()
    with Engine(config=Config()) as engine:
        assert await engine.run_graph(graph) == ExitCode.FAIL
    elapsed = time.perf_counter() - start_time
    assert elapsed < 0.2
    stdout, stderr = capfdbinary.readouterr()
    assert stdout == b"Hello\n"
    assert stderr == b""


@pytest.mark.asyncio
async def test_with_storage_config(local_storage_config, data_dir):
    config = Config(storage=local_storage_config)
    root_node = Node(name="RootNode")
    n1 = Node(name="n1", command="echo 'Hello' && echo 'world'")
    graph = Graph(name=_callername(), nodes=(root_node, n1), edges=((root_node, n1),))
    with Engine(config=config) as engine:
        assert await engine.run_graph(graph) == 0
    log_path = data_dir / "test_with_storage_config" / "n1" / "stdout.txt"
    assert log_path.exists()
    assert log_path.read_text() == "Hello\nworld\n"


@pytest.mark.asyncio
async def test_long_with_short(capfdbinary):
    n1 = Node(name="n1", command="echo 1")
    n2 = Node(name="n2", command="sleep 0.1 && echo 2")
    n3 = Node(name="n3", command="sleep 0.01 && echo 3")
    n4 = Node(name="n4", command="echo 4")
    graph = Graph(
        name="test_long_with_short",
        nodes=(n1, n2, n3, n4),
        edges=(
            (n1, n2),
            (n1, n3),
            (n3, n4),
        ),
    )
    with Engine(config=Config()) as engine:
        await engine.run_graph(graph)
    stdout, stderr = capfdbinary.readouterr()
    assert stderr == b""
    assert stdout == b"1\n3\n4\n2\n"
