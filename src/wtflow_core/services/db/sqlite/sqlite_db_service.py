from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import closing, contextmanager
from dataclasses import asdict
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Generator, Protocol
from uuid import UUID

from wtflow_core.services.db.db_service import DBService

if TYPE_CHECKING:
    from wtflow_core.graph import Graph
    from wtflow_core.node import Node


class Dataclass(Protocol):
    __dataclass_fields__: ClassVar[dict[str, Any]]


@lru_cache
def _digest(node: Dataclass) -> str:
    data = json.dumps(
        asdict(node),
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(data.encode("utf-8")).hexdigest()


def get_graph_by_digest(conn: sqlite3.Connection, graph_digest: str) -> int | None:
    cursor = conn.execute(
        """
        SELECT id
        FROM graphs
        WHERE digest = ?
        LIMIT 1
        """,
        (graph_digest,),
    )
    res = cursor.fetchone()
    return res[0] if res else None


def get_node_by_digest(conn: sqlite3.Connection, node_digest: str) -> int | None:
    cursor = conn.execute(
        """
        SELECT id
        FROM nodes
        WHERE digest = ?
        LIMIT 1
        """,
        (node_digest,),
    )
    res = cursor.fetchone()
    return res[0] if res else None


def _create_node(conn: sqlite3.Connection, node: Node) -> int:
    node_digest = _digest(node)
    if node_id := get_node_by_digest(conn, node_digest):
        return node_id

    cursor = conn.execute(
        """
        INSERT INTO nodes (
            digest,
            name,
            command,
            timeout
        )
        VALUES (?, ?, ?, ?)
        """,
        (
            node_digest,
            node.name,
            node.command,
            node.timeout,
        ),
    )

    assert cursor.lastrowid
    node_id = cursor.lastrowid
    return node_id


class Sqlite3DBService(DBService):
    def __init__(self, database_path: str | Path) -> None:
        self.database_path = Path(database_path)
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self._create_tables()

        self._execution_ids: dict[UUID, int] = {}
        self._run_ids: dict[UUID, int] = {}
        self._graph_ids: dict[str, int] = {}
        self._node_ids: dict[str, int] = {}

    @contextmanager
    def _get_connection(self) -> Generator[sqlite3.Connection]:
        sqlite3.register_adapter(datetime, self._adapt_datetime)

        with closing(sqlite3.connect(self.database_path)) as cx:
            cx.execute("PRAGMA foreign_keys = ON")
            yield cx
            cx.commit()

    @staticmethod
    def _adapt_datetime(dt: datetime) -> str:
        return dt.isoformat()

    async def save_run(self, graph: Graph) -> int:
        graph_digest = _digest(graph)

        with self._get_connection() as conn:
            graph_id = get_graph_by_digest(conn, graph_digest)
            if not graph_id:
                cursor = conn.execute(
                    """
                    INSERT INTO graphs (digest, name)
                    VALUES (?, ?)
                    ON CONFLICT(digest) DO NOTHING
                    """,
                    (graph_digest, graph.name),
                )

                assert cursor.lastrowid
                graph_id = cursor.lastrowid
            node_ids: dict[str, int] = {}

            for node in graph.nodes:
                node_id = _create_node(conn, node)
                node_ids[_digest(node)] = node_id

                conn.execute(
                    """
                    INSERT INTO graph_nodes (
                        graph_id,
                        node_id
                    )
                    VALUES (?, ?)
                    ON CONFLICT(graph_id, node_id) DO NOTHING
                    """,
                    (
                        graph_id,
                        node_id,
                    ),
                )

            for from_node, to_node in graph.edges:
                from_node_id = node_ids[_digest(from_node)]
                to_node_id = node_ids[_digest(to_node)]

                conn.execute(
                    """
                    INSERT INTO graph_edges (
                        graph_id,
                        from_node_id,
                        to_node_id
                    )
                    VALUES (?, ?, ?)
                    ON CONFLICT(
                        graph_id,
                        from_node_id,
                        to_node_id
                    ) DO NOTHING
                    """,
                    (
                        graph_id,
                        from_node_id,
                        to_node_id,
                    ),
                )

            cursor = conn.execute(
                """
                INSERT INTO runs (graph_id, created_at)
                VALUES (?, ?)
                """,
                (graph_id, datetime.now(tz=timezone.utc)),
            )
            assert cursor.lastrowid
            return cursor.lastrowid

    async def start_run(self, run_id: int) -> None:
        with self._get_connection() as conn:
            conn.execute(
                """
                UPDATE runs
                SET start_time = ?
                WHERE id = ?
                """,
                (datetime.now(tz=timezone.utc), run_id),
            )

    async def finish_run(self, run_id: int) -> None:
        with self._get_connection() as conn:
            conn.execute(
                """
                UPDATE runs
                SET end_time = ?
                WHERE id = ?
                """,
                (
                    datetime.now(tz=timezone.utc),
                    run_id,
                ),
            )

    async def start_execution(self, run_id: int, node: Node) -> int:
        node_digest = _digest(node)

        with self._get_connection() as conn:
            node_id = get_node_by_digest(conn, node_digest)
            curser = conn.execute(
                """
                INSERT INTO executions (
                    run_id,
                    node_id,
                    start_time
                )
                VALUES (?, ?, ?)
                """,
                (
                    run_id,
                    node_id,
                    datetime.now(tz=timezone.utc),
                ),
            )
            assert curser.lastrowid
            return curser.lastrowid

    async def finish_execution(self, run_id: int, execution_id: int) -> None:
        with self._get_connection() as conn:
            conn.execute(
                """
                UPDATE executions
                SET end_time = ?
                WHERE id = ? AND run_id = ?
                """,
                (
                    datetime.now(tz=timezone.utc),
                    execution_id,
                    run_id,
                ),
            )

    def _create_tables(self) -> None:
        with self._get_connection() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS nodes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    digest TEXT NOT NULL UNIQUE,
                    name TEXT NOT NULL,
                    command TEXT,
                    timeout REAL
                );

                CREATE TABLE IF NOT EXISTS graphs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    digest TEXT NOT NULL UNIQUE,
                    name TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS graph_nodes (
                    graph_id INTEGER NOT NULL,
                    node_id INTEGER NOT NULL,

                    PRIMARY KEY (graph_id, node_id),

                    FOREIGN KEY (graph_id)
                        REFERENCES graphs(id)
                        ON DELETE CASCADE,

                    FOREIGN KEY (node_id)
                        REFERENCES nodes(id)
                        ON DELETE RESTRICT
                );

                CREATE TABLE IF NOT EXISTS graph_edges (
                    graph_id INTEGER NOT NULL,
                    from_node_id INTEGER NOT NULL,
                    to_node_id INTEGER NOT NULL,

                    PRIMARY KEY (
                        graph_id,
                        from_node_id,
                        to_node_id
                    ),

                    FOREIGN KEY (graph_id)
                        REFERENCES graphs(id)
                        ON DELETE CASCADE,

                    FOREIGN KEY (from_node_id)
                        REFERENCES nodes(id)
                        ON DELETE RESTRICT,

                    FOREIGN KEY (to_node_id)
                        REFERENCES nodes(id)
                        ON DELETE RESTRICT
                );

                CREATE TABLE IF NOT EXISTS runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    graph_id INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    start_time TEXT,
                    end_time TEXT,

                    FOREIGN KEY (graph_id)
                        REFERENCES graphs(id)
                        ON DELETE RESTRICT
                );

                CREATE TABLE IF NOT EXISTS executions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER NOT NULL,
                    node_id INTEGER NOT NULL,
                    start_time TEXT,
                    end_time TEXT,

                    FOREIGN KEY (run_id)
                        REFERENCES runs(id)
                        ON DELETE CASCADE,

                    FOREIGN KEY (node_id)
                        REFERENCES nodes(id)
                        ON DELETE RESTRICT
                );

                CREATE INDEX IF NOT EXISTS idx_graph_nodes_node_id
                ON graph_nodes(node_id);

                CREATE INDEX IF NOT EXISTS idx_graph_edges_from_node_id
                ON graph_edges(from_node_id);

                CREATE INDEX IF NOT EXISTS idx_graph_edges_to_node_id
                ON graph_edges(to_node_id);

                CREATE INDEX IF NOT EXISTS idx_runs_graph_id
                ON runs(graph_id);

                CREATE INDEX IF NOT EXISTS idx_executions_run_id
                ON executions(run_id);

                CREATE INDEX IF NOT EXISTS idx_executions_node_id
                ON executions(node_id);
                """
            )
            conn.commit()
