"""Idempotent Postgres sink for Spark micro-batches.

Spark's JDBC writer can only append or overwrite, and a streaming job re-emits
the same open window on every trigger - so a plain append would pile up
duplicates and an overwrite would nuke history. What we want is an upsert.

This does it partition-wise: each Spark partition opens one connection and runs
a batched INSERT ... ON CONFLICT DO UPDATE. No driver-side collect, so it keeps
working if the volume grows past a laptop.
"""
from __future__ import annotations

import logging
from collections.abc import Iterable, Sequence

from pyspark.sql import DataFrame

from streaming import settings

log = logging.getLogger(__name__)


def _make_writer(
    table: str,
    columns: Sequence[str],
    conflict_cols: Sequence[str],
    update_cols: Sequence[str],
    conn_params: dict,
):
    """Build the per-partition closure. Everything it captures must be picklable."""
    col_list = ", ".join(f'"{c}"' for c in columns)
    conflict_list = ", ".join(f'"{c}"' for c in conflict_cols)

    if update_cols:
        assignments = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in update_cols)
        conflict_action = f"DO UPDATE SET {assignments}"
    else:
        conflict_action = "DO NOTHING"

    statement = (
        f"INSERT INTO {table} ({col_list}) VALUES %s "
        f"ON CONFLICT ({conflict_list}) {conflict_action}"
    )

    def write_partition(rows: Iterable) -> None:
        import psycopg2
        from psycopg2.extras import execute_values

        batch = [tuple(row[c] for c in columns) for row in rows]
        if not batch:
            return
        conn = psycopg2.connect(**conn_params)
        try:
            with conn, conn.cursor() as cur:
                execute_values(cur, statement, batch, page_size=500)
        finally:
            conn.close()

    return write_partition


def upsert(
    df: DataFrame,
    table: str,
    conflict_cols: Sequence[str],
    update_cols: Sequence[str] | None = None,
    max_connections: int = settings.SINK_PARTITIONS,
) -> None:
    """Upsert a (batch) DataFrame into `table` keyed on `conflict_cols`."""
    columns = list(df.columns)
    if update_cols is None:
        update_cols = [c for c in columns if c not in set(conflict_cols)]

    # Postgres refuses an INSERT ... ON CONFLICT DO UPDATE that would touch the
    # same target row twice in one statement ("cannot affect row a second time"),
    # so the key has to be unique *within* the batch, not just against the table.
    # At-least-once delivery makes duplicates a certainty rather than a risk: a
    # producer retry after a broker leadership change re-sends a byte-identical
    # tick, and it then lives in the topic forever, killing the query on every
    # replay of that offset range. Rows sharing a key here are redundant copies
    # of one event, so keeping any one of them is correct.
    df = df.dropDuplicates(list(conflict_cols))

    writer = _make_writer(table, columns, conflict_cols, update_cols, settings.pg_conn_params())
    # Coalescing caps both the Postgres connections and - the reason this
    # defaults to 1 - the number of Python worker processes forked per write.
    # Six concurrent queries each forking workers next to a multi-GB JVM will
    # exhaust a small Docker engine, and the kernel reaps a worker rather than
    # the container, which surfaces as an unexplained py4j connection reset.
    df.coalesce(max_connections).foreachPartition(writer)


def read_table(spark, query: str):
    """Static read used for stream-static joins and batch-layer lookups."""
    return (
        spark.read.format("jdbc")
        .option("url", settings.JDBC_URL)
        .option("user", settings.PG_USER)
        .option("password", settings.PG_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .option("query", query)
        .load()
    )


def record_run(run_id: str, job_name: str, status: str, rows: int, detail: str = "") -> None:
    """Write an audit row straight from the driver (one row, no Spark needed)."""
    import psycopg2

    conn = psycopg2.connect(**settings.pg_conn_params())
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline_runs (run_id, job_name, started_at, finished_at,
                                           status, rows_written, detail)
                VALUES (%s, %s, now(), now(), %s, %s, %s)
                ON CONFLICT (run_id) DO UPDATE
                   SET finished_at = now(), status = EXCLUDED.status,
                       rows_written = EXCLUDED.rows_written, detail = EXCLUDED.detail
                """,
                (run_id, job_name, status, rows, detail),
            )
    finally:
        conn.close()
