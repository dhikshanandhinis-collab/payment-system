"""Connection pool wrapper with explicit transactions."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

import psycopg
from psycopg_pool import ConnectionPool

from common.config import DatabaseSettings


def _conninfo_kwargs(settings: DatabaseSettings, **overrides: str) -> dict:
    kwargs = settings.conninfo(**overrides)
    kwargs["options"] = kwargs["options"] + " -c TimeZone=UTC"
    return kwargs


def connect(settings: DatabaseSettings, autocommit: bool = False, **overrides: str) -> psycopg.Connection:
    """Open a single, non-pooled connection (tooling, tests, batch jobs)."""
    return psycopg.connect(autocommit=autocommit, **_conninfo_kwargs(settings, **overrides))


class Database:
    def __init__(self, settings: DatabaseSettings, name: str = "recon", max_size: int | None = None) -> None:
        self.settings = settings
        self.pool = ConnectionPool(
            kwargs=_conninfo_kwargs(settings),
            min_size=settings.pool_min_size,
            max_size=max_size or settings.pool_max_size,
            name=name,
            open=False,
            check=ConnectionPool.check_connection,
            timeout=settings.connect_timeout_s * 2,
            reconnect_timeout=60,
        )

    def open(self, wait: bool = True, timeout: float = 30.0) -> None:
        self.pool.open(wait=wait, timeout=timeout)

    def close(self) -> None:
        self.pool.close()

    @contextmanager
    def transaction(self) -> Iterator[psycopg.Connection]:
        """Yield a connection inside an explicit transaction; commit on success, roll back on error."""
        with self.pool.connection() as conn:
            with conn.transaction():
                yield conn

    def ping(self) -> bool:
        try:
            with self.pool.connection(timeout=self.settings.connect_timeout_s) as conn:
                conn.execute("SELECT 1")
            return True
        except Exception:  # noqa: BLE001 - health probe must never raise
            return False
