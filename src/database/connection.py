"""
Database connection management module
Handles SQLite connection pooling and configuration
"""

import sqlite3
import logging
import threading
from typing import List
from contextlib import contextmanager

logger = logging.getLogger(__name__)


class DatabaseConnection:
    """SQLite database connection manager with connection pooling and WAL mode"""

    def __init__(self, db_path: str = "meshtastic.db"):
        self.db_path = db_path
        self._lock = threading.RLock()
        self._connection_pool: List[sqlite3.Connection] = []
        self._max_connections = 5
        self._connection_timeout = 30

    @contextmanager
    def get_connection(self):
        """Get a database connection from the pool or create a new one"""
        conn = None
        try:
            with self._lock:
                # Try to get a connection from the pool
                if self._connection_pool:
                    conn = self._connection_pool.pop()
                else:
                    # Create a new connection
                    conn = sqlite3.connect(
                        self.db_path,
                        timeout=30,
                        check_same_thread=False
                    )
                    conn.row_factory = sqlite3.Row
                    self._configure_connection(conn)

                yield conn

        except Exception as e:
            if conn:
                conn.rollback()
            raise e
        finally:
            if conn:
                try:
                    conn.commit()
                    # Return connection to pool if not full
                    with self._lock:
                        if len(self._connection_pool) < self._max_connections:
                            self._connection_pool.append(conn)
                        else:
                            conn.close()
                except (sqlite3.Error, OSError) as e:
                    logger.warning("Error returning connection to pool: %s", e)
                    if conn:
                        conn.close()

    def _configure_connection(self, conn: sqlite3.Connection):
        """Configure connection with WAL mode and optimizations"""
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
        conn.execute("PRAGMA cache_size = -2000")  # 2MB cache
        conn.execute("PRAGMA temp_store = MEMORY")

    def close_all_connections(self):
        """Close all connections in the pool"""
        with self._lock:
            for conn in self._connection_pool:
                try:
                    conn.close()
                except (sqlite3.Error, OSError) as e:
                    logger.warning("Error closing connection: %s", e)
            self._connection_pool.clear()
            logger.info("All database connections closed")
