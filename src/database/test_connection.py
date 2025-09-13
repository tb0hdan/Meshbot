"""Tests for database connection management."""
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from src.database.connection import DatabaseConnection


class TestDatabaseConnection:
    """Test cases for DatabaseConnection class."""

    def test_connection_creation(self, temp_db_path):
        """Test basic connection creation."""
        conn_manager = DatabaseConnection(temp_db_path)

        with conn_manager.get_connection() as conn:
            assert isinstance(conn, sqlite3.Connection)
            assert conn.row_factory == sqlite3.Row

        conn_manager.close_all_connections()

    def test_connection_configuration(self, temp_db_path):
        """Test that connections are properly configured with WAL mode."""
        conn_manager = DatabaseConnection(temp_db_path)

        with conn_manager.get_connection() as conn:
            cursor = conn.cursor()

            # Check WAL mode
            cursor.execute("PRAGMA journal_mode")
            journal_mode = cursor.fetchone()[0]
            assert journal_mode.upper() == 'WAL'

            # Check synchronous mode
            cursor.execute("PRAGMA synchronous")
            sync_mode = cursor.fetchone()[0]
            assert sync_mode == 1  # NORMAL

            # Check cache size
            cursor.execute("PRAGMA cache_size")
            cache_size = cursor.fetchone()[0]
            assert cache_size == -2000

        conn_manager.close_all_connections()

    def test_connection_pooling(self, temp_db_path):
        """Test connection pooling functionality."""
        conn_manager = DatabaseConnection(temp_db_path)
        connections_created = []

        # Create multiple connections
        for _ in range(3):
            with conn_manager.get_connection() as conn:
                connections_created.append(id(conn))

        # Verify connections are being reused from pool
        with conn_manager.get_connection() as conn:
            assert id(conn) in connections_created

        conn_manager.close_all_connections()

    def test_max_connections_limit(self, temp_db_path):
        """Test that connection pool respects max connections limit."""
        conn_manager = DatabaseConnection(temp_db_path)
        conn_manager._max_connections = 2

        # Fill the pool
        connections = []
        for _ in range(3):
            with conn_manager.get_connection() as conn:
                connections.append(conn)

        # Pool should not exceed max size
        assert len(conn_manager._connection_pool) <= conn_manager._max_connections

        conn_manager.close_all_connections()

    def test_connection_error_handling(self, temp_db_path):
        """Test error handling in connection management."""
        conn_manager = DatabaseConnection(temp_db_path)

        with conn_manager.get_connection() as conn:
            # Force an error
            try:
                conn.execute("INVALID SQL STATEMENT")
                assert False, "Should have raised an exception"
            except sqlite3.Error:
                pass  # Expected

        # Connection should still work after error
        with conn_manager.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            assert result[0] == 1

        conn_manager.close_all_connections()

    def test_thread_safety(self, temp_db_path):
        """Test thread-safe connection management."""
        conn_manager = DatabaseConnection(temp_db_path)
        results = []
        errors = []

        def worker():
            try:
                with conn_manager.get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute("CREATE TABLE IF NOT EXISTS test_table (id INTEGER)")
                    cursor.execute("INSERT INTO test_table (id) VALUES (?)", (threading.current_thread().ident,))
                    cursor.execute("SELECT COUNT(*) FROM test_table")
                    count = cursor.fetchone()[0]
                    results.append(count)
                    time.sleep(0.01)  # Small delay to encourage race conditions
            except Exception as e:
                errors.append(e)

        # Run multiple threads
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(worker) for _ in range(10)]
            for future in futures:
                future.result()

        # Verify no errors and results make sense
        assert len(errors) == 0, f"Errors occurred: {errors}"
        assert len(results) == 10
        assert max(results) == 10  # Final count should be 10

        conn_manager.close_all_connections()

    def test_context_manager_transaction_handling(self, temp_db_path):
        """Test that context manager properly handles transactions."""
        conn_manager = DatabaseConnection(temp_db_path)

        # Create test table
        with conn_manager.get_connection() as conn:
            conn.execute("CREATE TABLE IF NOT EXISTS test_table (id INTEGER, value TEXT)")

        # Test successful transaction
        with conn_manager.get_connection() as conn:
            conn.execute("INSERT INTO test_table (id, value) VALUES (1, 'test')")
            # Transaction should commit automatically when exiting context

        # Verify data was committed
        with conn_manager.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM test_table WHERE id = 1")
            count = cursor.fetchone()[0]
            assert count == 1

        # Test rollback on exception
        try:
            with conn_manager.get_connection() as conn:
                conn.execute("INSERT INTO test_table (id, value) VALUES (2, 'test2')")
                raise ValueError("Test exception")
        except ValueError:
            pass

        # Verify rollback occurred
        with conn_manager.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM test_table WHERE id = 2")
            count = cursor.fetchone()[0]
            assert count == 0

        conn_manager.close_all_connections()

    def test_close_all_connections(self, temp_db_path):
        """Test closing all connections in the pool."""
        conn_manager = DatabaseConnection(temp_db_path)

        # Create some connections to populate the pool
        for _ in range(3):
            with conn_manager.get_connection() as conn:
                conn.execute("SELECT 1")

        # Verify pool has connections
        initial_pool_size = len(conn_manager._connection_pool)
        assert initial_pool_size > 0

        # Close all connections
        conn_manager.close_all_connections()

        # Verify pool is empty
        assert len(conn_manager._connection_pool) == 0

    def test_connection_timeout(self, temp_db_path):
        """Test connection timeout setting."""
        conn_manager = DatabaseConnection(temp_db_path)

        with conn_manager.get_connection() as conn:
            # Verify timeout was set (indirect test via successful connection)
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            assert result[0] == 1

        conn_manager.close_all_connections()
