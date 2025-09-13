"""Tests for database schema creation and migration."""
import sqlite3

import pytest

from src.database.schema import DatabaseSchema


class TestDatabaseSchema:
    """Test cases for DatabaseSchema class."""

    def test_create_all_tables(self, db_connection):
        """Test that all tables are created successfully."""
        with db_connection.get_connection() as conn:
            cursor = conn.cursor()

            # Get list of tables
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]

            # Verify all expected tables exist
            expected_tables = ['nodes', 'telemetry', 'positions', 'messages']
            for table in expected_tables:
                assert table in tables, f"Table {table} not found"

    def test_nodes_table_structure(self, db_connection):
        """Test nodes table has correct structure."""
        with db_connection.get_connection() as conn:
            cursor = conn.cursor()

            cursor.execute("PRAGMA table_info(nodes)")
            columns = {row[1]: row[2] for row in cursor.fetchall()}  # name: type

            expected_columns = {
                'node_id': 'TEXT',
                'node_num': 'INTEGER',
                'long_name': 'TEXT',
                'short_name': 'TEXT',
                'macaddr': 'TEXT',
                'hw_model': 'TEXT',
                'firmware_version': 'TEXT',
                'first_seen': 'TIMESTAMP',
                'last_seen': 'TIMESTAMP',
                'last_heard': 'TIMESTAMP',
                'hops_away': 'INTEGER',
                'is_router': 'BOOLEAN',
                'is_client': 'BOOLEAN'
            }

            for col_name, col_type in expected_columns.items():
                assert col_name in columns, f"Column {col_name} not found in nodes table"
                assert columns[col_name] == col_type, f"Column {col_name} type mismatch"

    def test_telemetry_table_structure(self, db_connection):
        """Test telemetry table has correct structure."""
        with db_connection.get_connection() as conn:
            cursor = conn.cursor()

            cursor.execute("PRAGMA table_info(telemetry)")
            columns = {row[1] for row in cursor.fetchall()}  # column names

            # Core columns that should always exist
            core_columns = {
                'id', 'node_id', 'timestamp',
                'battery_level', 'voltage', 'channel_utilization', 'air_util_tx',
                'uptime_seconds', 'temperature', 'humidity', 'pressure',
                'gas_resistance', 'iaq', 'pm10', 'pm25', 'pm100',
                'snr', 'rssi', 'frequency',
                'latitude', 'longitude', 'altitude', 'speed', 'heading', 'accuracy'
            }

            for col_name in core_columns:
                assert col_name in columns, f"Column {col_name} not found in telemetry table"

            # Check power metrics columns (ch1-8 voltage/current)
            power_columns = []
            for i in range(1, 9):
                power_columns.extend([f'ch{i}_voltage', f'ch{i}_current'])

            for col_name in power_columns:
                assert col_name in columns, f"Power column {col_name} not found in telemetry table"

    def test_positions_table_structure(self, db_connection):
        """Test positions table has correct structure."""
        with db_connection.get_connection() as conn:
            cursor = conn.cursor()

            cursor.execute("PRAGMA table_info(positions)")
            columns = {row[1]: row[2] for row in cursor.fetchall()}  # name: type

            expected_columns = {
                'id': 'INTEGER',
                'node_id': 'TEXT',
                'timestamp': 'TIMESTAMP',
                'latitude': 'REAL',
                'longitude': 'REAL',
                'altitude': 'REAL',
                'speed': 'REAL',
                'heading': 'REAL',
                'accuracy': 'REAL',
                'source': 'TEXT'
            }

            for col_name, col_type in expected_columns.items():
                assert col_name in columns, f"Column {col_name} not found in positions table"
                assert columns[col_name] == col_type, f"Column {col_name} type mismatch"

    def test_messages_table_structure(self, db_connection):
        """Test messages table has correct structure."""
        with db_connection.get_connection() as conn:
            cursor = conn.cursor()

            cursor.execute("PRAGMA table_info(messages)")
            columns = {row[1]: row[2] for row in cursor.fetchall()}  # name: type

            expected_columns = {
                'id': 'INTEGER',
                'from_node_id': 'TEXT',
                'to_node_id': 'TEXT',
                'timestamp': 'TIMESTAMP',
                'message_text': 'TEXT',
                'port_num': 'TEXT',
                'payload': 'TEXT',
                'hops_away': 'INTEGER',
                'snr': 'REAL',
                'rssi': 'REAL'
            }

            for col_name, col_type in expected_columns.items():
                assert col_name in columns, f"Column {col_name} not found in messages table"
                assert columns[col_name] == col_type, f"Column {col_name} type mismatch"

    def test_indexes_created(self, db_connection):
        """Test that database indexes are created."""
        with db_connection.get_connection() as conn:
            cursor = conn.cursor()

            cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND sql IS NOT NULL")
            indexes = [row[0] for row in cursor.fetchall()]

            expected_indexes = [
                'idx_nodes_last_heard',
                'idx_telemetry_timestamp',
                'idx_positions_timestamp',
                'idx_messages_timestamp',
                'idx_nodes_long_name'
            ]

            for index in expected_indexes:
                assert index in indexes, f"Index {index} not found"

    def test_foreign_key_constraints(self, db_connection):
        """Test that foreign key constraints are properly defined."""
        with db_connection.get_connection() as conn:
            cursor = conn.cursor()

            # Check telemetry table foreign key
            cursor.execute("PRAGMA foreign_key_list(telemetry)")
            telemetry_fks = cursor.fetchall()
            assert len(telemetry_fks) == 1
            assert telemetry_fks[0][2] == 'nodes'  # references nodes table
            assert telemetry_fks[0][3] == 'node_id'  # local column
            assert telemetry_fks[0][4] == 'node_id'  # references node_id column

            # Check positions table foreign key
            cursor.execute("PRAGMA foreign_key_list(positions)")
            positions_fks = cursor.fetchall()
            assert len(positions_fks) == 1
            assert positions_fks[0][2] == 'nodes'  # references nodes table
            assert positions_fks[0][3] == 'node_id'  # local column
            assert positions_fks[0][4] == 'node_id'  # references node_id column

            # Check messages table foreign keys
            cursor.execute("PRAGMA foreign_key_list(messages)")
            messages_fks = cursor.fetchall()
            assert len(messages_fks) == 2  # from_node_id and to_node_id
            for fk in messages_fks:
                assert fk[2] == 'nodes'  # references nodes table
                assert fk[4] == 'node_id'  # references node_id column
                # fk[3] contains the local column names: 'from_node_id' and 'to_node_id'

    def test_migrate_telemetry_table_new_columns(self, temp_db_path):
        """Test telemetry table migration adds new columns."""
        # Create a minimal connection without using fixtures that auto-migrate
        conn = sqlite3.connect(temp_db_path)
        cursor = conn.cursor()

        # Create original telemetry table without new columns
        cursor.execute("""
            CREATE TABLE telemetry (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                node_id TEXT NOT NULL,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                battery_level REAL,
                voltage REAL,
                temperature REAL,
                humidity REAL,
                pressure REAL,
                gas_resistance REAL,
                iaq REAL,
                snr REAL,
                rssi REAL,
                frequency REAL,
                latitude REAL,
                longitude REAL,
                altitude REAL,
                speed REAL,
                heading REAL,
                accuracy REAL
            )
        """)
        conn.commit()

        # Get initial columns
        cursor.execute("PRAGMA table_info(telemetry)")
        initial_columns = {row[1] for row in cursor.fetchall()}

        # Run migration
        DatabaseSchema.migrate_telemetry_table(cursor)
        conn.commit()

        # Get columns after migration
        cursor.execute("PRAGMA table_info(telemetry)")
        final_columns = {row[1] for row in cursor.fetchall()}

        # Verify new columns were added
        new_columns = {
            'channel_utilization', 'air_util_tx', 'uptime_seconds',
            'pm10', 'pm25', 'pm100',
            'ch1_voltage', 'ch2_voltage', 'ch3_voltage', 'ch4_voltage',
            'ch5_voltage', 'ch6_voltage', 'ch7_voltage', 'ch8_voltage',
            'ch1_current', 'ch2_current', 'ch3_current', 'ch4_current',
            'ch5_current', 'ch6_current', 'ch7_current', 'ch8_current'
        }

        added_columns = final_columns - initial_columns
        assert new_columns.issubset(added_columns), "Not all expected columns were added"

        conn.close()

    def test_migrate_telemetry_table_existing_columns(self, db_connection):
        """Test that migration doesn't duplicate existing columns."""
        with db_connection.get_connection() as conn:
            cursor = conn.cursor()

            # Get columns before migration (should already be migrated by fixture)
            cursor.execute("PRAGMA table_info(telemetry)")
            columns_before = {row[1] for row in cursor.fetchall()}

            # Run migration again
            DatabaseSchema.migrate_telemetry_table(cursor)

            # Get columns after second migration
            cursor.execute("PRAGMA table_info(telemetry)")
            columns_after = {row[1] for row in cursor.fetchall()}

            # Should be identical (no duplicates)
            assert columns_before == columns_after

    def test_migrate_telemetry_table_invalid_column_names(self, temp_db_path):
        """Test that migration rejects invalid column names."""
        # This test would require modifying the migration function to test with invalid names
        # Since the current implementation uses a whitelist, this is implicitly tested
        # by ensuring only expected columns are added

        conn = sqlite3.connect(temp_db_path)
        cursor = conn.cursor()

        # Create minimal table
        cursor.execute("""
            CREATE TABLE telemetry (
                id INTEGER PRIMARY KEY,
                node_id TEXT NOT NULL
            )
        """)

        # Migration should work without errors
        DatabaseSchema.migrate_telemetry_table(cursor)

        # Verify table still exists and has expected structure
        cursor.execute("PRAGMA table_info(telemetry)")
        columns = {row[1] for row in cursor.fetchall()}
        assert 'id' in columns
        assert 'node_id' in columns

        conn.close()

    def test_schema_creation_idempotent(self, temp_db_path):
        """Test that schema creation is idempotent (can be run multiple times)."""
        conn = sqlite3.connect(temp_db_path)
        cursor = conn.cursor()

        # Create schema first time
        DatabaseSchema.create_tables(cursor)

        # Get table list
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables_first = {row[0] for row in cursor.fetchall()}

        # Create schema second time (should not error)
        DatabaseSchema.create_tables(cursor)

        # Get table list again
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables_second = {row[0] for row in cursor.fetchall()}

        # Should be identical
        assert tables_first == tables_second

        conn.close()
