"""
Database schema definitions for Meshtastic bot
Contains all table creation and migration logic
"""

import sqlite3
import logging
from typing import List, Tuple

logger = logging.getLogger(__name__)


class DatabaseSchema:
    """Handles database schema creation and migration"""

    @staticmethod
    def create_tables(cursor: sqlite3.Cursor):
        """Create all database tables"""
        DatabaseSchema._create_nodes_table(cursor)
        DatabaseSchema._create_telemetry_table(cursor)
        DatabaseSchema._create_positions_table(cursor)
        DatabaseSchema._create_messages_table(cursor)
        DatabaseSchema._create_indexes(cursor)

    @staticmethod
    def _create_nodes_table(cursor: sqlite3.Cursor):
        """Create nodes table"""
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS nodes (
                node_id TEXT PRIMARY KEY,
                node_num INTEGER,
                long_name TEXT NOT NULL,
                short_name TEXT,
                macaddr TEXT,
                hw_model TEXT,
                firmware_version TEXT,
                first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_heard TIMESTAMP,
                hops_away INTEGER DEFAULT 0,
                is_router BOOLEAN DEFAULT FALSE,
                is_client BOOLEAN DEFAULT TRUE
            )
        """)

    @staticmethod
    def _create_telemetry_table(cursor: sqlite3.Cursor):
        """Create telemetry table"""
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS telemetry (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                node_id TEXT NOT NULL,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                -- Device metrics
                battery_level REAL,
                voltage REAL,
                channel_utilization REAL,
                air_util_tx REAL,
                uptime_seconds REAL,
                -- Environment metrics
                temperature REAL,
                humidity REAL,
                pressure REAL,
                gas_resistance REAL,
                iaq REAL,
                -- Air quality metrics
                pm10 REAL,
                pm25 REAL,
                pm100 REAL,
                -- Power metrics
                ch1_voltage REAL,
                ch2_voltage REAL,
                ch3_voltage REAL,
                ch4_voltage REAL,
                ch5_voltage REAL,
                ch6_voltage REAL,
                ch7_voltage REAL,
                ch8_voltage REAL,
                ch1_current REAL,
                ch2_current REAL,
                ch3_current REAL,
                ch4_current REAL,
                ch5_current REAL,
                ch6_current REAL,
                ch7_current REAL,
                ch8_current REAL,
                -- Radio metrics
                snr REAL,
                rssi REAL,
                frequency REAL,
                -- Position data
                latitude REAL,
                longitude REAL,
                altitude REAL,
                speed REAL,
                heading REAL,
                accuracy REAL,
                FOREIGN KEY (node_id) REFERENCES nodes (node_id)
            )
        """)

    @staticmethod
    def _create_positions_table(cursor: sqlite3.Cursor):
        """Create positions table"""
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS positions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                node_id TEXT NOT NULL,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                latitude REAL,
                longitude REAL,
                altitude REAL,
                speed REAL,
                heading REAL,
                accuracy REAL,
                source TEXT,
                FOREIGN KEY (node_id) REFERENCES nodes (node_id)
            )
        """)

    @staticmethod
    def _create_messages_table(cursor: sqlite3.Cursor):
        """Create messages table"""
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                from_node_id TEXT,
                to_node_id TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                message_text TEXT,
                port_num TEXT,
                payload TEXT,
                hops_away INTEGER,
                snr REAL,
                rssi REAL,
                FOREIGN KEY (from_node_id) REFERENCES nodes (node_id),
                FOREIGN KEY (to_node_id) REFERENCES nodes (node_id)
            )
        """)

    @staticmethod
    def _create_indexes(cursor: sqlite3.Cursor):
        """Create database indexes for better performance"""
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_nodes_last_heard ON nodes (last_heard)",
            "CREATE INDEX IF NOT EXISTS idx_telemetry_timestamp ON telemetry (timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_positions_timestamp ON positions (timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_messages_timestamp ON messages (timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_nodes_long_name ON nodes (long_name)"
        ]
        
        for index_sql in indexes:
            cursor.execute(index_sql)

    @staticmethod
    def migrate_telemetry_table(cursor: sqlite3.Cursor):
        """Migrate telemetry table to add new sensor columns"""
        try:
            # Get current table schema
            cursor.execute("PRAGMA table_info(telemetry)")
            columns = [row[1] for row in cursor.fetchall()]

            # New columns to add - use whitelist of safe column names
            new_columns: List[Tuple[str, str]] = [
                ('channel_utilization', 'REAL'),
                ('air_util_tx', 'REAL'),
                ('uptime_seconds', 'REAL'),
                ('pm10', 'REAL'),
                ('pm25', 'REAL'),
                ('pm100', 'REAL'),
                ('ch1_voltage', 'REAL'),
                ('ch2_voltage', 'REAL'),
                ('ch3_voltage', 'REAL'),
                ('ch4_voltage', 'REAL'),
                ('ch5_voltage', 'REAL'),
                ('ch6_voltage', 'REAL'),
                ('ch7_voltage', 'REAL'),
                ('ch8_voltage', 'REAL'),
                ('ch1_current', 'REAL'),
                ('ch2_current', 'REAL'),
                ('ch3_current', 'REAL'),
                ('ch4_current', 'REAL'),
                ('ch5_current', 'REAL'),
                ('ch6_current', 'REAL'),
                ('ch7_current', 'REAL'),
                ('ch8_current', 'REAL')
            ]

            # Add missing columns - using parameterized approach
            for column_name, column_type in new_columns:
                if column_name not in columns:
                    # Validate column name contains only alphanumeric and underscore
                    if not column_name.replace('_', '').isalnum():
                        logger.warning("Skipping invalid column name: %s", column_name)
                        continue
                    # Use safe SQL construction
                    sql = f"ALTER TABLE telemetry ADD COLUMN {column_name} {column_type}"
                    cursor.execute(sql)
                    logger.info("Added column %s to telemetry table", column_name)

        except (sqlite3.Error, ValueError) as e:
            logger.error("Error migrating telemetry table: %s", e)
            # Don't raise - this is a migration, not critical