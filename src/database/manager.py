"""
Main database manager module
Coordinates all database operations and provides a unified interface
"""

import sqlite3
import logging
from typing import Dict, Any, List, Optional, Tuple

from .connection import DatabaseConnection
from .schema import DatabaseSchema
from .nodes import NodeOperations
from .telemetry import TelemetryOperations
from .positions import PositionOperations
from .messages import MessageOperations
from .maintenance import DatabaseMaintenance

logger = logging.getLogger(__name__)


class MeshtasticDatabase:
    """Main database manager that coordinates all database operations"""

    def __init__(self, db_path: str = "meshtastic.db"):
        self.db_path = db_path
        
        # Initialize connection manager
        self.connection_manager = DatabaseConnection(db_path)
        
        # Initialize operation modules
        self.nodes = NodeOperations(self.connection_manager)
        self.telemetry = TelemetryOperations(self.connection_manager)
        self.positions = PositionOperations(self.connection_manager)
        self.messages = MessageOperations(self.connection_manager)
        self.maintenance = DatabaseMaintenance(self.connection_manager)
        
        # Initialize database and start maintenance
        self.init_database()
        self.maintenance.start_maintenance_task()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def init_database(self):
        """Initialize database tables with WAL mode and optimizations"""
        try:
            with self.connection_manager.get_connection() as conn:
                cursor = conn.cursor()

                # Enable WAL mode for better concurrency
                cursor.execute("PRAGMA journal_mode = WAL")
                cursor.execute("PRAGMA synchronous = NORMAL")
                cursor.execute("PRAGMA cache_size = -2000")  # 2MB cache
                cursor.execute("PRAGMA temp_store = MEMORY")
                cursor.execute("PRAGMA mmap_size = 268435456")  # 256MB
                cursor.execute("PRAGMA optimize")

                # Create all tables and indexes
                DatabaseSchema.create_tables(cursor)

                # Migrate existing telemetry table to add new columns
                DatabaseSchema.migrate_telemetry_table(cursor)

                logger.info("Database initialized successfully with WAL mode")

        except sqlite3.OperationalError as e:
            logger.error("Database operational error: %s", e)
            raise
        except sqlite3.Error as e:
            logger.error("Database error: %s", e)
            raise
        except Exception as e:
            logger.error("Unexpected error initializing database: %s", e)
            raise

    # Node operations - delegate to nodes module
    def add_or_update_node(self, node_data: Dict[str, Any]) -> Tuple[bool, bool]:
        """Add new node or update existing node information"""
        return self.nodes.add_or_update_node(node_data)

    def get_active_nodes(self, minutes: int = 60) -> List[Dict[str, Any]]:
        """Get nodes active in the last N minutes"""
        return self.nodes.get_active_nodes(minutes)

    def get_all_nodes(self) -> List[Dict[str, Any]]:
        """Get all known nodes"""
        return self.nodes.get_all_nodes()

    def find_node_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """Find node by fuzzy matching on long name"""
        return self.nodes.find_node_by_name(name)

    def get_node_display_name(self, node_id: str) -> str:
        """Return the best human-friendly name for a node_id"""
        return self.nodes.get_node_display_name(node_id)

    # Telemetry operations - delegate to telemetry module
    def add_telemetry(self, node_id: str, telemetry_data: Dict[str, Any]) -> bool:
        """Add telemetry data for a node"""
        return self.telemetry.add_telemetry(node_id, telemetry_data)

    def get_telemetry_summary(self, minutes: int = 60) -> Dict[str, Any]:
        """Get telemetry summary for active nodes"""
        return self.telemetry.get_telemetry_summary(minutes)

    def get_telemetry_history(
        self, node_id: str, hours: int = 24, limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get telemetry history for a specific node"""
        return self.telemetry.get_telemetry_history(node_id, hours, limit)

    # Position operations - delegate to positions module
    def add_position(self, node_id: str, position_data: Dict[str, Any]) -> bool:
        """Add position data for a node"""
        return self.positions.add_position(node_id, position_data)

    def get_last_position(self, node_id: str) -> Optional[Dict[str, Any]]:
        """Get the last known position for a node"""
        return self.positions.get_last_position(node_id)

    # Message operations - delegate to messages module
    def add_message(self, message_data: Dict[str, Any]) -> bool:
        """Add message to database"""
        return self.messages.add_message(message_data)

    def get_network_topology(self) -> Dict[str, Any]:
        """Get network topology information"""
        return self.messages.get_network_topology()

    def get_message_statistics(self, hours: int = 24) -> Dict[str, Any]:
        """Get message statistics for the specified time period"""
        return self.messages.get_message_statistics(hours)

    # Maintenance operations - delegate to maintenance module
    def cleanup_old_data(self, days: int = 30):
        """Clean up old telemetry and position data"""
        self.maintenance.cleanup_old_data(days)

    # Connection management
    def close_connections(self):
        """Close all connections in the pool"""
        self.connection_manager.close_all_connections()

    def close(self):
        """Clean shutdown of database resources"""
        try:
            # Stop maintenance task
            self.maintenance.stop_maintenance()

            # Close all connections
            self.close_connections()

            logger.info("Database shutdown complete")
        except Exception as e:
            logger.error("Error during database shutdown: %s", e)