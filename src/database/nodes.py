"""
Node database operations module
Handles all node-related database operations
"""

import sqlite3
import logging
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)


class NodeOperations:
    """Handles all node-related database operations"""

    def __init__(self, connection_manager):
        self.connection_manager = connection_manager

    def add_or_update_node(self, node_data: Dict[str, Any]) -> Tuple[bool, bool]:
        """Add new node or update existing node information"""
        try:
            with self.connection_manager.get_connection() as conn:
                cursor = conn.cursor()

                # Check if node exists
                cursor.execute(
                    "SELECT node_id FROM nodes WHERE node_id = ?",
                    (node_data['node_id'],)
                )
                exists = cursor.fetchone()

                if exists:
                    # Update existing node
                    cursor.execute("""
                        UPDATE nodes SET
                            node_num = ?,
                            long_name = ?,
                            short_name = ?,
                            macaddr = ?,
                            hw_model = ?,
                            firmware_version = ?,
                            last_seen = CURRENT_TIMESTAMP,
                            last_heard = ?,
                            hops_away = ?,
                            is_router = ?,
                            is_client = ?
                        WHERE node_id = ?
                    """, (
                        node_data.get('node_num'),
                        node_data.get('long_name', 'Unknown'),
                        node_data.get('short_name'),
                        node_data.get('macaddr'),
                        node_data.get('hw_model'),
                        node_data.get('firmware_version'),
                        node_data.get('last_heard'),
                        node_data.get('hops_away') if node_data.get('hops_away') is not None
                        else 0,
                        node_data.get('is_router') if node_data.get('is_router') is not None
                        else False,
                        node_data.get('is_client') if node_data.get('is_client') is not None
                        else True,
                        node_data['node_id']
                    ))

                    # Check if this is a new node (first time seen)
                    cursor.execute(
                        "SELECT first_seen FROM nodes WHERE node_id = ?",
                        (node_data['node_id'],)
                    )
                    first_seen = cursor.fetchone()[0]

                    if first_seen == node_data.get('last_heard'):
                        # This is a new node
                        return True, True  # (success, is_new_node)

                else:
                    # Insert new node
                    cursor.execute("""
                        INSERT INTO nodes (
                            node_id, node_num, long_name, short_name, macaddr,
                            hw_model, firmware_version, last_heard, hops_away,
                            is_router, is_client
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        node_data['node_id'],
                        node_data.get('node_num'),
                        node_data.get('long_name', 'Unknown'),
                        node_data.get('short_name'),
                        node_data.get('macaddr'),
                        node_data.get('hw_model'),
                        node_data.get('firmware_version'),
                        node_data.get('last_heard'),
                        node_data.get('hops_away') if node_data.get('hops_away') is not None
                        else 0,
                        node_data.get('is_router') if node_data.get('is_router') is not None
                        else False,
                        node_data.get('is_client') if node_data.get('is_client') is not None
                        else True
                    ))
                    return True, True  # (success, is_new_node)

                return True, False  # (success, not_new_node)

        except sqlite3.OperationalError as e:
            logger.error("Database operational error adding/updating node: %s", e)
            return False, False
        except sqlite3.Error as e:
            logger.error("Database error adding/updating node: %s", e)
            return False, False
        except (KeyError, ValueError, TypeError) as e:
            logger.error("Unexpected error adding/updating node: %s", e)
            return False, False

    def get_active_nodes(self, minutes: int = 60) -> List[Dict[str, Any]]:
        """Get nodes active in the last N minutes"""
        try:
            with self.connection_manager.get_connection() as conn:
                cursor = conn.cursor()

                cutoff_time = datetime.now(timezone.utc) - timedelta(minutes=minutes)

                cursor.execute("""
                    SELECT n.*,
                           t.battery_level, t.voltage, t.temperature, t.humidity,
                           t.pressure, t.gas_resistance, t.iaq, t.snr, t.rssi,
                           p.latitude, p.longitude, p.altitude, p.speed, p.heading
                    FROM nodes n
                    LEFT JOIN (
                        SELECT node_id, battery_level, voltage, temperature, humidity,
                               pressure, gas_resistance, iaq, snr, rssi
                        FROM telemetry
                        WHERE timestamp = (
                            SELECT MAX(timestamp) FROM telemetry t2
                            WHERE t2.node_id = telemetry.node_id
                        )
                    ) t ON n.node_id = t.node_id
                    LEFT JOIN (
                        SELECT node_id, latitude, longitude, altitude, speed, heading
                        FROM positions
                        WHERE timestamp = (
                            SELECT MAX(timestamp) FROM positions p2
                            WHERE p2.node_id = positions.node_id
                        )
                    ) p ON n.node_id = p.node_id
                    WHERE n.last_heard > ?
                    ORDER BY n.last_heard DESC
                """, (cutoff_time.isoformat(),))

                columns = [description[0] for description in cursor.description]
                rows = cursor.fetchall()

                return [dict(zip(columns, row)) for row in rows]

        except sqlite3.OperationalError as e:
            logger.error("Database operational error getting active nodes: %s", e)
            return []
        except sqlite3.Error as e:
            logger.error("Database error getting active nodes: %s", e)
            return []
        except (ValueError, TypeError) as e:
            logger.error("Unexpected error getting active nodes: %s", e)
            return []

    def get_all_nodes(self) -> List[Dict[str, Any]]:
        """Get all known nodes"""
        try:
            with self.connection_manager.get_connection() as conn:
                cursor = conn.cursor()

                cursor.execute("""
                    SELECT n.*,
                           t.battery_level, t.voltage, t.temperature, t.humidity,
                           t.pressure, t.gas_resistance, t.iaq, t.snr, t.rssi,
                           p.latitude, p.longitude, p.altitude, p.speed, p.heading
                    FROM nodes n
                    LEFT JOIN (
                        SELECT node_id, battery_level, voltage, temperature, humidity,
                               pressure, gas_resistance, iaq, snr, rssi
                        FROM telemetry
                        WHERE timestamp = (
                            SELECT MAX(timestamp) FROM telemetry t2
                            WHERE t2.node_id = telemetry.node_id
                        )
                    ) t ON n.node_id = t.node_id
                    LEFT JOIN (
                        SELECT node_id, latitude, longitude, altitude, speed, heading
                        FROM positions
                        WHERE timestamp = (
                            SELECT MAX(timestamp) FROM positions p2
                            WHERE p2.node_id = positions.node_id
                        )
                    ) p ON n.node_id = p.node_id
                    ORDER BY n.last_heard DESC
                """)

                columns = [description[0] for description in cursor.description]
                rows = cursor.fetchall()

                return [dict(zip(columns, row)) for row in rows]

        except sqlite3.OperationalError as e:
            logger.error("Database operational error getting all nodes: %s", e)
            return []
        except sqlite3.Error as e:
            logger.error("Database error getting all nodes: %s", e)
            return []
        except (ValueError, TypeError) as e:
            logger.error("Unexpected error getting all nodes: %s", e)
            return []

    def find_node_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """Find node by fuzzy matching on long name"""
        try:
            with self.connection_manager.get_connection() as conn:
                cursor = conn.cursor()

                # Try exact match first
                cursor.execute("""
                    SELECT * FROM nodes WHERE long_name = ? OR short_name = ?
                """, (name, name))
                result = cursor.fetchone()

                if result:
                    columns = [description[0] for description in cursor.description]
                    return dict(zip(columns, result))

                # Try partial match
                cursor.execute("""
                    SELECT * FROM nodes WHERE long_name LIKE ? OR short_name LIKE ?
                    ORDER BY
                        CASE
                            WHEN long_name = ? THEN 1
                            WHEN long_name LIKE ? THEN 2
                            WHEN short_name = ? THEN 3
                            WHEN short_name LIKE ? THEN 4
                            ELSE 5
                        END,
                        last_heard DESC
                    LIMIT 1
                """, (f"%{name}%", f"%{name}%", name, f"{name}%", name, f"{name}%"))

                result = cursor.fetchone()
                if result:
                    columns = [description[0] for description in cursor.description]
                    return dict(zip(columns, result))

                return None

        except sqlite3.OperationalError as e:
            logger.error("Database operational error finding node by name: %s", e)
            return None
        except sqlite3.Error as e:
            logger.error("Database error finding node by name: %s", e)
            return None
        except (ValueError, TypeError) as e:
            logger.error("Unexpected error finding node by name: %s", e)
            return None

    def get_node_display_name(self, node_id: str) -> str:
        """Return the best human-friendly name for a node_id (long_name > short_name > node_id)"""
        try:
            with self.connection_manager.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    SELECT
                        CASE
                            WHEN long_name IS NOT NULL AND TRIM(long_name) <> '' THEN long_name
                            WHEN short_name IS NOT NULL AND TRIM(short_name) <> '' THEN short_name
                            ELSE node_id
                        END AS display_name
                    FROM nodes WHERE node_id = ?
                    """,
                    (node_id,)
                )
                row = cursor.fetchone()
                if row and row[0]:
                    return str(row[0])
        except (sqlite3.Error, ValueError, TypeError) as e:
            logger.warning("Failed to lookup display name for %s: %s", node_id, e)
        return str(node_id)
