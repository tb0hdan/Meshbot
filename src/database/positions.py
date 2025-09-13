"""
Position database operations module
Handles all position-related database operations
"""
# pylint: disable=duplicate-code

import sqlite3
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


class PositionOperations:
    """Handles all position-related database operations"""

    def __init__(self, connection_manager):
        self.connection_manager = connection_manager

    def add_position(self, node_id: str, position_data: Dict[str, Any]) -> bool:
        """Add position data for a node"""
        try:
            with self.connection_manager.get_connection() as conn:
                cursor = conn.cursor()

                cursor.execute("""
                    INSERT INTO positions (
                        node_id, latitude, longitude, altitude, speed, heading, accuracy, source
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    node_id,
                    position_data.get('latitude'),
                    position_data.get('longitude'),
                    position_data.get('altitude'),
                    position_data.get('speed'),
                    position_data.get('heading'),
                    position_data.get('accuracy'),
                    position_data.get('source', 'unknown')
                ))

                return True

        except sqlite3.OperationalError as e:
            logger.error("Database operational error adding position: %s", e)
            return False
        except sqlite3.Error as e:
            logger.error("Database error adding position: %s", e)
            return False
        except (KeyError, ValueError, TypeError) as e:
            logger.error("Unexpected error adding position: %s", e)
            return False

    def get_last_position(self, node_id: str) -> Optional[Dict[str, Any]]:
        """Get the last known position for a node"""
        try:
            with self.connection_manager.get_connection() as conn:
                cursor = conn.cursor()

                cursor.execute("""
                    SELECT latitude, longitude, altitude, speed, heading, accuracy, source, timestamp
                    FROM positions
                    WHERE node_id = ?
                    ORDER BY timestamp DESC, id DESC
                    LIMIT 1
                """, (node_id,))

                row = cursor.fetchone()
                if row:
                    return {
                        'latitude': row[0],
                        'longitude': row[1],
                        'altitude': row[2],
                        'speed': row[3],
                        'heading': row[4],
                        'accuracy': row[5],
                        'source': row[6],
                        'timestamp': row[7]
                    }
                return None

        except sqlite3.OperationalError as e:
            logger.error("Database operational error getting last position: %s", e)
            return None
        except sqlite3.Error as e:
            logger.error("Database error getting last position: %s", e)
            return None
        except (ValueError, TypeError) as e:
            logger.error("Unexpected error getting last position: %s", e)
            return None
