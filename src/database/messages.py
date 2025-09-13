"""
Message database operations module
Handles all message-related database operations
"""
# pylint: disable=duplicate-code

import sqlite3
import logging
from typing import Dict, Any
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class MessageOperations:
    """Handles all message-related database operations"""

    def __init__(self, connection_manager):
        self.connection_manager = connection_manager

    def add_message(self, message_data: Dict[str, Any]) -> bool:
        """Add message to database"""
        try:
            with self.connection_manager.get_connection() as conn:
                cursor = conn.cursor()

                cursor.execute("""
                    INSERT INTO messages (
                        from_node_id, to_node_id, message_text, port_num, payload,
                        hops_away, snr, rssi
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    message_data.get('from_node_id'),
                    message_data.get('to_node_id'),
                    message_data.get('message_text'),
                    message_data.get('port_num'),
                    message_data.get('payload'),
                    message_data.get('hops_away'),
                    message_data.get('snr'),
                    message_data.get('rssi')
                ))

                return True

        except sqlite3.OperationalError as e:
            logger.error("Database operational error adding message: %s", e)
            return False
        except sqlite3.Error as e:
            logger.error("Database error adding message: %s", e)
            return False
        except (KeyError, ValueError, TypeError) as e:
            logger.error("Unexpected error adding message: %s", e)
            return False

    def get_network_topology(self) -> Dict[str, Any]:
        """Get network topology information"""
        try:
            with self.connection_manager.get_connection() as conn:
                cursor = conn.cursor()

                # Get node connections based on message routing
                cursor.execute("""
                    SELECT
                        from_node_id,
                        to_node_id,
                        COUNT(*) as message_count,
                        AVG(hops_away) as avg_hops,
                        AVG(snr) as avg_snr,
                        MAX(timestamp) as last_communication
                    FROM messages
                    WHERE timestamp > datetime('now', '-24 hours')
                    GROUP BY from_node_id, to_node_id
                    HAVING message_count > 0
                    ORDER BY message_count DESC
                """)

                connections = []
                for row in cursor.fetchall():
                    connections.append({
                        'from_node': row[0],
                        'to_node': row[1],
                        'message_count': row[2],
                        'avg_hops': row[3],
                        'avg_snr': row[4],
                        'last_communication': row[5]
                    })

                # Get node statistics
                cursor.execute("""
                    SELECT
                        COUNT(*) as total_nodes,
                        COUNT(CASE WHEN last_heard > datetime('now', '-1 hour') THEN 1 END) as active_nodes,
                        COUNT(CASE WHEN is_router = 1 THEN 1 END) as router_nodes,
                        AVG(hops_away) as avg_hops
                    FROM nodes
                """)

                stats = cursor.fetchone()

                return {
                    'connections': connections,
                    'total_nodes': stats[0] or 0,
                    'active_nodes': stats[1] or 0,
                    'router_nodes': stats[2] or 0,
                    'avg_hops': stats[3] or 0
                }

        except (sqlite3.Error, ValueError, TypeError) as e:
            logger.error("Error getting network topology: %s", e)
            return {
                'connections': [],
                'total_nodes': 0,
                'active_nodes': 0,
                'router_nodes': 0,
                'avg_hops': 0
            }

    def get_message_statistics(self, hours: int = 24) -> Dict[str, Any]:
        """Get message statistics for the specified time period"""
        try:
            with self.connection_manager.get_connection() as conn:
                cursor = conn.cursor()

                if hours > 0:
                    cutoff_time = datetime.now() - timedelta(hours=hours)
                    time_filter = "WHERE timestamp > ?"
                    params: tuple[str, ...] = (cutoff_time.isoformat(),)
                else:
                    # hours=0 means no time filter
                    time_filter = ""
                    params = ()

                cursor.execute(f"""
                    SELECT
                        COUNT(*) as total_messages,
                        COUNT(DISTINCT from_node_id) as unique_senders,
                        COUNT(DISTINCT to_node_id) as unique_recipients,
                        AVG(hops_away) as avg_hops,
                        AVG(snr) as avg_snr,
                        AVG(rssi) as avg_rssi
                    FROM messages
                    {time_filter}
                """, params)

                stats = cursor.fetchone()

                # Get hourly message distribution
                cursor.execute(f"""
                    SELECT
                        strftime('%H', timestamp) as hour,
                        COUNT(*) as message_count
                    FROM messages
                    {time_filter}
                    GROUP BY strftime('%H', timestamp)
                    ORDER BY hour
                """, params)

                hourly_distribution = {row[0]: row[1] for row in cursor.fetchall()}

                return {
                    'total_messages': stats[0] or 0,
                    'unique_senders': stats[1] or 0,
                    'unique_recipients': stats[2] or 0,
                    'avg_hops': stats[3] or 0,
                    'avg_snr': stats[4] or 0,
                    'avg_rssi': stats[5] or 0,
                    'hourly_distribution': hourly_distribution
                }

        except (sqlite3.Error, ValueError, TypeError) as e:
            logger.error("Error getting message statistics: %s", e)
            return {}
