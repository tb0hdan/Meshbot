"""Meshtastic node processing module."""
import logging
import time
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple

from src.database import MeshtasticDatabase

logger = logging.getLogger(__name__)


class MeshtasticNodeProcessor:
    """Handles Meshtastic node processing and database operations"""

    def __init__(self, connection, database: Optional[MeshtasticDatabase] = None):
        self.connection = connection
        self.database = database
        self.last_node_refresh = 0.0

    def process_nodes(self) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Process and store nodes in database"""
        iface = self.connection.get_interface()
        if not iface or not self.database:
            return [], []

        try:
            if not hasattr(iface, 'nodes'):
                logger.debug("Interface has no nodes attribute")
                return [], []

            nodes = iface.nodes
            if not nodes:
                logger.debug("No nodes available to process")
                return [], []

            processed_nodes = []
            new_nodes = []

            logger.info("Processing %s nodes from Meshtastic interface", len(nodes))

            for node_id, node_data in nodes.items():
                try:
                    # Extract node information with better error handling
                    node_info = self._extract_node_info(node_id, node_data)

                    # Store in database
                    success, is_new = self._store_node_in_database(node_info)
                    if success:
                        processed_nodes.append(node_info)
                        if is_new:
                            new_nodes.append(node_info)
                            logger.info(
                                "New node added: %s (%s)",
                                node_info['long_name'],
                                node_info['node_id']
                            )

                        # Store additional data (telemetry, position)
                        self._store_additional_data(node_info['node_id'], node_data)

                except Exception as e:
                    logger.error("Error processing node %s: %s", node_id, e)
                    continue

            self.last_node_refresh = time.time()
            logger.info("Processed %s nodes, %s new", len(processed_nodes), len(new_nodes))
            return processed_nodes, new_nodes

        except Exception as e:
            logger.error("Error processing nodes: %s", e)
            return [], []

    def get_nodes_from_db(self) -> List[Dict[str, Any]]:
        """Get nodes from database"""
        if not self.database:
            return []
        try:
            nodes = self.database.get_all_nodes()
            logger.debug("Retrieved %s nodes from database", len(nodes))
            return nodes
        except Exception as e:
            logger.error("Error getting nodes from database: %s", e)
            return []

    def _extract_node_info(self, node_id: str, node_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract node information from raw node data"""
        return {
            'node_id': str(node_id),
            'node_num': node_data.get('num'),
            'long_name': str(
                node_data.get('user', {}).get('longName', 'Unknown')
            ),
            'short_name': str(
                node_data.get('user', {}).get('shortName', '')
            ),
            'macaddr': node_data.get('macaddr'),
            'hw_model': node_data.get('hwModel'),
            'firmware_version': node_data.get('firmwareVersion'),
            'last_heard': datetime.fromtimestamp(
                node_data.get('lastHeard', time.time())
            ).isoformat(),
            'hops_away': node_data.get('hopsAway', 0),
            'is_router': node_data.get('isRouter', False),
            'is_client': node_data.get('isClient', True)
        }

    def _store_node_in_database(self, node_info: Dict[str, Any]) -> Tuple[bool, bool]:
        """Store node in database and return (success, is_new)"""
        if not self.database:
            return False, False
        try:
            return self.database.add_or_update_node(node_info)
        except Exception as db_error:
            logger.error("Database error for node %s: %s", node_info['node_id'], db_error)
            return False, False

    def _store_additional_data(self, node_id: str, node_data: Dict[str, Any]):
        """Store telemetry and position data for a node"""
        self._store_telemetry_data(node_id, node_data)
        self._store_position_data(node_id, node_data)

    def _store_telemetry_data(self, node_id: str, node_data: Dict[str, Any]):
        """Store telemetry data if available"""
        telemetry_data = {}

        # Check for actual telemetry values
        telemetry_fields = ['snr', 'rssi', 'frequency', 'latitude', 'longitude',
                           'altitude', 'speed', 'heading', 'accuracy']

        for field in telemetry_fields:
            if node_data.get(field) is not None:
                telemetry_data[field] = node_data.get(field)

        # Only store telemetry if we have actual data
        if telemetry_data and self.database:
            try:
                self.database.add_telemetry(node_id, telemetry_data)
                logger.debug("Stored telemetry for %s: %s", node_id, telemetry_data)
            except Exception as telemetry_error:
                logger.error("Error storing telemetry for node %s: %s", node_id, telemetry_error)

    def _store_position_data(self, node_id: str, node_data: Dict[str, Any]):
        """Store position data if available"""
        if (node_data.get('latitude') is not None and
            node_data.get('longitude') is not None and
            self.database):
            try:
                position_data = {
                    'latitude': node_data.get('latitude'),
                    'longitude': node_data.get('longitude'),
                    'altitude': node_data.get('altitude'),
                    'speed': node_data.get('speed'),
                    'heading': node_data.get('heading'),
                    'accuracy': node_data.get('accuracy'),
                    'source': 'meshtastic'
                }
                self.database.add_position(node_id, position_data)
                logger.debug("Stored position for %s", node_id)
            except Exception as position_error:
                logger.error("Error storing position for node %s: %s", node_id, position_error)

