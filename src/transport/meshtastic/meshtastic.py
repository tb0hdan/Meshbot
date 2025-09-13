import asyncio
import logging
import time
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple

import meshtastic
import meshtastic.tcp_interface
import meshtastic.serial_interface

from src.database import MeshtasticDatabase

logger = logging.getLogger(__name__)


class MeshtasticInterface:
    """Handles Meshtastic radio communication"""

    def __init__(
        self, hostname: Optional[str] = None, database: Optional[MeshtasticDatabase] = None
    ):
        self.hostname = hostname
        self.iface = None  # Changed to match reference implementation
        self.database = database
        self.last_node_refresh = 0

    async def connect(self) -> bool:
        """Connect to Meshtastic radio"""
        try:
            if self.hostname and len(self.hostname) > 1:
                logger.info("Connecting to Meshtastic via TCP: %s", self.hostname)
                self.iface = meshtastic.tcp_interface.TCPInterface(self.hostname)
            else:
                logger.info("Connecting to Meshtastic via Serial")
                self.iface = meshtastic.serial_interface.SerialInterface()

            # Wait for connection
            await asyncio.sleep(2)

            # Check connection status more safely
            try:
                if hasattr(self.iface, 'isConnected') and callable(self.iface.isConnected):
                    if self.iface.isConnected():  # pylint: disable=not-callable
                        logger.info("Successfully connected to Meshtastic")
                        return True
                    else:
                        logger.error("Failed to connect to Meshtastic")
                        return False
                else:
                    # If isConnected method doesn't exist, assume connection is successful
                    logger.info("Connected to Meshtastic (connection status unknown)")
                    return True

            except Exception as conn_check_error:
                logger.warning("Could not check connection status: %s", conn_check_error)
                logger.info("Assuming connection is successful")
                return True

        except Exception as e:
            logger.error("Error connecting to Meshtastic: %s", e)
            return False

    def send_text(self, message: str, destination_id: Optional[str] = None) -> bool:
        """Send text message via Meshtastic"""
        try:
            if not self.iface:
                logger.error("No Meshtastic interface available")
                return False

            if destination_id:
                logger.info(
                    "Attempting to send message to node %s (type: %s)",
                    destination_id,
                    type(destination_id)
                )
                # Use the correct Meshtastic API based on the reference implementation
                try:
                    # Try the standard Meshtastic API
                    self.iface.sendText(message, destinationId=destination_id)
                    logger.info("Sent message to node %s: %s...", destination_id, message[:50])
                except Exception as e:
                    logger.error("Error sending to specific node %s: %s", destination_id, e)
                    # Fallback to primary channel
                    logger.warning("Falling back to primary channel")
                    self.iface.sendText(message)
                    return True
            else:
                self.iface.sendText(message)
                logger.info("Sent message to primary channel: %s...", message[:50])
            return True
        except Exception as e:
            logger.error("Error sending message: %s", e)
            return False

    def process_nodes(self) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Process and store nodes in database"""
        if not self.iface or not self.database:
            return [], []

        try:
            if not hasattr(self.iface, 'nodes'):
                logger.debug("Interface has no nodes attribute")
                return [], []

            nodes = self.iface.nodes
            if not nodes:
                logger.debug("No nodes available to process")
                return [], []

            processed_nodes = []
            new_nodes = []

            logger.info("Processing %s nodes from Meshtastic interface", len(nodes))

            for node_id, node_data in nodes.items():
                try:
                    # Extract node information with better error handling
                    node_info = {
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

                                    # Store in database
                    try:
                        success, is_new = self.database.add_or_update_node(node_info)
                        if success:
                            processed_nodes.append(node_info)
                            if is_new:
                                new_nodes.append(node_info)
                                logger.info(
                                    "New node added: %s (%s)", 
                                    node_info['long_name'], 
                                    node_info['node_id']
                                )
                    except Exception as db_error:
                        logger.error("Database error for node %s: %s", node_id, db_error)
                        continue

                    # Store telemetry if available - check for actual values
                    telemetry_data = {}
                    if node_data.get('snr') is not None:
                        telemetry_data['snr'] = node_data.get('snr')
                    if node_data.get('rssi') is not None:
                        telemetry_data['rssi'] = node_data.get('rssi')
                    if node_data.get('frequency') is not None:
                        telemetry_data['frequency'] = node_data.get('frequency')
                    if node_data.get('latitude') is not None:
                        telemetry_data['latitude'] = node_data.get('latitude')
                    if node_data.get('longitude') is not None:
                        telemetry_data['longitude'] = node_data.get('longitude')
                    if node_data.get('altitude') is not None:
                        telemetry_data['altitude'] = node_data.get('altitude')
                    if node_data.get('speed') is not None:
                        telemetry_data['speed'] = node_data.get('speed')
                    if node_data.get('heading') is not None:
                        telemetry_data['heading'] = node_data.get('heading')
                    if node_data.get('accuracy') is not None:
                        telemetry_data['accuracy'] = node_data.get('accuracy')

                    # Only store telemetry if we have actual data
                    if telemetry_data:
                        try:
                            self.database.add_telemetry(node_info['node_id'], telemetry_data)
                            logger.debug(
                                "Stored telemetry for %s: %s", 
                                node_info['long_name'], 
                                telemetry_data
                            )
                        except Exception as telemetry_error:
                            logger.error(
                                "Error storing telemetry for node %s: %s", 
                                node_id, 
                                telemetry_error
                            )

                    # Store position if available
                    if (node_data.get('latitude') is not None and
                        node_data.get('longitude') is not None):
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
                            self.database.add_position(node_info['node_id'], position_data)
                            logger.debug("Stored position for %s", node_info['long_name'])
                        except Exception as position_error:
                            logger.error(
                                "Error storing position for node %s: %s", 
                                node_id, 
                                position_error
                            )

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