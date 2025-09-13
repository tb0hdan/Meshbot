"""Unified Meshtastic interface module."""
import logging
from typing import Optional, Dict, Any, List, Tuple

from src.database import MeshtasticDatabase
from .connection import MeshtasticConnection
from .messaging import MeshtasticMessaging
from .nodes import MeshtasticNodeProcessor

logger = logging.getLogger(__name__)


class MeshtasticInterface:
    """Unified Meshtastic interface using modular components"""

    def __init__(
        self, hostname: Optional[str] = None, database: Optional[MeshtasticDatabase] = None
    ):
        # Initialize modular components
        self.connection = MeshtasticConnection(hostname)
        self.messaging = MeshtasticMessaging(self.connection)
        self.node_processor = MeshtasticNodeProcessor(self.connection, database)

        # Maintain backward compatibility
        self.database = database
        self.hostname = hostname

    async def connect(self) -> bool:
        """Connect to Meshtastic radio"""
        return await self.connection.connect()

    def send_text(self, message: str, destination_id: Optional[str] = None) -> bool:
        """Send text message via Meshtastic"""
        return self.messaging.send_text(message, destination_id)

    def process_nodes(self) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Process and store nodes in database"""
        return self.node_processor.process_nodes()

    def get_nodes_from_db(self) -> List[Dict[str, Any]]:
        """Get nodes from database"""
        return self.node_processor.get_nodes_from_db()

    @property
    def iface(self):
        """Get the underlying Meshtastic interface for backward compatibility"""
        return self.connection.get_interface()

    @property
    def last_node_refresh(self):
        """Get last node refresh timestamp for backward compatibility"""
        return self.node_processor.last_node_refresh

    def is_connected(self) -> bool:
        """Check if interface is connected"""
        return self.connection.is_connected()

    def disconnect(self):
        """Disconnect from Meshtastic radio"""
        self.connection.disconnect()

