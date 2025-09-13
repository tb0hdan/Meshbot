"""Meshtastic messaging module."""
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class MeshtasticMessaging:
    """Handles Meshtastic message sending functionality"""

    def __init__(self, connection):
        self.connection = connection

    def send_text(self, message: str, destination_id: Optional[str] = None) -> bool:
        """Send text message via Meshtastic"""
        try:
            iface = self.connection.get_interface()
            if not iface:
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
                    iface.sendText(message, destinationId=destination_id)
                    logger.info("Sent message to node %s: %s...", destination_id, message[:50])
                except Exception as e:
                    logger.error("Error sending to specific node %s: %s", destination_id, e)
                    # Fallback to primary channel
                    logger.warning("Falling back to primary channel")
                    iface.sendText(message)
                    return True
            else:
                iface.sendText(message)
                logger.info("Sent message to primary channel: %s...", message[:50])
            return True
        except Exception as e:
            logger.error("Error sending message: %s", e)
            return False

    def is_ready(self) -> bool:
        """Check if messaging is ready (connection is available)"""
        return self.connection.is_connected()

