"""Meshtastic connection management module."""
import asyncio
import logging
from typing import Optional

import meshtastic  # type: ignore[import-untyped]
import meshtastic.tcp_interface  # type: ignore[import-untyped]
import meshtastic.serial_interface  # type: ignore[import-untyped]

logger = logging.getLogger(__name__)


class MeshtasticConnection:
    """Handles Meshtastic radio connection management"""

    def __init__(self, hostname: Optional[str] = None):
        self.hostname = hostname
        self.iface = None

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
                if (self.iface is not None and
                    hasattr(self.iface, 'isConnected') and
                    callable(self.iface.isConnected)):
                    if self.iface.isConnected():  # pylint: disable=not-callable
                        logger.info("Successfully connected to Meshtastic")
                        return True
                    logger.error("Failed to connect to Meshtastic")
                    return False
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

    def is_connected(self) -> bool:
        """Check if interface is connected"""
        if not self.iface:
            return False

        try:
            if hasattr(self.iface, 'isConnected') and callable(self.iface.isConnected):
                return self.iface.isConnected()  # pylint: disable=not-callable
            # If isConnected method doesn't exist, assume connected if iface exists
            return True
        except Exception:
            return False

    def get_interface(self):
        """Get the underlying Meshtastic interface"""
        return self.iface

    def disconnect(self):
        """Disconnect from Meshtastic radio"""
        if self.iface:
            try:
                if hasattr(self.iface, 'close'):
                    self.iface.close()
                logger.info("Disconnected from Meshtastic")
            except Exception as e:
                logger.error("Error disconnecting from Meshtastic: %s", e)
            finally:
                self.iface = None

