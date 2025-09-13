"""Discord bot implementation for Meshbot application.

Provides Discord client functionality for bridging with Meshtastic networks.
Handles message processing, command routing, and telemetry display.
"""
# Standard library imports
import asyncio
import logging
import queue
from typing import Optional, Dict, Any

# Third party imports
import discord
from pubsub import pub  # type: ignore[import-untyped]

# Local imports
from src.config import Config
from src.commands import CommandHandler
from .message_handlers import MessageProcessor
from .packet_processors import PacketProcessor
from .task_managers import BackgroundTaskManager, PingHandler

# Configure logging
logger = logging.getLogger(__name__)


class DiscordBot(discord.Client):
    """Enhanced Discord bot with Meshtastic integration and database"""

    def __init__(self, config: Config, meshtastic, database):
        intents = discord.Intents.default()
        intents.message_content = True

        super().__init__(intents=intents)
        self.config = config
        self.meshtastic = meshtastic
        self.database = database

        # Queues for communication with size limits
        self.mesh_to_discord: queue.Queue[Dict[str, Any]] = queue.Queue(maxsize=self.config.max_queue_size)
        self.discord_to_mesh: queue.Queue[Dict[str, Any]] = queue.Queue(maxsize=self.config.max_queue_size)

        # Initialize command handler after queues are created
        self.command_handler = CommandHandler(meshtastic, self.discord_to_mesh, database)

        # Initialize processors and managers
        self.message_processor = MessageProcessor(database, meshtastic)
        self.packet_processor = PacketProcessor(database, self.mesh_to_discord, meshtastic, self.command_handler)
        self.task_manager = BackgroundTaskManager(
            self, config, meshtastic, database, self.message_processor, self.packet_processor
        )
        self.ping_handler = PingHandler(meshtastic)

    async def setup_hook(self) -> None:
        """Setup bot when starting"""
        self.task_manager.start_tasks()

    async def on_ready(self):
        """Called when bot is ready"""
        logger.info(f'Logged in as {self.user} (ID: {self.user.id})')
        logger.info('------')

        # Setup mesh subscriptions
        await self.setup_mesh_subscriptions()

        # Connect to Meshtastic
        if not await self.meshtastic.connect():
            logger.error("Failed to connect to Meshtastic. Exiting.")
            await self.close()
            return

    async def on_message(self, message):
        """Handle incoming messages"""
        if message.author.id == self.user.id:
            return

        # Handle ping/pong functionality
        if message.content.strip().lower() == "ping":
            await self._handle_ping(message)
            return

        # Handle commands
        if message.content.startswith('$'):
            if self.command_handler:
                await self.command_handler.handle_command(message)

    async def _handle_ping(self, message):
        """Handle ping command - send pong to mesh and announce to Discord"""
        await self.ping_handler.handle_ping(message)

    async def setup_mesh_subscriptions(self):
        """Setup Meshtastic event subscriptions"""
        # Subscribe to Meshtastic events
        pub.subscribe(self.on_mesh_receive, "meshtastic.receive")
        pub.subscribe(self.on_mesh_connection, "meshtastic.connection.established")








    def on_mesh_receive(self, packet, interface):
        """Handle incoming mesh packets"""
        try:
            if 'decoded' not in packet:
                logger.debug("Received packet without decoded data")
                return

            portnum = packet['decoded']['portnum']
            from_id = packet.get('fromId', 'Unknown')
            to_id = packet.get('toId', 'Primary')
            hops_away = packet.get('hopsAway', 0)
            snr = packet.get('snr', 'N/A')
            rssi = packet.get('rssi', 'N/A')

            # Get node display name for logging
            from_name = self.database.get_node_display_name(from_id) if self.database else from_id

            # Log packet reception
            logger.info(
                "📦 PACKET RECEIVED: %s from %s (%s) -> %s | Hops: %s | SNR: %s | RSSI: %s",
                portnum, from_name, from_id, to_id, hops_away, snr, rssi
            )

            # Add to live monitor buffer
            packet_info = {
                'type': 'packet',
                'portnum': portnum,
                'from_name': from_name,
                'from_id': from_id,
                'to_id': to_id,
                'hops': hops_away,
                'snr': snr,
                'rssi': rssi
            }
            if hasattr(self, 'command_handler') and self.command_handler:
                self.command_handler.add_packet_to_buffer(packet_info)

            # Process different packet types using the packet processor
            if portnum == 'TEXT_MESSAGE_APP':
                logger.info("💬 TEXT: Processing message from %s", from_name)
                self.packet_processor.process_text_packet(packet)
            elif portnum == 'TELEMETRY_APP':
                if from_id and from_id != 'Unknown' and from_id is not None:
                    logger.info("📊 TELEMETRY: Processing sensor data from %s", from_name)
                    self.packet_processor.process_telemetry_packet(packet)
                else:
                    logger.warning("📊 TELEMETRY: Skipping packet with invalid fromId: %s", from_id)
            elif portnum == 'POSITION_APP':
                logger.info("📍 POSITION: Location update from %s", from_name)
                self.packet_processor.process_position_packet(packet)
            elif portnum == 'ROUTING_APP':
                logger.info("🛣️ ROUTING: Processing traceroute from %s", from_name)
                self.packet_processor.process_routing_packet(packet)
            elif portnum == 'NODEINFO_APP':
                logger.info("👤 NODE INFO: Node information from %s", from_name)
                # Node info packets are handled by Meshtastic library automatically
            elif portnum == 'ADMIN_APP':
                logger.info("⚙️ ADMIN: Administrative message from %s", from_name)
                # Admin packets are handled by Meshtastic library automatically
            else:
                logger.info("❓ UNKNOWN: %s packet from %s", portnum, from_name)

        except Exception as e:
            logger.error("Error processing mesh packet: %s", e)





    def on_mesh_connection(self, interface, topic=pub.AUTO_TOPIC):
        """Handle mesh connection events"""
        logger.info("Connected to Meshtastic: %s", interface.myInfo)

    async def close(self):
        """Clean shutdown of the bot"""
        try:
            logger.info("Shutting down bot...")

            # Stop background tasks
            if hasattr(self, 'task_manager'):
                await self.task_manager.stop_tasks()

            # Properly close database with all cleanup
            if self.database:
                try:
                    self.database.close()
                except Exception as db_error:
                    logger.warning("Error closing database: %s", db_error)

            # Close Meshtastic interface
            if self.meshtastic and self.meshtastic.iface:
                try:
                    self.meshtastic.iface.close()
                except Exception as e:
                    logger.warning("Error closing Meshtastic interface: %s", e)

            # Close Discord connection
            await super().close()
            logger.info("Bot shutdown complete")

        except Exception as e:
            logger.error("Error during bot shutdown: %s", e)
            await super().close()

