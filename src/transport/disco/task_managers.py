"""Task management utilities for Discord bot background operations.

Handles background tasks like telemetry updates, node processing, and cleanup.
"""
import asyncio
import logging
import time
from datetime import datetime
from typing import Optional

from .embed_utils import EmbedBuilder

logger = logging.getLogger(__name__)


class BackgroundTaskManager:
    """Manages background tasks for the Discord bot"""

    def __init__(self, bot, config, meshtastic, database, message_processor, packet_processor):
        self.bot = bot
        self.config = config
        self.meshtastic = meshtastic
        self.database = database
        self.message_processor = message_processor
        self.packet_processor = packet_processor

        # Task references
        self.bg_task = None
        self.telemetry_task = None

        # Track last telemetry update hour
        self.last_telemetry_hour = datetime.now().hour

    def start_tasks(self):
        """Start all background tasks"""
        if self.bot.loop:
            self.bg_task = self.bot.loop.create_task(self.background_task())
            self.telemetry_task = self.bot.loop.create_task(self.telemetry_update_task())
            logger.info("Background tasks started")

    async def stop_tasks(self):
        """Stop all background tasks"""
        tasks_to_cancel = []

        if self.bg_task and not self.bg_task.done():
            tasks_to_cancel.append(self.bg_task)

        if self.telemetry_task and not self.telemetry_task.done():
            tasks_to_cancel.append(self.telemetry_task)

        # Cancel all tasks
        for task in tasks_to_cancel:
            task.cancel()

        # Wait for cancellation
        for task in tasks_to_cancel:
            try:
                await task
            except asyncio.CancelledError:
                pass

        logger.info("Background tasks stopped")

    async def background_task(self):
        """Main background task for handling queues and processing"""
        await self.bot.wait_until_ready()

        channel = self.bot.get_channel(self.config.channel_id)
        if not channel:
            logger.error("Could not find channel with ID %s", self.config.channel_id)
            return

        logger.info("Background task started")

        # Performance counters
        last_cleanup = time.time()
        cleanup_interval = 300  # 5 minutes

        while not self.bot.is_closed():
            try:
                # Process mesh to Discord messages
                await self.message_processor.process_mesh_to_discord(
                    self.bot.mesh_to_discord, channel, self.bot.command_handler
                )

                # Process Discord to mesh messages
                await self.message_processor.process_discord_to_mesh(self.bot.discord_to_mesh)

                # Process nodes periodically
                if time.time() - self.meshtastic.last_node_refresh >= self.config.node_refresh_interval:
                    await self._process_nodes(channel)

                # Periodic cleanup
                now = time.time()
                if now - last_cleanup >= cleanup_interval:
                    await self._periodic_cleanup()
                    last_cleanup = now

                await asyncio.sleep(1)  # Check every second

            except Exception as e:
                logger.error("Error in background task: %s", e)
                await asyncio.sleep(5)

    async def telemetry_update_task(self):
        """Task for hourly telemetry updates"""
        await self.bot.wait_until_ready()

        while not self.bot.is_closed():
            try:
                current_hour = datetime.now().hour

                # Check if it's a new hour
                if current_hour != self.last_telemetry_hour:
                    await self._send_telemetry_update()
                    self.last_telemetry_hour = current_hour

                await asyncio.sleep(60)  # Check every minute

            except Exception as e:
                logger.error("Error in telemetry update task: %s", e)
                await asyncio.sleep(60)

    async def _process_nodes(self, channel):
        """Process and store nodes, announce new ones"""
        try:
            result = self.meshtastic.process_nodes()
            if result and len(result) == 2:
                processed_nodes, new_nodes = result

                logger.info("Node processing result: %s processed, %s new", len(processed_nodes), len(new_nodes))

                # Announce new nodes
                for node in new_nodes:
                    await self._announce_new_node(channel, node)
            else:
                logger.debug("No nodes processed or invalid result format")

        except Exception as e:
            logger.error("Error processing nodes: %s", e)

    async def _announce_new_node(self, channel, node):
        """Announce new node with embed"""
        try:
            embed = EmbedBuilder.create_new_node_embed(node)
            await channel.send(embed=embed)
            logger.info("Announced new node: %s", node['long_name'])

        except Exception as e:
            logger.error("Error announcing new node: %s", e)

    async def _send_telemetry_update(self):
        """Send hourly telemetry update"""
        try:
            channel = self.bot.get_channel(self.config.channel_id)
            if not channel:
                return

            try:
                summary = self.database.get_telemetry_summary(60)
                if not summary:
                    return
            except Exception as db_error:
                logger.error("Database error getting telemetry summary for update: %s", db_error)
                return

            embed = EmbedBuilder.create_telemetry_update_embed(summary)
            await channel.send(embed=embed)
            logger.info("Sent hourly telemetry update")

        except Exception as e:
            logger.error("Error sending telemetry update: %s", e)

    async def _periodic_cleanup(self):
        """Perform periodic cleanup tasks"""
        try:
            # Clear command handler cache
            if hasattr(self.bot.command_handler, 'clear_cache'):
                self.bot.command_handler.clear_cache()

            # Clean up old database data
            if hasattr(self.database, 'cleanup_old_data'):
                self.database.cleanup_old_data(30)  # Keep 30 days

            logger.debug("Periodic cleanup completed")

        except Exception as e:
            logger.error("Error during periodic cleanup: %s", e)


class PingHandler:
    """Handles ping/pong functionality"""

    def __init__(self, meshtastic):
        self.meshtastic = meshtastic

    async def handle_ping(self, message):
        """Handle ping command - send pong to mesh and announce to Discord"""
        try:
            # Create initial ping embed
            embed = EmbedBuilder.create_ping_embed(
                action="Sending Pong! to mesh network",
                description="Testing mesh network connectivity...",
                author_name=message.author.display_name
            )

            # Send initial response
            await message.channel.send(embed=embed)

            # Send pong to mesh network
            pong_sent = self.meshtastic.send_text("Pong!")
            # Small delay to prevent timing issues
            await asyncio.sleep(0.5)

            if pong_sent:
                # Send success response
                success_embed = EmbedBuilder.create_ping_success_embed(message.author.display_name)
                await message.channel.send(embed=success_embed)
                logger.info("Ping/pong handled from %s", message.author.name)
            else:
                # Send failure response
                fail_embed = EmbedBuilder.create_ping_failure_embed(message.author.display_name)
                await message.channel.send(embed=fail_embed)

        except Exception as e:
            logger.error("Error handling ping: %s", e)
            error_embed = EmbedBuilder.create_ping_error_embed(str(e), message.author.display_name)
            await message.channel.send(embed=error_embed)


class NodeProcessor:
    """Handles node-related processing and announcements"""

    def __init__(self, database, meshtastic):
        self.database = database
        self.meshtastic = meshtastic

    async def process_and_announce_nodes(self, channel):
        """Process nodes and announce new ones"""
        try:
            result = self.meshtastic.process_nodes()
            if not result or len(result) != 2:
                logger.debug("No nodes processed or invalid result format")
                return

            processed_nodes, new_nodes = result
            logger.info("Node processing result: %s processed, %s new", len(processed_nodes), len(new_nodes))

            # Announce new nodes
            for node in new_nodes:
                embed = EmbedBuilder.create_new_node_embed(node)
                await channel.send(embed=embed)
                logger.info("Announced new node: %s", node['long_name'])

        except Exception as e:
            logger.error("Error processing and announcing nodes: %s", e)


class TelemetryManager:
    """Manages telemetry updates and summaries"""

    def __init__(self, database, config):
        self.database = database
        self.config = config
        self.last_telemetry_hour = datetime.now().hour

    async def send_hourly_update(self, channel):
        """Send hourly telemetry update if it's a new hour"""
        current_hour = datetime.now().hour

        if current_hour != self.last_telemetry_hour:
            try:
                summary = self.database.get_telemetry_summary(60)
                if summary:
                    embed = EmbedBuilder.create_telemetry_update_embed(summary)
                    await channel.send(embed=embed)
                    logger.info("Sent hourly telemetry update")

                self.last_telemetry_hour = current_hour

            except Exception as e:
                logger.error("Error sending telemetry update: %s", e)

    def should_send_update(self) -> bool:
        """Check if it's time to send a telemetry update"""
        current_hour = datetime.now().hour
        return current_hour != self.last_telemetry_hour
