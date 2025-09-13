"""Modular command handler for Meshbot Discord bot.

Handles parsing and execution of Discord commands for Meshtastic network interaction.
"""
import asyncio
import logging
import queue
import time
from typing import Dict, Any

import discord

from src.database import MeshtasticDatabase
from .basic import BasicCommands
from .monitoring import MonitoringCommands
from .network import NetworkCommands
from .debug import DebugCommands

logger = logging.getLogger(__name__)


class CommandHandler:
    """Handles Discord bot commands with modular command structure"""

    def __init__(
        self,
        meshtastic,
        discord_to_mesh: queue.Queue,
        database: MeshtasticDatabase
    ):
        self.meshtastic = meshtastic
        self.discord_to_mesh = discord_to_mesh
        self.database = database

        # Initialize command modules
        self.basic_commands = BasicCommands(meshtastic, discord_to_mesh, database)
        self.monitoring_commands = MonitoringCommands(meshtastic, discord_to_mesh, database)
        self.network_commands = NetworkCommands(meshtastic, discord_to_mesh, database)
        self.debug_commands = DebugCommands(meshtastic, discord_to_mesh, database)

        # Command routing table
        self.commands = {
            # Basic commands
            '$help': self.basic_commands.cmd_help,
            '$txt': self.basic_commands.cmd_send_primary,
            '$send': self.basic_commands.cmd_send_node,
            '$activenodes': self.basic_commands.cmd_active_nodes,
            '$nodes': self.basic_commands.cmd_all_nodes,

            # Monitoring commands
            '$telem': self.monitoring_commands.cmd_telemetry,
            '$status': self.monitoring_commands.cmd_status,
            '$live': self.monitoring_commands.cmd_live_monitor,

            # Network analysis commands
            '$topo': self.network_commands.cmd_topology_tree,
            '$topology': self.network_commands.cmd_network_topology,
            '$stats': self.network_commands.cmd_message_statistics,
            '$trace': self.network_commands.cmd_trace_route,
            '$leaderboard': self.network_commands.cmd_leaderboard,
            '$art': self.network_commands.cmd_network_art,

            # Debug/Admin commands
            '$clear': self.debug_commands.cmd_clear_database,
            '$debug': self.debug_commands.cmd_debug_info
        }

        # Rate limiting
        self._command_cooldowns = {}
        self._cooldown_duration = 2  # 2 seconds between commands per user

    async def handle_command(self, message: discord.Message) -> bool:
        """Route command to appropriate handler with rate limiting"""
        content = message.content.strip()

        # Rate limiting check
        user_id = message.author.id
        now = time.time()

        if user_id in self._command_cooldowns:
            if now - self._command_cooldowns[user_id] < self._cooldown_duration:
                await self._safe_send(
                    message.channel,
                    "⏰ Please wait a moment before using another command."
                )
                return True

        for cmd, handler in self.commands.items():
            if content.startswith(cmd):
                try:
                    await handler(message)
                    # Update cooldown only after successful command execution
                    self._command_cooldowns[user_id] = now
                    return True
                except Exception as e:
                    logger.error("Error handling command %s: %s", cmd, e)
                    await self._safe_send(message.channel, f"❌ Error executing command: {e}")
                    return True

        return False

    def clear_cache(self):
        """Clear all cached data across command modules"""
        self.basic_commands.clear_cache()
        self.monitoring_commands.clear_cache()
        self.network_commands.clear_cache()
        self.debug_commands.clear_cache()
        logger.info("All command handler caches cleared")

    async def add_packet_to_buffer(self, packet_info: dict):
        """Add packet information to the live monitor buffer"""
        await self.monitoring_commands.add_packet_to_buffer(packet_info)

    def calculate_distance(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Calculate distance between two coordinates in meters using Haversine formula"""
        return self.basic_commands.calculate_distance(lat1, lon1, lat2, lon2)

    async def _safe_send(self, channel, message: str):
        """Safely send a message to a channel with error handling"""
        try:
            await channel.send(message)
        except Exception as e:
            logger.error("Error sending message to channel: %s", e)
