"""Debug and administrative command implementations for Meshbot."""
import logging
import time

import discord

from .base import BaseCommandMixin, get_utc_time

logger = logging.getLogger(__name__)


class DebugCommands(BaseCommandMixin):
    """Debug and administrative command functionality"""

    def __init__(self, meshtastic, discord_to_mesh, database):
        super().__init__()
        self.meshtastic = meshtastic
        self.discord_to_mesh = discord_to_mesh
        self.database = database

    async def cmd_clear_database(self, message: discord.Message):
        """Clear database and force fresh start"""
        try:
            # Clear all data from database
            with self.database._get_connection() as conn:
                cursor = conn.cursor()

                # Clear all tables
                cursor.execute("DELETE FROM telemetry")
                cursor.execute("DELETE FROM positions")
                cursor.execute("DELETE FROM messages")
                cursor.execute("DELETE FROM nodes")

                # Reset auto-increment counters
                cursor.execute("DELETE FROM sqlite_sequence WHERE name IN ('telemetry', 'positions', 'messages')")

                conn.commit()

                logger.info("Database cleared by user command")

            # Clear command handler cache
            self.clear_cache()

            embed = discord.Embed(
                title="🗑️ Database Cleared",
                description="All data has been cleared from the database",
                color=0xff6b6b,
                timestamp=get_utc_time()
            )

            embed.add_field(
                name="✅ **Cleared Tables**",
                value="• Nodes\n• Telemetry\n• Positions\n• Messages\n• Cache",
                inline=True
            )

            embed.add_field(
                name="🔄 **Next Steps**",
                value=(
                    "The bot will now collect fresh data from the mesh network. "
                    "Use `$nodes` to see new data as it's collected."
                ),
                inline=True
            )

            await message.channel.send(embed=embed)

        except Exception as e:
            logger.error("Error clearing database: %s", e)
            await self._safe_send(message.channel, f"❌ Error clearing database: {e}")

    async def cmd_debug_info(self, message: discord.Message):
        """Show debug information about database and data storage"""
        try:
            # Get database counts
            with self.database._get_connection() as conn:
                cursor = conn.cursor()

                # Count records in each table
                cursor.execute("SELECT COUNT(*) FROM nodes")
                node_count = cursor.fetchone()[0]

                cursor.execute("SELECT COUNT(*) FROM telemetry")
                telemetry_count = cursor.fetchone()[0]

                cursor.execute("SELECT COUNT(*) FROM positions")
                position_count = cursor.fetchone()[0]

                cursor.execute("SELECT COUNT(*) FROM messages")
                message_count = cursor.fetchone()[0]

            # Get some sample data
            nodes = self.database.get_all_nodes()
            recent_telemetry = []
            if nodes:
                # Get recent telemetry for first node
                recent_telemetry = self.database.get_telemetry_history(nodes[0]['node_id'], hours=1, limit=5)

            embed = discord.Embed(
                title="🔍 Debug Information",
                description="Database and data storage status",
                color=0x00bfff,
                timestamp=get_utc_time()
            )

            embed.add_field(
                name="📊 **Database Counts**",
                value=f"""Nodes: {node_count}
Telemetry: {telemetry_count}
Positions: {position_count}
Messages: {message_count}""",
                inline=True
            )

            embed.add_field(
                name="🔄 **Cache Status**",
                value=f"""Cache Entries: {len(self._node_cache)}
Cache TTL: {self._cache_ttl}s
Last Refresh: {time.time() - self.meshtastic.last_node_refresh:.1f}s ago""",
                inline=True
            )

            if nodes:
                embed.add_field(
                    name="📡 **Sample Node Data**",
                    value=f"""Total Nodes: {len(nodes)}
First Node: {nodes[0]['long_name']}
Has SNR: {nodes[0].get('snr') is not None}
Has Battery: {nodes[0].get('battery_level') is not None}
Last Heard: {nodes[0].get('last_heard', 'Unknown')}""",
                    inline=False
                )

            if recent_telemetry:
                embed.add_field(
                    name="📈 **Recent Telemetry**",
                    value=f"Found {len(recent_telemetry)} recent telemetry records for {nodes[0]['long_name']}",
                    inline=False
                )
            else:
                embed.add_field(
                    name="📈 **Recent Telemetry**",
                    value="No recent telemetry data found",
                    inline=False
                )

            await message.channel.send(embed=embed)

        except Exception as e:
            logger.error("Error getting debug info: %s", e)
            await self._safe_send(message.channel, f"❌ Error getting debug info: {e}")
