"""Basic command implementations for Meshbot."""
# pylint: disable=duplicate-code
import logging
import queue

import discord

from .base import BaseCommandMixin, get_utc_time

logger = logging.getLogger(__name__)


class BasicCommands(BaseCommandMixin):
    """Basic command functionality"""

    def __init__(self, meshtastic, discord_to_mesh: queue.Queue, database):
        super().__init__()
        self.meshtastic = meshtastic
        self.discord_to_mesh = discord_to_mesh
        self.database = database

    async def cmd_help(self, message: discord.Message):
        """Show help information"""
        embed = discord.Embed(
            title="🤖 Meshtastic Discord Bridge Commands",
            description="Complete command reference for the mesh network bridge",
            color=0x00ff00,
            timestamp=get_utc_time()
        )
        embed.set_thumbnail(
            url="https://raw.githubusercontent.com/meshtastic/firmware/master/"
                "docs/assets/logo/meshtastic-logo.png"
        )
        embed.set_footer(text="🌍 UTC Time | Use $help <command> for detailed info")

        # Basic Commands
        embed.add_field(
            name="📡 **Basic Commands**",
            value="""`$help` - Show this help message
`$txt <message>` - Send message to primary channel (max 225 chars)
`$send <longname> <message>` - Send message to specific node by name
`$activenodes` - Show nodes active in last 60 minutes
`$nodes` - Show all known nodes
`$telem` - Show telemetry information
`$status` - Show bridge status
`ping` - Send "Pong!" to mesh network""",
            inline=False
        )

        # Advanced Commands
        embed.add_field(
            name="🔍 **Advanced Commands**",
            value="""`$topo` - Show visual tree of all radio connections
`$topology` - Show network topology and connections
`$stats` - Show message statistics and network activity
`$trace <node_name>` - Trace route to a specific node""",
            inline=False
        )

        # Epic Analytics Commands
        embed.add_field(
            name="📊 **Epic Analytics**",
            value="""`$leaderboard` - Network performance leaderboards
`$live` - Real-time network monitor (1 min)
`$art` - ASCII network art""",
            inline=False
        )

        # Admin Commands
        embed.add_field(
            name="🔧 **Admin Commands**",
            value="""`$debug` - Show debug information""",
            inline=False
        )

        embed.add_field(
            name="💡 **Command Examples**",
            value="""`$send John Hello there!`
`$send "John Doe" Hello there!` (use quotes for names with spaces)
`$trace Node123` - Trace route to Node123
`ping` - Test mesh connectivity""",
            inline=False
        )

        embed.set_footer(text="Use any command to get started!")

        await message.channel.send(embed=embed)

    async def cmd_send_primary(self, message: discord.Message):
        """Send message to primary channel (renamed from $sendprimary to $txt)"""
        content = message.content
        if ' ' not in content:
            await self._safe_send(message.channel, "❌ Please provide a message to send.")
            return

        message_text = content[content.find(' ')+1:][:225]
        if not message_text.strip():
            await self._safe_send(message.channel, "❌ Message cannot be empty.")
            return

        await self._safe_send(
            message.channel,
            f"📤 Sending to primary channel:\n```{message_text}```"
        )
        self.discord_to_mesh.put(message_text)

    async def cmd_send_node(self, message: discord.Message):
        """Send message to specific node using fuzzy name matching"""
        content = message.content

        if not content.startswith('$send '):
            await self._safe_send(message.channel, "❌ Use format: `$send <longname> <message>`")
            return

        try:
            # Extract node name and message
            parts = content.split(' ', 2)
            if len(parts) < 3:
                await self._safe_send(message.channel, "❌ Use format: `$send <longname> <message>`")
                return

            node_name = parts[1]
            message_text = parts[2][:225]

            if not message_text.strip():
                await self._safe_send(message.channel, "❌ Message cannot be empty.")
                return

            # Find node by name using fuzzy matching
            try:
                logger.info("Searching for node with name: '%s'", node_name)
                node = self.database.find_node_by_name(node_name)
                if not node:
                    await self._safe_send(
                        message.channel,
                        f"❌ No node found with name '{node_name}'. "
                        f"Try using `$nodes` to see available nodes."
                    )
                    return

                logger.info("Found node: %s with ID: %s", node['long_name'], node['node_id'])
            except (KeyError, ValueError, TypeError, AttributeError) as db_error:
                logger.error("Database error finding node by name: %s", db_error)
                await self._safe_send(message.channel, "❌ Error searching for node in database.")
                return

            # Clean the node ID (remove any prefixes like '!' that Meshtastic doesn't expect)
            clean_node_id = node['node_id'].lstrip('!')

            # Log the node data for debugging
            logger.info("Node data: %s", node)
            logger.info("Original node_id: '%s', Cleaned: '%s'", node['node_id'], clean_node_id)

            # Try to convert to integer format that Meshtastic expects
            try:
                # Convert hex string to integer (this is what Meshtastic typically expects)
                node_id_int = int(clean_node_id, 16)
                logger.info("Converted to integer: %s", node_id_int)
                final_node_id = node_id_int
            except ValueError:
                # If conversion fails, use the cleaned string
                logger.info("Could not convert '%s' to integer, using string", clean_node_id)
                final_node_id = clean_node_id

            # Validate message doesn't contain control characters
            if any(ord(c) < 32 and c not in '\n\r\t' for c in message_text):
                await self._safe_send(
                    message.channel,
                    "❌ Message contains invalid control characters."
                )
                return

            # Try to add to queue with timeout
            try:
                self.discord_to_mesh.put(f"nodenum={final_node_id} {message_text}", timeout=1)
                await self._safe_send(
                    message.channel,
                    f"📤 Sending to node **{node['long_name']}** "
                    f"(ID: {final_node_id}):\n```{message_text}```"
                )
                logger.info("Sent message with node ID: %s", final_node_id)
            except queue.Full:
                await self._safe_send(
                    message.channel,
                    "❌ Message queue is full. Please try again later."
                )
                logger.warning("Discord to mesh queue is full")

        except (ValueError, IndexError, AttributeError) as e:
            logger.error("Error parsing send command: %s", e)
            await self._safe_send(
                message.channel,
                "❌ Error parsing command. Use format: `$send <longname> <message>`"
            )

    async def cmd_active_nodes(self, message: discord.Message):
        """Show active nodes from last 60 minutes"""
        try:
            # Use caching for better performance
            nodes = self._get_cached_data(
                "active_nodes_60",
                self.database.get_active_nodes,
                60
            )
            if not nodes:
                embed = discord.Embed(
                    title="📡 Active Nodes",
                    description="No active nodes in the last 60 minutes",
                    color=0xff6b6b,
                    timestamp=get_utc_time()
                )
                embed.set_thumbnail(
                    url="https://raw.githubusercontent.com/meshtastic/firmware/master/"
                        "docs/assets/logo/meshtastic-logo.png"
                )
                embed.set_footer(text="🌍 UTC Time | Check back later for activity")
                await message.channel.send(embed=embed)
                return
        except (KeyError, ValueError, TypeError, AttributeError) as db_error:
            logger.error("Database error getting active nodes: %s", db_error)
            await self._safe_send(message.channel, "❌ Error retrieving node data from database.")
            return

        active_nodes = []
        for node in nodes:
            try:
                node_info = self._format_node_info(node)
                active_nodes.append(node_info)
            except (KeyError, ValueError, TypeError) as e:
                logger.error("Error processing node %s: %s", node.get('node_id', 'Unknown'), e)
                continue

        response = "📡 **Active Nodes (Last 60 minutes):**\n" + "\n".join(active_nodes)
        try:
            await self._send_long_message(message.channel, response)
        except discord.HTTPException as send_error:
            logger.error("Error sending message to channel: %s", send_error)
            await message.channel.send("❌ Error sending message to channel.")

    async def cmd_all_nodes(self, message: discord.Message):
        """Show all known nodes"""
        try:
            # Use caching for better performance
            nodes = self._get_cached_data(
                "all_nodes",
                self.database.get_all_nodes
            )
            if not nodes:
                await message.channel.send("📡 No nodes available.")
                return
        except (KeyError, ValueError, TypeError, AttributeError) as db_error:
            logger.error("Database error getting all nodes: %s", db_error)
            await message.channel.send("❌ Error retrieving node data from database.")
            return

        node_list = []
        for node in nodes:
            try:
                node_info = self._format_node_info(node)
                node_list.append(node_info)
            except (KeyError, ValueError, TypeError) as e:
                logger.error("Error processing node %s: %s", node.get('node_id', 'Unknown'), e)
                continue

        response = "📡 **All Known Nodes:**\n" + "\n".join(node_list)
        try:
            await self._send_long_message(message.channel, response)
        except discord.HTTPException as send_error:
            logger.error("Error sending message to channel: %s", send_error)
            await self._safe_send(message.channel, "❌ Error sending message to channel.")
