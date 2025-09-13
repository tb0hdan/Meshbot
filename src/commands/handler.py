"""Command handler for Meshbot Discord bot.

Handles parsing and execution of Discord commands for Meshtastic network interaction.
"""
import asyncio
import functools
import logging
import queue
import time
from datetime import datetime, timedelta
from typing import Dict, Any

import discord

from src.database import MeshtasticDatabase

logger = logging.getLogger(__name__)


def cache_result(ttl_seconds=300):
    """Cache function results for a specified time (thread-safe)"""
    def decorator(func):
        # Use the function object as the cache key base
        if not hasattr(func, '_cache'):
            func._cache = {}
            func._cache_timestamps = {}
            func._cache_lock = asyncio.Lock()
        
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            # Create a cache key from function arguments
            cache_key = str(args) + str(sorted(kwargs.items()))
            
            async with func._cache_lock:  # Thread-safe access
                current_time = time.time()
                
                # Check if cached result exists and is still valid
                if (cache_key in func._cache and 
                    cache_key in func._cache_timestamps and
                    current_time - func._cache_timestamps[cache_key] < ttl_seconds):
                    logger.debug("Cache hit for %s", func.__name__)
                    return func._cache[cache_key]
                
                # Call the actual function
                logger.debug("Cache miss for %s", func.__name__)
                result = await func(*args, **kwargs)
                
                # Store result in cache
                func._cache[cache_key] = result
                func._cache_timestamps[cache_key] = current_time
                
                # Clean old cache entries
                expired_keys = [
                    key for key, timestamp in func._cache_timestamps.items()
                    if current_time - timestamp >= ttl_seconds
                ]
                for key in expired_keys:
                    func._cache.pop(key, None)
                    func._cache_timestamps.pop(key, None)
                
                return result
        
        return wrapper
    return decorator


def get_utc_time():
    """Get current time in UTC"""
    return datetime.utcnow()


def format_utc_time(dt=None, format_str="%Y-%m-%d %H:%M:%S UTC"):
    """Format datetime in UTC"""
    if dt is None:
        dt = get_utc_time()
    return dt.strftime(format_str)

class CommandHandler:
    """Handles Discord bot commands with caching and performance optimizations"""

    def __init__(
        self, 
        meshtastic, 
        discord_to_mesh: queue.Queue, 
        database: MeshtasticDatabase
    ):
        self.meshtastic = meshtastic
        self.discord_to_mesh = discord_to_mesh
        self.database = database
        self.commands = {
            '$help': self.cmd_help,
            '$txt': self.cmd_send_primary,  # Changed from $sendprimary
            '$send': self.cmd_send_node,    # Changed to use fuzzy name matching
            '$activenodes': self.cmd_active_nodes,
            '$nodes': self.cmd_all_nodes,
            '$telem': self.cmd_telemetry,
            '$status': self.cmd_status,
            '$topo': self.cmd_topology_tree,
            '$topology': self.cmd_network_topology,
            '$stats': self.cmd_message_statistics,
            '$trace': self.cmd_trace_route,
            '$leaderboard': self.cmd_leaderboard,
            '$live': self.cmd_live_monitor,
            '$art': self.cmd_network_art,
            '$clear': self.cmd_clear_database,
            '$debug': self.cmd_debug_info
        }

        # Cache for frequently accessed data
        self._node_cache = {}
        self._cache_timestamps = {}
        self._cache_ttl = 60  # 1 minute cache TTL

        # Rate limiting
        self._command_cooldowns = {}
        self._cooldown_duration = 2  # 2 seconds between commands per user

        # Live monitor state
        self._live_monitors = {}  # user_id -> {'active': bool, 'task': asyncio.Task}
        self._packet_buffer = []  # Store recent packets for live display
        self._max_packet_buffer = 50  # Keep last 50 packets
        self._packet_buffer_lock = asyncio.Lock()  # Thread safety for packet buffer

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

        # Update cooldown (only after successful command execution)
        # We'll move this to after the command is executed

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

    def _get_cached_data(self, key: str, fetch_func, *args, **kwargs):
        """Get data from cache or fetch if not available"""
        now = time.time()

        if (key in self._node_cache and
            key in self._cache_timestamps and
            now - self._cache_timestamps[key] < self._cache_ttl):
            return self._node_cache[key]

        # Fetch fresh data
        try:
            data = fetch_func(*args, **kwargs)
            self._node_cache[key] = data
            self._cache_timestamps[key] = now
            return data
        except Exception as e:
            logger.error("Error fetching data for cache key %s: %s", key, e)
            # Return cached data if available, even if stale
            return self._node_cache.get(key, [])

    def clear_cache(self):
        """Clear all cached data"""
        self._node_cache.clear()
        self._cache_timestamps.clear()
        logger.info("Command handler cache cleared")

    async def add_packet_to_buffer(self, packet_info: dict):
        """Add packet information to the live monitor buffer (thread-safe)"""
        try:
            # Add timestamp
            packet_info['timestamp'] = datetime.utcnow().isoformat()

            # Add to buffer with lock
            async with self._packet_buffer_lock:
                self._packet_buffer.append(packet_info)

                # Keep only the last N packets
                if len(self._packet_buffer) > self._max_packet_buffer:
                    self._packet_buffer.pop(0)

        except Exception as e:
            logger.error("Error adding packet to buffer: %s", e)

    def calculate_distance(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Calculate distance between two coordinates in meters using Haversine formula"""
        try:
            import math

            # Convert to radians
            lat1_rad = math.radians(lat1)
            lon1_rad = math.radians(lon1)
            lat2_rad = math.radians(lat2)
            lon2_rad = math.radians(lon2)

            # Haversine formula
            dlat = lat2_rad - lat1_rad
            dlon = lon2_rad - lon1_rad
            a = math.sin(dlat/2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon/2)**2
            c = 2 * math.asin(math.sqrt(a))

            # Earth's radius in meters
            earth_radius = 6371000
            distance = earth_radius * c

            return distance
        except Exception as e:
            logger.error("Error calculating distance: %s", e)
            return 0.0

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
            except Exception as db_error:
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

        except Exception as e:
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
        except Exception as db_error:
            logger.error("Database error getting active nodes: %s", db_error)
            await self._safe_send(message.channel, "❌ Error retrieving node data from database.")
            return

        active_nodes = []
        for node in nodes:
            try:
                node_info = self._format_node_info(node)
                active_nodes.append(node_info)
            except Exception as e:
                logger.error("Error processing node %s: %s", node.get('node_id', 'Unknown'), e)
                continue

        response = "📡 **Active Nodes (Last 60 minutes):**\n" + "\n".join(active_nodes)
        try:
            await self._send_long_message(message.channel, response)
        except Exception as send_error:
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
        except Exception as db_error:
            logger.error("Database error getting all nodes: %s", db_error)
            await message.channel.send("❌ Error retrieving node data from database.")
            return

        node_list = []
        for node in nodes:
            try:
                node_info = self._format_node_info(node)
                node_list.append(node_info)
            except Exception as e:
                logger.error("Error processing node %s: %s", node.get('node_id', 'Unknown'), e)
                continue

        response = "📡 **All Known Nodes:**\n" + "\n".join(node_list)
        try:
            await self._send_long_message(message.channel, response)
        except Exception as send_error:
            logger.error("Error sending message to channel: %s", send_error)
            await self._safe_send(message.channel, "❌ Error sending message to channel.")

    async def cmd_telemetry(self, message: discord.Message):
        """Show telemetry information"""
        try:
            summary = self.database.get_telemetry_summary(60)
            if not summary:
                embed = discord.Embed(
                    title="📊 Telemetry Summary",
                    description="No telemetry data available in the last 60 minutes",
                    color=0xff6b6b,
                    timestamp=get_utc_time()
                )
                embed.set_thumbnail(
                    url="https://raw.githubusercontent.com/meshtastic/firmware/master/"
                        "docs/assets/logo/meshtastic-logo.png"
                )
                embed.set_footer(text="🌍 UTC Time | Data collection in progress...")
                await message.channel.send(embed=embed)
                return
        except Exception as db_error:
            logger.error("Database error getting telemetry summary: %s", db_error)
            await self._safe_send(
                message.channel, 
                "❌ Error retrieving telemetry data from database."
            )
            return

        # Check connection status safely
        connection_status = "❌ Disconnected"
        if self.meshtastic.iface:
            try:
                if (hasattr(self.meshtastic.iface, 'isConnected') and 
                    callable(self.meshtastic.iface.isConnected)):
                    if self.meshtastic.iface.isConnected():
                        connection_status = "✅ Connected"
            except Exception:
                connection_status = "❌ Disconnected"

        embed = discord.Embed(
            title="📊 Telemetry Summary",
            description="Last 60 minutes of network telemetry data",
            color=0x00ff00,
            timestamp=get_utc_time()
        )

        # Node statistics
        embed.add_field(
            name="📡 **Network Status**",
            value=f"""Total Nodes: {summary.get('total_nodes', 0)}
Active Nodes: {summary.get('active_nodes', 0)}
Connection: {connection_status}""",
            inline=True
        )

        # Environmental data
        env_data = ""
        if summary.get('avg_battery') is not None:
            env_data += f"🔋 Battery: {summary['avg_battery']:.1f}%\n"
        else:
            env_data += "🔋 Battery: N/A\n"

        if summary.get('avg_temperature') is not None:
            env_data += f"🌡️ Temperature: {summary['avg_temperature']:.1f}°C\n"
        else:
            env_data += "🌡️ Temperature: N/A\n"

        if summary.get('avg_humidity') is not None:
            env_data += f"💧 Humidity: {summary['avg_humidity']:.1f}%\n"
        else:
            env_data += "💧 Humidity: N/A\n"

        embed.add_field(
            name="🌍 **Environmental**",
            value=env_data,
            inline=True
        )

        # Signal quality
        signal_data = ""
        if summary.get('avg_snr') is not None:
            signal_data += f"📶 SNR: {summary['avg_snr']:.1f} dB\n"
        else:
            signal_data += "📶 SNR: N/A\n"

        if summary.get('avg_rssi') is not None:
            signal_data += f"📡 RSSI: {summary['avg_rssi']:.1f} dBm\n"
        else:
            signal_data += "📡 RSSI: N/A\n"

        embed.add_field(
            name="📶 **Signal Quality**",
            value=signal_data,
            inline=True
        )

        await message.channel.send(embed=embed)

    async def cmd_status(self, message: discord.Message):
        """Show bridge status"""
        # Check Meshtastic connection status safely
        meshtastic_status = "❌ Disconnected"
        if self.meshtastic.iface:
            try:
                if (hasattr(self.meshtastic.iface, 'isConnected') and 
                    callable(self.meshtastic.iface.isConnected)):
                    if self.meshtastic.iface.isConnected():
                        meshtastic_status = "✅ Connected"
            except Exception:
                meshtastic_status = "❌ Disconnected"

        # Get database statistics
        try:
            db_stats = self.database.get_telemetry_summary(60)
            node_count = db_stats.get('total_nodes', 0)
            active_count = db_stats.get('active_nodes', 0)
        except Exception:
            node_count = 0
            active_count = 0

        # Determine status color and emoji
        if meshtastic_status == "✅ Connected":
            status_color = 0x00ff00
            status_emoji = "🟢"
            status_text = "All Systems Operational"
        else:
            status_color = 0xff6b6b
            status_emoji = "🔴"
            status_text = "Service Issues Detected"

        embed = discord.Embed(
            title=f"{status_emoji} Bridge Status",
            description=f"**{status_text}**\n*Real-time system monitoring*",
            color=status_color,
            timestamp=get_utc_time()
        )
        embed.set_thumbnail(
            url="https://raw.githubusercontent.com/meshtastic/firmware/master/"
                "docs/assets/logo/meshtastic-logo.png"
        )
        embed.set_footer(text="🌍 UTC Time | Last updated")

        # Service status
        embed.add_field(
            name="🖥️ **Services**",
            value=f"""Discord: ✅ Connected
Meshtastic: {meshtastic_status}
Database: ✅ Connected""",
            inline=True
        )

        # Network status
        embed.add_field(
            name="📡 **Network**",
            value=f"""Total Nodes: {node_count}
Active Nodes: {active_count}
Current Time: {format_utc_time()}""",
            inline=True
        )

        # System health
        health_score = 0
        if meshtastic_status == "✅ Connected":
            health_score += 50
        if node_count > 0:
            health_score += 30
        if active_count > 0:
            health_score += 20

        if health_score >= 80:
            health_status = "🟢 Excellent"
        elif health_score >= 60:
            health_status = "🟡 Good"
        elif health_score >= 40:
            health_status = "🟠 Fair"
        else:
            health_status = "🔴 Poor"

        embed.add_field(
            name="💚 **System Health**",
            value=f"Status: {health_status}\nScore: {health_score}/100",
            inline=True
        )

        await message.channel.send(embed=embed)

    async def cmd_network_topology(self, message: discord.Message):
        """Show network topology and connections with ASCII network diagram"""
        try:
            topology = self.database.get_network_topology()
            nodes = self._get_cached_data("all_nodes", self.database.get_all_nodes)

            embed = discord.Embed(
                title="🌐 Network Topology",
                description="**Mesh Network Structure & Connections**\n"
                           "*Real-time network visualization*",
                color=0x0099ff,
                timestamp=get_utc_time()
            )
            embed.set_thumbnail(
                url="https://raw.githubusercontent.com/meshtastic/firmware/master/"
                    "docs/assets/logo/meshtastic-logo.png"
            )
            embed.set_footer(text="🌍 UTC Time | Network analysis")

            # Create ASCII network diagram
            ascii_network = self._create_network_diagram(nodes, topology['connections'])

            embed.add_field(
                name="🌳 **Network Tree Diagram**",
                value=f"```\n{ascii_network}\n```",
                inline=False
            )

            # Network statistics
            embed.add_field(
                name="📊 **Network Stats**",
                value=f"""Total Nodes: {topology['total_nodes']}
Active Nodes: {topology['active_nodes']}
Router Nodes: {topology['router_nodes']}
Avg Hops: {topology['avg_hops']:.1f}""",
                inline=True
            )

            # Top connections
            if topology['connections']:
                top_connections = topology['connections'][:5]  # Top 5 connections
                connections_text = ""
                for conn in top_connections:
                    from_name = self.database.get_node_display_name(conn['from_node'])
                    to_name = self.database.get_node_display_name(conn['to_node'])
                    connections_text += f"**{from_name}** → **{to_name}**\n"
                    connections_text += (f"Messages: {conn['message_count']}, "
                                         f"Hops: {conn['avg_hops']:.1f}\n\n")

                embed.add_field(
                    name="🔗 **Top Connections**",
                    value=connections_text[:1024],  # Discord field limit
                    inline=True
                )
            else:
                embed.add_field(
                    name="🔗 **Connections**",
                    value="No recent connections found",
                    inline=True
                )

            await message.channel.send(embed=embed)

        except Exception as e:
            logger.error("Error getting network topology: %s", e)
            await self._safe_send(message.channel, "❌ Error retrieving network topology.")

    async def cmd_topology_tree(self, message: discord.Message):
        """Show visual tree of all radio connections"""
        try:
            nodes = self._get_cached_data("all_nodes", self.database.get_all_nodes)
            topology = self.database.get_network_topology()

            if not nodes:
                await self._safe_send(
                    message.channel, 
                    "📡 **No nodes available for topology analysis**"
                )
                return

            # Create readable connection tree
            connection_tree = self._create_connection_tree(nodes, topology['connections'])

            # Add summary info
            total_nodes = len(nodes)
            active_connections = len(topology['connections'])
            avg_hops = topology.get('avg_hops', 0)

            # Send the formatted tree
            await self._send_long_message(message.channel, connection_tree)

            # Send summary
            summary = (f"\n📊 **Network Summary:** {total_nodes} radios | "
                      f"{active_connections} routes | {avg_hops:.1f} avg hops")
            await message.channel.send(summary)

        except Exception as e:
            logger.error("Error creating topology tree: %s", e)
            await self._safe_send(message.channel, "❌ Error creating connection tree.")

    async def cmd_message_statistics(self, message: discord.Message):
        """Show message statistics and network activity"""
        try:
            stats = self.database.get_message_statistics(hours=24)

            embed = discord.Embed(
                title="📊 Message Statistics",
                description="24-hour network activity summary",
                color=0x9b59b6,
                timestamp=get_utc_time()
            )

            # Basic statistics
            embed.add_field(
                name="📈 **Activity**",
                value=f"""Total Messages: {stats.get('total_messages', 0)}
Unique Senders: {stats.get('unique_senders', 0)}
Unique Recipients: {stats.get('unique_recipients', 0)}""",
                inline=True
            )

            # Signal quality - safe formatting
            avg_hops = stats.get('avg_hops', 0) or 0
            avg_snr = stats.get('avg_snr', 0) or 0
            avg_rssi = stats.get('avg_rssi', 0) or 0

            embed.add_field(
                name="📶 **Signal Quality**",
                value=f"""Avg Hops: {avg_hops:.1f}
Avg SNR: {avg_snr:.1f} dB
Avg RSSI: {avg_rssi:.1f} dBm""",
                inline=True
            )

            # Hourly distribution
            hourly_dist = stats.get('hourly_distribution', {})
            if hourly_dist:
                # Find peak hours
                peak_hour = (max(hourly_dist.items(), key=lambda x: x[1]) 
                            if hourly_dist else ("N/A", 0))
                quiet_hour = (min(hourly_dist.items(), key=lambda x: x[1]) 
                             if hourly_dist else ("N/A", 0))

                embed.add_field(
                    name="⏰ **Activity Pattern**",
                    value=f"""Peak Hour: {peak_hour[0]}:00 ({peak_hour[1]} msgs)
Quiet Hour: {quiet_hour[0]}:00 ({quiet_hour[1]} msgs)
Active Hours: {len(hourly_dist)}""",
                    inline=True
                )

            await message.channel.send(embed=embed)

        except Exception as e:
            logger.error("Error getting message statistics: %s", e)
            await self._safe_send(message.channel, "❌ Error retrieving message statistics.")

    async def cmd_trace_route(self, message: discord.Message):
        """Trace route to a specific node with visual hop-by-hop path"""
        content = message.content.strip()

        if not content.startswith('$trace '):
            await self._safe_send(message.channel, "❌ Use format: `$trace <node_name>`")
            return

        try:
            node_name = content[7:].strip()  # Remove '$trace '
            if not node_name:
                await self._safe_send(message.channel, "❌ Please specify a node name.")
                return

            # Find the target node
            target_node = self.database.find_node_by_name(node_name)
            if not target_node:
                await self._safe_send(
                    message.channel, 
                    f"❌ No node found with name '{node_name}'. "
                    f"Try using `$nodes` to see available nodes."
                )
                return

            # Get network topology and analyze routing
            topology = self.database.get_network_topology()
            route_path = self._analyze_route_to_node(target_node['node_id'], topology)

            embed = discord.Embed(
                title=f"🛣️ Trace Route to {target_node['long_name']}",
                description=f"Analyzing network path to **{target_node['node_id']}**",
                color=0x00bfff,
                timestamp=get_utc_time()
            )

            # Target node info
            embed.add_field(
                name="🎯 **Target Node**",
                value=f"""**Name:** {target_node['long_name']}
**ID:** `{target_node['node_id']}`
**Hops Away:** {target_node.get('hops_away', 'Unknown')}
**Last Heard:** {target_node.get('last_heard', 'Unknown')}""",
                inline=True
            )

            # Route path visualization
            if route_path:
                route_text = self._format_route_path(route_path)
                embed.add_field(
                    name="🛤️ **Route Path**",
                    value=route_text,
                    inline=False
                )

                # Route statistics
                total_hops = len(route_path) - 1  # -1 because we don't count the source
                avg_snr = (sum(hop.get('snr', 0) for hop in route_path[1:]) / 
                          max(1, len(route_path) - 1))
                avg_rssi = (sum(hop.get('rssi', 0) for hop in route_path[1:]) / 
                           max(1, len(route_path) - 1))

                embed.add_field(
                    name="📊 **Route Statistics**",
                    value=f"""**Total Hops:** {total_hops}
**Avg SNR:** {avg_snr:.1f} dB
**Avg RSSI:** {avg_rssi:.1f} dBm
**Path Quality:** {self._assess_route_quality(avg_snr, total_hops)}""",
                    inline=True
                )
            else:
                embed.add_field(
                    name="🛤️ **Route Path**",
                    value="❌ **No route found** - Node may be unreachable or "
                          "no recent communication data available",
                    inline=False
                )

            # Network overview
            embed.add_field(
                name="🌐 **Network Overview**",
                value=f"""**Active Nodes:** {topology['active_nodes']}
**Total Connections:** {len(topology['connections'])}
**Network Avg Hops:** {topology['avg_hops']:.1f}""",
                inline=True
            )

            # Connection quality to target
            connections_to_target = [
                conn for conn in topology['connections'] 
                if conn['to_node'] == target_node['node_id']
            ]
            if connections_to_target:
                best_connection = max(connections_to_target, key=lambda x: x['message_count'])
                from_name = self.database.get_node_display_name(best_connection['from_node'])

                embed.add_field(
                    name="🔗 **Best Connection**",
                    value=f"""**From:** {from_name}
**Messages:** {best_connection['message_count']}
**Avg Hops:** {best_connection['avg_hops']:.1f}
**Avg SNR:** {best_connection['avg_snr']:.1f} dB""",
                    inline=True
                )

            embed.set_footer(text=f"Route analysis completed at")
            await message.channel.send(embed=embed)

        except Exception as e:
            logger.error("Error tracing route: %s", e)
            await self._safe_send(message.channel, "❌ Error tracing route to node.")

    def _analyze_route_to_node(self, target_node_id: str, topology: dict) -> list:
        """Analyze the route to a specific node based on message data"""
        try:
            # Get all messages to the target node
            with self.database._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT from_node_id, to_node_id, hops_away, snr, rssi, timestamp
                    FROM messages
                    WHERE to_node_id = ?
                    ORDER BY timestamp DESC
                    LIMIT 100
                """, (target_node_id,))

                messages = cursor.fetchall()

            if not messages:
                return []

            # Find the most common path by analyzing message patterns
            # Group messages by hops_away to understand the path
            hop_groups = {}
            for msg in messages:
                hops = msg[2]  # hops_away
                if hops not in hop_groups:
                    hop_groups[hops] = []
                hop_groups[hops].append(msg)

            # Build route path from hop groups
            route_path = []
            sorted_hops = sorted(hop_groups.keys())

            for hop_count in sorted_hops:
                # Get the most recent message for this hop count
                recent_msg = max(hop_groups[hop_count], key=lambda x: x[5])  # timestamp
                from_node_id = recent_msg[0]
                snr = recent_msg[3]
                rssi = recent_msg[4]

                # Get node display name
                from_name = self.database.get_node_display_name(from_node_id)

                route_path.append({
                    'node_id': from_node_id,
                    'node_name': from_name,
                    'hops_away': hop_count,
                    'snr': snr,
                    'rssi': rssi
                })

            # Add target node at the end
            target_name = self.database.get_node_display_name(target_node_id)
            route_path.append({
                'node_id': target_node_id,
                'node_name': target_name,
                'hops_away': 0,
                'snr': None,
                'rssi': None
            })

            return route_path

        except Exception as e:
            logger.error("Error analyzing route to node %s: %s", target_node_id, e)
            return []

    def _format_route_path(self, route_path: list) -> str:
        """Format the route path for display with visual indicators"""
        if not route_path:
            return "No route data available"

        path_lines = []

        for i, hop in enumerate(route_path):
            node_name = hop['node_name']
            node_id = hop['node_id']
            hops_away = hop['hops_away']
            snr = hop.get('snr')
            rssi = hop.get('rssi')

            # Determine hop indicator
            if i == 0:
                # Source node
                hop_indicator = "🏠"
                hop_text = "SOURCE"
            elif i == len(route_path) - 1:
                # Target node
                hop_indicator = "🎯"
                hop_text = "TARGET"
            else:
                # Intermediate hop
                hop_indicator = f"🔄 {i}"
                hop_text = f"HOP {i}"

            # Format signal quality
            signal_info = ""
            if snr is not None and rssi is not None:
                signal_quality = self._get_signal_quality_icon(snr)
                signal_info = f" {signal_quality} SNR:{snr:.1f}dB RSSI:{rssi:.1f}dBm"
            elif snr is not None:
                signal_quality = self._get_signal_quality_icon(snr)
                signal_info = f" {signal_quality} SNR:{snr:.1f}dB"
            elif rssi is not None:
                signal_info = f" 📶 RSSI:{rssi:.1f}dBm"

            # Format the hop line
            hop_line = f"{hop_indicator} **{hop_text}:** {node_name}"
            if len(node_id) > 8:  # Only show short ID if it's long
                hop_line += f" (`{node_id[:8]}...`)"
            else:
                hop_line += f" (`{node_id}`)"

            hop_line += signal_info

            path_lines.append(hop_line)

            # Add connection line (except for the last hop)
            if i < len(route_path) - 1:
                path_lines.append("    ⬇️")

        return "\n".join(path_lines)

    def _get_signal_quality_icon(self, snr: float) -> str:
        """Get signal quality icon based on SNR"""
        if snr > 10:
            return "🟢"  # Excellent
        elif snr > 5:
            return "🟡"  # Good
        elif snr > 0:
            return "🟠"  # Fair
        else:
            return "🔴"  # Poor

    def _assess_route_quality(self, avg_snr: float, total_hops: int) -> str:
        """Assess overall route quality"""
        if avg_snr > 10 and total_hops <= 2:
            return "🟢 Excellent"
        elif avg_snr > 5 and total_hops <= 4:
            return "🟡 Good"
        elif avg_snr > 0 and total_hops <= 6:
            return "🟠 Fair"
        else:
            return "🔴 Poor"

    async def cmd_leaderboard(self, message: discord.Message):
        """Show network performance leaderboards"""
        try:
            nodes = self._get_cached_data("all_nodes", self.database.get_all_nodes)
            stats = self.database.get_message_statistics(24)

            if not nodes:
                await self._safe_send(message.channel, "📡 No nodes available for leaderboard.")
                return

            embed = discord.Embed(
                title="🏆 Network Performance Leaderboard",
                description="Top performing nodes and network statistics",
                color=0xffd700,
                timestamp=get_utc_time()
            )

            # Most Active Nodes (by message count)
            active_leaderboard = ""
            if stats.get('total_messages', 0) > 0:
                # This would need message count per node - simplified for now
                active_leaderboard = "📊 **Most Active Nodes**\n"
                active_leaderboard += "• Data collection in progress...\n"
                active_leaderboard += "• Check back after more activity!\n"
            else:
                active_leaderboard = "📊 **Most Active Nodes**\nNo message data available yet"

            embed.add_field(
                name="🏆 **Activity Leaders**",
                value=active_leaderboard,
                inline=True
            )

            # Best Signal Quality
            signal_leaderboard = ""
            nodes_with_signal = [n for n in nodes if n.get('snr') is not None]
            if nodes_with_signal:
                # Sort by SNR (highest first)
                sorted_nodes = sorted(
                    nodes_with_signal, key=lambda x: x.get('snr', 0), reverse=True
                )
                signal_leaderboard = "📶 **Best Signal Quality**\n"
                for i, node in enumerate(sorted_nodes[:5]):
                    if i == 0:
                        medal = "🥇"
                    elif i == 1:
                        medal = "🥈"
                    elif i == 2:
                        medal = "🥉"
                    else:
                        medal = "🏅"
                    signal_leaderboard += (
                        f"{medal} **{node['long_name']}** - {node.get('snr', 0):.1f} dB\n"
                    )
            else:
                signal_leaderboard = (
                    "📶 **Best Signal Quality**\nNo signal data available"
                )

            embed.add_field(
                name="📡 **Signal Champions**",
                value=signal_leaderboard,
                inline=True
            )

            # Longest Uptime (simplified)
            uptime_leaderboard = "⏰ **Longest Active**\n"
            active_nodes = [n for n in nodes if n.get('last_heard')]
            if active_nodes:
                # Sort by last_heard (most recent first)
                sorted_uptime = sorted(
                    active_nodes, key=lambda x: x.get('last_heard', ''), reverse=True
                )
                for i, node in enumerate(sorted_uptime[:5]):
                    if i == 0:
                        medal = "🥇"
                    elif i == 1:
                        medal = "🥈"
                    elif i == 2:
                        medal = "🥉"
                    else:
                        medal = "🏅"
                    last_heard = node.get('last_heard', 'Unknown')
                    uptime_leaderboard += f"{medal} **{node['long_name']}** - {last_heard}\n"
            else:
                uptime_leaderboard += "No activity data available"

            embed.add_field(
                name="⏰ **Uptime Champions**",
                value=uptime_leaderboard,
                inline=True
            )

            # Network Statistics
            total_nodes = len(nodes)
            active_count = 0
            for n in nodes:
                if n.get('last_heard'):
                    try:
                        last_heard = datetime.fromisoformat(n['last_heard'].replace('Z', '+00:00'))
                        if last_heard > datetime.now() - timedelta(hours=1):
                            active_count += 1
                    except (ValueError, TypeError) as e:
                        logger.warning("Error parsing last_heard for node %s: %s", n.get('long_name', 'Unknown'), e)
                        continue

            embed.add_field(
                name="📊 **Network Stats**",
                value=f"""Total Nodes: {total_nodes}
Active (1h): {active_count}
Total Messages: {stats.get('total_messages', 0)}
Unique Senders: {stats.get('unique_senders', 0)}""",
                inline=False
            )

            await message.channel.send(embed=embed)

        except Exception as e:
            logger.error("Error creating leaderboard: %s", e)
            await self._safe_send(message.channel, "❌ Error creating leaderboard.")

    async def cmd_network_art(self, message: discord.Message):
        """Create ASCII network art"""
        try:
            nodes = self._get_cached_data("all_nodes", self.database.get_all_nodes)
            topology = self.database.get_network_topology()

            if not nodes:
                await self._safe_send(message.channel, "📡 No nodes available for network art.")
                return

            embed = discord.Embed(
                title="🎨 Network Art",
                description="ASCII art representation of your mesh network",
                color=0xff69b4,
                timestamp=get_utc_time()
            )

            # Create simple ASCII network diagram
            art_lines = []
            art_lines.append("```")
            art_lines.append("🌐 MESHTASTIC NETWORK ART 🌐")
            art_lines.append("=" * 40)
            art_lines.append("")

            # Show active nodes as a simple diagram
            active_nodes = []
            for n in nodes:
                if n.get('last_heard'):
                    try:
                        last_heard = datetime.fromisoformat(n['last_heard'].replace('Z', '+00:00'))
                        if last_heard > datetime.now() - timedelta(hours=1):
                            active_nodes.append(n)
                    except (ValueError, TypeError) as e:
                        logger.warning("Error parsing last_heard for node %s: %s", n.get('long_name', 'Unknown'), e)
                        continue

            if active_nodes:
                art_lines.append("🟢 ACTIVE NODES:")
                for i, node in enumerate(active_nodes[:8]):  # Limit to 8 for ASCII art
                    snr = node.get('snr')
                    if snr is not None:
                        if snr > 5:
                            status_icon = "🟢"
                        elif snr > 0:
                            status_icon = "🟡"
                        else:
                            status_icon = "🔴"
                    else:
                        status_icon = "⚪"
                    art_lines.append(f"  {status_icon} {node['long_name'][:15]}")

                if len(active_nodes) > 8:
                    art_lines.append(f"  ... and {len(active_nodes) - 8} more")
            else:
                art_lines.append("⚪ No active nodes")

            art_lines.append("")

            # Show connections as lines
            if topology.get('connections'):
                art_lines.append("🔗 CONNECTIONS:")
                for i, conn in enumerate(topology['connections'][:5]):
                    from_name = self.database.get_node_display_name(conn['from_node'])[:10]
                    to_name = self.database.get_node_display_name(conn['to_node'])[:10]
                    art_lines.append(f"  {from_name} ─── {to_name}")

                if len(topology['connections']) > 5:
                    art_lines.append(f"  ... and {len(topology['connections']) - 5} more")
            else:
                art_lines.append("🔗 No connections detected")

            art_lines.append("")
            art_lines.append("=" * 40)
            art_lines.append("```")

            # Create the art
            art_text = "\n".join(art_lines)

            embed.add_field(
                name="🎨 **Network Diagram**",
                value=art_text,
                inline=False
            )

            # Network stats for the art
            total_nodes = len(nodes)
            active_count = len(active_nodes)
            connection_count = len(topology.get('connections', []))

            embed.add_field(
                name="📊 **Art Stats**",
                value=f"""Total Nodes: {total_nodes}
Active Nodes: {active_count}
Connections: {connection_count}
Art Quality: {'🎨' * min(5, total_nodes // 2)}""",
                inline=True
            )

            await message.channel.send(embed=embed)

        except Exception as e:
            logger.error("Error creating network art: %s", e)
            await self._safe_send(message.channel, "❌ Error creating network art.")

    async def cmd_live_monitor(self, message: discord.Message):
        """Real-time network monitor showing live packet activity"""
        user_id = message.author.id

        # Check if user already has a live monitor running
        if user_id in self._live_monitors and self._live_monitors[user_id]['active']:
            # Stop the existing monitor
            try:
                logger.debug("Stopping live monitor for user %s", user_id)
                self._live_monitors[user_id]['active'] = False

                if 'task' in self._live_monitors[user_id]:
                    task = self._live_monitors[user_id]['task']
                    try:
                        if not task.done():
                            task.cancel()
                            logger.debug("Cancelled live monitor task for user %s", user_id)
                        else:
                            logger.debug("Live monitor task for user %s was already done", user_id)
                    except Exception as task_error:
                        logger.error("Error cancelling task for user %s: %s", user_id, task_error)
                        # Continue with cleanup even if task cancellation fails

                await message.channel.send("🛑 **Live monitor stopped**")
                del self._live_monitors[user_id]
                logger.debug("Successfully stopped live monitor for user %s", user_id)
                return
            except Exception as e:
                logger.error("Error stopping live monitor for user %s: %s: %s", user_id, type(e).__name__, str(e))
                logger.error("Exception details: %s", repr(e))
                await message.channel.send("🛑 **Live monitor stopped** (with errors)")
                # Clean up even if there was an error
                if user_id in self._live_monitors:
                    del self._live_monitors[user_id]
                return

        # Start live monitor (cooldown is handled globally in handle_command)

        embed = discord.Embed(
            title="📡 Live Network Monitor",
            description=(
                "**Starting live packet monitoring...**\n\n"
                "*Monitoring will run for 1 minute or until you type `$live` again*"
            ),
            color=0x00ff00,
            timestamp=get_utc_time()
        )
        embed.add_field(
            name="📊 **What you'll see:**",
            value=(
                "• Packet types and sources\n• Message content previews\n"
                "• Telemetry data summaries\n• Traceroute information\n• Signal quality metrics"
            ),
            inline=False
        )
        embed.set_footer(text=f"Requested by {message.author.display_name}")

        status_message = await message.channel.send(embed=embed)

        # Start the live monitoring task
        try:
            task = asyncio.create_task(self._run_live_monitor(message.channel, user_id, status_message))
            self._live_monitors[user_id] = {'active': True, 'task': task}
            logger.info("Started live monitor for user %s (%s)", message.author.display_name, user_id)
        except Exception as e:
            logger.error("Error starting live monitor for user %s: %s", user_id, e)
            await message.channel.send(f"❌ **Error starting live monitor:** {str(e)}")
            return

    async def _run_live_monitor(self, channel, user_id, status_message):
        """Run the live monitor for 10 seconds"""
        try:
            start_time = time.time()
            last_update = start_time
            packet_count = 0

            while time.time() - start_time < 60:  # 1 minute timeout
                if user_id not in self._live_monitors or not self._live_monitors[user_id]['active']:
                    break

                # Check for new packets in buffer
                current_packets = len(self._packet_buffer)
                if current_packets > packet_count:
                    # New packets available, update display
                    new_packets = self._packet_buffer[packet_count:]
                    packet_count = current_packets

                    # Update status message with new packets
                    await self._update_live_display(channel, status_message, new_packets, time.time() - start_time)
                    last_update = time.time()

                # Check every 0.5 seconds
                await asyncio.sleep(0.5)

            # Final update
            if user_id in self._live_monitors and self._live_monitors[user_id]['active']:
                await self._finalize_live_monitor(channel, status_message, packet_count, time.time() - start_time)

        except asyncio.CancelledError:
            logger.info("Live monitor cancelled for user %s", user_id)
        except Exception as e:
            logger.error("Error in live monitor for user %s: %s: %s", user_id, type(e).__name__, str(e))
            logger.error("Exception details: %s", repr(e))
            try:
                await channel.send(f"❌ **Live monitor error:** {str(e)}")
            except Exception as send_error:
                logger.error("Error sending error message: %s", send_error)
        finally:
            # Clean up
            if user_id in self._live_monitors:
                del self._live_monitors[user_id]

    async def _update_live_display(self, channel, status_message, new_packets, elapsed_time):
        """Update the live monitor display with new packets"""
        try:
            if not new_packets:
                return

            # Create embed for new packets
            embed = discord.Embed(
                title="📡 Live Network Monitor",
                description=f"**Live packet monitoring** - {elapsed_time:.1f}s elapsed",
                color=0x00bfff,
                timestamp=datetime.utcnow()
            )

            # Add packet information
            packet_text = ""
            for packet in new_packets[-10:]:  # Show last 10 packets
                packet_type = packet.get('type', 'UNKNOWN')
                from_name = packet.get('from_name', 'Unknown')
                portnum = packet.get('portnum', 'UNKNOWN')
                hops = packet.get('hops', 0)
                snr = packet.get('snr', 'N/A')
                rssi = packet.get('rssi', 'N/A')

                # Format packet info
                if packet_type == 'text':
                    text_preview = packet.get('text', '')[:30]
                    if len(packet.get('text', '')) > 30:
                        text_preview += "..."
                    packet_text += f"💬 **{from_name}** ({portnum}) - `{text_preview}`\n"
                elif packet_type == 'telemetry':
                    sensor_data = packet.get('sensor_data', [])
                    sensor_summary = ", ".join(sensor_data[:3]) if sensor_data else "No data"
                    packet_text += f"📊 **{from_name}** ({portnum}) - {sensor_summary}\n"
                elif packet_type == 'traceroute':
                    to_name = packet.get('to_name', 'Unknown')
                    hops_count = packet.get('hops_count', 0)
                    packet_text += f"🛣️ **{from_name}** → **{to_name}** ({hops_count} hops)\n"
                elif packet_type == 'movement':
                    distance_moved = packet.get('distance_moved', 0)
                    packet_text += f"🚶 **{from_name}** moved {distance_moved:.1f}m\n"
                else:
                    packet_text += f"📦 **{from_name}** ({portnum}) - {packet_type}\n"

                # Add signal info
                packet_text += f"   └─ Hops: {hops} | SNR: {snr} | RSSI: {rssi}\n\n"

            if packet_text:
                embed.add_field(
                    name="📦 **Recent Packets:**",
                    value=packet_text[:1024],  # Discord field limit
                    inline=False
                )

            embed.set_footer(text="Type `$live` again to stop monitoring")

            await status_message.edit(embed=embed)

        except Exception as e:
            logger.error("Error updating live display: %s", e)

    async def _finalize_live_monitor(self, channel, status_message, total_packets, elapsed_time):
        """Finalize the live monitor with summary"""
        try:
            embed = discord.Embed(
                title="📡 Live Network Monitor - Complete",
                description=f"**Monitoring completed** - {elapsed_time:.1f}s total",
                color=0x00ff00,
                timestamp=datetime.utcnow()
            )

            embed.add_field(
                name="📊 **Summary:**",
                value=(
                    f"Total Packets: {total_packets}\n"
                    f"Duration: {elapsed_time:.1f}s\n"
                    f"Average Rate: {total_packets/elapsed_time:.1f} packets/sec"
                ),
                inline=False
            )

            embed.set_footer(text="Live monitoring session ended")

            await status_message.edit(embed=embed)

        except Exception as e:
            logger.error("Error finalizing live monitor: %s", e)

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

    def _create_signal_tree(self, excellent_nodes, good_nodes, poor_nodes, unknown_nodes):
        """Create ASCII tree for signal strength visualization"""
        tree_lines = []
        tree_lines.append("🔥 SIGNAL STRENGTH TREE")
        tree_lines.append("=" * 50)
        tree_lines.append("")

        # Root of tree
        tree_lines.append("🌐 Mesh Network")
        tree_lines.append("│")

        # Excellent signal branch
        if excellent_nodes:
            tree_lines.append("├─ 🟢 Excellent Signal")
            for i, node in enumerate(excellent_nodes[:8]):  # Limit to 8 for space
                if i == len(excellent_nodes[:8]) - 1 and len(excellent_nodes) > 8:
                    tree_lines.append(
                        f"│  └─ {node['long_name'][:20]} ({node.get('snr', 0):.1f}dB) "
                        f"+{len(excellent_nodes)-8} more"
                    )
                else:
                    tree_lines.append(f"│  ├─ {node['long_name'][:20]} ({node.get('snr', 0):.1f}dB)")
        else:
            tree_lines.append("├─ 🟢 Excellent Signal (None)")

        # Good signal branch
        if good_nodes:
            tree_lines.append("├─ 🟡 Good Signal")
            for i, node in enumerate(good_nodes[:6]):
                if i == len(good_nodes[:6]) - 1 and len(good_nodes) > 6:
                    tree_lines.append(f"│  └─ {node['long_name'][:20]} ({node.get('snr', 0):.1f}dB) +{len(good_nodes)-6} more")
                else:
                    tree_lines.append(f"│  ├─ {node['long_name'][:20]} ({node.get('snr', 0):.1f}dB)")
        else:
            tree_lines.append("├─ 🟡 Good Signal (None)")

        # Poor signal branch
        if poor_nodes:
            tree_lines.append("├─ 🔴 Poor Signal")
            for i, node in enumerate(poor_nodes[:4]):
                if i == len(poor_nodes[:4]) - 1 and len(poor_nodes) > 4:
                    tree_lines.append(f"│  └─ {node['long_name'][:20]} ({node.get('snr', 0):.1f}dB) +{len(poor_nodes)-4} more")
                else:
                    tree_lines.append(f"│  ├─ {node['long_name'][:20]} ({node.get('snr', 0):.1f}dB)")
        else:
            tree_lines.append("├─ 🔴 Poor Signal (None)")

        # Unknown signal branch
        if unknown_nodes:
            tree_lines.append("└─ ⚪ Unknown Signal")
            for i, node in enumerate(unknown_nodes[:4]):
                if i == len(unknown_nodes[:4]) - 1 and len(unknown_nodes) > 4:
                    tree_lines.append(f"   └─ {node['long_name'][:20]} +{len(unknown_nodes)-4} more")
                else:
                    tree_lines.append(f"   ├─ {node['long_name'][:20]}")
        else:
            tree_lines.append("└─ ⚪ Unknown Signal (None)")

        tree_lines.append("")
        tree_lines.append("Legend: 🟢 >10dB  🟡 5-10dB  🔴 <5dB  ⚪ Unknown")

        return "\n".join(tree_lines)

    def _create_network_diagram(self, nodes, connections):
        """Create ASCII network diagram for topology visualization"""
        diagram_lines = []
        diagram_lines.append("🌐 NETWORK TOPOLOGY DIAGRAM")
        diagram_lines.append("=" * 50)
        diagram_lines.append("")

        # Group nodes by activity and hops
        active_nodes = []
        for n in nodes:
            if n.get('last_heard'):
                try:
                    last_heard = datetime.fromisoformat(n['last_heard'].replace('Z', '+00:00'))
                    if last_heard > datetime.now() - timedelta(hours=1):
                        active_nodes.append(n)
                except (ValueError, TypeError) as e:
                    logger.warning("Error parsing last_heard for node %s: %s", n.get('long_name', 'Unknown'), e)
                    continue

        # Sort by hops away
        active_nodes.sort(key=lambda x: x.get('hops_away', 0))

        if not active_nodes:
            diagram_lines.append("⚪ No active nodes detected")
            return "\n".join(diagram_lines)

        # Create hierarchical diagram
        diagram_lines.append("📡 Active Network Nodes:")
        diagram_lines.append("")

        # Group by hops
        hop_groups = {}
        for node in active_nodes:
            hops = node.get('hops_away', 0)
            if hops not in hop_groups:
                hop_groups[hops] = []
            hop_groups[hops].append(node)

        # Draw the network tree
        for hops in sorted(hop_groups.keys()):
            nodes_at_hop = hop_groups[hops]

            if hops == 0:
                diagram_lines.append("🏠 DIRECT CONNECTIONS (0 hops)")
            else:
                diagram_lines.append(f"🔗 HOP {hops} NODES")

            for i, node in enumerate(nodes_at_hop[:6]):  # Limit to 6 per hop
                # Get signal quality indicator
                snr = node.get('snr')
                if snr is not None:
                    if snr > 10:
                        signal_icon = "🟢"
                    elif snr > 5:
                        signal_icon = "🟡"
                    else:
                        signal_icon = "🔴"
                else:
                    signal_icon = "⚪"

                # Get battery indicator
                battery = node.get('battery_level')
                if battery is not None:
                    if battery > 80:
                        battery_icon = "🔋"
                    elif battery > 40:
                        battery_icon = "🪫"
                    else:
                        battery_icon = "🔴"
                else:
                    battery_icon = "❓"

                # Format node name
                node_name = node['long_name'][:15]
                if i == len(nodes_at_hop) - 1 and len(nodes_at_hop) > 6:
                    diagram_lines.append(f"   └─ {signal_icon}{battery_icon} {node_name} +{len(nodes_at_hop)-6} more")
                else:
                    diagram_lines.append(f"   ├─ {signal_icon}{battery_icon} {node_name}")

            diagram_lines.append("")

        # Show connections if available
        if connections:
            diagram_lines.append("🔗 TOP CONNECTIONS:")
            for i, conn in enumerate(connections[:5]):
                from_name = self.database.get_node_display_name(conn['from_node'])[:12]
                to_name = self.database.get_node_display_name(conn['to_node'])[:12]
                msg_count = conn['message_count']
                avg_hops = conn['avg_hops']

                if i == len(connections[:5]) - 1 and len(connections) > 5:
                    diagram_lines.append(f"   {from_name} ──→ {to_name} ({msg_count}msgs) +{len(connections)-5} more")
                else:
                    diagram_lines.append(f"   {from_name} ──→ {to_name} ({msg_count}msgs)")

        diagram_lines.append("")
        diagram_lines.append("Legend: 🟢🟡🔴 Signal Quality | 🔋🪫🔴 Battery | ⚪❓ Unknown")

        return "\n".join(diagram_lines)

    def _format_node_info(self, node: Dict[str, Any]) -> str:
        """Format node information for display"""
        try:
            long_name = str(node.get('long_name', 'Unknown'))
            node_id = str(node.get('node_id', 'Unknown'))
            node_num = str(node.get('node_num', 'Unknown'))
            hops_away = str(node.get('hops_away', '0'))
            snr = str(node.get('snr', '?'))
            battery = f"{node.get('battery_level', 'N/A')}%" if node.get('battery_level') is not None else "N/A"
            temperature = f"{node.get('temperature', 'N/A'):.1f}°C" if node.get('temperature') is not None else "N/A"

            if node.get('last_heard'):
                try:
                    last_heard = datetime.fromisoformat(node['last_heard'])
                    time_str = last_heard.strftime('%H:%M:%S')
                except (ValueError, TypeError, AttributeError):
                    time_str = "Unknown"
            else:
                time_str = "Unknown"

            return (
                f"**{long_name}** (ID: {node_id}, Num: {node_num}) - "
                f"Hops: {hops_away}, SNR: {snr}, Battery: {battery}, "
                f"Temp: {temperature}, Last: {time_str}"
            )

        except Exception as e:
            logger.error("Error formatting node info: %s", e)
            return f"**Node {node.get('node_id', 'Unknown')}** - Error formatting data"

    def _create_connection_tree(self, nodes, connections):
        """Create readable ASCII tree for Discord showing network topology"""
        tree_lines = []
        tree_lines.append("🌐 MESH NETWORK TOPOLOGY")
        tree_lines.append("=" * 50)

        # Get active nodes (last 2 hours)
        active_nodes = []
        for n in nodes:
            if n.get('last_heard'):
                try:
                    last_heard = datetime.fromisoformat(n['last_heard'].replace('Z', '+00:00'))
                    if last_heard > datetime.now() - timedelta(hours=2):
                        active_nodes.append(n)
                except (ValueError, TypeError):
                    continue

        if not active_nodes:
            tree_lines.append("📡 No active nodes found")
            return "\n".join(tree_lines)

        # Build routing map
        routing_map = {}
        for conn in connections:
            from_node = conn['from_node']
            to_node = conn['to_node']
            if from_node not in routing_map:
                routing_map[from_node] = []
            if to_node not in routing_map:
                routing_map[to_node] = []
            routing_map[from_node].append({'node': to_node, 'msgs': conn['message_count']})
            routing_map[to_node].append({'node': from_node, 'msgs': conn['message_count']})

        # Sort and group by hops
        active_nodes.sort(key=lambda x: x.get('hops_away') or 0)
        hop_groups = {}
        for node in active_nodes:
            hops = node.get('hops_away') or 0
            if hops not in hop_groups:
                hop_groups[hops] = []
            hop_groups[hops].append(node)

        # Build readable tree
        for hops in sorted(hop_groups.keys()):
            nodes_at_hop = hop_groups[hops]

            # Hop header
            if hops == 0:
                tree_lines.append(f"\n📡 DIRECT CONNECTIONS (0 hops):")
            else:
                tree_lines.append(f"\n🔗 {hops} HOP{'S' if hops > 1 else ''} AWAY:")

            # Show nodes with better formatting
            for i, node in enumerate(nodes_at_hop):
                snr = node.get('snr')
                battery = node.get('battery_level')
                node_id = node.get('node_id')
                long_name = node.get('long_name', 'Unknown')

                # Signal quality indicators
                if snr is not None:
                    if snr > 10:
                        sig_icon = "🟢"  # Good
                        sig_text = "Good"
                    elif snr > 5:
                        sig_icon = "🟡"  # OK
                        sig_text = "OK"
                    else:
                        sig_icon = "🔴"  # Poor
                        sig_text = "Poor"
                else:
                    sig_icon = "⚪"  # Unknown
                    sig_text = "Unknown"

                # Battery level
                if battery is not None:
                    if battery > 80:
                        bat_icon = "🔋"  # Full
                        bat_text = "Full"
                    elif battery > 40:
                        bat_icon = "🪫"  # Low
                        bat_text = "Low"
                    else:
                        bat_icon = "🔋"  # Empty
                        bat_text = "Empty"
                else:
                    bat_icon = "❓"  # Unknown
                    bat_text = "Unknown"

                # Node type
                node_type = "Router" if node.get('is_router') else "Client"
                type_icon = "📡" if node.get('is_router') else "📱"

                # Find routing parent
                via_text = ""
                if hops > 0 and node_id in routing_map:
                    for parent_hops in range(hops):
                        parent_nodes = hop_groups.get(parent_hops, [])
                        for parent in parent_nodes:
                            parent_id = parent.get('node_id')
                            if parent_id and parent_id in routing_map:
                                for route in routing_map[parent_id]:
                                    if route['node'] == node_id:
                                        via_text = f" via {parent.get('long_name', 'Unknown')[:15]}"
                                        break
                                if via_text:
                                    break
                        if via_text:
                            break

                # Format node line
                node_line = (
                    f"  {type_icon} {long_name[:20]:<20} | {sig_icon} {sig_text:<6} | "
                    f"{bat_icon} {bat_text:<6} | ID: {node_id}{via_text}"
                )
                tree_lines.append(node_line)

        # Top connections
        if connections:
            tree_lines.append(f"\n🔗 TOP CONNECTIONS:")
            sorted_conns = sorted(connections, key=lambda x: x['message_count'], reverse=True)
            for i, conn in enumerate(sorted_conns[:5]):  # Top 5 connections
                from_name = self.database.get_node_display_name(conn['from_node'])[:15]
                to_name = self.database.get_node_display_name(conn['to_node'])[:15]
                msgs = conn['message_count']
                avg_hops = conn.get('avg_hops', 0)

                tree_lines.append(f"  {from_name} ↔ {to_name} ({msgs} msgs, {avg_hops:.1f} avg hops)")

        return "\n".join(tree_lines)

    def _calculate_tree_depth(self, connections):
        """Calculate the maximum depth of the connection tree"""
        if not connections:
            return 0

        # Simple heuristic: max hops in connections + 1
        max_hops = 0
        for conn in connections:
            hops = conn.get('avg_hops', 0)
            if hops is not None and hops > max_hops:
                max_hops = hops

        return int(max_hops) + 1

    def _analyze_connection_quality(self, nodes, connections):
        """Analyze and summarize connection quality"""
        if not nodes or not connections:
            return "No connection data available"

        # Analyze signal quality
        excellent_signal = 0
        good_signal = 0
        poor_signal = 0

        for node in nodes:
            snr = node.get('snr')
            if snr is not None:
                if snr > 10:
                    excellent_signal += 1
                elif snr > 5:
                    good_signal += 1
                else:
                    poor_signal += 1

        total_with_signal = excellent_signal + good_signal + poor_signal

        if total_with_signal == 0:
            signal_quality = "No signal data"
        else:
            excellent_pct = (excellent_signal / total_with_signal) * 100
            good_pct = (good_signal / total_with_signal) * 100
            poor_pct = (poor_signal / total_with_signal) * 100

            if excellent_pct > 60:
                signal_quality = f"🟢 Excellent ({excellent_pct:.0f}%)"
            elif good_pct > 40:
                signal_quality = f"🟡 Good ({good_pct:.0f}%)"
            else:
                signal_quality = f"🔴 Poor ({poor_pct:.0f}%)"

        # Analyze battery health
        high_battery = sum(1 for n in nodes if n.get('battery_level') is not None and n.get('battery_level') > 80)
        low_battery = sum(1 for n in nodes if n.get('battery_level') is not None and n.get('battery_level') < 40)

        if high_battery > low_battery:
            battery_health = f"🔋 Good ({high_battery} high)"
        elif low_battery > 0:
            battery_health = f"🪫 Low ({low_battery} critical)"
        else:
            battery_health = "❓ Unknown"

        return f"""Signal: {signal_quality}
Battery: {battery_health}
Connections: {len(connections)} active"""

    async def _send_long_message(self, channel, message: str):
        """Send long messages by splitting if needed"""
        try:
            if len(message) <= 2000:
                await channel.send(message)
            else:
                # Split into chunks
                chunks = [message[i:i+1900] for i in range(0, len(message), 1900)]
                for chunk in chunks:
                    await channel.send(chunk)
        except Exception as e:
            logger.error("Error sending long message: %s", e)
            # Try to send a simple error message
            try:
                await channel.send("❌ Error sending message to channel.")
            except discord.HTTPException:
                pass  # Already logged the main error

    async def _safe_send(self, channel, message: str):
        """Safely send a message to a channel with error handling"""
        try:
            await channel.send(message)
        except Exception as e:
            logger.error("Error sending message to channel: %s", e)
