"""Monitoring command implementations for Meshbot."""
import asyncio
import logging
import time
from datetime import datetime

import discord

from .base import BaseCommandMixin, get_utc_time

logger = logging.getLogger(__name__)


class MonitoringCommands(BaseCommandMixin):
    """Monitoring and telemetry command functionality"""
    
    def __init__(self, meshtastic, discord_to_mesh, database):
        super().__init__()
        self.meshtastic = meshtastic
        self.discord_to_mesh = discord_to_mesh
        self.database = database
        
        # Live monitor state
        self._live_monitors = {}  # user_id -> {'active': bool, 'task': asyncio.Task}
        self._packet_buffer = []  # Store recent packets for live display
        self._max_packet_buffer = 50  # Keep last 50 packets
        self._packet_buffer_lock = asyncio.Lock()  # Thread safety for packet buffer

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
Current Time: {get_utc_time().strftime('%H:%M:%S UTC')}""",
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
        """Run the live monitor for 60 seconds"""
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