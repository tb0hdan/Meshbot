"""Message handling utilities for Discord/Mesh communication.

Handles processing of messages between Discord and Meshtastic networks.
"""
import asyncio
import logging
import queue
from datetime import datetime
from typing import Optional, Dict, Any

import discord

logger = logging.getLogger(__name__)


def get_utc_time():
    """Get current time in UTC"""
    return datetime.utcnow()


class MessageProcessor:
    """Processes messages between Discord and Mesh networks"""

    def __init__(self, database, meshtastic):
        self.database = database
        self.meshtastic = meshtastic

    async def process_mesh_to_discord(self, mesh_to_discord_queue: queue.Queue, channel, command_handler):
        """Process messages from mesh to Discord with improved error handling"""
        try:
            processed_count = 0
            max_batch_size = 10  # Process max 10 messages at once

            while not mesh_to_discord_queue.empty() and processed_count < max_batch_size:
                item = mesh_to_discord_queue.get_nowait()
                try:
                    if isinstance(item, dict):
                        if item.get('type') == 'text':
                            await self._process_text_message(item, channel)
                        elif item.get('type') == 'traceroute':
                            await self._process_traceroute_message(item, channel)
                        elif item.get('type') == 'movement':
                            await self._process_movement_message(item, channel)

                        # Special handling for ping messages
                        if item.get('type') == 'text' and item.get('text', '').strip().lower() == "ping":
                            await self._handle_ping_response(item, channel)
                    else:
                        # Handle other message types
                        message_text = f"📡 **Mesh Message:** {str(item)[:1900]}"
                        await channel.send(message_text)

                    processed_count += 1

                except discord.HTTPException as e:
                    logger.error("Discord API error sending message: %s", e)
                except Exception as e:
                    logger.error("Error processing individual mesh message: %s", e)
                finally:
                    mesh_to_discord_queue.task_done()

        except queue.Empty:
            pass
        except Exception as e:
            logger.error("Error processing mesh to Discord: %s", e)
            await self._clear_queue_on_error(mesh_to_discord_queue)

    async def _process_text_message(self, item: Dict[str, Any], channel):
        """Process a text message for Discord display"""
        from_name = item.get('from_name', item.get('from_id', 'Unknown'))
        to_name = item.get('to_name', item.get('to_id', 'Unknown'))
        text = str(item.get('text', ''))
        hops = item.get('hops_away', 0)

        # Validate message content
        if not text.strip():
            logger.warning("Empty message from %s", from_name)
            return

        # Format destination - use "Longfast Channel" for broadcasts
        if to_name == "^all" or to_name == "^all(^all)":
            destination = "Longfast Channel"
        else:
            destination = to_name

        # Format hops with bunny emoji
        hops_text = f"🐰{hops} hops" if hops is not None else "🐰0 hops"

        # Create single line message with length limit
        message_text = f"📨 **{from_name}** → **{destination}** {hops_text}: {text}"
        if len(message_text) > 2000:
            message_text = message_text[:1997] + "..."

        await channel.send(message_text)
        logger.info(
            "📤 DISCORD: Sent message to Discord - '%s%s' from %s",
            text[:30], '...' if len(text) > 30 else '', from_name
        )

    async def _process_traceroute_message(self, item: Dict[str, Any], channel):
        """Process a traceroute message for Discord display"""
        from_name = item.get('from_name', item.get('from_id', 'Unknown'))
        to_name = item.get('to_name', item.get('to_id', 'Unknown'))
        route_text = item.get('route_text', '')
        hops_count = item.get('hops_count', 0)

        # Create traceroute embed
        embed = discord.Embed(
            title="🛣️ Traceroute Result",
            description=f"**{from_name}** traced route to **{to_name}**",
            color=0x00bfff,
            timestamp=datetime.utcnow()
        )

        embed.add_field(
            name="📍 Route Path",
            value=route_text,
            inline=False
        )

        embed.add_field(
            name="📊 Statistics",
            value=f"Total Hops: {hops_count}",
            inline=True
        )

        embed.set_footer(text=f"Traceroute completed at")

        await channel.send(embed=embed)
        logger.info("🛣️ DISCORD: Sent traceroute info - %s → %s (%s hops)", from_name, to_name, hops_count)

    async def _process_movement_message(self, item: Dict[str, Any], channel):
        """Process a movement message for Discord display"""
        from_name = item.get('from_name', item.get('from_id', 'Unknown'))
        distance_moved = item.get('distance_moved', 0)
        old_lat = item.get('old_lat', 0)
        old_lon = item.get('old_lon', 0)
        new_lat = item.get('new_lat', 0)
        new_lon = item.get('new_lon', 0)
        new_alt = item.get('new_alt', 0)

        # Format coordinates for display
        old_coords = f"{old_lat:.6f}, {old_lon:.6f}"
        new_coords = f"{new_lat:.6f}, {new_lon:.6f}"

        # Create movement embed
        embed = discord.Embed(
            title="🚶 Node is on the move!",
            description=f"**{from_name}** has moved a significant distance",
            color=0xff6b35,
            timestamp=datetime.utcnow()
        )

        # Add movement details
        movement_text = f"**Distance:** {distance_moved:.1f} meters\n"
        movement_text += f"**From:** `{old_coords}`\n"
        movement_text += f"**To:** `{new_coords}`"

        if new_alt != 0:
            movement_text += f"\n**Altitude:** {new_alt}m"

        embed.add_field(name="📍 Movement Details", value=movement_text, inline=False)

        # Add a fun movement indicator
        if distance_moved > 1000:
            embed.add_field(name="🏃 Speed", value="Moving fast!", inline=True)
        elif distance_moved > 500:
            embed.add_field(name="🚶 Speed", value="Walking pace", inline=True)
        else:
            embed.add_field(name="🐌 Speed", value="Slow movement", inline=True)

        embed.set_footer(text=f"Movement detected at")
        await channel.send(embed=embed)
        logger.info("🚶 DISCORD: Sent movement notification - %s moved %.1fm", from_name, distance_moved)

    async def _handle_ping_response(self, item: Dict[str, Any], channel):
        """Handle ping message response"""
        from_name = item.get('from_name', item.get('from_id', 'Unknown'))

        # Wait a moment for the ping message to be displayed first
        await asyncio.sleep(1.0)

        # Then show the pong response
        pong_embed = discord.Embed(
            title="🏓 Pong Response",
            description=f"Pong! sent to mesh network in response to **{from_name}**",
            color=0x00ff00,
            timestamp=get_utc_time()
        )
        pong_embed.set_footer(text="🌍 UTC Time | Mesh network response")
        await channel.send(embed=pong_embed)
        logger.info("Pong response announced for ping from %s", from_name)

    async def _clear_queue_on_error(self, message_queue: queue.Queue):
        """Clear queue on error to prevent memory buildup"""
        try:
            while not message_queue.empty():
                try:
                    message_queue.get_nowait()
                    message_queue.task_done()
                except queue.Empty:
                    break
        except Exception as e:
            logger.warning("Error clearing message queue: %s", e)

    async def process_discord_to_mesh(self, discord_to_mesh_queue: queue.Queue):
        """Process messages from Discord to mesh"""
        try:
            while not discord_to_mesh_queue.empty():
                try:
                    message = discord_to_mesh_queue.get_nowait()
                except queue.Empty:
                    break

                if message.startswith('nodenum='):
                    await self._send_direct_message(message)
                else:
                    await self._send_broadcast_message(message)

                discord_to_mesh_queue.task_done()

        except queue.Empty:
            pass
        except Exception as e:
            logger.error("Error processing Discord to mesh: %s", e)

    async def _send_direct_message(self, message: str):
        """Send direct message to specific node"""
        # Extract node ID and message
        parts = message.split(' ', 1)
        if len(parts) == 2:
            node_id = parts[0][8:]  # Remove 'nodenum='
            message_text = parts[1]
            logger.info(
                "📤 MESH: Sending message to node %s - '%s%s'",
                node_id, message_text[:50], '...' if len(message_text) > 50 else ''
            )
            try:
                self.meshtastic.send_text(message_text, destination_id=node_id)
                logger.info("✅ MESH: Message sent successfully to node %s", node_id)
            except Exception as send_error:
                logger.error("❌ MESH: Error sending message to node %s: %s", node_id, send_error)

    async def _send_broadcast_message(self, message: str):
        """Send broadcast message to primary channel"""
        logger.info(
            "📤 MESH: Sending message to primary channel - '%s%s'",
            message[:50], '...' if len(message) > 50 else ''
        )
        try:
            self.meshtastic.send_text(message)
            logger.info("✅ MESH: Message sent successfully to primary channel")
        except Exception as send_error:
            logger.error("❌ MESH: Error sending message to primary channel: %s", send_error)
