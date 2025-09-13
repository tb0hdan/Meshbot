"""Discord bot implementation for Meshbot application.

Provides Discord client functionality for bridging with Meshtastic networks.
Handles message processing, command routing, and telemetry display.
"""
# Standard library imports
import asyncio
import logging
import math
import queue
import time
from datetime import datetime
from typing import Optional, Dict, Any

# Third party imports
import discord
from pubsub import pub

# Local imports will be done dynamically to avoid circular imports
from src.config import Config
from src.commands import CommandHandler

# Configure logging
logger = logging.getLogger(__name__)

def get_utc_time():
    """Get current time in UTC"""
    return datetime.utcnow()


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
        self.mesh_to_discord = queue.Queue(maxsize=self.config.max_queue_size)
        self.discord_to_mesh = queue.Queue(maxsize=self.config.max_queue_size)

        # Initialize command handler after queues are created
        self.command_handler = CommandHandler(meshtastic, self.discord_to_mesh, database)

        # Background task
        self.bg_task = None
        self.telemetry_task = None

        # Track last telemetry update hour
        self.last_telemetry_hour = datetime.now().hour

    async def setup_hook(self) -> None:
        """Setup bot when starting"""
        self.bg_task = self.loop.create_task(self.background_task())
        self.telemetry_task = self.loop.create_task(self.telemetry_update_task())

    async def on_ready(self):
        """Called when bot is ready"""
        logger.info(f'Logged in as {self.user} (ID: {self.user.id})')
        logger.info('------')

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
        try:
            # Create a nice embed for the ping response
            embed = discord.Embed(
                title="🏓 Ping Test",
                description="Testing mesh network connectivity...",
                color=0x00ff00,
                timestamp=get_utc_time()
            )
            embed.add_field(
                name="📡 **Action**",
                value="Sending Pong! to mesh network",
                inline=False
            )
            embed.set_footer(text=f"Requested by {message.author.display_name}")

            # Send initial response
            await message.channel.send(embed=embed)

            # Send pong to mesh network
            pong_sent = self.meshtastic.send_text("Pong!")
            # Small delay to prevent timing issues
            await asyncio.sleep(0.5)
            if pong_sent:
                # Update with success
                success_embed = discord.Embed(
                    title="✅ Ping Successful",
                    description="Pong! sent to mesh network successfully",
                    color=0x00ff00,
                    timestamp=get_utc_time()
                )
                success_embed.add_field(
                    name="📡 **Status**",
                    value="✅ Message sent to Longfast Channel",
                    inline=False
                )
                success_embed.set_footer(text=f"Completed for {message.author.display_name}")
                await message.channel.send(embed=success_embed)
                logger.info("Ping/pong handled from %s", message.author.name)
            else:
                # Update with failure
                fail_embed = discord.Embed(
                    title="❌ Ping Failed",
                    description="Failed to send pong to mesh network",
                    color=0xff0000,
                    timestamp=get_utc_time()
                )
                fail_embed.add_field(
                    name="📡 **Status**",
                    value="❌ Unable to send to Longfast Channel",
                    inline=False
                )
                fail_embed.set_footer(text=f"Failed for {message.author.display_name}")
                await message.channel.send(embed=fail_embed)
        except Exception as e:
            logger.error("Error handling ping: %s", e)
            error_embed = discord.Embed(
                title="❌ Ping Error",
                description="An error occurred while testing connectivity",
                color=0xff0000,
                timestamp=get_utc_time()
            )
            error_embed.add_field(
                name="📡 **Error**",
                value=f"```{str(e)[:500]}```",
                inline=False
            )
            await message.channel.send(embed=error_embed)

    async def background_task(self):
        """Background task for handling queues and Meshtastic events"""
        await self.wait_until_ready()

        # Subscribe to Meshtastic events
        pub.subscribe(self.on_mesh_receive, "meshtastic.receive")
        pub.subscribe(self.on_mesh_connection, "meshtastic.connection.established")

        channel = self.get_channel(self.config.channel_id)
        if not channel:
            logger.error("Could not find channel with ID %s", self.config.channel_id)
            return

        logger.info("Background task started")

        # Performance counters
        last_cleanup = time.time()
        cleanup_interval = 300  # 5 minutes

        while not self.is_closed():
            try:
                # Process mesh to Discord messages
                await self._process_mesh_to_discord(channel)

                # Process Discord to mesh messages
                await self._process_discord_to_mesh()

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

    async def _periodic_cleanup(self):
        """Perform periodic cleanup tasks"""
        try:
            # Clear command handler cache
            if hasattr(self.command_handler, 'clear_cache'):
                self.command_handler.clear_cache()

            # Clean up old database data
            if hasattr(self.database, 'cleanup_old_data'):
                self.database.cleanup_old_data(30)  # Keep 30 days

            logger.debug("Periodic cleanup completed")

        except Exception as e:
            logger.error("Error during periodic cleanup: %s", e)

    async def telemetry_update_task(self):
        """Task for hourly telemetry updates"""
        await self.wait_until_ready()

        while not self.is_closed():
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

    async def _announce_new_node(self, channel, node: Dict[str, Any]):
        """Announce new node with embed"""
        try:
            embed = discord.Embed(
                title="🆕 New Node Detected!",
                description=f"**{node['long_name']}** has joined the mesh network",
                color=0x00ff00,
                timestamp=get_utc_time()
            )

            embed.add_field(name="Node ID", value=node['node_id'], inline=True)
            embed.add_field(name="Node Number", value=node.get('node_num', 'N/A'), inline=True)
            embed.add_field(name="Hardware", value=node.get('hw_model', 'Unknown'), inline=True)
            embed.add_field(name="Firmware", value=node.get('firmware_version', 'Unknown'), inline=True)
            embed.add_field(name="Hops Away", value=node.get('hops_away', 0), inline=True)

            await channel.send(embed=embed)
            logger.info("Announced new node: %s", node['long_name'])

        except Exception as e:
            logger.error("Error announcing new node: %s", e)

    async def _send_telemetry_update(self):
        """Send hourly telemetry update"""
        try:
            channel = self.get_channel(self.config.channel_id)
            if not channel:
                return

            try:
                summary = self.database.get_telemetry_summary(60)
                if not summary:
                    return
            except Exception as db_error:
                logger.error("Database error getting telemetry summary for update: %s", db_error)
                return

            embed = discord.Embed(
                title="📊 Hourly Telemetry Update",
                description="Latest telemetry data from active nodes",
                color=0x0099ff,
                timestamp=get_utc_time()
            )

            embed.add_field(name="Active Nodes", value=summary.get('active_nodes', 0), inline=True)
            embed.add_field(name="Total Nodes", value=summary.get('total_nodes', 0), inline=True)

            if summary.get('avg_battery') is not None:
                embed.add_field(name="Avg Battery", value=f"{summary['avg_battery']:.1f}%", inline=True)
            if summary.get('avg_temperature') is not None:
                embed.add_field(name="Avg Temperature", value=f"{summary['avg_temperature']:.1f}°C", inline=True)
            if summary.get('avg_humidity') is not None:
                embed.add_field(name="Avg Humidity", value=f"{summary['avg_humidity']:.1f}%", inline=True)
            if summary.get('avg_snr') is not None:
                embed.add_field(name="Avg SNR", value=f"{summary['avg_snr']:.1f} dB", inline=True)

            await channel.send(embed=embed)
            logger.info("Sent hourly telemetry update")

        except Exception as e:
            logger.error("Error sending telemetry update: %s", e)

    async def _process_mesh_to_discord(self, channel):
        """Process messages from mesh to Discord with improved error handling"""
        try:
            processed_count = 0
            max_batch_size = 10  # Process max 10 messages at once

            while not self.mesh_to_discord.empty() and processed_count < max_batch_size:
                item = self.mesh_to_discord.get_nowait()
                try:
                    if isinstance(item, dict):
                        if item.get('type') == 'text':
                            # Format as single line message
                            from_name = item.get('from_name', item.get('from_id', 'Unknown'))
                            to_name = item.get('to_name', item.get('to_id', 'Unknown'))
                            text = str(item.get('text', ''))
                            hops = item.get('hops_away', 0)

                            # Validate message content
                            if not text.strip():
                                logger.warning("Empty message from %s", from_name)
                                continue

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
                            processed_count += 1

                        elif item.get('type') == 'traceroute':
                            # Format traceroute information
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
                            processed_count += 1

                        elif item.get('type') == 'movement':
                            # Format movement notification
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
                        processed_count += 1

                        # Special handling for ping messages - show pong response after a delay
                        if item.get('type') == 'text' and item.get('text', '').strip().lower() == "ping":
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

                    else:
                        # Handle other message types
                        message_text = f"📡 **Mesh Message:** {str(item)[:1900]}"
                        await channel.send(message_text)
                        processed_count += 1

                except discord.HTTPException as e:
                    logger.error("Discord API error sending message: %s", e)
                    # Don't re-queue the message, just log and continue
                except Exception as e:
                    logger.error("Error processing individual mesh message: %s", e)
                finally:
                    self.mesh_to_discord.task_done()

        except queue.Empty:
            pass
        except Exception as e:
            logger.error("Error processing mesh to Discord: %s", e)
            # Try to clear the queue to prevent memory buildup
            try:
                while not self.mesh_to_discord.empty():
                    try:
                        self.mesh_to_discord.get_nowait()
                        self.mesh_to_discord.task_done()
                    except queue.Empty:
                        break
            except Exception as e:
                logger.warning("Error clearing mesh to discord queue: %s", e)

    async def _process_discord_to_mesh(self):
        """Process messages from Discord to mesh"""
        try:
            while not self.discord_to_mesh.empty():
                try:
                    message = self.discord_to_mesh.get_nowait()
                except queue.Empty:
                    break

                if message.startswith('nodenum='):
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
                else:
                    # Send to primary channel
                    logger.info(
                        "📤 MESH: Sending message to primary channel - '%s%s'", 
                        message[:50], '...' if len(message) > 50 else ''
                    )
                    try:
                        self.meshtastic.send_text(message)
                        logger.info("✅ MESH: Message sent successfully to primary channel")
                    except Exception as send_error:
                        logger.error("❌ MESH: Error sending message to primary channel: %s", send_error)

                self.discord_to_mesh.task_done()

        except queue.Empty:
            pass
        except Exception as e:
            logger.error("Error processing Discord to mesh: %s", e)

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

            # Handle text messages
            if portnum == 'TEXT_MESSAGE_APP':
                to_id = packet.get('toId', 'Primary')
                text = packet['decoded']['text']

                from_name = self.database.get_node_display_name(from_id) if self.database else from_id
                to_name = self.database.get_node_display_name(to_id) if self.database else to_id

                # Check for ping messages from mesh
                if text.strip().lower() == "ping":
                    logger.info("Ping received from mesh node %s", from_name)
                    # Send pong back to mesh with sender's name
                    try:
                        pong_message = f"Pong! - - > {from_name}"
                        self.meshtastic.send_text(pong_message)
                        logger.info("Pong sent to mesh network: %s", pong_message)

                    except Exception as pong_error:
                        logger.error("Error sending pong to mesh: %s", pong_error)

                    # Continue processing the ping message normally (don't return early)

                msg_payload = {
                    'type': 'text',
                    'from_id': from_id,
                    'from_name': from_name,
                    'to_id': to_id,
                    'to_name': to_name,
                    'text': text,
                    'hops_away': packet.get('hopsAway', 0),
                    'snr': packet.get('snr'),
                    'rssi': packet.get('rssi'),
                    'timestamp': datetime.utcnow().isoformat() + 'Z'
                }
                self.mesh_to_discord.put(msg_payload)
                logger.info(
                    "💬 MESSAGE: Queued for Discord - '%s%s' from %s", 
                    text[:50], '...' if len(text) > 50 else '', from_name
                )

                # Add text message to live monitor buffer
                text_packet_info = {
                    'type': 'text',
                    'portnum': portnum,
                    'from_name': from_name,
                    'from_id': from_id,
                    'to_id': to_id,
                    'text': text,
                    'hops': hops_away,
                    'snr': snr,
                    'rssi': rssi
                }
                if hasattr(self, 'command_handler') and self.command_handler:
                    self.command_handler.add_packet_to_buffer(text_packet_info)

                # Store message in database
                try:
                    message_data = {
                        'from_node_id': from_id,
                        'to_node_id': to_id,
                        'message_text': text,
                        'port_num': packet['decoded']['portnum'],
                        'payload': str(packet.get('payload', '')),
                        'hops_away': packet.get('hopsAway', 0),
                        'snr': packet.get('snr'),
                        'rssi': packet.get('rssi')
                    }
                    self.database.add_message(message_data)
                except Exception as msg_error:
                    logger.error("Error storing message in database: %s", msg_error)

            # Handle telemetry packets
            elif portnum == 'TELEMETRY_APP':
                if from_id and from_id != 'Unknown' and from_id is not None:
                    logger.info("📊 TELEMETRY: Processing sensor data from %s", from_name)
                    self._process_telemetry_packet(packet)
                else:
                    logger.warning("📊 TELEMETRY: Skipping packet with invalid fromId: %s", from_id)

            # Handle position packets
            elif portnum == 'POSITION_APP':
                logger.info("📍 POSITION: Location update from %s", from_name)
                self._process_position_packet(packet)

            # Handle node info packets
            elif portnum == 'NODEINFO_APP':
                logger.info("👤 NODE INFO: Node information from %s", from_name)
                # Node info packets are handled by Meshtastic library automatically

            # Handle routing packets (traceroute)
            elif portnum == 'ROUTING_APP':
                logger.info("🛣️ ROUTING: Processing traceroute from %s", from_name)
                self._process_routing_packet(packet)

            # Handle admin packets
            elif portnum == 'ADMIN_APP':
                logger.info("⚙️ ADMIN: Administrative message from %s", from_name)
                # Admin packets are handled by Meshtastic library automatically

            # Handle other packet types
            else:
                logger.info("❓ UNKNOWN: %s packet from %s", portnum, from_name)

        except Exception as e:
            logger.error("Error processing mesh packet: %s", e)

    def _process_telemetry_packet(self, packet):
        """Process telemetry packet and extract sensor data"""
        try:
            from_id = packet.get('fromId', 'Unknown')

            # Skip if we don't have a valid node ID
            if not from_id or from_id == 'Unknown' or from_id is None:
                logger.warning("Skipping telemetry packet with invalid fromId: %s", from_id)
                return

            decoded = packet.get('decoded', {})
            telemetry_data = decoded.get('telemetry', {})

            if not telemetry_data:
                logger.debug("No telemetry data in packet from %s", from_id)
                return

            # Extract different types of telemetry data
            extracted_data = {}

            # Device metrics (battery, voltage, uptime, etc.)
            if 'deviceMetrics' in telemetry_data:
                device_metrics = telemetry_data['deviceMetrics']
                if device_metrics.get('batteryLevel') is not None:
                    extracted_data['battery_level'] = device_metrics['batteryLevel']
                if device_metrics.get('voltage') is not None:
                    extracted_data['voltage'] = device_metrics['voltage']
                if device_metrics.get('channelUtilization') is not None:
                    extracted_data['channel_utilization'] = device_metrics['channelUtilization']
                if device_metrics.get('airUtilTx') is not None:
                    extracted_data['air_util_tx'] = device_metrics['airUtilTx']
                if device_metrics.get('uptimeSeconds') is not None:
                    extracted_data['uptime_seconds'] = device_metrics['uptimeSeconds']

            # Environment metrics (temperature, humidity, pressure, etc.)
            if 'environmentMetrics' in telemetry_data:
                env_metrics = telemetry_data['environmentMetrics']
                if env_metrics.get('temperature') is not None:
                    extracted_data['temperature'] = env_metrics['temperature']
                if env_metrics.get('relativeHumidity') is not None:
                    extracted_data['humidity'] = env_metrics['relativeHumidity']
                if env_metrics.get('barometricPressure') is not None:
                    extracted_data['pressure'] = env_metrics['barometricPressure']
                if env_metrics.get('gasResistance') is not None:
                    extracted_data['gas_resistance'] = env_metrics['gasResistance']

            # Air quality metrics
            if 'airQualityMetrics' in telemetry_data:
                air_metrics = telemetry_data['airQualityMetrics']
                if air_metrics.get('pm10Environmental') is not None:
                    extracted_data['pm10'] = air_metrics['pm10Environmental']
                if air_metrics.get('pm25Environmental') is not None:
                    extracted_data['pm25'] = air_metrics['pm25Environmental']
                if air_metrics.get('pm100Environmental') is not None:
                    extracted_data['pm100'] = air_metrics['pm100Environmental']
                if air_metrics.get('aqi') is not None:
                    extracted_data['iaq'] = air_metrics['aqi']

            # Power metrics
            if 'powerMetrics' in telemetry_data:
                power_metrics = telemetry_data['powerMetrics']
                if power_metrics.get('ch1Voltage') is not None:
                    extracted_data['ch1_voltage'] = power_metrics['ch1Voltage']
                if power_metrics.get('ch2Voltage') is not None:
                    extracted_data['ch2_voltage'] = power_metrics['ch2Voltage']
                if power_metrics.get('ch3Voltage') is not None:
                    extracted_data['ch3_voltage'] = power_metrics['ch3Voltage']

            # Add radio metrics from packet
            if packet.get('snr') is not None:
                extracted_data['snr'] = packet['snr']
            if packet.get('rssi') is not None:
                extracted_data['rssi'] = packet['rssi']
            if packet.get('frequency') is not None:
                extracted_data['frequency'] = packet['frequency']

            # Store telemetry data if we have any
            if extracted_data:
                try:
                    success = self.database.add_telemetry(from_id, extracted_data)
                    if success:
                        logger.info("Stored telemetry data for %s: %s", from_id, list(extracted_data.keys()))

                        # Add telemetry to live monitor buffer
                        if from_id and from_id != 'Unknown' and from_id is not None:
                            telemetry_packet_info = {
                                'type': 'telemetry',
                                'portnum': 'TELEMETRY_APP',
                                'from_name': self.database.get_node_display_name(from_id) if self.database else from_id,
                                'from_id': from_id,
                                'sensor_data': list(extracted_data.keys()),
                                'hops': 0,  # Telemetry doesn't have hops info
                                'snr': 'N/A',
                                'rssi': 'N/A'
                            }
                            if hasattr(self, 'command_handler') and self.command_handler:
                                self.command_handler.add_packet_to_buffer(telemetry_packet_info)
                    else:
                        logger.warning("Failed to store telemetry data for %s", from_id)
                except Exception as telemetry_error:
                    logger.error("Error storing telemetry data for %s: %s", from_id, telemetry_error)
            else:
                logger.debug("No extractable telemetry data from %s", from_id)

        except Exception as e:
            logger.error("Error processing telemetry packet: %s", e)

    def _process_routing_packet(self, packet):
        """Process routing packet and display traceroute information in Discord"""
        try:
            from_id = packet.get('fromId', 'Unknown')
            to_id = packet.get('toId', 'Primary')
            decoded = packet.get('decoded', {})

            # Check if this is a RouteDiscovery packet
            if 'routing' in decoded and 'routeDiscovery' in decoded['routing']:
                route_data = decoded['routing']['routeDiscovery']

                # Get node display names
                from_name = self.database.get_node_display_name(from_id) if self.database else from_id
                to_name = self.database.get_node_display_name(to_id) if self.database else to_id

                # Extract route information
                route = route_data.get('route', [])
                route_back = route_data.get('routeBack', [])
                snr_towards = route_data.get('snrTowards', [])
                snr_back = route_data.get('snrBack', [])

                # Build route string
                route_parts = []

                # Route towards destination
                if route:
                    route_parts.append(f"**Towards {to_name}:**")
                    current_route = f"{from_name}"

                    for i, node_num in enumerate(route):
                        node_name = (
                            self.database.get_node_display_name(f"!{node_num:08x}") 
                            if self.database else f"!{node_num:08x}"
                        )
                        snr = ""
                        if i < len(snr_towards) and snr_towards[i] != -128:  # -128 is UNK_SNR
                            snr = f" ({snr_towards[i]/4:.1f}dB)"
                        current_route += f" → {node_name}{snr}"

                    # Add destination
                    if snr_towards and len(snr_towards) > len(route):
                        snr = f" ({snr_towards[-1]/4:.1f}dB)" if snr_towards[-1] != -128 else ""
                    else:
                        snr = ""
                    current_route += f" → {to_name}{snr}"

                    route_parts.append(current_route)

                # Route back from destination
                if route_back:
                    route_parts.append(f"**Back from {to_name}:**")
                    back_route = f"{to_name}"

                    for i, node_num in enumerate(route_back):
                        node_name = (
                            self.database.get_node_display_name(f"!{node_num:08x}") 
                            if self.database else f"!{node_num:08x}"
                        )
                        snr = ""
                        if i < len(snr_back) and snr_back[i] != -128:  # -128 is UNK_SNR
                            snr = f" ({snr_back[i]/4:.1f}dB)"
                        back_route += f" → {node_name}{snr}"

                    # Add origin
                    if snr_back and len(snr_back) > len(route_back):
                        snr = f" ({snr_back[-1]/4:.1f}dB)" if snr_back[-1] != -128 else ""
                    else:
                        snr = ""
                    back_route += f" → {from_name}{snr}"

                    route_parts.append(back_route)

                # Create Discord message
                if route_parts:
                    route_text = "\n".join(route_parts)
                    hops_count = len(route) + len(route_back) if route_back else len(route)

                    # Queue for Discord display
                    traceroute_payload = {
                        'type': 'traceroute',
                        'from_id': from_id,
                        'from_name': from_name,
                        'to_id': to_id,
                        'to_name': to_name,
                        'route_text': route_text,
                        'hops_count': hops_count,
                        'timestamp': datetime.utcnow().isoformat() + 'Z'
                    }
                    self.mesh_to_discord.put(traceroute_payload)
                    logger.info("🛣️ TRACEROUTE: Queued route info - %s → %s (%s hops)", from_name, to_name, hops_count)

                    # Add traceroute to live monitor buffer
                    traceroute_packet_info = {
                        'type': 'traceroute',
                        'portnum': 'ROUTING_APP',
                        'from_name': from_name,
                        'from_id': from_id,
                        'to_name': to_name,
                        'to_id': to_id,
                        'hops_count': hops_count,
                        'hops': 0,  # Route hops, not packet hops
                        'snr': 'N/A',
                        'rssi': 'N/A'
                    }
                    if hasattr(self, 'command_handler') and self.command_handler:
                        self.command_handler.add_packet_to_buffer(traceroute_packet_info)
                else:
                    logger.debug("No route information in routing packet from %s", from_name)
            else:
                logger.debug("Routing packet from %s does not contain RouteDiscovery data", from_name)

        except Exception as e:
            logger.error("Error processing routing packet: %s", e)

    def _process_position_packet(self, packet):
        """Process position packet and detect movement"""
        try:
            from_id = packet.get('fromId', 'Unknown')
            decoded = packet.get('decoded', {})
            position_data = decoded.get('position', {})

            if not position_data:
                logger.debug("No position data in packet from %s", from_id)
                return

            # Extract position coordinates
            new_lat = position_data.get('latitude_i', 0) / 1e7  # Convert from integer to float
            new_lon = position_data.get('longitude_i', 0) / 1e7
            new_alt = position_data.get('altitude', 0)

            # Skip if coordinates are invalid (0,0)
            if new_lat == 0 and new_lon == 0:
                logger.debug("Invalid position coordinates (0,0) from %s", from_id)
                return

            # Get last known position from database
            if self.database:
                last_position = self.database.get_last_position(from_id)

                if last_position:
                    last_lat = last_position.get('latitude', 0)
                    last_lon = last_position.get('longitude', 0)

                    # Calculate distance moved
                    if last_lat != 0 and last_lon != 0:
                        distance_moved = self.calculate_distance(last_lat, last_lon, new_lat, new_lon)

                        # Movement threshold: 100 meters (configurable)
                        movement_threshold = 100.0

                        if distance_moved > movement_threshold:
                            # Node has moved significantly!
                            from_name = self.database.get_node_display_name(from_id) if self.database else from_id

                            # Create movement notification
                            movement_payload = {
                                'type': 'movement',
                                'from_id': from_id,
                                'from_name': from_name,
                                'distance_moved': distance_moved,
                                'old_lat': last_lat,
                                'old_lon': last_lon,
                                'new_lat': new_lat,
                                'new_lon': new_lon,
                                'new_alt': new_alt,
                                'timestamp': datetime.utcnow().isoformat() + 'Z'
                            }

                            # Queue for Discord
                            self.mesh_to_discord.put(movement_payload)
                            logger.info("🚶 MOVEMENT: %s moved %.1fm from last position", from_name, distance_moved)

                            # Add to live monitor buffer
                            if hasattr(self, 'command_handler') and self.command_handler:
                                movement_packet_info = {
                                    'type': 'movement',
                                    'portnum': 'POSITION_APP',
                                    'from_name': from_name,
                                    'from_id': from_id,
                                    'distance_moved': distance_moved,
                                    'hops': 0,
                                    'snr': 'N/A',
                                    'rssi': 'N/A'
                                }
                                self.command_handler.add_packet_to_buffer(movement_packet_info)

            # Store new position in database
            if self.database:
                try:
                    position_data_to_store = {
                        'latitude': new_lat,
                        'longitude': new_lon,
                        'altitude': new_alt,
                        'speed': position_data.get('speed', 0),
                        'heading': position_data.get('ground_track', 0),
                        'accuracy': position_data.get('precision_bits', 0),
                        'source': 'meshtastic'
                    }
                    self.database.add_position(from_id, position_data_to_store)
                    logger.debug("Stored position for %s: %.6f, %.6f", from_id, new_lat, new_lon)
                except Exception as pos_error:
                    logger.error("Error storing position for %s: %s", from_id, pos_error)

        except Exception as e:
            logger.error("Error processing position packet: %s", e)

    def calculate_distance(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Calculate distance between two coordinates in meters using Haversine formula"""
        try:
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

    def on_mesh_connection(self, interface, topic=pub.AUTO_TOPIC):
        """Handle mesh connection events"""
        logger.info("Connected to Meshtastic: %s", interface.myInfo)

    async def close(self):
        """Clean shutdown of the bot"""
        try:
            logger.info("Shutting down bot...")

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

            # Cancel background tasks
            if self.bg_task and not self.bg_task.done():
                self.bg_task.cancel()
                try:
                    await self.bg_task
                except asyncio.CancelledError:
                    pass

            if self.telemetry_task and not self.telemetry_task.done():
                self.telemetry_task.cancel()
                try:
                    await self.telemetry_task
                except asyncio.CancelledError:
                    pass

            # Close Discord connection
            await super().close()
            logger.info("Bot shutdown complete")

        except Exception as e:
            logger.error("Error during bot shutdown: %s", e)
            await super().close()
