import discord
import asyncio
import os
import sys
import io
import logging
from typing import Optional, Dict, Any, List, Tuple
from dataclasses import dataclass
from dotenv import load_dotenv
from pubsub import pub
import meshtastic
import meshtastic.tcp_interface
import meshtastic.serial_interface
import queue
import time
from datetime import datetime, timedelta
from database import MeshtasticDatabase

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

@dataclass
class Config:
    """Configuration class for bot settings"""
    discord_token: str
    channel_id: int
    meshtastic_hostname: Optional[str]
    message_max_length: int = 225
    node_refresh_interval: int = 60  # seconds
    active_node_threshold: int = 60  # minutes (changed from 15 to 60)
    telemetry_update_interval: int = 3600  # 1 hour in seconds

class MeshtasticInterface:
    """Handles Meshtastic radio communication"""
    
    def __init__(self, hostname: Optional[str] = None, database: Optional[MeshtasticDatabase] = None):
        self.hostname = hostname
        self.bridge_enabled = True
        self.iface = None  # Changed to match reference implementation
        self.database = database
        self.last_node_refresh = 0
        
    async def connect(self) -> bool:
        """Connect to Meshtastic radio"""
        try:
            if self.hostname and len(self.hostname) > 1:
                logger.info(f"Connecting to Meshtastic via TCP: {self.hostname}")
                self.iface = meshtastic.tcp_interface.TCPInterface(self.hostname)
            else:
                logger.info("Connecting to Meshtastic via Serial")
                self.iface = meshtastic.serial_interface.SerialInterface()
            
            # Wait for connection
            await asyncio.sleep(2)
            
            # Check connection status more safely
            try:
                if hasattr(self.iface, 'isConnected') and callable(self.iface.isConnected):
                    if self.iface.isConnected():
                        logger.info("Successfully connected to Meshtastic")
                        return True
                    else:
                        logger.error("Failed to connect to Meshtastic")
                        return False
                else:
                    # If isConnected method doesn't exist, assume connection is successful
                    logger.info("Connected to Meshtastic (connection status unknown)")
                    return True
                    
            except Exception as conn_check_error:
                logger.warning(f"Could not check connection status: {conn_check_error}")
                logger.info("Assuming connection is successful")
                return True
                
        except Exception as e:
            logger.error(f"Error connecting to Meshtastic: {e}")
            return False
    
    def send_text(self, message: str, destination_id: Optional[str] = None) -> bool:
        """Send text message via Meshtastic"""
        try:
            if not self.iface:
                logger.error("No Meshtastic interface available")
                return False
                
            if destination_id:
                logger.info(f"Attempting to send message to node {destination_id} (type: {type(destination_id)})")
                # Use the correct Meshtastic API based on the reference implementation
                try:
                    # Try the standard Meshtastic API
                    self.iface.sendText(message, destinationId=destination_id)
                    logger.info(f"Sent message to node {destination_id}: {message[:50]}...")
                except Exception as e:
                    logger.error(f"Error sending to specific node {destination_id}: {e}")
                    # Fallback to primary channel
                    logger.warning(f"Falling back to primary channel")
                    self.iface.sendText(message)
                    return True
            else:
                self.iface.sendText(message)
                logger.info(f"Sent message to primary channel: {message[:50]}...")
            return True
        except Exception as e:
            logger.error(f"Error sending message: {e}")
            return False
    
    def process_nodes(self) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Process and store nodes in database"""
        if not self.iface or not self.database:
            return [], []
        
        try:
            if not hasattr(self.iface, 'nodes'):
                logger.debug("Interface has no nodes attribute")
                return [], []
                
            nodes = self.iface.nodes
            if not nodes:
                logger.debug("No nodes available to process")
                return [], []
                
            processed_nodes = []
            new_nodes = []
            
            for node_id, node_data in nodes.items():
                try:
                    # Extract node information
                    node_info = {
                        'node_id': str(node_id),
                        'node_num': node_data.get('num'),
                        'long_name': str(node_data.get('user', {}).get('longName', 'Unknown')),
                        'short_name': str(node_data.get('user', {}).get('shortName', '')),
                        'macaddr': node_data.get('macaddr'),
                        'hw_model': node_data.get('hwModel'),
                        'firmware_version': node_data.get('firmwareVersion'),
                        'last_heard': datetime.fromtimestamp(node_data.get('lastHeard', time.time())).isoformat(),
                        'hops_away': node_data.get('hopsAway', 0),
                        'is_router': node_data.get('isRouter', False),
                        'is_client': node_data.get('isClient', True)
                    }
                    
                                    # Store in database
                    try:
                        success, is_new = self.database.add_or_update_node(node_info)
                        if success:
                            processed_nodes.append(node_info)
                            if is_new:
                                new_nodes.append(node_info)
                    except Exception as db_error:
                        logger.error(f"Database error for node {node_id}: {db_error}")
                        continue
                    
                    # Store telemetry if available
                    if 'snr' in node_data:
                        try:
                            telemetry_data = {
                                'snr': node_data.get('snr'),
                                'rssi': node_data.get('rssi'),
                                'frequency': node_data.get('frequency'),
                                'latitude': node_data.get('latitude'),
                                'longitude': node_data.get('longitude'),
                                'altitude': node_data.get('altitude'),
                                'speed': node_data.get('speed'),
                                'heading': node_data.get('heading'),
                                'accuracy': node_data.get('accuracy')
                            }
                            self.database.add_telemetry(node_info['node_id'], telemetry_data)
                        except Exception as telemetry_error:
                            logger.error(f"Error storing telemetry for node {node_id}: {telemetry_error}")
                    
                    # Store position if available
                    if 'latitude' in node_data and node_data['latitude'] is not None:
                        try:
                            position_data = {
                                'latitude': node_data.get('latitude'),
                                'longitude': node_data.get('longitude'),
                                'altitude': node_data.get('altitude'),
                                'speed': node_data.get('speed'),
                                'heading': node_data.get('heading'),
                                'accuracy': node_data.get('accuracy'),
                                'source': 'meshtastic'
                            }
                            self.database.add_position(node_info['node_id'], position_data)
                        except Exception as position_error:
                            logger.error(f"Error storing position for node {node_id}: {position_error}")
                        
                except Exception as e:
                    logger.error(f"Error processing node {node_id}: {e}")
                    continue
            
            self.last_node_refresh = time.time()
            logger.debug(f"Processed {len(processed_nodes)} nodes, {len(new_nodes)} new")
            return processed_nodes, new_nodes
            
        except Exception as e:
            logger.error(f"Error processing nodes: {e}")
            return [], []
    
    def get_nodes_from_db(self) -> List[Dict[str, Any]]:
        """Get nodes from database"""
        if not self.database:
            return []
        return self.database.get_all_nodes()

class CommandHandler:
    """Handles Discord bot commands"""
    
    def __init__(self, meshtastic: MeshtasticInterface, discord_to_mesh: queue.Queue, database: MeshtasticDatabase):
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
            '$status': self.cmd_status
        }
    
    async def handle_command(self, message: discord.Message) -> bool:
        """Route command to appropriate handler"""
        content = message.content.strip()
        
        for cmd, handler in self.commands.items():
            if content.startswith(cmd):
                await handler(message)
                return True
        
        return False
    
    async def cmd_help(self, message: discord.Message):
        """Show help information"""
        help_text = """**Meshtastic Discord Bridge Commands**

`$help` - Show this help message
`$txt <message>` - Send message to primary channel (max 225 chars)
`$send <longname> <message>` - Send message to specific node by name
`$activenodes` - Show nodes active in last 60 minutes
`$nodes` - Show all known nodes
`$bridge [on|off]` - Enable/disable mesh→Discord relay
`$last [N]` - Show last N text messages (default 10)
`$find <text> [N]` - Search messages
`$whois <query>` - Lookup node by name/ID
`$telem` - Show telemetry information
`$status` - Show bridge status

**Examples:**
`$txt Hello mesh network!`
`$send John Hello there!`
`$send "John Doe" Hello there!` (use quotes for names with spaces)"""
        
        await self._safe_send(message.channel, help_text)
    
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
        
        await self._safe_send(message.channel, f"📤 Sending to primary channel:\n```{message_text}```")
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
                logger.info(f"Searching for node with name: '{node_name}'")
                node = self.database.find_node_by_name(node_name)
                if not node:
                    await self._safe_send(message.channel, f"❌ No node found with name '{node_name}'. Try using `$nodes` to see available nodes.")
                    return
                
                logger.info(f"Found node: {node['long_name']} with ID: {node['node_id']}")
            except Exception as db_error:
                logger.error(f"Database error finding node by name: {db_error}")
                await self._safe_send(message.channel, "❌ Error searching for node in database.")
                return
            
            # Clean the node ID (remove any prefixes like '!' that Meshtastic doesn't expect)
            clean_node_id = node['node_id'].lstrip('!')
            
            # Log the node data for debugging
            logger.info(f"Node data: {node}")
            logger.info(f"Original node_id: '{node['node_id']}', Cleaned: '{clean_node_id}'")
            
            # Try to convert to integer format that Meshtastic expects
            try:
                # Convert hex string to integer (this is what Meshtastic typically expects)
                node_id_int = int(clean_node_id, 16)
                logger.info(f"Converted to integer: {node_id_int}")
                final_node_id = node_id_int
            except ValueError:
                # If conversion fails, use the cleaned string
                logger.info(f"Could not convert '{clean_node_id}' to integer, using string")
                final_node_id = clean_node_id
            
            await self._safe_send(message.channel, f"📤 Sending to node **{node['long_name']}** (ID: {final_node_id}):\n```{message_text}```")
            self.discord_to_mesh.put(f"nodenum={final_node_id} {message_text}")
            logger.info(f"Sent message with node ID: {final_node_id}")
            
        except Exception as e:
            logger.error(f"Error parsing send command: {e}")
            await self._safe_send(message.channel, "❌ Error parsing command. Use format: `$send <longname> <message>`")
    
    async def cmd_active_nodes(self, message: discord.Message):

        """Show nodes (paged embeds with stats)."""
        try:
            nodes = self.database.get_all_nodes() if cmd_active_nodes=="cmd_all_nodes" else self.database.get_active_nodes(60)
            if not nodes:
                await self._safe_send(message.channel, "📡 No nodes available.")
                return
            # Sort by last_heard desc
            def safe_key(n): 
                v = n.get('last_heard') or ''
                return v
            nodes.sort(key=safe_key, reverse=True)

            # Build embeds with up to 20 fields per page
            page_size = 20
            chunks = [nodes[i:i+page_size] for i in range(0, len(nodes), page_size)]
            for idx, chunk in enumerate(chunks, start=1):
                title = "All Known Nodes" if cmd_active_nodes=="cmd_all_nodes" else "Active Nodes (last 60m)"
                embed = discord.Embed(title=f"📡 {title} — Page {idx}/{len(chunks)}", timestamp=datetime.utcnow())
                for n in chunk:
                    name = n.get('long_name') or n.get('short_name') or n.get('node_id')
                    chips = []
                    if n.get('battery_level') is not None: chips.append(f"🔋 {n['battery_level']:.0f}%")
                    if n.get('temperature') is not None: chips.append(f"🌡️ {n['temperature']:.1f}°C")
                    if n.get('snr') is not None: chips.append(f"📶 {n['snr']:.1f} dB")
                    if n.get('rssi') is not None: chips.append(f"📶 RSSI {n['rssi']:.0f}")
                    lh = n.get('last_heard') or "unknown"
                    value = f"`{n.get('node_id','?')}`\n{', '.join(chips) if chips else '—'}\nLast heard: {lh}"
                    embed.add_field(name=name, value=value, inline=True)
                await message.channel.send(embed=embed)
        except Exception as e:
            logger.error(f"Error in cmd_active_nodes: {e}")
            await self._safe_send(message.channel, "❌ Error retrieving node list.")

    
    async def cmd_all_nodes(self, message: discord.Message):

        """Show nodes (paged embeds with stats)."""
        try:
            nodes = self.database.get_all_nodes() if cmd_all_nodes=="cmd_all_nodes" else self.database.get_active_nodes(60)
            if not nodes:
                await self._safe_send(message.channel, "📡 No nodes available.")
                return
            # Sort by last_heard desc
            def safe_key(n): 
                v = n.get('last_heard') or ''
                return v
            nodes.sort(key=safe_key, reverse=True)

            # Build embeds with up to 20 fields per page
            page_size = 20
            chunks = [nodes[i:i+page_size] for i in range(0, len(nodes), page_size)]
            for idx, chunk in enumerate(chunks, start=1):
                title = "All Known Nodes" if cmd_all_nodes=="cmd_all_nodes" else "Active Nodes (last 60m)"
                embed = discord.Embed(title=f"📡 {title} — Page {idx}/{len(chunks)}", timestamp=datetime.utcnow())
                for n in chunk:
                    name = n.get('long_name') or n.get('short_name') or n.get('node_id')
                    chips = []
                    if n.get('battery_level') is not None: chips.append(f"🔋 {n['battery_level']:.0f}%")
                    if n.get('temperature') is not None: chips.append(f"🌡️ {n['temperature']:.1f}°C")
                    if n.get('snr') is not None: chips.append(f"📶 {n['snr']:.1f} dB")
                    if n.get('rssi') is not None: chips.append(f"📶 RSSI {n['rssi']:.0f}")
                    lh = n.get('last_heard') or "unknown"
                    value = f"`{n.get('node_id','?')}`\n{', '.join(chips) if chips else '—'}\nLast heard: {lh}"
                    embed.add_field(name=name, value=value, inline=True)
                await message.channel.send(embed=embed)
        except Exception as e:
            logger.error(f"Error in cmd_all_nodes: {e}")
            await self._safe_send(message.channel, "❌ Error retrieving node list.")

    
    async def cmd_telemetry(self, message: discord.Message):
        """Show telemetry information"""
        try:
            summary = self.database.get_telemetry_summary(60)
            if not summary:
                await self._safe_send(message.channel, "❌ No telemetry data available.")
                return
        except Exception as db_error:
            logger.error(f"Database error getting telemetry summary: {db_error}")
            await self._safe_send(message.channel, "❌ Error retrieving telemetry data from database.")
            return
        
        response = f"📊 **Telemetry Summary (Last 60 minutes):**\n"
        response += f"Total nodes: {summary.get('total_nodes', 0)}\n"
        response += f"Active nodes: {summary.get('active_nodes', 0)}\n"
        if summary.get('avg_battery') is not None:
            response += f"Avg battery: {summary['avg_battery']:.1f}%\n"
        else:
            response += "Avg battery: N/A\n"
            
        if summary.get('avg_temperature') is not None:
            response += f"Avg temperature: {summary['avg_temperature']:.1f}°C\n"
        else:
            response += "Avg temperature: N/A\n"
            
        if summary.get('avg_humidity') is not None:
            response += f"Avg humidity: {summary['avg_humidity']:.1f}%\n"
        else:
            response += "Avg humidity: N/A\n"
            
        if summary.get('avg_snr') is not None:
            response += f"Avg SNR: {summary['avg_snr']:.1f} dB\n"
        else:
            response += "Avg SNR: N/A\n"
        # Check connection status safely
        connection_status = "❌ Disconnected"
        if self.meshtastic.iface:
            try:
                if hasattr(self.meshtastic.iface, 'isConnected') and callable(self.meshtastic.iface.isConnected):
                    if self.meshtastic.iface.isConnected():
                        connection_status = "✅ Connected"
            except Exception:
                connection_status = "❌ Disconnected"
        
        response += f"Connection: {connection_status}"
        
        await self._safe_send(message.channel, response)
    
    async def cmd_status(self, message: discord.Message):
        """Show bridge status"""
        status = "🔧 **Bridge Status:**\n"
        status += f"Discord: ✅ Connected\n"
        # Check Meshtastic connection status safely
        meshtastic_status = "❌ Disconnected"
        if self.meshtastic.iface:
            try:
                if hasattr(self.meshtastic.iface, 'isConnected') and callable(self.meshtastic.iface.isConnected):
                    if self.meshtastic.iface.isConnected():
                        meshtastic_status = "✅ Connected"
            except Exception:
                meshtastic_status = "❌ Disconnected"
        
        status += f"Meshtastic: {meshtastic_status}\n"
        status += f"Database: ✅ Connected\n"
        status += f"Uptime: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        
        await self._safe_send(message.channel, status)
    
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
                except:
                    time_str = "Unknown"
            else:
                time_str = "Unknown"
            
            return f"**{long_name}** (ID: {node_id}, Num: {node_num}) - Hops: {hops_away}, SNR: {snr}, Battery: {battery}, Temp: {temperature}, Last: {time_str}"
            
        except Exception as e:
            logger.error(f"Error formatting node info: {e}")
            return f"**Node {node.get('node_id', 'Unknown')}** - Error formatting data"
    
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
            logger.error(f"Error sending long message: {e}")
            # Try to send a simple error message
            try:
                await channel.send("❌ Error sending message to channel.")
            except:
                pass
    
    async def _safe_send(self, channel, message: str):
        """Safely send a message to a channel with error handling"""
        try:
            await channel.send(message)
        except Exception as e:
            logger.error(f"Error sending message to channel: {e}")


    async def cmd_bridge(self, message: discord.Message):
        """Toggle or show bridge status. Usage: $bridge [on|off]"""
        try:
            parts = message.content.strip().split(maxsplit=1)
            if len(parts) == 1:
                await self._safe_send(message.channel, f"🔧 Mesh→Discord relay is **{'enabled' if self.meshtastic.bridge_enabled else 'disabled'}**.")
                return
            arg = parts[1].strip().lower()
            if arg in ("on","enable","enabled"):
                self.meshtastic.bridge_enabled = True
                await self._safe_send(message.channel, "🔧 Mesh→Discord relay: **enabled**")
            elif arg in ("off","disable","disabled"):
                self.meshtastic.bridge_enabled = False
                await self._safe_send(message.channel, "🔧 Mesh→Discord relay: **disabled**")
            else:
                await self._safe_send(message.channel, "Usage: `$bridge on` or `$bridge off`")
        except Exception as e:
            logger.error(f"cmd_bridge error: {e}")

    async def cmd_last(self, message: discord.Message):
        """Show last N messages. Usage: $last [N]"""
        try:
            parts = message.content.strip().split()
            n = 10
            if len(parts) > 1 and parts[1].isdigit():
                n = min(max(int(parts[1]), 1), 50)
            rows = self.database.get_recent_messages(n)
            if not rows:
                await self._safe_send(message.channel, "🕰️ No recent messages.")
                return
            lines = []
            for r in rows:
                from_name = r.get('from_long') or r.get('from_short') or r.get('from_node_id')
                to_name   = r.get('to_long') or r.get('to_short') or r.get('to_node_id')
                ts = r.get('timestamp', '') or ''
                text = (r.get('message_text') or '').replace('\n',' ')[:self.meshtastic.config.message_max_length if hasattr(self.meshtastic,'config') else 225]
                meta = []
                if r.get('hops_away') is not None: meta.append(f"hops {r['hops_away']}")
                if r.get('snr') is not None: meta.append(f"SNR {r['snr']}")
                if r.get('rssi') is not None: meta.append(f"RSSI {r['rssi']}")
                meta_s = f" ({', '.join(meta)})" if meta else ''
                lines.append(f"`{ts}` **{from_name} → {to_name}**: {text}{meta_s}")
            await self._send_long_message(message.channel, "\n".join(lines))
        except Exception as e:
            logger.error(f"cmd_last error: {e}")
            await self._safe_send(message.channel, "❌ Error fetching recent messages.")

    async def cmd_find(self, message: discord.Message):
        """Search messages. Usage: $find <substring> [N]"""
        try:
            parts = message.content.strip().split(maxsplit=2)
            if len(parts) < 2:
                await self._safe_send(message.channel, "Usage: `$find <text> [N]`")
                return
            query = parts[1]
            n = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 10
            n = min(max(n,1),50)
            rows = self.database.search_messages(query, n)
            if not rows:
                await self._safe_send(message.channel, f"🔎 No matches for `{query}`.")
                return
            lines = []
            for r in rows:
                from_name = r.get('from_long') or r.get('from_short') or r.get('from_node_id')
                to_name   = r.get('to_long') or r.get('to_short') or r.get('to_node_id')
                ts = r.get('timestamp', '') or ''
                text = (r.get('message_text') or '').replace('\n',' ')
                lines.append(f"`{ts}` **{from_name} → {to_name}**: {text}")
            await self._send_long_message(message.channel, "\n".join(lines))
        except Exception as e:
            logger.error(f"cmd_find error: {e}")
            await self._safe_send(message.channel, "❌ Error searching messages.")

    async def cmd_whois(self, message: discord.Message):
        """Resolve a node by name or ID. Usage: $whois <query>"""
        try:
            parts = message.content.strip().split(maxsplit=1)
            if len(parts) < 2:
                await self._safe_send(message.channel, "Usage: `$whois <name-or-id>`")
                return
            q = parts[1].strip()
            node = self.database.find_node_by_name(q) or self.database.get_node_by_id(q)
            if not node:
                await self._safe_send(message.channel, f"❓ No matching node for `{q}`.")
                return
            embed = discord.Embed(title="📇 Node", description=node.get('long_name') or node.get('short_name') or node.get('node_id'))
            embed.add_field(name="Node ID", value=node.get('node_id'), inline=True)
            if node.get('short_name'): embed.add_field(name="Short", value=node.get('short_name'), inline=True)
            if node.get('node_num'):   embed.add_field(name="Num", value=node.get('node_num'), inline=True)
            if node.get('hops_away') is not None: embed.add_field(name="Hops", value=str(node.get('hops_away')), inline=True)
            if node.get('last_heard'): embed.add_field(name="Last heard", value=str(node.get('last_heard')), inline=False)
            await message.channel.send(embed=embed)
        except Exception as e:
            logger.error(f"cmd_whois error: {e}")
            await self._safe_send(message.channel, "❌ Error resolving node.")
class DiscordBot(discord.Client):
    """Enhanced Discord bot with Meshtastic integration and database"""
    
    def __init__(self, config: Config, meshtastic: MeshtasticInterface, database: MeshtasticDatabase):
        intents = discord.Intents.default()
        intents.message_content = True
        
        super().__init__(intents=intents)
        self.config = config
        self.meshtastic = meshtastic
        self.database = database
        
        # Queues for communication
        self.mesh_to_discord = queue.Queue()
        self.discord_to_mesh = queue.Queue()
        
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
        
        # Handle commands
        if message.content.startswith('$'):
            await self.command_handler.handle_command(message)
    
    async def background_task(self):
        """Background task for handling queues and Meshtastic events"""
        await self.wait_until_ready()
        
        # Subscribe to Meshtastic events
        pub.subscribe(self.on_mesh_receive, "meshtastic.receive")
        pub.subscribe(self.on_mesh_connection, "meshtastic.connection.established")
        
        channel = self.get_channel(self.config.channel_id)
        if not channel:
            logger.error(f"Could not find channel with ID {self.config.channel_id}")
            return
        
        logger.info("Background task started")
        
        while not self.is_closed():
            try:
                # Process mesh to Discord messages
                await self._process_mesh_to_discord(channel)
                
                # Process Discord to mesh messages
                await self._process_discord_to_mesh()
                
                # Process nodes periodically
                if time.time() - self.meshtastic.last_node_refresh >= self.config.node_refresh_interval:
                    await self._process_nodes(channel)
                
                await asyncio.sleep(1)  # Check every second
                
            except Exception as e:
                logger.error(f"Error in background task: {e}")
                await asyncio.sleep(5)
    
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
                logger.error(f"Error in telemetry update task: {e}")
                await asyncio.sleep(60)
    
    async def _process_nodes(self, channel):
        """Process and store nodes, announce new ones"""
        try:
            result = self.meshtastic.process_nodes()
            if result and len(result) == 2:
                processed_nodes, new_nodes = result
                
                # Announce new nodes
                for node in new_nodes:
                    await self._announce_new_node(channel, node)
            else:
                logger.debug("No nodes processed or invalid result format")
                
        except Exception as e:
            logger.error(f"Error processing nodes: {e}")
    
    async def _announce_new_node(self, channel, node: Dict[str, Any]):
        """Announce new node with embed"""
        try:
            embed = discord.Embed(
                title="🆕 New Node Detected!",
                description=f"**{node['long_name']}** has joined the mesh network",
                color=0x00ff00,
                timestamp=datetime.now()
            )
            
            embed.add_field(name="Node ID", value=node['node_id'], inline=True)
            embed.add_field(name="Node Number", value=node.get('node_num', 'N/A'), inline=True)
            embed.add_field(name="Hardware", value=node.get('hw_model', 'Unknown'), inline=True)
            embed.add_field(name="Firmware", value=node.get('firmware_version', 'Unknown'), inline=True)
            embed.add_field(name="Hops Away", value=node.get('hops_away', 0), inline=True)
            
            await channel.send(embed=embed)
            logger.info(f"Announced new node: {node['long_name']}")
            
        except Exception as e:
            logger.error(f"Error announcing new node: {e}")
    
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
                logger.error(f"Database error getting telemetry summary for update: {db_error}")
                return
            
            embed = discord.Embed(
                title="📊 Hourly Telemetry Update",
                description="Latest telemetry data from active nodes",
                color=0x0099ff,
                timestamp=datetime.now()
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
            logger.error(f"Error sending telemetry update: {e}")
    
    
    async def _process_mesh_to_discord(self, channel):
        """Process messages from mesh to Discord"""
        try:
            while not self.mesh_to_discord.empty():
                item = self.mesh_to_discord.get_nowait()
                try:
                    if isinstance(item, dict) and item.get('type') == 'text':
                        embed = discord.Embed(title="📨 Mesh Text", description=str(item.get('text', '')), timestamp=datetime.utcnow())
                        frm = f"{item.get('from_name', item.get('from_id'))} (`{item.get('from_id')}`)"
                        to  = f"{item.get('to_name', item.get('to_id'))} (`{item.get('to_id')}`)"
                        embed.add_field(name="From", value=frm, inline=True)
                        embed.add_field(name="To", value=to, inline=True)
                        hops = item.get('hops_away')
                        snr = item.get('snr')
                        rssi = item.get('rssi')
                        sig_bits = []
                        if hops is not None: sig_bits.append(f"hops {hops}")
                        if snr is not None: sig_bits.append(f"SNR {snr}")
                        if rssi is not None: sig_bits.append(f"RSSI {rssi}")
                        if sig_bits:
                            embed.add_field(name="Link", value=", ".join(sig_bits), inline=False)
                        await channel.send(embed=embed)
                    else:
                        await channel.send(f"📡 **Mesh Message:** {item}")
                finally:
                    self.mesh_to_discord.task_done()
        except queue.Empty:
            pass
        except Exception as e:
            logger.error(f"Error processing mesh to Discord: {e}")
    
    async def _process_discord_to_mesh(self):
        """Process messages from Discord to mesh"""
        try:
            while not self.discord_to_mesh.empty():
                message = self.discord_to_mesh.get_nowait()
                
                if message.startswith('nodenum='):
                    # Extract node ID and message
                    parts = message.split(' ', 1)
                    if len(parts) == 2:
                        node_id = parts[0][8:]  # Remove 'nodenum='
                        message_text = parts[1]
                        logger.info(f"Processing node message: node_id='{node_id}', message='{message_text[:50]}...'")
                        try:
                            self.meshtastic.send_text(message_text, destination_id=node_id)
                        except Exception as send_error:
                            logger.error(f"Error sending message to node {node_id}: {send_error}")
                else:
                    # Send to primary channel
                    try:
                        self.meshtastic.send_text(message)
                    except Exception as send_error:
                        logger.error(f"Error sending message to primary channel: {send_error}")
                
                self.discord_to_mesh.task_done()
                
        except queue.Empty:
            pass
        except Exception as e:
            logger.error(f"Error processing Discord to mesh: {e}")
    
    def on_mesh_receive(self, packet, interface):
        """Handle incoming mesh packets"""
        try:
            if 'decoded' in packet and packet['decoded']['portnum'] == 'TEXT_MESSAGE_APP':
                from_id = packet.get('fromId', 'Unknown')
                to_id = packet.get('toId', 'Primary')
                text = packet['decoded']['text']
                
                from_name = self.database.get_node_display_name(from_id) if self.database else from_id
                to_name = self.database.get_node_display_name(to_id) if self.database else to_id
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
                    logger.error(f"Error storing message in database: {msg_error}")
                
        except Exception as e:
            logger.error(f"Error processing mesh packet: {e}")
    
    def on_mesh_connection(self, interface, topic=pub.AUTO_TOPIC):
        """Handle mesh connection events"""
        logger.info(f"Connected to Meshtastic: {interface.myInfo}")

def main():
    """Main function to run the bot"""
    try:
        # Load configuration
        config = Config(
            discord_token=os.getenv("DISCORD_TOKEN"),
            channel_id=int(os.getenv("DISCORD_CHANNEL_ID", "0")),
            meshtastic_hostname=os.getenv("MESHTASTIC_HOSTNAME")
        )
        
        # Validate configuration
        if not config.discord_token:
            logger.error("DISCORD_TOKEN not found in environment variables")
            sys.exit(1)
        
        if not config.channel_id:
            logger.error("DISCORD_CHANNEL_ID not found or invalid in environment variables")
            sys.exit(1)
        
        # Initialize database
        try:
            database = MeshtasticDatabase()
            logger.info("Database initialized successfully")
        except Exception as db_error:
            logger.error(f"Failed to initialize database: {db_error}")
            sys.exit(1)
        
        # Create Meshtastic interface
        try:
            meshtastic_interface = MeshtasticInterface(config.meshtastic_hostname, database)
            logger.info("Meshtastic interface created successfully")
        except Exception as mesh_error:
            logger.error(f"Failed to create Meshtastic interface: {mesh_error}")
            sys.exit(1)
        
        # Create and run bot
        try:
            bot = DiscordBot(config, meshtastic_interface, database)
            logger.info("Discord bot created successfully")
            bot.run(config.discord_token)
        except Exception as bot_error:
            logger.error(f"Failed to create or run Discord bot: {bot_error}")
            sys.exit(1)
        
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()