"""Network analysis command implementations for Meshbot."""
# pylint: disable=duplicate-code
import logging
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any

import discord

from .base import BaseCommandMixin, get_utc_time

logger = logging.getLogger(__name__)


class NetworkCommands(BaseCommandMixin):
    """Network analysis and topology command functionality"""

    def __init__(self, meshtastic, discord_to_mesh, database):
        super().__init__()
        self.meshtastic = meshtastic
        self.discord_to_mesh = discord_to_mesh
        self.database = database

    async def cmd_network_topology(  # pylint: disable=too-many-branches
            self, message: discord.Message):
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

        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.error("Error getting network topology: %s", e)
            await self._safe_send(message.channel, "❌ Error retrieving network topology.")

    async def cmd_topology_tree(  # pylint: disable=too-many-branches
            self, message: discord.Message):
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

        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.error("Error creating topology tree: %s", e)
            await self._safe_send(message.channel, "❌ Error creating connection tree.")

    async def cmd_message_statistics(  # pylint: disable=too-many-branches
            self, message: discord.Message):
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

        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.error("Error getting message statistics: %s", e)
            await self._safe_send(message.channel, "❌ Error retrieving message statistics.")

    async def cmd_trace_route(  # pylint: disable=too-many-branches,too-many-statements,too-many-locals
            self, message: discord.Message):
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

            embed.set_footer(text="Route analysis completed")
            await message.channel.send(embed=embed)

        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.error("Error tracing route: %s", e)
            await self._safe_send(message.channel, "❌ Error tracing route to node.")

    async def cmd_leaderboard(  # pylint: disable=too-many-branches,too-many-locals,too-many-statements
            self, message: discord.Message):
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
                        logger.warning(
                            "Error parsing last_heard for node %s: %s",
                            n.get('long_name', 'Unknown'), e
                        )
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

        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.error("Error creating leaderboard: %s", e)
            await self._safe_send(message.channel, "❌ Error creating leaderboard.")

    async def cmd_network_art(  # pylint: disable=too-many-branches,too-many-locals,too-many-statements
            self, message: discord.Message):
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
                    except (ValueError, TypeError) as e:  # pylint: disable=broad-exception-caught
                        logger.warning(
                            "Error parsing last_heard for node %s: %s",
                            n.get('long_name', 'Unknown'), e
                        )
                        continue

            if active_nodes:
                art_lines.append("🟢 ACTIVE NODES:")
                for node in active_nodes[:8]:  # Limit to 8 for ASCII art
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
                for conn in topology['connections'][:5]:
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

        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.error("Error creating network art: %s", e)
            await self._safe_send(message.channel, "❌ Error creating network art.")

    def _analyze_route_to_node(  # pylint: disable=too-many-locals,unused-argument
            self, target_node_id: str, topology: dict) -> list:
        """Analyze the route to a specific node based on message data"""
        try:
            # Get all messages to the target node
            with self.database._get_connection() as conn:  # pylint: disable=protected-access
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
            hop_groups: Dict[int, List[Any]] = {}
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

        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.error("Error analyzing route to node %s: %s", target_node_id, e)
            return []

    def _format_route_path(  # pylint: disable=too-many-locals,too-many-branches,too-many-statements
            self, route_path: list) -> str:
        """Format the route path for display with visual indicators"""
        if not route_path:
            return "No route data available"

        path_lines = []

        for i, hop in enumerate(route_path):
            node_name = hop['node_name']
            node_id = hop['node_id']
            # hops_away = hop['hops_away']  # pylint: disable=unused-variable
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
        if snr > 5:
            return "🟡"  # Good
        if snr > 0:
            return "🟠"  # Fair
        return "🔴"  # Poor

    def _assess_route_quality(self, avg_snr: float, total_hops: int) -> str:
        """Assess overall route quality"""
        if avg_snr > 10 and total_hops <= 2:
            return "🟢 Excellent"
        if avg_snr > 5 and total_hops <= 4:
            return "🟡 Good"
        if avg_snr > 0 and total_hops <= 6:
            return "🟠 Fair"
        return "🔴 Poor"

    def _create_network_diagram(self, nodes, connections):  # pylint: disable=too-many-locals,too-many-branches,too-many-statements,too-many-nested-blocks
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
                    if last_heard > datetime.now(timezone.utc) - timedelta(hours=1):
                        active_nodes.append(n)
                except (ValueError, TypeError) as e:  # pylint: disable=broad-exception-caught
                    logger.warning(
                        "Error parsing last_heard for node %s: %s",
                        n.get('long_name', 'Unknown'), e
                    )
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
                    diagram_lines.append(
                        f"   └─ {signal_icon}{battery_icon} {node_name} +{len(nodes_at_hop)-6} more"
                    )
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
                # avg_hops = conn['avg_hops']  # pylint: disable=unused-variable

                if i == len(connections[:5]) - 1 and len(connections) > 5:
                    diagram_lines.append(
                        f"   {from_name} ──→ {to_name} ({msg_count}msgs) +{len(connections)-5} more"
                    )
                else:
                    diagram_lines.append(f"   {from_name} ──→ {to_name} ({msg_count}msgs)")

        diagram_lines.append("")
        diagram_lines.append(
            "Legend: 🟢🟡🔴 Signal Quality | 🔋🪫🔴 Battery | ⚪❓ Unknown"
        )

        return "\n".join(diagram_lines)

    def _create_connection_tree(self, nodes, connections):  # pylint: disable=too-many-locals,too-many-branches,too-many-statements,too-many-nested-blocks
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
        for hops in sorted(hop_groups.keys()):  # pylint: disable=too-many-nested-blocks
            nodes_at_hop = hop_groups[hops]

            # Hop header
            if hops == 0:
                tree_lines.append("\n📡 DIRECT CONNECTIONS (0 hops):")
            else:
                tree_lines.append(f"\n🔗 {hops} HOP{'S' if hops > 1 else ''} AWAY:")

            # Show nodes with better formatting
            for node in nodes_at_hop:
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
                # node_type = "Router" if node.get('is_router') else "Client"  # pylint: disable=unused-variable
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
            tree_lines.append("\n🔗 TOP CONNECTIONS:")
            sorted_conns = sorted(
                connections, key=lambda x: x['message_count'], reverse=True
            )
            for conn in sorted_conns[:5]:  # Top 5 connections
                from_name = self.database.get_node_display_name(conn['from_node'])[:15]
                to_name = self.database.get_node_display_name(conn['to_node'])[:15]
                msgs = conn['message_count']
                avg_hops = conn.get('avg_hops', 0)

                tree_lines.append(
                    f"  {from_name} ↔ {to_name} ({msgs} msgs, {avg_hops:.1f} avg hops)"
                )

        return "\n".join(tree_lines)
