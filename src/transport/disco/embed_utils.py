"""Discord embed utilities for Meshbot application.

Provides standardized embed creation for various message types.
"""
from datetime import datetime
from typing import Dict, Any, Optional

import discord


def get_utc_time():
    """Get current time in UTC"""
    return datetime.utcnow()


class EmbedBuilder:
    """Utility class for creating Discord embeds"""

    @staticmethod
    def create_ping_embed(action: str, description: str, color: int = 0x00ff00,
                         author_name: str = "Unknown") -> discord.Embed:
        """Create a ping test embed"""
        embed = discord.Embed(
            title="🏓 Ping Test",
            description=description,
            color=color,
            timestamp=get_utc_time()
        )
        embed.add_field(
            name="📡 **Action**",
            value=action,
            inline=False
        )
        embed.set_footer(text=f"Requested by {author_name}")
        return embed

    @staticmethod
    def create_ping_success_embed(author_name: str = "Unknown") -> discord.Embed:
        """Create a successful ping response embed"""
        embed = discord.Embed(
            title="✅ Ping Successful",
            description="Pong! sent to mesh network successfully",
            color=0x00ff00,
            timestamp=get_utc_time()
        )
        embed.add_field(
            name="📡 **Status**",
            value="✅ Message sent to Longfast Channel",
            inline=False
        )
        embed.set_footer(text=f"Completed for {author_name}")
        return embed

    @staticmethod
    def create_ping_failure_embed(author_name: str = "Unknown") -> discord.Embed:
        """Create a failed ping response embed"""
        embed = discord.Embed(
            title="❌ Ping Failed",
            description="Failed to send pong to mesh network",
            color=0xff0000,
            timestamp=get_utc_time()
        )
        embed.add_field(
            name="📡 **Status**",
            value="❌ Unable to send to Longfast Channel",
            inline=False
        )
        embed.set_footer(text=f"Failed for {author_name}")
        return embed

    @staticmethod
    def create_ping_error_embed(error_message: str, author_name: str = "Unknown") -> discord.Embed:
        """Create a ping error embed"""
        embed = discord.Embed(
            title="❌ Ping Error",
            description="An error occurred while testing connectivity",
            color=0xff0000,
            timestamp=get_utc_time()
        )
        embed.add_field(
            name="📡 **Error**",
            value=f"```{str(error_message)[:500]}```",
            inline=False
        )
        embed.set_footer(text=f"Error for {author_name}")
        return embed

    @staticmethod
    def create_pong_response_embed(from_name: str) -> discord.Embed:
        """Create a pong response embed"""
        embed = discord.Embed(
            title="🏓 Pong Response",
            description=f"Pong! sent to mesh network in response to **{from_name}**",
            color=0x00ff00,
            timestamp=get_utc_time()
        )
        embed.set_footer(text="🌍 UTC Time | Mesh network response")
        return embed

    @staticmethod
    def create_new_node_embed(node: Dict[str, Any]) -> discord.Embed:
        """Create a new node announcement embed"""
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

        return embed

    @staticmethod
    def create_telemetry_update_embed(summary: Dict[str, Any]) -> discord.Embed:
        """Create an hourly telemetry update embed"""
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

        return embed

    @staticmethod
    def create_traceroute_embed(from_name: str, to_name: str, route_text: str,
                               hops_count: int) -> discord.Embed:
        """Create a traceroute result embed"""
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
        return embed

    @staticmethod
    def create_movement_embed(from_name: str, distance_moved: float,
                            old_lat: float, old_lon: float,
                            new_lat: float, new_lon: float,
                            new_alt: float) -> discord.Embed:
        """Create a movement notification embed"""
        embed = discord.Embed(
            title="🚶 Node is on the move!",
            description=f"**{from_name}** has moved a significant distance",
            color=0xff6b35,
            timestamp=datetime.utcnow()
        )

        # Format coordinates for display
        old_coords = f"{old_lat:.6f}, {old_lon:.6f}"
        new_coords = f"{new_lat:.6f}, {new_lon:.6f}"

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
        return embed

    @staticmethod
    def create_error_embed(title: str, description: str, error_details: Optional[str] = None) -> discord.Embed:
        """Create a generic error embed"""
        embed = discord.Embed(
            title=title,
            description=description,
            color=0xff0000,
            timestamp=get_utc_time()
        )

        if error_details:
            embed.add_field(
                name="Error Details",
                value=f"```{error_details[:500]}```",
                inline=False
            )

        return embed

    @staticmethod
    def create_success_embed(title: str, description: str, details: Optional[str] = None) -> discord.Embed:
        """Create a generic success embed"""
        embed = discord.Embed(
            title=title,
            description=description,
            color=0x00ff00,
            timestamp=get_utc_time()
        )

        if details:
            embed.add_field(
                name="Details",
                value=details,
                inline=False
            )

        return embed

    @staticmethod
    def create_info_embed(title: str, description: str, fields: Optional[Dict[str, str]] = None) -> discord.Embed:
        """Create a generic info embed"""
        embed = discord.Embed(
            title=title,
            description=description,
            color=0x0099ff,
            timestamp=get_utc_time()
        )

        if fields:
            for name, value in fields.items():
                embed.add_field(name=name, value=value, inline=True)

        return embed
