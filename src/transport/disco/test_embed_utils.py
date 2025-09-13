"""Tests for Discord embed utilities."""
from datetime import datetime, timezone
from unittest.mock import patch

import pytest
import discord

from .embed_utils import EmbedBuilder, get_utc_time


class TestGetUtcTime:
    """Tests for get_utc_time function."""

    def test_get_utc_time_returns_datetime(self):
        """Test that get_utc_time returns a datetime object."""
        result = get_utc_time()
        assert isinstance(result, datetime)

    def test_get_utc_time_is_recent(self):
        """Test that get_utc_time returns recent time."""
        now = datetime.utcnow()
        result = get_utc_time()
        diff = abs((result - now).total_seconds())
        assert diff < 1.0  # Should be within 1 second


class TestEmbedBuilder:
    """Tests for EmbedBuilder class."""

    def test_create_ping_embed_basic(self):
        """Test basic ping embed creation."""
        embed = EmbedBuilder.create_ping_embed(
            action="Test Action",
            description="Test Description"
        )

        assert isinstance(embed, discord.Embed)
        assert embed.title == "🏓 Ping Test"
        assert embed.description == "Test Description"
        assert embed.color.value == 0x00ff00
        assert len(embed.fields) == 1
        assert embed.fields[0].name == "📡 **Action**"
        assert embed.fields[0].value == "Test Action"
        assert embed.footer.text == "Requested by Unknown"

    def test_create_ping_embed_with_custom_params(self):
        """Test ping embed with custom parameters."""
        embed = EmbedBuilder.create_ping_embed(
            action="Custom Action",
            description="Custom Description",
            color=0xff0000,
            author_name="TestUser"
        )

        assert embed.color.value == 0xff0000
        assert embed.footer.text == "Requested by TestUser"

    def test_create_ping_success_embed(self):
        """Test ping success embed creation."""
        embed = EmbedBuilder.create_ping_success_embed("TestUser")

        assert isinstance(embed, discord.Embed)
        assert embed.title == "✅ Ping Successful"
        assert "Pong! sent to mesh network successfully" in embed.description
        assert embed.color.value == 0x00ff00
        assert embed.footer.text == "Completed for TestUser"

    def test_create_ping_failure_embed(self):
        """Test ping failure embed creation."""
        embed = EmbedBuilder.create_ping_failure_embed("TestUser")

        assert isinstance(embed, discord.Embed)
        assert embed.title == "❌ Ping Failed"
        assert "Failed to send pong to mesh network" in embed.description
        assert embed.color.value == 0xff0000
        assert embed.footer.text == "Failed for TestUser"

    def test_create_ping_error_embed(self):
        """Test ping error embed creation."""
        error_msg = "Connection timeout"
        embed = EmbedBuilder.create_ping_error_embed(error_msg, "TestUser")

        assert isinstance(embed, discord.Embed)
        assert embed.title == "❌ Ping Error"
        assert "An error occurred while testing connectivity" in embed.description
        assert embed.color.value == 0xff0000
        assert error_msg in embed.fields[0].value
        assert embed.footer.text == "Error for TestUser"

    def test_create_ping_error_embed_long_message(self):
        """Test ping error embed with long error message."""
        error_msg = "A" * 600  # Long error message
        embed = EmbedBuilder.create_ping_error_embed(error_msg, "TestUser")

        # Should truncate to 500 characters
        assert len(embed.fields[0].value) <= 506  # 500 + "```" wrapper

    def test_create_pong_response_embed(self):
        """Test pong response embed creation."""
        embed = EmbedBuilder.create_pong_response_embed("TestNode")

        assert isinstance(embed, discord.Embed)
        assert embed.title == "🏓 Pong Response"
        assert "TestNode" in embed.description
        assert embed.color.value == 0x00ff00
        assert "🌍 UTC Time | Mesh network response" in embed.footer.text

    def test_create_new_node_embed(self, sample_node_data):
        """Test new node embed creation."""
        embed = EmbedBuilder.create_new_node_embed(sample_node_data)

        assert isinstance(embed, discord.Embed)
        assert embed.title == "🆕 New Node Detected!"
        assert sample_node_data['long_name'] in embed.description
        assert embed.color.value == 0x00ff00

        # Check fields
        field_names = [field.name for field in embed.fields]
        assert "Node ID" in field_names
        assert "Hardware" in field_names

    def test_create_new_node_embed_missing_fields(self):
        """Test new node embed with missing optional fields."""
        minimal_node = {
            'long_name': 'Minimal Node',
            'node_id': '!12345678'
        }

        embed = EmbedBuilder.create_new_node_embed(minimal_node)

        assert isinstance(embed, discord.Embed)
        assert embed.title == "🆕 New Node Detected!"
        assert "Minimal Node" in embed.description

        # Should handle missing fields gracefully
        field_values = [field.value for field in embed.fields]
        assert "N/A" in field_values or "Unknown" in field_values

    def test_create_telemetry_update_embed(self, sample_telemetry_summary):
        """Test telemetry update embed creation."""
        embed = EmbedBuilder.create_telemetry_update_embed(sample_telemetry_summary)

        assert isinstance(embed, discord.Embed)
        assert embed.title == "📊 Hourly Telemetry Update"
        assert embed.color.value == 0x0099ff

        # Check that summary data is included
        field_names = [field.name for field in embed.fields]
        assert "Active Nodes" in field_names
        assert "Total Nodes" in field_names
        assert "Avg Battery" in field_names

    def test_create_telemetry_update_embed_partial_data(self):
        """Test telemetry update embed with partial data."""
        partial_summary = {
            'active_nodes': 3,
            'total_nodes': 5,
            'avg_battery': None,  # Missing data
            'avg_temperature': 22.5
        }

        embed = EmbedBuilder.create_telemetry_update_embed(partial_summary)

        assert isinstance(embed, discord.Embed)
        field_names = [field.name for field in embed.fields]
        assert "Active Nodes" in field_names
        assert "Avg Temperature" in field_names
        # Should not include fields with None values
        assert "Avg Battery" not in field_names

    def test_create_traceroute_embed(self):
        """Test traceroute embed creation."""
        embed = EmbedBuilder.create_traceroute_embed(
            from_name="NodeA",
            to_name="NodeB",
            route_text="NodeA → Router1 → NodeB",
            hops_count=2
        )

        assert isinstance(embed, discord.Embed)
        assert embed.title == "🛣️ Traceroute Result"
        assert "NodeA" in embed.description
        assert "NodeB" in embed.description
        assert embed.color.value == 0x00bfff

        # Check route information
        route_field = next(field for field in embed.fields if "Route Path" in field.name)
        assert "NodeA → Router1 → NodeB" in route_field.value

        stats_field = next(field for field in embed.fields if "Statistics" in field.name)
        assert "2" in stats_field.value

    def test_create_movement_embed(self):
        """Test movement embed creation."""
        embed = EmbedBuilder.create_movement_embed(
            from_name="MobileNode",
            distance_moved=250.5,
            old_lat=40.7128,
            old_lon=-74.0060,
            new_lat=40.7130,
            new_lon=-74.0058,
            new_alt=15.0
        )

        assert isinstance(embed, discord.Embed)
        assert embed.title == "🚶 Node is on the move!"
        assert "MobileNode" in embed.description
        assert embed.color.value == 0xff6b35

        # Check movement details
        movement_field = next(field for field in embed.fields if "Movement Details" in field.name)
        assert "250.5 meters" in movement_field.value
        assert "40.712800" in movement_field.value
        assert "15.0m" in movement_field.value

        # Check speed indication
        speed_field = next(field for field in embed.fields if "Speed" in field.name)
        assert "🐌" in speed_field.name  # Slow movement for 250m

    def test_create_movement_embed_fast_movement(self):
        """Test movement embed with fast movement."""
        embed = EmbedBuilder.create_movement_embed(
            from_name="FastNode",
            distance_moved=1500.0,
            old_lat=40.7128,
            old_lon=-74.0060,
            new_lat=40.7200,
            new_lon=-74.0000,
            new_alt=0.0
        )

        speed_field = next(field for field in embed.fields if "Speed" in field.name)
        assert "🏃" in speed_field.name  # Fast movement for >1000m

        # Should not include altitude if 0
        movement_field = next(field for field in embed.fields if "Movement Details" in field.name)
        assert "Altitude" not in movement_field.value

    def test_create_error_embed(self):
        """Test error embed creation."""
        embed = EmbedBuilder.create_error_embed(
            title="Test Error",
            description="Something went wrong",
            error_details="Detailed error message"
        )

        assert isinstance(embed, discord.Embed)
        assert embed.title == "Test Error"
        assert embed.description == "Something went wrong"
        assert embed.color.value == 0xff0000

        error_field = next(field for field in embed.fields if "Error Details" in field.name)
        assert "Detailed error message" in error_field.value

    def test_create_error_embed_no_details(self):
        """Test error embed without error details."""
        embed = EmbedBuilder.create_error_embed(
            title="Simple Error",
            description="Basic error message"
        )

        assert isinstance(embed, discord.Embed)
        assert embed.title == "Simple Error"
        assert len(embed.fields) == 0

    def test_create_success_embed(self):
        """Test success embed creation."""
        embed = EmbedBuilder.create_success_embed(
            title="Success!",
            description="Operation completed",
            details="All tests passed"
        )

        assert isinstance(embed, discord.Embed)
        assert embed.title == "Success!"
        assert embed.description == "Operation completed"
        assert embed.color.value == 0x00ff00

        details_field = next(field for field in embed.fields if "Details" in field.name)
        assert "All tests passed" in details_field.value

    def test_create_info_embed(self):
        """Test info embed creation."""
        fields = {
            "Field 1": "Value 1",
            "Field 2": "Value 2"
        }

        embed = EmbedBuilder.create_info_embed(
            title="Information",
            description="Some info",
            fields=fields
        )

        assert isinstance(embed, discord.Embed)
        assert embed.title == "Information"
        assert embed.description == "Some info"
        assert embed.color.value == 0x0099ff
        assert len(embed.fields) == 2

        field_names = [field.name for field in embed.fields]
        assert "Field 1" in field_names
        assert "Field 2" in field_names

    def test_create_info_embed_no_fields(self):
        """Test info embed without fields."""
        embed = EmbedBuilder.create_info_embed(
            title="Simple Info",
            description="Just basic info"
        )

        assert isinstance(embed, discord.Embed)
        assert embed.title == "Simple Info"
        assert len(embed.fields) == 0

    def test_embed_timestamps(self):
        """Test that embeds include timestamps."""
        # Test that timestamp is set in embed - actual time testing is done separately
        embed = EmbedBuilder.create_ping_embed("test", "test")

        # Discord may convert timezone, so just check that timestamp is set
        assert embed.timestamp is not None
        # Verify it's a datetime object
        assert isinstance(embed.timestamp, datetime)

    def test_error_embed_long_details(self):
        """Test error embed with very long error details."""
        long_details = "A" * 600
        embed = EmbedBuilder.create_error_embed(
            title="Long Error",
            description="Error with long details",
            error_details=long_details
        )

        error_field = next(field for field in embed.fields if "Error Details" in field.name)
        # Should truncate to 500 characters
        assert len(error_field.value) <= 506  # 500 + "```" wrapper
        assert error_field.value.startswith("```")
        assert error_field.value.endswith("```")
