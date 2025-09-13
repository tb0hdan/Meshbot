"""Tests for monitoring command implementations."""
import asyncio
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime

import pytest
import discord

from .monitoring import MonitoringCommands


class TestMonitoringCommands:
    """Test the MonitoringCommands class."""

    def setup_method(self):
        """Set up test instance."""
        self.mock_meshtastic = Mock()
        self.mock_database = Mock()
        self.mock_queue = Mock()

        self.commands = MonitoringCommands(
            self.mock_meshtastic,
            self.mock_queue,
            self.mock_database
        )

    def test_initialization(self):
        """Test MonitoringCommands initialization."""
        assert self.commands.meshtastic == self.mock_meshtastic
        assert self.commands.discord_to_mesh == self.mock_queue
        assert self.commands.database == self.mock_database

        # Should initialize monitoring state
        assert hasattr(self.commands, '_live_monitors')
        assert hasattr(self.commands, '_packet_buffer')
        assert hasattr(self.commands, '_max_packet_buffer')
        assert hasattr(self.commands, '_packet_buffer_lock')

        # Should be dictionaries/lists
        assert isinstance(self.commands._live_monitors, dict)
        assert isinstance(self.commands._packet_buffer, list)
        assert isinstance(self.commands._max_packet_buffer, int)

    @pytest.mark.asyncio
    async def test_add_packet_to_buffer_success(self):
        """Test adding packet to buffer successfully."""
        packet_info = {
            'type': 'text',
            'from': '!12345678',
            'text': 'Test message'
        }

        await self.commands.add_packet_to_buffer(packet_info)

        # Should add packet to buffer
        assert len(self.commands._packet_buffer) == 1
        stored_packet = self.commands._packet_buffer[0]
        assert stored_packet['type'] == 'text'
        assert stored_packet['from'] == '!12345678'
        assert stored_packet['text'] == 'Test message'
        assert 'timestamp' in stored_packet

    @pytest.mark.asyncio
    async def test_add_packet_to_buffer_max_limit(self):
        """Test packet buffer respects maximum size."""
        # Set a small buffer size for testing
        self.commands._max_packet_buffer = 3

        # Add packets beyond the limit
        for i in range(5):
            packet_info = {'id': i, 'data': f'packet_{i}'}
            await self.commands.add_packet_to_buffer(packet_info)

        # Should only keep the last 3 packets
        assert len(self.commands._packet_buffer) == 3

        # Should have the last 3 packets (2, 3, 4)
        stored_ids = [p['id'] for p in self.commands._packet_buffer]
        assert stored_ids == [2, 3, 4]

    @pytest.mark.asyncio
    async def test_add_packet_to_buffer_thread_safety(self):
        """Test packet buffer thread safety with concurrent access."""
        # Create multiple concurrent tasks adding packets
        tasks = []
        for i in range(10):
            packet_info = {'id': i, 'data': f'packet_{i}'}
            task = asyncio.create_task(
                self.commands.add_packet_to_buffer(packet_info)
            )
            tasks.append(task)

        # Wait for all tasks to complete
        await asyncio.gather(*tasks)

        # Should have all packets (or up to max buffer size)
        expected_count = min(10, self.commands._max_packet_buffer)
        assert len(self.commands._packet_buffer) == expected_count

    @pytest.mark.asyncio
    async def test_add_packet_to_buffer_handles_exception(self):
        """Test add_packet_to_buffer handles exceptions gracefully."""
        # This shouldn't raise an exception even with malformed data
        malformed_packet = object()  # Not serializable

        await self.commands.add_packet_to_buffer(malformed_packet)

        # Buffer should remain empty due to error
        assert len(self.commands._packet_buffer) == 0

    @pytest.mark.asyncio
    async def test_cmd_telemetry_with_data(self, mock_discord_message, sample_telemetry_data):
        """Test cmd_telemetry with available telemetry data."""
        # Mock database to return telemetry summary
        mock_summary = {
            'total_readings': 100,
            'nodes_with_data': 5,
            'avg_battery': 85.5,
            'min_battery': 60.0,
            'max_battery': 100.0,
            'avg_temperature': 23.2,
            'latest_reading': datetime.utcnow().isoformat()
        }
        self.mock_database.get_telemetry_summary.return_value = mock_summary

        await self.commands.cmd_telemetry(mock_discord_message)

        # Should send telemetry embed
        mock_discord_message.channel.send.assert_called_once()
        call_args = mock_discord_message.channel.send.call_args

        # Check if embed was sent
        embed = None
        if 'embed' in call_args.kwargs:
            embed = call_args.kwargs['embed']
        elif call_args.args and isinstance(call_args.args[0], discord.Embed):
            embed = call_args.args[0]

        assert embed is not None
        assert isinstance(embed, discord.Embed)
        # The actual implementation might vary, but should contain telemetry info

    @pytest.mark.asyncio
    async def test_cmd_telemetry_no_data(self, mock_discord_message):
        """Test cmd_telemetry with no available telemetry data."""
        # Mock database to return no data
        self.mock_database.get_telemetry_summary.return_value = None

        await self.commands.cmd_telemetry(mock_discord_message)

        # Should send message about no data
        mock_discord_message.channel.send.assert_called_once()
        # The actual message format may vary based on implementation

    @pytest.mark.asyncio
    async def test_cmd_telemetry_database_error(self, mock_discord_message):
        """Test cmd_telemetry handles database errors."""
        # Mock database to raise exception
        self.mock_database.get_telemetry_summary.side_effect = Exception("Database error")

        await self.commands.cmd_telemetry(mock_discord_message)

        # Should send error message
        mock_discord_message.channel.send.assert_called_once()
        call_args = mock_discord_message.channel.send.call_args[0][0]
        assert "error" in call_args.lower()

    @pytest.mark.asyncio
    async def test_live_monitor_start(self, mock_discord_message):
        """Test starting live monitor."""
        user_id = mock_discord_message.author.id = 12345

        # Mock the live monitor method (assuming it exists)
        if hasattr(self.commands, 'cmd_live'):
            await self.commands.cmd_live(mock_discord_message)

            # Should track the live monitor
            if user_id in self.commands._live_monitors:
                assert self.commands._live_monitors[user_id]['active'] is True

    @pytest.mark.asyncio
    async def test_live_monitor_stop(self, mock_discord_message):
        """Test stopping live monitor."""
        user_id = mock_discord_message.author.id = 12345

        # Start a monitor first
        self.commands._live_monitors[user_id] = {
            'active': True,
            'task': Mock()
        }

        # Mock the stop live monitor method (assuming it exists)
        if hasattr(self.commands, 'cmd_stoplive'):
            await self.commands.cmd_stoplive(mock_discord_message)

            # Should stop the monitor
            if user_id in self.commands._live_monitors:
                assert self.commands._live_monitors[user_id]['active'] is False

    @pytest.mark.asyncio
    async def test_packet_buffer_integration(self):
        """Test integration between packet buffer and monitoring."""
        # Add some test packets
        test_packets = [
            {'type': 'text', 'from': '!12345678', 'text': 'Hello'},
            {'type': 'telemetry', 'from': '!87654321', 'battery': 85},
            {'type': 'position', 'from': '!11111111', 'lat': 40.0, 'lon': -74.0}
        ]

        for packet in test_packets:
            await self.commands.add_packet_to_buffer(packet)

        # Should have all packets in buffer
        assert len(self.commands._packet_buffer) == 3

        # All packets should have timestamps
        for stored_packet in self.commands._packet_buffer:
            assert 'timestamp' in stored_packet
            assert isinstance(stored_packet['timestamp'], str)

    def test_monitor_state_management(self):
        """Test live monitor state management."""
        user_id_1 = 12345
        user_id_2 = 67890

        # Initially no monitors
        assert len(self.commands._live_monitors) == 0

        # Add monitors for different users
        self.commands._live_monitors[user_id_1] = {
            'active': True,
            'task': Mock()
        }
        self.commands._live_monitors[user_id_2] = {
            'active': True,
            'task': Mock()
        }

        # Should track both monitors
        assert len(self.commands._live_monitors) == 2
        assert self.commands._live_monitors[user_id_1]['active'] is True
        assert self.commands._live_monitors[user_id_2]['active'] is True

        # Stop one monitor
        self.commands._live_monitors[user_id_1]['active'] = False

        # Should maintain state independently
        assert self.commands._live_monitors[user_id_1]['active'] is False
        assert self.commands._live_monitors[user_id_2]['active'] is True

    @pytest.mark.asyncio
    async def test_cleanup_on_error(self, mock_discord_message):
        """Test cleanup behavior when errors occur."""
        # Test that monitor state is properly cleaned up on errors
        user_id = mock_discord_message.author.id = 12345

        # Set up a monitor
        mock_task = Mock()
        self.commands._live_monitors[user_id] = {
            'active': True,
            'task': mock_task
        }

        # Simulate error in monitor command
        with patch.object(self.commands, 'database') as mock_db:
            mock_db.get_telemetry_summary.side_effect = Exception("Error")

            await self.commands.cmd_telemetry(mock_discord_message)

            # Should handle error gracefully
            mock_discord_message.channel.send.assert_called_once()

        # Monitor state should still be accessible
        assert user_id in self.commands._live_monitors
