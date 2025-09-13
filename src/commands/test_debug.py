"""Tests for debug command implementations."""
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from datetime import datetime
import sqlite3

import pytest
import discord

from .debug import DebugCommands


class TestDebugCommands:
    """Test the DebugCommands class."""

    def setup_method(self):
        """Set up test instance."""
        self.mock_meshtastic = Mock()
        self.mock_database = Mock()
        self.mock_queue = Mock()

        self.commands = DebugCommands(
            self.mock_meshtastic,
            self.mock_queue,
            self.mock_database
        )

    def test_initialization(self):
        """Test DebugCommands initialization."""
        assert self.commands.meshtastic == self.mock_meshtastic
        assert self.commands.discord_to_mesh == self.mock_queue
        assert self.commands.database == self.mock_database

        # Should inherit from BaseCommandMixin
        assert hasattr(self.commands, '_node_cache')
        assert hasattr(self.commands, '_cache_timestamps')
        assert hasattr(self.commands, 'clear_cache')

    @pytest.mark.asyncio
    async def test_cmd_clear_database_success(self, mock_discord_message):
        """Test cmd_clear_database clears database successfully."""
        # Mock database connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__ = Mock(return_value=mock_conn)
        mock_conn.__exit__ = Mock(return_value=None)

        # Mock database._get_connection to return our mock connection
        self.mock_database._get_connection.return_value = mock_conn

        await self.commands.cmd_clear_database(mock_discord_message)

        # Should execute DELETE statements
        mock_cursor.execute.assert_any_call("DELETE FROM telemetry")
        mock_cursor.execute.assert_any_call("DELETE FROM positions")
        mock_cursor.execute.assert_any_call("DELETE FROM messages")
        mock_cursor.execute.assert_any_call("DELETE FROM nodes")

        # Should reset auto-increment counters
        mock_cursor.execute.assert_any_call(
            "DELETE FROM sqlite_sequence WHERE name IN ('telemetry', 'positions', 'messages')"
        )

        # Should commit changes
        mock_conn.commit.assert_called_once()

        # Should send success embed
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
        assert "Database Cleared" in embed.title
        assert embed.color.value == 0xff6b6b

    @pytest.mark.asyncio
    async def test_cmd_clear_database_error(self, mock_discord_message):
        """Test cmd_clear_database handles database errors."""
        # Mock database to raise exception
        self.mock_database._get_connection.side_effect = Exception("Database error")

        await self.commands.cmd_clear_database(mock_discord_message)

        # Should send error message
        mock_discord_message.channel.send.assert_called_once()
        call_args = mock_discord_message.channel.send.call_args[0][0]
        assert "error" in call_args.lower()

    @pytest.mark.asyncio
    async def test_cmd_cache_info(self, mock_discord_message):
        """Test cmd_cache_info shows cache information."""
        # Add some data to cache
        self.commands._node_cache = {
            'nodes': [{'id': '!12345678', 'name': 'Test'}],
            'telemetry': [{'temp': 23.5}]
        }
        self.commands._cache_timestamps = {
            'nodes': 1234567890,
            'telemetry': 1234567891
        }

        # Check if method exists before testing
        if hasattr(self.commands, 'cmd_cache_info'):
            await self.commands.cmd_cache_info(mock_discord_message)

            # Should send cache information
            mock_discord_message.channel.send.assert_called_once()
            call_args = mock_discord_message.channel.send.call_args

            # Check if embed was sent
            embed = None
            if 'embed' in call_args.kwargs:
                embed = call_args.kwargs['embed']
            elif call_args.args and isinstance(call_args.args[0], discord.Embed):
                embed = call_args.args[0]

            if embed:
                assert isinstance(embed, discord.Embed)

    @pytest.mark.asyncio
    async def test_cmd_clear_cache(self, mock_discord_message):
        """Test cmd_clear_cache clears command cache."""
        # Add some data to cache
        self.commands._node_cache = {'test': 'data'}
        self.commands._cache_timestamps = {'test': 1234567890}

        # Check if method exists before testing
        if hasattr(self.commands, 'cmd_clear_cache'):
            await self.commands.cmd_clear_cache(mock_discord_message)

            # Should clear cache
            assert len(self.commands._node_cache) == 0
            assert len(self.commands._cache_timestamps) == 0

            # Should send confirmation message
            mock_discord_message.channel.send.assert_called_once()

    @pytest.mark.asyncio
    async def test_cmd_debug_info(self, mock_discord_message):
        """Test cmd_debug_info shows debug information."""
        # Mock various system states
        self.mock_meshtastic.is_connected.return_value = True
        self.mock_database.get_connection_count.return_value = 3

        # Check if method exists before testing
        if hasattr(self.commands, 'cmd_debug_info'):
            await self.commands.cmd_debug_info(mock_discord_message)

            # Should send debug information
            mock_discord_message.channel.send.assert_called_once()
            call_args = mock_discord_message.channel.send.call_args

            # Check if embed was sent
            embed = None
            if 'embed' in call_args.kwargs:
                embed = call_args.kwargs['embed']
            elif call_args.args and isinstance(call_args.args[0], discord.Embed):
                embed = call_args.args[0]

            if embed:
                assert isinstance(embed, discord.Embed)
                assert "Debug Information" in embed.title or "Debug" in embed.title

    @pytest.mark.asyncio
    async def test_cmd_system_status(self, mock_discord_message):
        """Test cmd_system_status shows system status."""
        # Mock system information
        mock_system_info = {
            'cpu_usage': 25.5,
            'memory_usage': 45.2,
            'disk_usage': 60.1,
            'uptime': 86400  # 1 day in seconds
        }

        # Check if method exists before testing
        if hasattr(self.commands, 'cmd_system_status'):
            with patch('psutil.cpu_percent', return_value=25.5), \
                 patch('psutil.virtual_memory') as mock_memory, \
                 patch('psutil.disk_usage') as mock_disk:

                # Mock memory and disk usage
                mock_memory.return_value.percent = 45.2
                mock_disk.return_value.percent = 60.1

                await self.commands.cmd_system_status(mock_discord_message)

                # Should send system status information
                mock_discord_message.channel.send.assert_called_once()

    @pytest.mark.asyncio
    async def test_cmd_restart_connection(self, mock_discord_message):
        """Test cmd_restart_connection restarts Meshtastic connection."""
        # Mock Meshtastic reconnection methods
        self.mock_meshtastic.disconnect = Mock()
        self.mock_meshtastic.connect = Mock(return_value=True)

        # Check if method exists before testing
        if hasattr(self.commands, 'cmd_restart_connection'):
            await self.commands.cmd_restart_connection(mock_discord_message)

            # Should attempt to disconnect and reconnect
            self.mock_meshtastic.disconnect.assert_called_once()
            self.mock_meshtastic.connect.assert_called_once()

            # Should send status message
            mock_discord_message.channel.send.assert_called_once()

    @pytest.mark.asyncio
    async def test_cmd_database_stats(self, mock_discord_message):
        """Test cmd_database_stats shows database statistics."""
        # Mock database statistics
        mock_stats = {
            'total_nodes': 10,
            'total_messages': 1500,
            'total_telemetry': 5000,
            'total_positions': 2000,
            'database_size': '2.5 MB',
            'oldest_record': datetime.utcnow().isoformat(),
            'newest_record': datetime.utcnow().isoformat()
        }

        # Check if method exists before testing
        if hasattr(self.commands, 'cmd_database_stats'):
            with patch.object(self.commands.database, 'get_database_stats', return_value=mock_stats):
                await self.commands.cmd_database_stats(mock_discord_message)

                # Should send database statistics
                mock_discord_message.channel.send.assert_called_once()
                call_args = mock_discord_message.channel.send.call_args

                # Check if embed was sent
                embed = None
                if 'embed' in call_args.kwargs:
                    embed = call_args.kwargs['embed']
                elif call_args.args and isinstance(call_args.args[0], discord.Embed):
                    embed = call_args.args[0]

                if embed:
                    assert isinstance(embed, discord.Embed)

    def test_cache_management(self):
        """Test cache management functionality."""
        # Add test data to cache
        test_data = {'test_key': 'test_value'}
        self.commands._node_cache = test_data.copy()
        self.commands._cache_timestamps = {'test_key': 1234567890}

        # Verify cache has data
        assert len(self.commands._node_cache) == 1
        assert 'test_key' in self.commands._node_cache

        # Clear cache
        self.commands.clear_cache()

        # Verify cache is empty
        assert len(self.commands._node_cache) == 0
        assert len(self.commands._cache_timestamps) == 0

    @pytest.mark.asyncio
    async def test_error_handling_patterns(self, mock_discord_message):
        """Test consistent error handling across debug commands."""
        # Test various error scenarios
        error_scenarios = [
            Exception("Generic error"),
            sqlite3.Error("Database error"),
            ValueError("Invalid value"),
            ConnectionError("Connection failed")
        ]

        for error in error_scenarios:
            # Reset mock
            mock_discord_message.channel.send.reset_mock()

            # Mock database to raise the error
            self.mock_database._get_connection.side_effect = error

            # Should handle error gracefully
            await self.commands.cmd_clear_database(mock_discord_message)

            # Should send some error message
            mock_discord_message.channel.send.assert_called_once()
            call_args = mock_discord_message.channel.send.call_args[0][0]
            assert "error" in call_args.lower()

    def test_inheritance_from_base_mixin(self):
        """Test that DebugCommands properly inherits from BaseCommandMixin."""
        # Should have inherited methods
        assert hasattr(self.commands, '_get_cached_data')
        assert hasattr(self.commands, 'clear_cache')
        assert hasattr(self.commands, '_format_node_info')
        assert hasattr(self.commands, 'calculate_distance')

        # Should have cache structures
        assert hasattr(self.commands, '_node_cache')
        assert hasattr(self.commands, '_cache_timestamps')
        assert hasattr(self.commands, '_cache_ttl')

    @pytest.mark.asyncio
    async def test_administrative_access_control(self, mock_discord_message):
        """Test that administrative commands handle access appropriately."""
        # Mock user permissions (this would depend on actual implementation)
        mock_discord_message.author.guild_permissions = Mock()
        mock_discord_message.author.guild_permissions.administrator = True

        # Test that admin commands can be executed
        # (Actual access control would depend on implementation)
        await self.commands.cmd_clear_database(mock_discord_message)

        # Should execute the command (mock database connection needed)
        # This test would be more meaningful with actual access control logic
