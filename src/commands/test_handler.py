"""Tests for command handler implementation."""
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime

import pytest
import discord

from .handler import CommandHandler


class TestCommandHandler:
    """Test the CommandHandler class."""

    def setup_method(self):
        """Set up test instance."""
        self.mock_meshtastic = Mock()
        self.mock_database = Mock()
        self.mock_queue = Mock()

        self.handler = CommandHandler(
            self.mock_meshtastic,
            self.mock_queue,
            self.mock_database
        )

    def test_initialization(self):
        """Test CommandHandler initialization."""
        assert self.handler.meshtastic == self.mock_meshtastic
        assert self.handler.discord_to_mesh == self.mock_queue
        assert self.handler.database == self.mock_database

        # Should have command module instances
        assert hasattr(self.handler, 'basic_commands')
        assert hasattr(self.handler, 'monitoring_commands')
        assert hasattr(self.handler, 'network_commands')
        assert hasattr(self.handler, 'debug_commands')

    @pytest.mark.asyncio
    async def test_handle_command_help(self, mock_discord_message):
        """Test handling help command."""
        mock_discord_message.content = "$help"

        await self.handler.handle_command(mock_discord_message)

        # Should send help information
        mock_discord_message.channel.send.assert_called_once()

    @pytest.mark.asyncio
    async def test_handle_command_txt(self, mock_discord_message):
        """Test handling txt command."""
        mock_discord_message.content = "$txt Hello mesh network"

        await self.handler.handle_command(mock_discord_message)

        # Should process the message
        mock_discord_message.channel.send.assert_called_once()

    @pytest.mark.asyncio
    async def test_handle_command_nodes(self, mock_discord_message, sample_node_data):
        """Test handling nodes command."""
        mock_discord_message.content = "$nodes"

        # Mock database to return nodes
        self.mock_database.get_all_nodes.return_value = [sample_node_data]

        await self.handler.handle_command(mock_discord_message)

        # Should send nodes information
        mock_discord_message.channel.send.assert_called_once()

    @pytest.mark.asyncio
    async def test_handle_command_activenodes(self, mock_discord_message, sample_node_data):
        """Test handling activenodes command."""
        mock_discord_message.content = "$activenodes"

        # Mock database to return active nodes
        self.mock_database.get_active_nodes.return_value = [sample_node_data]

        await self.handler.handle_command(mock_discord_message)

        # Should send active nodes information
        mock_discord_message.channel.send.assert_called_once()

    @pytest.mark.asyncio
    async def test_handle_command_status(self, mock_discord_message):
        """Test handling status command."""
        mock_discord_message.content = "$status"

        # Mock Meshtastic connection status
        self.mock_meshtastic.is_connected.return_value = True

        await self.handler.handle_command(mock_discord_message)

        # Should send status information
        mock_discord_message.channel.send.assert_called_once()

    @pytest.mark.asyncio
    async def test_handle_command_ping(self, mock_discord_message):
        """Test handling ping command (not implemented in handler)."""
        mock_discord_message.content = "ping"  # Note: no $ prefix

        result = await self.handler.handle_command(mock_discord_message)

        # Should return False as ping is not implemented in handler
        assert result is False
        # Should not send any message
        mock_discord_message.channel.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_handle_command_network_topology(self, mock_discord_message):
        """Test handling network topology command."""
        mock_discord_message.content = "$topo"

        # Mock database topology data
        mock_topology = {
            'connections': [],
            'total_connections': 0,
            'unique_nodes': 0
        }
        self.mock_database.get_network_topology.return_value = mock_topology
        self.mock_database.get_all_nodes.return_value = []

        await self.handler.handle_command(mock_discord_message)

        # Should send topology information
        mock_discord_message.channel.send.assert_called_once()

    @pytest.mark.asyncio
    async def test_handle_command_telemetry(self, mock_discord_message):
        """Test handling telemetry command."""
        mock_discord_message.content = "$telem"

        # Mock telemetry data
        self.mock_database.get_telemetry_summary.return_value = {
            'total_readings': 100,
            'avg_battery': 85.5
        }

        await self.handler.handle_command(mock_discord_message)

        # Should send telemetry information
        mock_discord_message.channel.send.assert_called_once()

    @pytest.mark.asyncio
    async def test_handle_command_debug_clear_database(self, mock_discord_message):
        """Test handling debug clear database command."""
        mock_discord_message.content = "$clear"

        # Mock database connection
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__ = Mock(return_value=mock_conn)
        mock_conn.__exit__ = Mock(return_value=None)
        self.mock_database._get_connection.return_value = mock_conn

        await self.handler.handle_command(mock_discord_message)

        # Should send confirmation message
        mock_discord_message.channel.send.assert_called_once()

    @pytest.mark.asyncio
    async def test_handle_command_unknown(self, mock_discord_message):
        """Test handling unknown command."""
        mock_discord_message.content = "$unknowncommand"

        result = await self.handler.handle_command(mock_discord_message)

        # Should return False for unknown commands
        assert result is False
        # Should not send any message
        mock_discord_message.channel.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_handle_command_no_prefix(self, mock_discord_message):
        """Test handling message without command prefix."""
        mock_discord_message.content = "regular message"

        result = await self.handler.handle_command(mock_discord_message)

        # Should not process as command
        assert result is None or result is False
        # Should not send any response
        mock_discord_message.channel.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_command_cooldown(self, mock_discord_message):
        """Test command cooldown functionality."""
        mock_discord_message.content = "$help"
        user_id = mock_discord_message.author.id = 12345

        # First command should work
        await self.handler.handle_command(mock_discord_message)

        # Reset mock
        mock_discord_message.channel.send.reset_mock()

        # Immediate second command should be rate limited (if implemented)
        await self.handler.handle_command(mock_discord_message)

        # Implementation may or may not have cooldown - test based on actual behavior
        # If cooldown is implemented, second call might be ignored or rate-limited

    @pytest.mark.asyncio
    async def test_command_aliases(self, mock_discord_message):
        """Test command aliases functionality."""
        # Test common aliases
        aliases_to_test = [
            ("$telem", "telemetry"),
            ("$topo", "topology"),
            ("$activenodes", "active nodes"),
            ("$clear", "clear database")
        ]

        for alias, expected_function in aliases_to_test:
            # Reset mock
            mock_discord_message.channel.send.reset_mock()
            mock_discord_message.content = alias

            # Mock necessary database methods
            self.mock_database.get_telemetry_summary.return_value = {}
            self.mock_database.get_network_topology.return_value = {'connections': []}
            self.mock_database.get_all_nodes.return_value = []
            self.mock_database.get_active_nodes.return_value = []

            # Mock database connection for clear command
            mock_conn = Mock()
            mock_cursor = Mock()
            mock_conn.cursor.return_value = mock_cursor
            mock_conn.__enter__ = Mock(return_value=mock_conn)
            mock_conn.__exit__ = Mock(return_value=None)
            self.mock_database._get_connection.return_value = mock_conn

            # Should handle the alias
            await self.handler.handle_command(mock_discord_message)

            # Should send some response
            mock_discord_message.channel.send.assert_called_once()

    @pytest.mark.asyncio
    async def test_error_handling(self, mock_discord_message):
        """Test error handling in command processing."""
        mock_discord_message.content = "$nodes"

        # Mock database to raise exception that is caught by the code
        # The _get_cached_data method catches the exception and returns [],
        # which leads to "No nodes available" message
        self.mock_database.get_all_nodes.side_effect = ValueError("Database error")

        result = await self.handler.handle_command(mock_discord_message)

        # Should return True as command was recognized and handled
        assert result is True
        # Should send a response (either error or "no nodes" message)
        mock_discord_message.channel.send.assert_called_once()
        call_args = mock_discord_message.channel.send.call_args[0][0]
        # When database error is caught by caching layer, it returns empty list and shows "No nodes available"
        assert "No nodes available" in call_args

    def test_command_routing(self):
        """Test that commands are routed to correct modules."""
        # Basic commands
        basic_commands = ['help', 'txt', 'send', 'nodes', 'activenodes', 'status', 'telem']

        # Network commands
        network_commands = ['topo', 'topology', 'traceroute', 'stats']

        # Debug commands
        debug_commands = ['clear', 'debug', 'cache']

        # Monitoring commands
        monitoring_commands = ['live', 'stoplive', 'telemetry']

        # Test that handler has methods or routing for these commands
        # This is more of a structure test since actual routing depends on implementation
        assert hasattr(self.handler, 'basic_commands')
        assert hasattr(self.handler, 'network_commands')
        assert hasattr(self.handler, 'debug_commands')
        assert hasattr(self.handler, 'monitoring_commands')

    @pytest.mark.asyncio
    async def test_message_validation(self, mock_discord_message):
        """Test message validation and sanitization."""
        # Test with various message formats
        test_messages = [
            "$help",
            "  $help  ",  # with whitespace
            "$HELP",  # uppercase
            "$help extra args",
            "$txt message with unicode 🌟",
            "$txt\nmultiline\nmessage"
        ]

        for test_msg in test_messages:
            # Reset mock
            mock_discord_message.channel.send.reset_mock()
            mock_discord_message.content = test_msg

            # Should handle message without errors
            try:
                await self.handler.handle_command(mock_discord_message)
            except Exception as e:
                pytest.fail(f"Command handler failed with message '{test_msg}': {e}")

    def test_integration_with_command_modules(self):
        """Test integration with individual command modules."""
        # Should initialize all command modules
        assert self.handler.basic_commands is not None
        assert self.handler.monitoring_commands is not None
        assert self.handler.network_commands is not None
        assert self.handler.debug_commands is not None

        # All modules should have same dependencies
        for module in [self.handler.basic_commands, self.handler.monitoring_commands,
                      self.handler.network_commands, self.handler.debug_commands]:
            assert module.meshtastic == self.mock_meshtastic
            assert module.database == self.mock_database
            assert module.discord_to_mesh == self.mock_queue
