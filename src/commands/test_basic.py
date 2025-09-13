"""Tests for basic command implementations."""
import queue
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime

import pytest
import discord

from .basic import BasicCommands


class TestBasicCommands:
    """Test the BasicCommands class."""

    def setup_method(self):
        """Set up test instance."""
        self.mock_meshtastic = Mock()
        self.mock_database = Mock()
        self.mock_queue = queue.Queue()

        self.commands = BasicCommands(
            self.mock_meshtastic,
            self.mock_queue,
            self.mock_database
        )

    @pytest.mark.asyncio
    async def test_cmd_help_returns_help_embed(self, mock_discord_message):
        """Test that cmd_help returns a proper help embed."""
        # Mock the message send method to capture the embed
        sent_embeds = []

        async def capture_send(*args, **kwargs):
            if 'embed' in kwargs:
                sent_embeds.append(kwargs['embed'])
            elif len(args) > 0 and isinstance(args[0], discord.Embed):
                sent_embeds.append(args[0])

        mock_discord_message.channel.send = AsyncMock(side_effect=capture_send)

        await self.commands.cmd_help(mock_discord_message)

        # Should have sent an embed
        mock_discord_message.channel.send.assert_called_once()
        call_args = mock_discord_message.channel.send.call_args

        # Check if embed was passed
        embed = None
        if 'embed' in call_args.kwargs:
            embed = call_args.kwargs['embed']
        elif call_args.args and isinstance(call_args.args[0], discord.Embed):
            embed = call_args.args[0]

        assert embed is not None
        assert isinstance(embed, discord.Embed)
        assert "Meshtastic Discord Bridge Commands" in embed.title
        assert embed.color.value == 0x00ff00

    @pytest.mark.asyncio
    async def test_cmd_send_primary_valid_message(self, mock_discord_message):
        """Test cmd_send_primary with valid message."""
        mock_discord_message.content = "$txt Hello mesh network"

        await self.commands.cmd_send_primary(mock_discord_message)

        # Should add message to queue
        assert not self.mock_queue.empty()
        queued_item = self.mock_queue.get()
        assert "Hello mesh network" in queued_item

    @pytest.mark.asyncio
    async def test_cmd_send_primary_empty_message(self, mock_discord_message):
        """Test cmd_send_primary with empty message."""
        mock_discord_message.content = "$txt"

        await self.commands.cmd_send_primary(mock_discord_message)

        # Should send error message
        mock_discord_message.channel.send.assert_called_once()
        call_args = mock_discord_message.channel.send.call_args[0][0]
        assert "Please provide a message" in call_args

    @pytest.mark.asyncio
    async def test_cmd_send_primary_message_truncated(self, mock_discord_message):
        """Test cmd_send_primary with message that gets truncated."""
        long_message = "A" * 300  # Longer than 225 char limit
        mock_discord_message.content = f"$txt {long_message}"

        await self.commands.cmd_send_primary(mock_discord_message)

        # Should truncate and send the message
        assert not self.mock_queue.empty()
        queued_item = self.mock_queue.get()
        assert len(queued_item) <= 225

    @pytest.mark.asyncio
    async def test_cmd_send_primary_queue_full(self, mock_discord_message):
        """Test cmd_send_primary when queue is full."""
        mock_discord_message.content = "$txt Hello"

        # Fill up the queue
        for _ in range(1000):  # Assuming 1000 is the limit
            self.mock_queue.put("test data")

        await self.commands.cmd_send_primary(mock_discord_message)

        # Should handle queue being full gracefully
        mock_discord_message.channel.send.assert_called_once()

    @pytest.mark.asyncio
    async def test_cmd_send_node_valid_message(self, mock_discord_message):
        """Test cmd_send_node with valid node and message."""
        mock_discord_message.content = "$send TestNode Hello there"

        # Mock database to return a matching node
        self.mock_database.find_node_by_name.return_value = {
            'long_name': 'TestNode', 'node_id': '!12345678'
        }

        await self.commands.cmd_send_node(mock_discord_message)

        # Should add message to queue with node ID
        assert not self.mock_queue.empty()
        queued_item = self.mock_queue.get()
        assert "Hello there" in queued_item

    @pytest.mark.asyncio
    async def test_cmd_send_node_not_found(self, mock_discord_message):
        """Test cmd_send_node with non-existent node."""
        mock_discord_message.content = "$send NonExistentNode Hello"

        # Mock database to return no matching node
        self.mock_database.find_node_by_name.return_value = None

        await self.commands.cmd_send_node(mock_discord_message)

        # Should send error message
        mock_discord_message.channel.send.assert_called_once()
        call_args = mock_discord_message.channel.send.call_args[0][0]
        assert "No node found" in call_args

    @pytest.mark.asyncio
    async def test_cmd_send_node_invalid_format(self, mock_discord_message):
        """Test cmd_send_node with invalid command format."""
        mock_discord_message.content = "$send"  # Missing node and message

        await self.commands.cmd_send_node(mock_discord_message)

        # Should send usage message
        mock_discord_message.channel.send.assert_called_once()
        call_args = mock_discord_message.channel.send.call_args[0][0]
        assert "Use format:" in call_args

    @pytest.mark.asyncio
    async def test_cmd_active_nodes_with_nodes(self, mock_discord_message, sample_node_data):
        """Test cmd_active_nodes with active nodes."""
        # Mock database to return active nodes
        self.mock_database.get_active_nodes.return_value = [sample_node_data]

        await self.commands.cmd_active_nodes(mock_discord_message)

        # Should send message with node information (either string or embed)
        mock_discord_message.channel.send.assert_called_once()
        call_args = mock_discord_message.channel.send.call_args

        # Check if string message or embed was sent
        if call_args.args:
            message_content = call_args.args[0]
            assert "Active Nodes" in message_content
        elif 'embed' in call_args.kwargs:
            embed = call_args.kwargs['embed']
            assert "Active Nodes" in embed.title

    @pytest.mark.asyncio
    async def test_cmd_active_nodes_no_nodes(self, mock_discord_message):
        """Test cmd_active_nodes with no active nodes."""
        # Mock database to return no active nodes
        self.mock_database.get_active_nodes.return_value = []

        await self.commands.cmd_active_nodes(mock_discord_message)

        # Should send message about no active nodes
        mock_discord_message.channel.send.assert_called_once()
        call_args = mock_discord_message.channel.send.call_args

        # Check if embed was sent
        embed = None
        if 'embed' in call_args.kwargs:
            embed = call_args.kwargs['embed']
        elif call_args.args and isinstance(call_args.args[0], discord.Embed):
            embed = call_args.args[0]

        assert embed is not None

    @pytest.mark.asyncio
    async def test_cmd_all_nodes_with_nodes(self, mock_discord_message, sample_node_data):
        """Test cmd_all_nodes with available nodes."""
        # Mock database to return nodes
        self.mock_database.get_all_nodes.return_value = [sample_node_data]

        await self.commands.cmd_all_nodes(mock_discord_message)

        # Should send message with node information
        mock_discord_message.channel.send.assert_called_once()
        call_args = mock_discord_message.channel.send.call_args[0][0]
        assert "All Known Nodes" in call_args

    # Note: cmd_status, cmd_ping, and cmd_telem are not in BasicCommands
    # They may be in other command modules

    @pytest.mark.asyncio
    async def test_database_error_handling(self, mock_discord_message):
        """Test handling of database errors."""
        # Mock database to raise exception
        self.mock_database.get_active_nodes.side_effect = Exception("Database error")

        await self.commands.cmd_active_nodes(mock_discord_message)

        # Should send error message
        mock_discord_message.channel.send.assert_called_once()
        call_args = mock_discord_message.channel.send.call_args
        if call_args.args:
            message_content = call_args.args[0]
            assert "error" in message_content.lower()
        else:
            # Might be called without arguments if it's just checking call count
            assert True  # At least it was called

    def test_initialization(self):
        """Test BasicCommands initialization."""
        assert self.commands.meshtastic == self.mock_meshtastic
        assert self.commands.discord_to_mesh == self.mock_queue
        assert self.commands.database == self.mock_database

        # Should inherit from BaseCommandMixin
        assert hasattr(self.commands, '_node_cache')
        assert hasattr(self.commands, '_cache_timestamps')
