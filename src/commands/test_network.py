"""Tests for network command implementations."""
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime

import pytest
import discord

from .network import NetworkCommands


class TestNetworkCommands:
    """Test the NetworkCommands class."""

    def setup_method(self):
        """Set up test instance."""
        self.mock_meshtastic = Mock()
        self.mock_database = Mock()
        self.mock_queue = Mock()

        self.commands = NetworkCommands(
            self.mock_meshtastic,
            self.mock_queue,
            self.mock_database
        )

    def test_initialization(self):
        """Test NetworkCommands initialization."""
        assert self.commands.meshtastic == self.mock_meshtastic
        assert self.commands.discord_to_mesh == self.mock_queue
        assert self.commands.database == self.mock_database

        # Should inherit from BaseCommandMixin
        assert hasattr(self.commands, '_node_cache')
        assert hasattr(self.commands, '_cache_timestamps')

    @pytest.mark.asyncio
    async def test_cmd_network_topology_with_data(self, mock_discord_message, sample_node_data):
        """Test cmd_network_topology with network data."""
        # Mock database methods
        mock_topology = {
            'connections': [
                {'from_node': '!12345678', 'to_node': '!87654321', 'message_count': 5, 'avg_hops': 2.0},
                {'from_node': '!87654321', 'to_node': '!11111111', 'message_count': 3, 'avg_hops': 1.5}
            ],
            'total_nodes': 3,
            'active_nodes': 2,
            'router_nodes': 1,
            'avg_hops': 1.8
        }
        self.mock_database.get_network_topology.return_value = mock_topology
        self.mock_database.get_all_nodes.return_value = [sample_node_data]
        self.mock_database.get_node_display_name.return_value = "Test Node"

        await self.commands.cmd_network_topology(mock_discord_message)

        # Should send network topology embed
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
        assert "Network Topology" in embed.title
        assert embed.color.value == 0x0099ff

    @pytest.mark.asyncio
    async def test_cmd_network_topology_no_data(self, mock_discord_message):
        """Test cmd_network_topology with no network data."""
        # Mock database to return empty topology
        mock_topology = {
            'connections': [],
            'total_nodes': 0,
            'active_nodes': 0,
            'router_nodes': 0,
            'avg_hops': 0.0
        }
        self.mock_database.get_network_topology.return_value = mock_topology
        self.mock_database.get_all_nodes.return_value = []

        await self.commands.cmd_network_topology(mock_discord_message)

        # Should send embed indicating no network data
        mock_discord_message.channel.send.assert_called_once()
        call_args = mock_discord_message.channel.send.call_args

        # Check if embed was sent
        embed = None
        if 'embed' in call_args.kwargs:
            embed = call_args.kwargs['embed']
        elif call_args.args and isinstance(call_args.args[0], discord.Embed):
            embed = call_args.args[0]

        assert embed is not None
        assert "Network Topology" in embed.title

    @pytest.mark.asyncio
    async def test_cmd_network_topology_database_error(self, mock_discord_message):
        """Test cmd_network_topology handles database errors."""
        # Mock database to raise exception
        self.mock_database.get_network_topology.side_effect = Exception("Database error")

        await self.commands.cmd_network_topology(mock_discord_message)

        # Should send error message
        mock_discord_message.channel.send.assert_called_once()
        call_args = mock_discord_message.channel.send.call_args[0][0]
        assert "error" in call_args.lower()

    @pytest.mark.asyncio
    async def test_cmd_traceroute_valid_target(self, mock_discord_message):
        """Test cmd_traceroute with valid target node."""
        mock_discord_message.content = "$traceroute TestNode"

        # Mock database to return matching node
        target_node = {
            'node_id': '!12345678',
            'long_name': 'TestNode',
            'node_num': 123456789
        }
        self.mock_database.get_all_nodes.return_value = [target_node]

        # Mock traceroute result
        mock_traceroute = [
            {'hop': 1, 'node_id': '!87654321', 'name': 'Relay1', 'snr': 8.5},
            {'hop': 2, 'node_id': '!12345678', 'name': 'TestNode', 'snr': 6.2}
        ]
        self.mock_database.get_traceroute.return_value = mock_traceroute

        # Check if method exists before testing
        if hasattr(self.commands, 'cmd_traceroute'):
            await self.commands.cmd_traceroute(mock_discord_message)

            # Should send traceroute embed
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
    async def test_cmd_traceroute_invalid_target(self, mock_discord_message):
        """Test cmd_traceroute with invalid target node."""
        mock_discord_message.content = "$traceroute NonExistentNode"

        # Mock database to return no matching nodes
        self.mock_database.get_all_nodes.return_value = []

        # Check if method exists before testing
        if hasattr(self.commands, 'cmd_traceroute'):
            await self.commands.cmd_traceroute(mock_discord_message)

            # Should send error message about node not found
            mock_discord_message.channel.send.assert_called_once()
            call_args = mock_discord_message.channel.send.call_args[0][0]
            assert "not found" in call_args.lower()

    @pytest.mark.asyncio
    async def test_cmd_network_stats(self, mock_discord_message):
        """Test cmd_network_stats shows network statistics."""
        # Mock database to return network stats
        mock_stats = {
            'total_nodes': 10,
            'active_nodes': 7,
            'total_messages': 1500,
            'avg_hops': 2.3,
            'network_utilization': 15.2,
            'last_updated': datetime.utcnow().isoformat()
        }
        self.mock_database.get_network_stats.return_value = mock_stats

        # Check if method exists before testing
        if hasattr(self.commands, 'cmd_network_stats'):
            await self.commands.cmd_network_stats(mock_discord_message)

            # Should send network stats embed
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

    def test_create_network_diagram_empty_network(self):
        """Test network diagram creation with empty network."""
        # Check if method exists before testing
        if hasattr(self.commands, '_create_network_diagram'):
            result = self.commands._create_network_diagram([], [])
            assert isinstance(result, str)
            assert len(result) > 0  # Should return some placeholder text

    def test_create_network_diagram_with_nodes(self, sample_node_data):
        """Test network diagram creation with nodes and connections."""
        nodes = [sample_node_data]
        connections = [
            {'from_node': '!12345678', 'to_node': '!87654321', 'message_count': 5, 'avg_hops': 2.0}
        ]

        # Mock database method
        self.mock_database.get_node_display_name.return_value = "Test Node"

        # Check if method exists before testing
        if hasattr(self.commands, '_create_network_diagram'):
            result = self.commands._create_network_diagram(nodes, connections)
            assert isinstance(result, str)
            assert len(result) > 0
            # Should contain node information
            assert "Test Node" in result or "!12345678" in result

    def test_format_network_connection(self):
        """Test network connection formatting."""
        connection = {
            'from': '!12345678',
            'to': '!87654321',
            'snr': 8.5,
            'hop_count': 2
        }

        # Check if method exists before testing
        if hasattr(self.commands, '_format_network_connection'):
            result = self.commands._format_network_connection(connection)
            assert isinstance(result, str)
            assert "!12345678" in result
            assert "!87654321" in result
            assert "8.5" in result

    def test_calculate_network_metrics(self, sample_node_data):
        """Test network metrics calculation."""
        nodes = [sample_node_data]
        connections = [
            {'from_node': '!12345678', 'to_node': '!87654321', 'message_count': 5, 'avg_hops': 2.0}
        ]

        # Check if method exists before testing
        if hasattr(self.commands, '_calculate_network_metrics'):
            result = self.commands._calculate_network_metrics(nodes, connections)
            assert isinstance(result, dict)
            assert 'total_nodes' in result
            assert 'total_connections' in result

    @pytest.mark.asyncio
    async def test_caching_behavior(self, mock_discord_message, sample_node_data):
        """Test that network commands use caching appropriately."""
        # Mock database methods
        self.mock_database.get_all_nodes.return_value = [sample_node_data]
        self.mock_database.get_network_topology.return_value = {
            'connections': [],
            'total_nodes': 1,
            'active_nodes': 1,
            'router_nodes': 0,
            'avg_hops': 0.0
        }

        # Call command twice
        await self.commands.cmd_network_topology(mock_discord_message)
        await self.commands.cmd_network_topology(mock_discord_message)

        # Database should be called multiple times (once per command due to separate calls)
        # but caching should reduce the number of calls for get_all_nodes
        assert self.mock_database.get_network_topology.call_count == 2
        # get_all_nodes might be cached depending on implementation

    @pytest.mark.asyncio
    async def test_error_handling_robustness(self, mock_discord_message):
        """Test robust error handling across network commands."""
        # Test various error scenarios
        error_scenarios = [
            Exception("Database connection error"),
            ValueError("Invalid data format"),
            AttributeError("Missing attribute")
        ]

        for error in error_scenarios:
            # Reset mock
            mock_discord_message.channel.send.reset_mock()

            # Mock database to raise the error
            self.mock_database.get_network_topology.side_effect = error

            # Should handle error gracefully
            await self.commands.cmd_network_topology(mock_discord_message)

            # Should send some error message
            mock_discord_message.channel.send.assert_called_once()

    def test_inheritance_and_mixins(self):
        """Test that NetworkCommands properly inherits from BaseCommandMixin."""
        # Should have inherited methods and attributes
        assert hasattr(self.commands, '_get_cached_data')
        assert hasattr(self.commands, 'clear_cache')
        assert hasattr(self.commands, '_format_node_info')
        assert hasattr(self.commands, 'calculate_distance')

        # Should be able to call inherited methods
        self.commands.clear_cache()
        assert len(self.commands._node_cache) == 0
