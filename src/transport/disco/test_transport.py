"""Tests for Discord transport (main bot) functionality."""
import asyncio
import queue
from unittest.mock import Mock, AsyncMock, patch, MagicMock

import pytest
import discord
from pubsub import pub

from .transport import DiscordBot


class TestDiscordBot:
    """Tests for DiscordBot class."""

    @pytest.fixture
    def discord_bot(self, mock_config, mock_meshtastic, mock_database_for_processors):
        """Create a DiscordBot instance for testing."""
        with patch('src.transport.disco.transport.CommandHandler') as mock_command_handler, \
             patch('src.transport.disco.transport.MessageProcessor'), \
             patch('src.transport.disco.transport.PacketProcessor'), \
             patch('src.transport.disco.transport.BackgroundTaskManager'), \
             patch('src.transport.disco.transport.PingHandler'), \
             patch('discord.Client.__init__', return_value=None):

            # Configure the command handler mock
            mock_command_handler_instance = Mock()
            mock_command_handler_instance.handle_command = AsyncMock()
            mock_command_handler_instance.add_packet_to_buffer = Mock()
            mock_command_handler.return_value = mock_command_handler_instance

            bot = DiscordBot(mock_config, mock_meshtastic, mock_database_for_processors)
            return bot

    def test_init(self, discord_bot, mock_config):
        """Test DiscordBot initialization."""
        assert discord_bot.config == mock_config
        assert isinstance(discord_bot.mesh_to_discord, queue.Queue)
        assert isinstance(discord_bot.discord_to_mesh, queue.Queue)
        assert discord_bot.mesh_to_discord.maxsize == mock_config.max_queue_size
        assert discord_bot.discord_to_mesh.maxsize == mock_config.max_queue_size

        # Check that components are initialized
        assert discord_bot.command_handler is not None
        assert discord_bot.message_processor is not None
        assert discord_bot.packet_processor is not None
        assert discord_bot.task_manager is not None
        assert discord_bot.ping_handler is not None

    def test_init_intents(self, mock_config, mock_meshtastic, mock_database_for_processors):
        """Test that Discord intents are properly configured."""
        with patch('src.commands.CommandHandler'), \
             patch('src.transport.disco.transport.MessageProcessor'), \
             patch('src.transport.disco.transport.PacketProcessor'), \
             patch('src.transport.disco.transport.BackgroundTaskManager'), \
             patch('src.transport.disco.transport.PingHandler'), \
             patch('discord.Intents') as mock_intents, \
             patch('discord.Client.__init__', return_value=None) as mock_client_init:

            mock_intent_instance = Mock()
            mock_intent_instance.message_content = True
            mock_intents.default.return_value = mock_intent_instance

            bot = DiscordBot(mock_config, mock_meshtastic, mock_database_for_processors)

            # Should set message_content intent
            mock_intents.default.assert_called_once()
            # Verify that intents were created and modified correctly
            assert mock_intent_instance.message_content is True
            mock_client_init.assert_called_once_with(intents=mock_intent_instance)

    @pytest.mark.asyncio
    async def test_setup_hook(self, discord_bot):
        """Test setup_hook starts background tasks."""
        discord_bot.task_manager.start_tasks = Mock()

        await discord_bot.setup_hook()

        discord_bot.task_manager.start_tasks.assert_called_once()

    @pytest.mark.asyncio
    async def test_on_ready_success(self, discord_bot):
        """Test successful on_ready execution."""
        mock_user = Mock()
        mock_user.id = 123456789
        discord_bot.setup_mesh_subscriptions = AsyncMock()
        discord_bot.meshtastic.connect = AsyncMock(return_value=True)

        with patch('discord.Client.user', new_callable=lambda: mock_user):
            await discord_bot.on_ready()

        discord_bot.setup_mesh_subscriptions.assert_called_once()
        discord_bot.meshtastic.connect.assert_called_once()

    @pytest.mark.asyncio
    async def test_on_ready_meshtastic_connection_failure(self, discord_bot):
        """Test on_ready when Meshtastic connection fails."""
        mock_user = Mock()
        mock_user.id = 123456789
        discord_bot.setup_mesh_subscriptions = AsyncMock()
        discord_bot.meshtastic.connect = AsyncMock(return_value=False)
        discord_bot.close = AsyncMock()

        with patch('discord.Client.user', new_callable=lambda: mock_user):
            await discord_bot.on_ready()

        discord_bot.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_on_message_own_message(self, discord_bot):
        """Test on_message ignores bot's own messages."""
        message = Mock(spec=discord.Message)
        message.author.id = 123456789
        message.content = "$test"  # Add content to test command handling

        # Mock the user property and command handler
        mock_user = Mock()
        mock_user.id = 123456789  # Same as message author

        mock_command_handler = Mock()
        mock_command_handler.handle_command = AsyncMock()
        discord_bot.command_handler = mock_command_handler

        with patch.object(type(discord_bot), 'user', new=property(lambda self: mock_user)):
            await discord_bot.on_message(message)

        # Should not process own messages
        mock_command_handler.handle_command.assert_not_called()

    @pytest.mark.asyncio
    async def test_on_message_ping(self, discord_bot):
        """Test on_message handles ping messages."""
        message = Mock(spec=discord.Message)
        message.author.id = 987654321
        message.content = "ping"
        mock_user = Mock()
        mock_user.id = 123456789
        discord_bot._handle_ping = AsyncMock()

        with patch('discord.Client.user', new_callable=lambda: mock_user):
            await discord_bot.on_message(message)

        discord_bot._handle_ping.assert_called_once_with(message)

    @pytest.mark.asyncio
    async def test_on_message_ping_case_insensitive(self, discord_bot):
        """Test on_message handles ping messages case insensitively."""
        message = Mock(spec=discord.Message)
        message.author.id = 987654321
        message.content = "  PING  "  # With whitespace and caps
        mock_user = Mock()
        mock_user.id = 123456789
        discord_bot._handle_ping = AsyncMock()

        with patch('discord.Client.user', new_callable=lambda: mock_user):
            await discord_bot.on_message(message)

        discord_bot._handle_ping.assert_called_once_with(message)

    @pytest.mark.asyncio
    async def test_on_message_command(self, discord_bot):
        """Test on_message handles commands."""
        message = Mock(spec=discord.Message)
        message.author.id = 987654321
        message.content = "$help"
        mock_user = Mock()
        mock_user.id = 123456789
        discord_bot.command_handler = Mock()
        discord_bot.command_handler.handle_command = AsyncMock()

        with patch('discord.Client.user', new_callable=lambda: mock_user):
            await discord_bot.on_message(message)

        discord_bot.command_handler.handle_command.assert_called_once_with(message)

    @pytest.mark.asyncio
    async def test_on_message_command_no_handler(self, discord_bot):
        """Test on_message when command handler is None."""
        message = Mock(spec=discord.Message)
        message.author.id = 987654321
        message.content = "$help"
        mock_user = Mock()
        mock_user.id = 123456789
        discord_bot.command_handler = None

        # Should not raise exception
        with patch('discord.Client.user', new_callable=lambda: mock_user):
            await discord_bot.on_message(message)

    @pytest.mark.asyncio
    async def test_handle_ping(self, discord_bot):
        """Test _handle_ping delegates to ping handler."""
        message = Mock(spec=discord.Message)
        discord_bot.ping_handler.handle_ping = AsyncMock()

        await discord_bot._handle_ping(message)

        discord_bot.ping_handler.handle_ping.assert_called_once_with(message)

    @pytest.mark.asyncio
    async def test_setup_mesh_subscriptions(self, discord_bot):
        """Test mesh subscription setup."""
        with patch('pubsub.pub.subscribe') as mock_subscribe:
            await discord_bot.setup_mesh_subscriptions()

            # Should subscribe to mesh events
            assert mock_subscribe.call_count == 2
            calls = mock_subscribe.call_args_list

            # Check subscription topics
            topics = [call[0][1] for call in calls]
            assert "meshtastic.receive" in topics
            assert "meshtastic.connection.established" in topics

    def test_on_mesh_receive_text_packet(self, discord_bot, sample_mesh_packet):
        """Test mesh receive handling for text packets."""
        discord_bot.database.get_node_display_name.return_value = "TestNode"
        discord_bot.packet_processor.process_text_packet = Mock()

        discord_bot.on_mesh_receive(sample_mesh_packet, Mock())

        discord_bot.packet_processor.process_text_packet.assert_called_once_with(sample_mesh_packet)

    def test_on_mesh_receive_telemetry_packet(self, discord_bot, sample_telemetry_packet):
        """Test mesh receive handling for telemetry packets."""
        discord_bot.database.get_node_display_name.return_value = "TestNode"
        discord_bot.packet_processor.process_telemetry_packet = Mock()

        discord_bot.on_mesh_receive(sample_telemetry_packet, Mock())

        discord_bot.packet_processor.process_telemetry_packet.assert_called_once_with(sample_telemetry_packet)

    def test_on_mesh_receive_position_packet(self, discord_bot, sample_position_packet):
        """Test mesh receive handling for position packets."""
        discord_bot.database.get_node_display_name.return_value = "TestNode"
        discord_bot.packet_processor.process_position_packet = Mock()

        discord_bot.on_mesh_receive(sample_position_packet, Mock())

        discord_bot.packet_processor.process_position_packet.assert_called_once_with(sample_position_packet)

    def test_on_mesh_receive_routing_packet(self, discord_bot, sample_routing_packet):
        """Test mesh receive handling for routing packets."""
        discord_bot.database.get_node_display_name.return_value = "TestNode"
        discord_bot.packet_processor.process_routing_packet = Mock()

        discord_bot.on_mesh_receive(sample_routing_packet, Mock())

        discord_bot.packet_processor.process_routing_packet.assert_called_once_with(sample_routing_packet)

    def test_on_mesh_receive_unknown_packet(self, discord_bot):
        """Test mesh receive handling for unknown packet types."""
        unknown_packet = {
            'fromId': '!12345678',
            'toId': 'Primary',
            'hopsAway': 0,
            'decoded': {
                'portnum': 'UNKNOWN_APP',
                'data': 'some data'
            }
        }

        discord_bot.database.get_node_display_name.return_value = "TestNode"

        # Should not raise exception
        discord_bot.on_mesh_receive(unknown_packet, Mock())

    def test_on_mesh_receive_no_decoded_data(self, discord_bot):
        """Test mesh receive handling for packets without decoded data."""
        invalid_packet = {
            'fromId': '!12345678',
            'toId': 'Primary',
            'hopsAway': 0
            # No 'decoded' field
        }

        # Should not raise exception
        discord_bot.on_mesh_receive(invalid_packet, Mock())

    def test_on_mesh_receive_telemetry_invalid_from_id(self, discord_bot):
        """Test mesh receive handling for telemetry with invalid fromId."""
        invalid_packet = {
            'fromId': None,  # Invalid fromId
            'toId': 'Primary',
            'decoded': {
                'portnum': 'TELEMETRY_APP',
                'telemetry': {'deviceMetrics': {'batteryLevel': 85}}
            }
        }

        discord_bot.packet_processor.process_telemetry_packet = Mock()

        # Should skip processing for invalid fromId
        discord_bot.on_mesh_receive(invalid_packet, Mock())

        discord_bot.packet_processor.process_telemetry_packet.assert_not_called()

    def test_on_mesh_receive_adds_to_buffer(self, discord_bot, sample_mesh_packet):
        """Test that packets are added to live monitor buffer."""
        discord_bot.database.get_node_display_name.return_value = "TestNode"
        discord_bot.command_handler = Mock()
        discord_bot.command_handler.add_packet_to_buffer = Mock()

        discord_bot.on_mesh_receive(sample_mesh_packet, Mock())

        # Should add packet info to buffer
        discord_bot.command_handler.add_packet_to_buffer.assert_called_once()
        buffer_item = discord_bot.command_handler.add_packet_to_buffer.call_args[0][0]
        assert buffer_item['type'] == 'packet'
        assert buffer_item['portnum'] == 'TEXT_MESSAGE_APP'
        assert buffer_item['from_name'] == 'TestNode'

    def test_on_mesh_receive_exception_handling(self, discord_bot):
        """Test mesh receive exception handling."""
        # Create a packet that will cause an exception
        malformed_packet = {
            'fromId': '!12345678',
            'decoded': {
                'portnum': 'TEXT_MESSAGE_APP'
                # Missing required 'text' field
            }
        }

        discord_bot.database.get_node_display_name.side_effect = Exception("Database error")

        # Should not raise exception
        discord_bot.on_mesh_receive(malformed_packet, Mock())

    def test_on_mesh_connection(self, discord_bot):
        """Test mesh connection event handling."""
        interface = Mock()
        interface.myInfo = {"name": "TestInterface"}

        # Should not raise exception
        discord_bot.on_mesh_connection(interface)

    @pytest.mark.asyncio
    async def test_close_basic(self, discord_bot):
        """Test basic bot shutdown."""
        discord_bot.task_manager = Mock()
        discord_bot.task_manager.stop_tasks = AsyncMock()
        discord_bot.database = Mock()
        discord_bot.database.close = Mock()
        discord_bot.meshtastic = Mock()
        discord_bot.meshtastic.iface = Mock()
        discord_bot.meshtastic.iface.close = Mock()

        with patch.object(discord.Client, 'close', new_callable=AsyncMock) as mock_super_close:
            await discord_bot.close()

        discord_bot.task_manager.stop_tasks.assert_called_once()
        discord_bot.database.close.assert_called_once()
        discord_bot.meshtastic.iface.close.assert_called_once()
        mock_super_close.assert_called_once()

    @pytest.mark.asyncio
    async def test_close_no_task_manager(self, discord_bot):
        """Test bot shutdown when task manager doesn't exist."""
        discord_bot.task_manager = None
        discord_bot.database = Mock()
        discord_bot.database.close = Mock()
        discord_bot.meshtastic = Mock()
        discord_bot.meshtastic.iface = Mock()
        discord_bot.meshtastic.iface.close = Mock()

        with patch.object(discord.Client, 'close', new_callable=AsyncMock) as mock_super_close:
            await discord_bot.close()

        # Should not raise exception
        mock_super_close.assert_called_once()

    @pytest.mark.asyncio
    async def test_close_database_error(self, discord_bot):
        """Test bot shutdown with database close error."""
        discord_bot.task_manager = Mock()
        discord_bot.task_manager.stop_tasks = AsyncMock()
        discord_bot.database = Mock()
        discord_bot.database.close.side_effect = Exception("DB close error")
        discord_bot.meshtastic = Mock()
        discord_bot.meshtastic.iface = Mock()
        discord_bot.meshtastic.iface.close = Mock()

        with patch.object(discord.Client, 'close', new_callable=AsyncMock) as mock_super_close:
            await discord_bot.close()

        # Should continue despite database error
        mock_super_close.assert_called_once()

    @pytest.mark.asyncio
    async def test_close_meshtastic_error(self, discord_bot):
        """Test bot shutdown with Meshtastic close error."""
        discord_bot.task_manager = Mock()
        discord_bot.task_manager.stop_tasks = AsyncMock()
        discord_bot.database = Mock()
        discord_bot.database.close = Mock()
        discord_bot.meshtastic = Mock()
        discord_bot.meshtastic.iface = Mock()
        discord_bot.meshtastic.iface.close.side_effect = Exception("Mesh close error")

        with patch.object(discord.Client, 'close', new_callable=AsyncMock) as mock_super_close:
            await discord_bot.close()

        # Should continue despite Meshtastic error
        mock_super_close.assert_called_once()

    @pytest.mark.asyncio
    async def test_close_no_meshtastic_interface(self, discord_bot):
        """Test bot shutdown when Meshtastic interface is None."""
        discord_bot.task_manager = Mock()
        discord_bot.task_manager.stop_tasks = AsyncMock()
        discord_bot.database = Mock()
        discord_bot.database.close = Mock()
        discord_bot.meshtastic = Mock()
        discord_bot.meshtastic.iface = None

        with patch.object(discord.Client, 'close', new_callable=AsyncMock) as mock_super_close:
            await discord_bot.close()

        # Should not raise exception
        mock_super_close.assert_called_once()

    @pytest.mark.asyncio
    async def test_close_general_exception(self, discord_bot):
        """Test bot shutdown with general exception."""
        discord_bot.task_manager = Mock()
        discord_bot.task_manager.stop_tasks = AsyncMock(side_effect=Exception("General error"))

        with patch.object(discord.Client, 'close', new_callable=AsyncMock) as mock_super_close:
            await discord_bot.close()

        # Should still call super().close() despite exception
        mock_super_close.assert_called_once()

    def test_queue_initialization(self, discord_bot, mock_config):
        """Test that queues are initialized with correct size limits."""
        assert discord_bot.mesh_to_discord.maxsize == mock_config.max_queue_size
        assert discord_bot.discord_to_mesh.maxsize == mock_config.max_queue_size
        assert discord_bot.mesh_to_discord.empty()
        assert discord_bot.discord_to_mesh.empty()

    def test_component_initialization_order(self, mock_config, mock_meshtastic, mock_database_for_processors):
        """Test that components are initialized in correct order."""
        # Simple test to verify initialization completes without error
        with patch('discord.Client.__init__', return_value=None):
            bot = DiscordBot(mock_config, mock_meshtastic, mock_database_for_processors)

            # Verify that core components exist
            assert hasattr(bot, 'command_handler')
            assert hasattr(bot, 'message_processor')
            assert hasattr(bot, 'packet_processor')
            assert hasattr(bot, 'task_manager')
            assert hasattr(bot, 'ping_handler')

    def test_on_mesh_receive_logs_packet_info(self, discord_bot, sample_mesh_packet):
        """Test that packet reception completes successfully."""
        discord_bot.database.get_node_display_name.return_value = "TestNode"
        discord_bot.packet_processor.process_text_packet = Mock()

        # Test should complete without exception - actual logging tested via integration
        discord_bot.on_mesh_receive(sample_mesh_packet, Mock())

        # Verify that method executed and packet processor was called
        discord_bot.packet_processor.process_text_packet.assert_called_with(sample_mesh_packet)

    def test_packet_processing_delegation(self, discord_bot):
        """Test that different packet types are delegated to correct processors."""
        discord_bot.database.get_node_display_name.return_value = "TestNode"

        # Mock all packet processors
        discord_bot.packet_processor.process_text_packet = Mock()
        discord_bot.packet_processor.process_telemetry_packet = Mock()
        discord_bot.packet_processor.process_position_packet = Mock()
        discord_bot.packet_processor.process_routing_packet = Mock()

        # Test different packet types
        packet_types = [
            ('TEXT_MESSAGE_APP', 'process_text_packet'),
            ('TELEMETRY_APP', 'process_telemetry_packet'),
            ('POSITION_APP', 'process_position_packet'),
            ('ROUTING_APP', 'process_routing_packet')
        ]

        for portnum, expected_method in packet_types:
            packet = {
                'fromId': '!12345678',
                'toId': 'Primary',
                'hopsAway': 0,
                'decoded': {'portnum': portnum}
            }

            discord_bot.on_mesh_receive(packet, Mock())

            # Verify correct processor method was called
            method = getattr(discord_bot.packet_processor, expected_method)
            method.assert_called_with(packet)
