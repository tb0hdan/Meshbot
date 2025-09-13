"""Tests for Discord message handlers."""
import asyncio
import queue
from datetime import datetime
from unittest.mock import Mock, AsyncMock, patch

import pytest
import discord

from .message_handlers import MessageProcessor, get_utc_time


class TestGetUtcTime:
    """Tests for get_utc_time function."""

    def test_get_utc_time_returns_datetime(self):
        """Test that get_utc_time returns a datetime object."""
        result = get_utc_time()
        assert isinstance(result, datetime)


class TestMessageProcessor:
    """Tests for MessageProcessor class."""

    @pytest.fixture
    def message_processor(self, mock_database, mock_meshtastic):
        """Create a MessageProcessor instance for testing."""
        return MessageProcessor(mock_database, mock_meshtastic)

    @pytest.fixture
    def mock_channel(self):
        """Create a mock Discord channel."""
        channel = Mock(spec=discord.TextChannel)
        channel.send = AsyncMock()
        return channel

    @pytest.fixture
    def mock_command_handler(self):
        """Create a mock command handler."""
        handler = Mock()
        handler.add_packet_to_buffer = Mock()
        return handler

    @pytest.mark.asyncio
    async def test_process_mesh_to_discord_text_message(self, message_processor, mock_channel, mock_command_handler):
        """Test processing text message from mesh to Discord."""
        mesh_queue = queue.Queue()
        text_item = {
            'type': 'text',
            'from_name': 'TestNode',
            'to_name': 'Target',
            'text': 'Hello Discord!',
            'hops_away': 1
        }
        mesh_queue.put(text_item)

        await message_processor.process_mesh_to_discord(mesh_queue, mock_channel, mock_command_handler)

        mock_channel.send.assert_called_once()
        call_args = mock_channel.send.call_args[0][0]
        assert "TestNode" in call_args
        assert "Target" in call_args
        assert "Hello Discord!" in call_args
        assert "🐰1 hops" in call_args

    @pytest.mark.asyncio
    async def test_process_mesh_to_discord_traceroute(self, message_processor, mock_channel, mock_command_handler):
        """Test processing traceroute message from mesh to Discord."""
        mesh_queue = queue.Queue()
        traceroute_item = {
            'type': 'traceroute',
            'from_name': 'NodeA',
            'to_name': 'NodeB',
            'route_text': 'NodeA → Router1 → NodeB',
            'hops_count': 2
        }
        mesh_queue.put(traceroute_item)

        await message_processor.process_mesh_to_discord(mesh_queue, mock_channel, mock_command_handler)

        mock_channel.send.assert_called_once()
        # Should send embed for traceroute
        call_args = mock_channel.send.call_args
        assert 'embed' in call_args.kwargs
        embed = call_args.kwargs['embed']
        assert embed.title == "🛣️ Traceroute Result"

    @pytest.mark.asyncio
    async def test_process_mesh_to_discord_movement(self, message_processor, mock_channel, mock_command_handler):
        """Test processing movement message from mesh to Discord."""
        mesh_queue = queue.Queue()
        movement_item = {
            'type': 'movement',
            'from_name': 'MobileNode',
            'distance_moved': 150.5,
            'old_lat': 40.7128,
            'old_lon': -74.0060,
            'new_lat': 40.7130,
            'new_lon': -74.0058,
            'new_alt': 10.0
        }
        mesh_queue.put(movement_item)

        await message_processor.process_mesh_to_discord(mesh_queue, mock_channel, mock_command_handler)

        mock_channel.send.assert_called_once()
        call_args = mock_channel.send.call_args
        assert 'embed' in call_args.kwargs
        embed = call_args.kwargs['embed']
        assert embed.title == "🚶 Node is on the move!"

    @pytest.mark.asyncio
    async def test_process_mesh_to_discord_ping_message(self, message_processor, mock_channel, mock_command_handler):
        """Test processing ping message triggers pong response."""
        mesh_queue = queue.Queue()
        ping_item = {
            'type': 'text',
            'from_name': 'PingNode',
            'to_name': 'Longfast Channel',
            'text': 'ping',
            'hops_away': 0
        }
        mesh_queue.put(ping_item)

        with patch('asyncio.sleep', new_callable=AsyncMock):
            await message_processor.process_mesh_to_discord(mesh_queue, mock_channel, mock_command_handler)

        # Should send both the original message and pong response
        assert mock_channel.send.call_count == 2

    @pytest.mark.asyncio
    async def test_process_mesh_to_discord_batch_limit(self, message_processor, mock_channel, mock_command_handler):
        """Test that processing respects batch size limit."""
        mesh_queue = queue.Queue()

        # Add more than batch size (10) messages
        for i in range(15):
            mesh_queue.put({
                'type': 'text',
                'from_name': f'Node{i}',
                'to_name': 'Target',
                'text': f'Message {i}',
                'hops_away': 0
            })

        await message_processor.process_mesh_to_discord(mesh_queue, mock_channel, mock_command_handler)

        # Should process only 10 messages (batch limit)
        assert mock_channel.send.call_count == 10
        # Should have 5 messages remaining
        assert mesh_queue.qsize() == 5

    @pytest.mark.asyncio
    async def test_process_mesh_to_discord_empty_queue(self, message_processor, mock_channel, mock_command_handler):
        """Test processing empty queue doesn't error."""
        mesh_queue = queue.Queue()

        await message_processor.process_mesh_to_discord(mesh_queue, mock_channel, mock_command_handler)

        mock_channel.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_process_mesh_to_discord_discord_error(self, message_processor, mock_channel, mock_command_handler):
        """Test handling Discord API errors."""
        mesh_queue = queue.Queue()
        mesh_queue.put({
            'type': 'text',
            'from_name': 'TestNode',
            'text': 'Test message',
            'hops_away': 0
        })

        # Mock Discord error
        mock_channel.send.side_effect = discord.HTTPException(Mock(), "API Error")

        # Should not raise exception
        await message_processor.process_mesh_to_discord(mesh_queue, mock_channel, mock_command_handler)

    @pytest.mark.asyncio
    async def test_process_text_message_broadcast(self, message_processor, mock_channel):
        """Test processing text message to broadcast channel."""
        item = {
            'from_name': 'TestNode',
            'to_name': '^all',
            'text': 'Broadcast message',
            'hops_away': 2
        }

        await message_processor._process_text_message(item, mock_channel)

        mock_channel.send.assert_called_once()
        call_args = mock_channel.send.call_args[0][0]
        assert "Longfast Channel" in call_args
        assert "🐰2 hops" in call_args

    @pytest.mark.asyncio
    async def test_process_text_message_empty_text(self, message_processor, mock_channel):
        """Test processing empty text message."""
        item = {
            'from_name': 'TestNode',
            'to_name': 'Target',
            'text': '   ',  # Empty/whitespace text
            'hops_away': 0
        }

        await message_processor._process_text_message(item, mock_channel)

        # Should not send message for empty text
        mock_channel.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_process_text_message_long_text(self, message_processor, mock_channel):
        """Test processing very long text message."""
        long_text = "A" * 2000  # Very long message
        item = {
            'from_name': 'TestNode',
            'to_name': 'Target',
            'text': long_text,
            'hops_away': 0
        }

        await message_processor._process_text_message(item, mock_channel)

        mock_channel.send.assert_called_once()
        call_args = mock_channel.send.call_args[0][0]
        # Should be truncated to 2000 characters max
        assert len(call_args) <= 2000
        assert call_args.endswith("...")

    @pytest.mark.asyncio
    async def test_process_discord_to_mesh_broadcast(self, message_processor):
        """Test processing Discord to mesh broadcast message."""
        discord_queue = queue.Queue()
        discord_queue.put("Hello mesh network!")

        await message_processor.process_discord_to_mesh(discord_queue)

        message_processor.meshtastic.send_text.assert_called_once_with("Hello mesh network!")

    @pytest.mark.asyncio
    async def test_process_discord_to_mesh_direct_message(self, message_processor):
        """Test processing Discord to mesh direct message."""
        discord_queue = queue.Queue()
        discord_queue.put("nodenum=12345678 Direct message to node")

        await message_processor.process_discord_to_mesh(discord_queue)

        message_processor.meshtastic.send_text.assert_called_once_with(
            "Direct message to node",
            destination_id="12345678"
        )

    @pytest.mark.asyncio
    async def test_process_discord_to_mesh_malformed_direct(self, message_processor):
        """Test processing malformed direct message."""
        discord_queue = queue.Queue()
        discord_queue.put("nodenum=")  # Malformed - no message part

        await message_processor.process_discord_to_mesh(discord_queue)

        # Should not send anything for malformed message
        message_processor.meshtastic.send_text.assert_not_called()

    @pytest.mark.asyncio
    async def test_process_discord_to_mesh_send_error(self, message_processor):
        """Test handling Meshtastic send errors."""
        discord_queue = queue.Queue()
        discord_queue.put("Test message")

        # Mock send error
        message_processor.meshtastic.send_text.side_effect = Exception("Send failed")

        # Should not raise exception
        await message_processor.process_discord_to_mesh(discord_queue)

    @pytest.mark.asyncio
    async def test_handle_ping_response(self, message_processor, mock_channel):
        """Test ping response handling."""
        item = {
            'from_name': 'PingNode',
            'from_id': '!12345678'
        }

        with patch('asyncio.sleep', new_callable=AsyncMock) as mock_sleep:
            await message_processor._handle_ping_response(item, mock_channel)

        mock_sleep.assert_called_once_with(1.0)
        mock_channel.send.assert_called_once()
        call_args = mock_channel.send.call_args
        assert 'embed' in call_args.kwargs
        embed = call_args.kwargs['embed']
        assert embed.title == "🏓 Pong Response"

    @pytest.mark.asyncio
    async def test_clear_queue_on_error(self, message_processor):
        """Test clearing queue when errors occur."""
        test_queue = queue.Queue()
        test_queue.put("item1")
        test_queue.put("item2")
        test_queue.put("item3")

        await message_processor._clear_queue_on_error(test_queue)

        assert test_queue.empty()

    @pytest.mark.asyncio
    async def test_clear_queue_on_error_empty_queue(self, message_processor):
        """Test clearing already empty queue."""
        test_queue = queue.Queue()

        # Should not raise exception
        await message_processor._clear_queue_on_error(test_queue)

        assert test_queue.empty()

    @pytest.mark.asyncio
    async def test_process_traceroute_message_details(self, message_processor, mock_channel):
        """Test traceroute message processing with detailed validation."""
        item = {
            'from_name': 'NodeA',
            'from_id': '!12345678',
            'to_name': 'NodeB',
            'to_id': '!87654321',
            'route_text': 'NodeA → Router1 (8.0dB) → NodeB',
            'hops_count': 2
        }

        await message_processor._process_traceroute_message(item, mock_channel)

        mock_channel.send.assert_called_once()
        call_args = mock_channel.send.call_args
        embed = call_args.kwargs['embed']

        # Validate embed content
        assert "NodeA" in embed.description
        assert "NodeB" in embed.description
        assert embed.color.value == 0x00bfff

        # Check route field
        route_field = next(field for field in embed.fields if "Route Path" in field.name)
        assert "Router1" in route_field.value

        # Check statistics field
        stats_field = next(field for field in embed.fields if "Statistics" in field.name)
        assert "2" in stats_field.value

    @pytest.mark.asyncio
    async def test_process_movement_message_details(self, message_processor, mock_channel):
        """Test movement message processing with detailed validation."""
        item = {
            'from_name': 'MobileNode',
            'from_id': '!12345678',
            'distance_moved': 750.25,
            'old_lat': 40.7128,
            'old_lon': -74.0060,
            'new_lat': 40.7150,
            'new_lon': -74.0040,
            'new_alt': 25.5
        }

        await message_processor._process_movement_message(item, mock_channel)

        mock_channel.send.assert_called_once()
        call_args = mock_channel.send.call_args
        embed = call_args.kwargs['embed']

        # Validate embed content
        assert "MobileNode" in embed.description
        assert embed.color.value == 0xff6b35

        # Check movement details
        movement_field = next(field for field in embed.fields if "Movement Details" in field.name)
        assert "750.2 meters" in movement_field.value
        assert "40.712800" in movement_field.value
        assert "25.5m" in movement_field.value

        # Check speed indicator (should be walking pace for 750m)
        speed_field = next(field for field in embed.fields if "Speed" in field.name)
        assert "🚶" in speed_field.name

    @pytest.mark.asyncio
    async def test_process_movement_message_no_altitude(self, message_processor, mock_channel):
        """Test movement message without altitude data."""
        item = {
            'from_name': 'MobileNode',
            'from_id': '!12345678',
            'distance_moved': 200.0,
            'old_lat': 40.7128,
            'old_lon': -74.0060,
            'new_lat': 40.7130,
            'new_lon': -74.0058,
            'new_alt': 0  # No altitude
        }

        await message_processor._process_movement_message(item, mock_channel)

        mock_channel.send.assert_called_once()
        call_args = mock_channel.send.call_args
        embed = call_args.kwargs['embed']

        movement_field = next(field for field in embed.fields if "Movement Details" in field.name)
        # Should not include altitude line
        assert "Altitude" not in movement_field.value

    @pytest.mark.asyncio
    async def test_text_message_fallback_names(self, message_processor, mock_channel):
        """Test text message processing with fallback names."""
        item = {
            'from_id': '!12345678',  # No from_name
            'to_id': '!87654321',   # No to_name
            'text': 'Fallback test',
            'hops_away': 1
        }

        await message_processor._process_text_message(item, mock_channel)

        mock_channel.send.assert_called_once()
        call_args = mock_channel.send.call_args[0][0]
        # Should use IDs as fallback
        assert "!12345678" in call_args
        assert "!87654321" in call_args

    @pytest.mark.asyncio
    async def test_process_mesh_to_discord_unknown_type(self, message_processor, mock_channel, mock_command_handler):
        """Test processing unknown message type."""
        mesh_queue = queue.Queue()
        unknown_item = "Unknown message format"
        mesh_queue.put(unknown_item)

        await message_processor.process_mesh_to_discord(mesh_queue, mock_channel, mock_command_handler)

        mock_channel.send.assert_called_once()
        call_args = mock_channel.send.call_args[0][0]
        assert "📡 **Mesh Message:**" in call_args
        assert "Unknown message format" in call_args
