"""Tests for Discord task managers."""
import asyncio
import time
from datetime import datetime
from unittest.mock import Mock, AsyncMock, patch

import pytest
import discord

from . import task_managers
from .task_managers import (
    BackgroundTaskManager, PingHandler, NodeProcessor, TelemetryManager
)


class TestBackgroundTaskManager:
    """Tests for BackgroundTaskManager class."""

    @pytest.fixture
    def task_manager(self, mock_discord_client, mock_config, mock_meshtastic,
                    mock_database, mock_command_handler):
        """Create a BackgroundTaskManager instance for testing."""
        message_processor = Mock()
        message_processor.process_mesh_to_discord = AsyncMock()
        message_processor.process_discord_to_mesh = AsyncMock()

        packet_processor = Mock()

        # Mock the bot with required attributes
        mock_discord_client.mesh_to_discord = Mock()
        mock_discord_client.discord_to_mesh = Mock()
        mock_discord_client.command_handler = mock_command_handler
        mock_discord_client.wait_until_ready = AsyncMock()
        mock_discord_client.get_channel = Mock()
        mock_discord_client.is_closed = Mock(return_value=False)

        # Create task manager
        task_manager = BackgroundTaskManager(
            mock_discord_client, mock_config, mock_meshtastic,
            mock_database, message_processor, packet_processor
        )

        return task_manager

    def test_init(self, task_manager):
        """Test BackgroundTaskManager initialization."""
        assert task_manager.bg_task is None
        assert task_manager.telemetry_task is None
        assert isinstance(task_manager.last_telemetry_hour, int)

    def test_start_tasks(self, task_manager):
        """Test starting background tasks."""
        mock_loop = Mock()
        mock_task = Mock()
        mock_loop.create_task.return_value = mock_task
        task_manager.bot.loop = mock_loop

        task_manager.start_tasks()

        assert mock_loop.create_task.call_count == 2
        assert task_manager.bg_task == mock_task
        assert task_manager.telemetry_task == mock_task

    @pytest.mark.asyncio
    async def test_stop_tasks_no_tasks(self, task_manager):
        """Test stopping tasks when no tasks are running."""
        # Should not raise exception
        await task_manager.stop_tasks()

    @pytest.mark.asyncio
    async def test_stop_tasks_with_running_tasks(self, task_manager):
        """Test stopping running tasks."""
        # Create dummy coroutines that can be cancelled
        async def dummy_coroutine():
            while True:
                await asyncio.sleep(1)

        # Create actual tasks
        mock_task1 = asyncio.create_task(dummy_coroutine())
        mock_task2 = asyncio.create_task(dummy_coroutine())

        task_manager.bg_task = mock_task1
        task_manager.telemetry_task = mock_task2

        await task_manager.stop_tasks()

        # Verify tasks are cancelled
        assert mock_task1.cancelled()
        assert mock_task2.cancelled()

    @pytest.mark.asyncio
    async def test_background_task_no_channel(self, task_manager):
        """Test background task when channel is not found."""
        task_manager.bot.get_channel.return_value = None

        # Should exit early without error
        await task_manager.background_task()

        # Should not call message processing
        task_manager.message_processor.process_mesh_to_discord.assert_not_called()

    @pytest.mark.asyncio
    async def test_background_task_message_processing(self, task_manager, mock_discord_channel):
        """Test background task message processing."""
        task_manager.bot.get_channel.return_value = mock_discord_channel
        task_manager.meshtastic.last_node_refresh = 0

        # Mock is_closed to return True after first iteration
        task_manager.bot.is_closed.side_effect = [False, True]

        with patch('time.time', return_value=1000):
            await task_manager.background_task()

        # Should process messages
        task_manager.message_processor.process_mesh_to_discord.assert_called_once()
        task_manager.message_processor.process_discord_to_mesh.assert_called_once()

    @pytest.mark.asyncio
    async def test_background_task_node_processing(self, task_manager, mock_discord_channel):
        """Test background task node processing."""
        task_manager.bot.get_channel.return_value = mock_discord_channel
        task_manager.meshtastic.last_node_refresh = 0
        task_manager.config.node_refresh_interval = 300

        # Mock time to trigger node refresh
        with patch('time.time', return_value=1000):
            # Mock is_closed to return True after first iteration
            task_manager.bot.is_closed.side_effect = [False, True]

            await task_manager.background_task()

    @pytest.mark.asyncio
    async def test_background_task_exception_handling(self, task_manager, mock_discord_channel):
        """Test background task exception handling."""
        task_manager.bot.get_channel.return_value = mock_discord_channel
        task_manager.message_processor.process_mesh_to_discord.side_effect = Exception("Test error")

        # Mock is_closed to return True after first iteration
        task_manager.bot.is_closed.side_effect = [False, True]

        with patch('asyncio.sleep', new_callable=AsyncMock):
            # Should not raise exception
            await task_manager.background_task()

    @pytest.mark.asyncio
    async def test_telemetry_update_task_new_hour(self, task_manager):
        """Test telemetry update task when it's a new hour."""
        # Set a known initial hour
        task_manager.last_telemetry_hour = 10

        # Mock channel with async send method
        mock_channel = Mock()
        mock_channel.send = AsyncMock()
        task_manager.bot.get_channel.return_value = mock_channel

        # Mock is_closed to return True after first iteration
        task_manager.bot.is_closed.side_effect = [False, True]

        # Mock telemetry data
        with patch.object(task_manager.database, 'get_telemetry_summary', return_value={'active_nodes': 5}):
            # Patch datetime where it's imported in the module
            with patch.object(task_managers, 'datetime') as mock_datetime:
                # Create a mock datetime instance with the hour we want
                mock_now = Mock()
                mock_now.hour = 11  # New hour
                mock_datetime.now.return_value = mock_now

                with patch('asyncio.sleep', new_callable=AsyncMock):
                    await task_manager.telemetry_update_task()

        # Should update last hour
        assert task_manager.last_telemetry_hour == 11

    @pytest.mark.asyncio
    async def test_telemetry_update_task_same_hour(self, task_manager):
        """Test telemetry update task when it's the same hour."""
        # Set a known initial hour
        task_manager.last_telemetry_hour = 10

        # Mock is_closed to return True after first iteration
        task_manager.bot.is_closed.side_effect = [False, True]

        # Patch datetime where it's imported in the module
        with patch.object(task_managers, 'datetime') as mock_datetime:
            # Create a mock datetime instance with the hour we want
            mock_now = Mock()
            mock_now.hour = 10  # Same hour
            mock_datetime.now.return_value = mock_now

            with patch('asyncio.sleep', new_callable=AsyncMock):
                await task_manager.telemetry_update_task()

        # Should not change last hour
        assert task_manager.last_telemetry_hour == 10

    @pytest.mark.asyncio
    async def test_process_nodes_success(self, task_manager, mock_discord_channel):
        """Test successful node processing."""
        new_node = {'long_name': 'New Node', 'node_id': '!12345678'}
        task_manager.meshtastic.process_nodes.return_value = ([], [new_node])

        await task_manager._process_nodes(mock_discord_channel)

        # Should announce new node
        mock_discord_channel.send.assert_called_once()
        call_args = mock_discord_channel.send.call_args
        assert 'embed' in call_args.kwargs

    @pytest.mark.asyncio
    async def test_process_nodes_no_new_nodes(self, task_manager, mock_discord_channel):
        """Test node processing with no new nodes."""
        task_manager.meshtastic.process_nodes.return_value = ([], [])

        await task_manager._process_nodes(mock_discord_channel)

        # Should not send any announcements
        mock_discord_channel.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_process_nodes_exception(self, task_manager, mock_discord_channel):
        """Test node processing exception handling."""
        task_manager.meshtastic.process_nodes.side_effect = Exception("Node error")

        # Should not raise exception
        await task_manager._process_nodes(mock_discord_channel)

    @pytest.mark.asyncio
    async def test_announce_new_node(self, task_manager, mock_discord_channel):
        """Test new node announcement."""
        node = {
            'long_name': 'Test Node',
            'node_id': '!12345678',
            'hw_model': 'TBEAM'
        }

        await task_manager._announce_new_node(mock_discord_channel, node)

        mock_discord_channel.send.assert_called_once()
        call_args = mock_discord_channel.send.call_args
        assert 'embed' in call_args.kwargs
        embed = call_args.kwargs['embed']
        assert "Test Node" in embed.description

    @pytest.mark.asyncio
    async def test_announce_new_node_exception(self, task_manager, mock_discord_channel):
        """Test new node announcement exception handling."""
        mock_discord_channel.send.side_effect = Exception("Send error")
        node = {'long_name': 'Test Node'}

        # Should not raise exception
        await task_manager._announce_new_node(mock_discord_channel, node)

    @pytest.mark.asyncio
    async def test_send_telemetry_update_success(self, task_manager, mock_discord_channel, sample_telemetry_summary):
        """Test successful telemetry update."""
        task_manager.bot.get_channel.return_value = mock_discord_channel
        with patch.object(task_manager.database, 'get_telemetry_summary', return_value=sample_telemetry_summary):
            await task_manager._send_telemetry_update()

        mock_discord_channel.send.assert_called_once()
        call_args = mock_discord_channel.send.call_args
        assert 'embed' in call_args.kwargs

    @pytest.mark.asyncio
    async def test_send_telemetry_update_no_channel(self, task_manager):
        """Test telemetry update when channel not found."""
        task_manager.bot.get_channel.return_value = None

        with patch.object(task_manager.database, 'get_telemetry_summary') as mock_get_summary:
            await task_manager._send_telemetry_update()
            # Should not attempt to get telemetry data
            mock_get_summary.assert_not_called()

    @pytest.mark.asyncio
    async def test_send_telemetry_update_no_data(self, task_manager, mock_discord_channel):
        """Test telemetry update when no data available."""
        task_manager.bot.get_channel.return_value = mock_discord_channel
        with patch.object(task_manager.database, 'get_telemetry_summary', return_value=None):
            await task_manager._send_telemetry_update()

        # Should not send message
        mock_discord_channel.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_send_telemetry_update_database_error(self, task_manager, mock_discord_channel):
        """Test telemetry update with database error."""
        task_manager.bot.get_channel.return_value = mock_discord_channel
        with patch.object(task_manager.database, 'get_telemetry_summary', side_effect=Exception("DB Error")):
            # Should not raise exception
            await task_manager._send_telemetry_update()
        mock_discord_channel.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_periodic_cleanup(self, task_manager):
        """Test periodic cleanup tasks."""
        # Mock cache clearing
        task_manager.bot.command_handler.clear_cache = Mock()
        task_manager.database.cleanup_old_data = Mock()

        await task_manager._periodic_cleanup()

        task_manager.bot.command_handler.clear_cache.assert_called_once()
        task_manager.database.cleanup_old_data.assert_called_once_with(30)

    @pytest.mark.asyncio
    async def test_periodic_cleanup_no_methods(self, task_manager):
        """Test periodic cleanup when cleanup methods don't exist."""
        # Mock the database to not have cleanup_old_data method
        with patch.object(task_manager, 'database', spec=[]):
            # Mock command_handler to not have clear_cache method
            with patch.object(task_manager.bot, 'command_handler', spec=[]):
                # Should not raise exception
                await task_manager._periodic_cleanup()


class TestPingHandler:
    """Tests for PingHandler class."""

    @pytest.fixture
    def ping_handler(self, mock_meshtastic):
        """Create a PingHandler instance for testing."""
        return PingHandler(mock_meshtastic)

    @pytest.mark.asyncio
    async def test_handle_ping_success(self, ping_handler, mock_discord_message):
        """Test successful ping handling."""
        ping_handler.meshtastic.send_text.return_value = True

        with patch('asyncio.sleep', new_callable=AsyncMock):
            await ping_handler.handle_ping(mock_discord_message)

        # Should send two messages (initial and success)
        assert mock_discord_message.channel.send.call_count == 2

        # Should send pong to mesh
        ping_handler.meshtastic.send_text.assert_called_once_with("Pong!")

    @pytest.mark.asyncio
    async def test_handle_ping_failure(self, ping_handler, mock_discord_message):
        """Test ping handling when mesh send fails."""
        ping_handler.meshtastic.send_text.return_value = False

        with patch('asyncio.sleep', new_callable=AsyncMock):
            await ping_handler.handle_ping(mock_discord_message)

        # Should send two messages (initial and failure)
        assert mock_discord_message.channel.send.call_count == 2

        # Check that failure embed was sent
        call_args = mock_discord_message.channel.send.call_args_list[1]
        embed = call_args.kwargs['embed']
        assert "Ping Failed" in embed.title

    @pytest.mark.asyncio
    async def test_handle_ping_exception(self, ping_handler, mock_discord_message):
        """Test ping handling when exception occurs."""
        ping_handler.meshtastic.send_text.side_effect = Exception("Send error")

        with patch('asyncio.sleep', new_callable=AsyncMock):
            await ping_handler.handle_ping(mock_discord_message)

        # Should send error embed
        call_args = mock_discord_message.channel.send.call_args_list[-1]
        embed = call_args.kwargs['embed']
        assert "Ping Error" in embed.title


class TestNodeProcessor:
    """Tests for NodeProcessor class."""

    @pytest.fixture
    def node_processor(self, mock_database, mock_meshtastic):
        """Create a NodeProcessor instance for testing."""
        return NodeProcessor(mock_database, mock_meshtastic)

    @pytest.mark.asyncio
    async def test_process_and_announce_nodes_success(self, node_processor, mock_discord_channel):
        """Test successful node processing and announcement."""
        new_node = {
            'long_name': 'New Node',
            'node_id': '!12345678',
            'hw_model': 'TBEAM'
        }
        node_processor.meshtastic.process_nodes.return_value = ([], [new_node])

        await node_processor.process_and_announce_nodes(mock_discord_channel)

        # Should announce new node
        mock_discord_channel.send.assert_called_once()
        call_args = mock_discord_channel.send.call_args
        assert 'embed' in call_args.kwargs

    @pytest.mark.asyncio
    async def test_process_and_announce_nodes_no_result(self, node_processor, mock_discord_channel):
        """Test node processing when no result returned."""
        node_processor.meshtastic.process_nodes.return_value = None

        await node_processor.process_and_announce_nodes(mock_discord_channel)

        # Should not send any announcements
        mock_discord_channel.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_process_and_announce_nodes_invalid_result(self, node_processor, mock_discord_channel):
        """Test node processing with invalid result format."""
        node_processor.meshtastic.process_nodes.return_value = ([])  # Wrong format

        await node_processor.process_and_announce_nodes(mock_discord_channel)

        # Should not send any announcements
        mock_discord_channel.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_process_and_announce_nodes_exception(self, node_processor, mock_discord_channel):
        """Test node processing exception handling."""
        node_processor.meshtastic.process_nodes.side_effect = Exception("Process error")

        # Should not raise exception
        await node_processor.process_and_announce_nodes(mock_discord_channel)


class TestTelemetryManager:
    """Tests for TelemetryManager class."""

    @pytest.fixture
    def telemetry_manager(self, mock_database, mock_config):
        """Create a TelemetryManager instance for testing."""
        return TelemetryManager(mock_database, mock_config)

    def test_init(self, telemetry_manager):
        """Test TelemetryManager initialization."""
        assert isinstance(telemetry_manager.last_telemetry_hour, int)

    @pytest.mark.asyncio
    async def test_send_hourly_update_new_hour(self, telemetry_manager, mock_discord_channel, sample_telemetry_summary):
        """Test sending hourly update when it's a new hour."""
        telemetry_manager.last_telemetry_hour = 10

        with patch.object(telemetry_manager.database, 'get_telemetry_summary', return_value=sample_telemetry_summary):
            with patch.object(task_managers, 'datetime') as mock_datetime:
                mock_now = Mock()
                mock_now.hour = 11  # New hour
                mock_datetime.now.return_value = mock_now

                await telemetry_manager.send_hourly_update(mock_discord_channel)

        # Should send update and update last hour
        mock_discord_channel.send.assert_called_once()
        assert telemetry_manager.last_telemetry_hour == 11

    @pytest.mark.asyncio
    async def test_send_hourly_update_same_hour(self, telemetry_manager, mock_discord_channel):
        """Test sending hourly update when it's the same hour."""
        telemetry_manager.last_telemetry_hour = 10

        with patch.object(telemetry_manager.database, 'get_telemetry_summary') as mock_get_summary:
            with patch.object(task_managers, 'datetime') as mock_datetime:
                mock_now = Mock()
                mock_now.hour = 10  # Same hour
                mock_datetime.now.return_value = mock_now

                await telemetry_manager.send_hourly_update(mock_discord_channel)

            # Should not send update
            mock_discord_channel.send.assert_not_called()
            mock_get_summary.assert_not_called()

    @pytest.mark.asyncio
    async def test_send_hourly_update_no_data(self, telemetry_manager, mock_discord_channel):
        """Test sending hourly update when no telemetry data."""
        telemetry_manager.last_telemetry_hour = 10

        with patch.object(telemetry_manager.database, 'get_telemetry_summary', return_value=None):
            with patch.object(task_managers, 'datetime') as mock_datetime:
                mock_now = Mock()
                mock_now.hour = 11  # New hour
                mock_datetime.now.return_value = mock_now

                await telemetry_manager.send_hourly_update(mock_discord_channel)

        # Should not send update but should update hour
        mock_discord_channel.send.assert_not_called()
        assert telemetry_manager.last_telemetry_hour == 11

    @pytest.mark.asyncio
    async def test_send_hourly_update_exception(self, telemetry_manager, mock_discord_channel):
        """Test sending hourly update with exception."""
        telemetry_manager.last_telemetry_hour = 10

        with patch.object(telemetry_manager.database, 'get_telemetry_summary', side_effect=Exception("DB Error")):
            with patch.object(task_managers, 'datetime') as mock_datetime:
                mock_now = Mock()
                mock_now.hour = 11  # New hour
                mock_datetime.now.return_value = mock_now

                # Should not raise exception
                await telemetry_manager.send_hourly_update(mock_discord_channel)

        # Should not send update and should NOT update hour due to exception
        mock_discord_channel.send.assert_not_called()
        assert telemetry_manager.last_telemetry_hour == 10  # Hour not updated due to exception

    def test_should_send_update_new_hour(self, telemetry_manager):
        """Test should_send_update when it's a new hour."""
        telemetry_manager.last_telemetry_hour = 10

        with patch.object(task_managers, 'datetime') as mock_datetime:
            mock_now = Mock()
            mock_now.hour = 11  # New hour
            mock_datetime.now.return_value = mock_now

            assert telemetry_manager.should_send_update() is True

    def test_should_send_update_same_hour(self, telemetry_manager):
        """Test should_send_update when it's the same hour."""
        telemetry_manager.last_telemetry_hour = 10

        with patch.object(task_managers, 'datetime') as mock_datetime:
            mock_now = Mock()
            mock_now.hour = 10  # Same hour
            mock_datetime.now.return_value = mock_now

            assert telemetry_manager.should_send_update() is False
