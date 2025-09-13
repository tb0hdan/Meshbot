"""Test fixtures for Discord transport tests."""
import asyncio
import queue
import tempfile
import os
from datetime import datetime, timezone, timedelta
from unittest.mock import Mock, AsyncMock, MagicMock, patch

import pytest
import discord

from src.config import Config
from src.database.manager import MeshtasticDatabase


@pytest.fixture
def mock_config():
    """Create a mock config for testing."""
    config = Mock(spec=Config)
    config.channel_id = 123456789
    config.max_queue_size = 1000
    config.node_refresh_interval = 300
    config.active_node_threshold = 3600
    return config


@pytest.fixture
def mock_meshtastic():
    """Create a mock Meshtastic interface for testing."""
    meshtastic = Mock()
    meshtastic.connect = AsyncMock(return_value=True)
    meshtastic.send_text = Mock(return_value=True)
    meshtastic.process_nodes = Mock(return_value=([], []))
    meshtastic.last_node_refresh = 0
    meshtastic.iface = Mock()
    meshtastic.iface.close = Mock()
    return meshtastic


@pytest.fixture
def temp_db_path():
    """Create a temporary database file for testing."""
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
        db_path = tmp.name

    yield db_path

    # Cleanup
    try:
        os.unlink(db_path)
    except OSError:
        pass


@pytest.fixture
def mock_database(temp_db_path):
    """Create a mock database for testing."""
    with patch('src.database.maintenance.DatabaseMaintenance.start_maintenance_task'):
        database = MeshtasticDatabase(temp_db_path)
        yield database
        database.close()


@pytest.fixture
def mock_database_for_processors():
    """Create a mock database for packet processor testing."""
    database = Mock()
    database.get_node_display_name = Mock(return_value="TestNode")
    database.update_node = Mock()
    database.store_telemetry = Mock()
    database.add_telemetry = Mock()
    database.store_position = Mock()
    database.add_position = Mock()
    database.get_last_position = Mock(return_value=None)
    database.store_message = Mock()
    database.add_message = Mock()
    database.close = Mock()
    return database


@pytest.fixture
def mock_discord_client():
    """Create a mock Discord client for testing."""
    client = Mock(spec=discord.Client)
    client.user = Mock()
    client.user.id = 987654321
    client.user.name = "TestBot"
    client.is_closed = Mock(return_value=False)
    client.wait_until_ready = AsyncMock()
    client.close = AsyncMock()
    client.loop = Mock()
    client.loop.create_task = Mock()
    return client


@pytest.fixture
def mock_discord_message():
    """Create a mock Discord message for testing."""
    message = Mock(spec=discord.Message)
    message.content = ""
    message.channel = Mock(spec=discord.TextChannel)
    message.channel.send = AsyncMock()
    message.author = Mock(spec=discord.Member)
    message.author.id = 123456789
    message.author.name = "TestUser"
    message.author.display_name = "Test User"
    return message


@pytest.fixture
def mock_discord_channel():
    """Create a mock Discord channel for testing."""
    channel = Mock(spec=discord.TextChannel)
    channel.send = AsyncMock()
    channel.id = 123456789
    return channel


@pytest.fixture
def sample_mesh_packet():
    """Create a sample mesh packet for testing."""
    return {
        'fromId': '!12345678',
        'toId': '!87654321',
        'hopsAway': 1,
        'snr': 10.5,
        'rssi': -75,
        'decoded': {
            'portnum': 'TEXT_MESSAGE_APP',
            'text': 'Hello from test node!'
        }
    }


@pytest.fixture
def sample_telemetry_packet():
    """Create a sample telemetry packet for testing."""
    return {
        'fromId': '!12345678',
        'toId': 'Primary',
        'hopsAway': 0,
        'snr': 12.0,
        'rssi': -65,
        'decoded': {
            'portnum': 'TELEMETRY_APP',
            'telemetry': {
                'deviceMetrics': {
                    'batteryLevel': 85,
                    'voltage': 4.1,
                    'channelUtilization': 12.5,
                    'airUtilTx': 8.2,
                    'uptimeSeconds': 86400
                },
                'environmentMetrics': {
                    'temperature': 23.5,
                    'relativeHumidity': 65.0,
                    'barometricPressure': 1013.25,
                    'gasResistance': 150000
                }
            }
        }
    }


@pytest.fixture
def sample_position_packet():
    """Create a sample position packet for testing."""
    return {
        'fromId': '!12345678',
        'toId': 'Primary',
        'hopsAway': 0,
        'snr': 10.0,
        'rssi': -70,
        'decoded': {
            'portnum': 'POSITION_APP',
            'position': {
                'latitude_i': 407128000,  # 40.7128 * 1e7
                'longitude_i': -740060000,  # -74.0060 * 1e7
                'altitude': 10,
                'speed': 0,
                'ground_track': 0,
                'precision_bits': 32
            }
        }
    }


@pytest.fixture
def sample_routing_packet():
    """Create a sample routing packet for testing."""
    return {
        'fromId': '!12345678',
        'toId': '!87654321',
        'hopsAway': 2,
        'snr': 8.0,
        'rssi': -80,
        'decoded': {
            'portnum': 'ROUTING_APP',
            'routing': {
                'routeDiscovery': {
                    'route': [111111111, 222222222],
                    'routeBack': [333333333, 444444444],
                    'snrTowards': [32, 28, 24],  # SNR values * 4
                    'snrBack': [20, 16, 12]
                }
            }
        }
    }


@pytest.fixture
def sample_node_data():
    """Create sample node data for testing."""
    return {
        'node_id': '!12345678',
        'node_num': 123456789,
        'long_name': 'Test Node Alpha',
        'short_name': 'ALPHA',
        'hw_model': 'TBEAM',
        'firmware_version': '2.3.2.abc123',
        'hops_away': 1,
        'last_seen': datetime.now(timezone.utc),
        'is_active': True
    }


@pytest.fixture
def mock_command_handler():
    """Create a mock command handler for testing."""
    handler = Mock()
    handler.handle_command = AsyncMock()
    handler.add_packet_to_buffer = Mock()
    handler.clear_cache = Mock()
    return handler


@pytest.fixture
def mock_queues():
    """Create mock queues for testing."""
    return {
        'mesh_to_discord': queue.Queue(maxsize=1000),
        'discord_to_mesh': queue.Queue(maxsize=1000)
    }


@pytest.fixture
def sample_telemetry_summary():
    """Create sample telemetry summary for testing."""
    return {
        'active_nodes': 5,
        'total_nodes': 10,
        'avg_battery': 78.5,
        'avg_temperature': 22.3,
        'avg_humidity': 58.2,
        'avg_snr': 9.1
    }


@pytest.fixture
def mock_pubsub():
    """Mock pypubsub for testing."""
    with patch('pubsub.pub') as mock_pub:
        mock_pub.subscribe = Mock()
        yield mock_pub


@pytest.fixture
def mock_asyncio_sleep():
    """Mock asyncio.sleep for testing."""
    with patch('asyncio.sleep', new_callable=AsyncMock) as mock_sleep:
        yield mock_sleep

