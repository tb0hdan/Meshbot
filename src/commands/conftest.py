"""Test fixtures for command tests."""
from datetime import datetime, timezone, timedelta
from unittest.mock import Mock, AsyncMock

import pytest
import discord


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
    return message


@pytest.fixture
def sample_node_data():
    """Create sample node data for testing."""
    return {
        'node_id': '!12345678',
        'node_num': 123456789,
        'long_name': 'Test Node',
        'short_name': 'TEST',
        'hardware_model': 'TBEAM',
        'last_seen': datetime.now(timezone.utc),
        'last_heard': (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat(),
        'latitude': 40.7128,
        'longitude': -74.0060,
        'battery_level': 85,
        'voltage': 4.1,
        'temperature': 23.5,
        'humidity': 65.0,
        'snr': 10.5,
        'rssi': -75,
        'hops_away': 1,
        'is_active': True
    }


@pytest.fixture
def sample_telemetry_data():
    """Create sample telemetry data for testing."""
    return {
        'node_id': '!12345678',
        'battery_level': 85,
        'voltage': 4.1,
        'channel_utilization': 12.5,
        'air_util_tx': 8.2,
        'temperature': 23.5,
        'relative_humidity': 65.0,
        'barometric_pressure': 1013.25,
        'gas_resistance': 150000,
        'timestamp': datetime.now(timezone.utc)
    }


@pytest.fixture
def sample_position_data():
    """Create sample position data for testing."""
    return {
        'node_id': '!12345678',
        'latitude': 40.7128,
        'longitude': -74.0060,
        'altitude': 10,
        'timestamp': datetime.now(timezone.utc),
        'sats_in_view': 8,
        'precision_bits': 32
    }
