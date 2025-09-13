"""Test fixtures for mesh transport tests."""
import tempfile
import os
from datetime import datetime, timezone, timedelta
from unittest.mock import Mock, AsyncMock
from typing import Dict, Any

import pytest

from src.database.connection import DatabaseConnection
from src.database.schema import DatabaseSchema
from src.database.manager import MeshtasticDatabase


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
def test_database(temp_db_path):
    """Create a test database manager with clean state."""
    # Mock the maintenance task to prevent it from starting
    with pytest.MonkeyPatch().context() as mp:
        mp.setattr('src.database.maintenance.DatabaseMaintenance.start_maintenance_task', Mock())
        db = MeshtasticDatabase(temp_db_path)

        yield db

        db.close()


@pytest.fixture
def mock_meshtastic_interface():
    """Create a mock Meshtastic interface for testing."""
    mock_iface = Mock()
    mock_iface.isConnected = Mock(return_value=True)
    mock_iface.sendText = Mock()
    mock_iface.close = Mock()
    mock_iface.nodes = {}
    return mock_iface


@pytest.fixture
def mock_tcp_interface():
    """Create a mock TCP interface for testing."""
    mock_tcp = Mock()
    mock_tcp.isConnected = Mock(return_value=True)
    mock_tcp.sendText = Mock()
    mock_tcp.close = Mock()
    mock_tcp.nodes = {}
    return mock_tcp


@pytest.fixture
def mock_serial_interface():
    """Create a mock Serial interface for testing."""
    mock_serial = Mock()
    mock_serial.isConnected = Mock(return_value=True)
    mock_serial.sendText = Mock()
    mock_serial.close = Mock()
    mock_serial.nodes = {}
    return mock_serial


@pytest.fixture
def sample_node_data():
    """Sample node data for testing."""
    return {
        'node_id': '!12345678',
        'node_num': 123456789,
        'long_name': 'Test Node Alpha',
        'short_name': 'ALPHA',
        'macaddr': '00:11:22:33:44:55',
        'hw_model': 'TBEAM',
        'firmware_version': '2.3.2.abc123',
        'last_heard': (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat(),
        'hops_away': 1,
        'is_router': False,
        'is_client': True
    }


@pytest.fixture
def sample_raw_node_data():
    """Sample raw node data from Meshtastic interface for testing."""
    return {
        'num': 123456789,
        'user': {
            'longName': 'Test Node Alpha',
            'shortName': 'ALPHA'
        },
        'macaddr': '00:11:22:33:44:55',
        'hwModel': 'TBEAM',
        'firmwareVersion': '2.3.2.abc123',
        'lastHeard': (datetime.now(timezone.utc) - timedelta(minutes=5)).timestamp(),
        'hopsAway': 1,
        'isRouter': False,
        'isClient': True,
        'snr': 10.5,
        'rssi': -75,
        'latitude': 40.7128,
        'longitude': -74.0060,
        'altitude': 10
    }


@pytest.fixture
def sample_telemetry_data():
    """Sample telemetry data for testing."""
    return {
        'battery_level': 85.5,
        'voltage': 4.12,
        'channel_utilization': 12.3,
        'air_util_tx': 8.7,
        'uptime_seconds': 86400,
        'temperature': 23.5,
        'humidity': 65.0,
        'pressure': 1013.25,
        'gas_resistance': 150000,
        'snr': 10.5,
        'rssi': -75,
        'frequency': 915.0,
        'latitude': 40.7128,
        'longitude': -74.0060,
        'altitude': 10,
        'speed': 2.5,
        'heading': 180.0,
        'accuracy': 3.0
    }


@pytest.fixture
def sample_position_data():
    """Sample position data for testing."""
    return {
        'latitude': 40.7128,
        'longitude': -74.0060,
        'altitude': 10,
        'speed': 2.5,
        'heading': 180.0,
        'accuracy': 3.0,
        'source': 'meshtastic'
    }


@pytest.fixture
def sample_packet_data():
    """Sample packet data for testing."""
    return {
        'from': 123456789,
        'to': 987654321,
        'timestamp': datetime.now(timezone.utc).timestamp(),
        'payload': 'test_payload',
        'decoded': {
            'text': 'Hello from test node!',
            'telemetry': {
                'temperature': 23.5,
                'battery_level': 85
            },
            'position': {
                'latitude': 40.7128,
                'longitude': -74.0060
            }
        }
    }


@pytest.fixture
def multiple_nodes_data():
    """Multiple node data samples for testing."""
    base_time = datetime.now(timezone.utc)
    return {
        '!12345678': {
            'num': 123456789,
            'user': {'longName': 'Node Alpha', 'shortName': 'ALPHA'},
            'lastHeard': (base_time - timedelta(minutes=5)).timestamp(),
            'hopsAway': 1
        },
        '!87654321': {
            'num': 987654321,
            'user': {'longName': 'Node Beta', 'shortName': 'BETA'},
            'lastHeard': (base_time - timedelta(minutes=10)).timestamp(),
            'hopsAway': 2
        },
        '!11223344': {
            'num': 112233445,
            'user': {'longName': 'Node Gamma', 'shortName': 'GAMMA'},
            'lastHeard': (base_time - timedelta(hours=2)).timestamp(),
            'hopsAway': 3
        }
    }


@pytest.fixture
def invalid_telemetry_data():
    """Invalid telemetry data for testing validation."""
    return {
        'temperature': 150,  # Too high
        'humidity': 150,  # Over 100%
        'snr': 50,  # Too high
        'rssi': 10,  # Too high (should be negative)
        'latitude': 200,  # Invalid latitude
        'longitude': 200,  # Invalid longitude
        'battery_level': 150,  # Over 100%
        'voltage': 50,  # Too high
    }
