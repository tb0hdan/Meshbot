"""Test fixtures for database tests."""
import tempfile
import os
from datetime import datetime, timezone, timedelta
from unittest.mock import Mock

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
def db_connection(temp_db_path):
    """Create a test database connection."""
    connection = DatabaseConnection(temp_db_path)

    # Initialize tables
    with connection.get_connection() as conn:
        cursor = conn.cursor()
        DatabaseSchema.create_tables(cursor)
        DatabaseSchema.migrate_telemetry_table(cursor)

    yield connection

    connection.close_all_connections()


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
        'iaq': 45,
        'pm10': 15.2,
        'pm25': 8.9,
        'pm100': 25.1,
        'snr': 10.5,
        'rssi': -75,
        'frequency': 915.0,
        'latitude': 40.7128,
        'longitude': -74.0060,
        'altitude': 10,
        'speed': 0.0,
        'heading': 0.0,
        'accuracy': 5.0
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
        'source': 'GPS'
    }


@pytest.fixture
def sample_message_data():
    """Sample message data for testing."""
    return {
        'from_node_id': '!12345678',
        'to_node_id': '!87654321',
        'message_text': 'Hello from test node!',
        'port_num': 'TEXT_MESSAGE_APP',
        'payload': 'test_payload',
        'hops_away': 2,
        'snr': 8.5,
        'rssi': -80
    }


@pytest.fixture
def multiple_nodes_data():
    """Multiple node data samples for testing."""
    base_time = datetime.now(timezone.utc)
    return [
        {
            'node_id': '!12345678',
            'node_num': 123456789,
            'long_name': 'Node Alpha',
            'short_name': 'ALPHA',
            'last_heard': (base_time - timedelta(minutes=5)).isoformat(),
            'hops_away': 1
        },
        {
            'node_id': '!87654321',
            'node_num': 987654321,
            'long_name': 'Node Beta',
            'short_name': 'BETA',
            'last_heard': (base_time - timedelta(minutes=10)).isoformat(),
            'hops_away': 2
        },
        {
            'node_id': '!11223344',
            'node_num': 112233445,
            'long_name': 'Node Gamma',
            'short_name': 'GAMMA',
            'last_heard': (base_time - timedelta(hours=2)).isoformat(),
            'hops_away': 3
        }
    ]
