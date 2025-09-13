"""Tests for position database operations."""
from datetime import datetime, timezone

import pytest

from src.database.positions import PositionOperations


class TestPositionOperations:
    """Test cases for PositionOperations class."""

    def test_add_position_basic(self, db_connection, sample_position_data):
        """Test adding basic position data."""
        position_ops = PositionOperations(db_connection)

        success = position_ops.add_position('!12345678', sample_position_data)
        assert success is True

        # Verify data was added
        with db_connection.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM positions WHERE node_id = ?", ('!12345678',))
            result = cursor.fetchone()

            assert result is not None
            assert result['node_id'] == '!12345678'
            assert result['latitude'] == sample_position_data['latitude']
            assert result['longitude'] == sample_position_data['longitude']
            assert result['altitude'] == sample_position_data['altitude']

    def test_add_position_all_fields(self, db_connection, sample_position_data):
        """Test adding position with all fields."""
        position_ops = PositionOperations(db_connection)

        success = position_ops.add_position('!test1', sample_position_data)
        assert success is True

        # Verify all fields were stored
        with db_connection.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM positions WHERE node_id = ?", ('!test1',))
            result = cursor.fetchone()

            assert result['latitude'] == sample_position_data['latitude']
            assert result['longitude'] == sample_position_data['longitude']
            assert result['altitude'] == sample_position_data['altitude']
            assert result['speed'] == sample_position_data['speed']
            assert result['heading'] == sample_position_data['heading']
            assert result['accuracy'] == sample_position_data['accuracy']
            assert result['source'] == sample_position_data['source']

    def test_add_position_minimal_data(self, db_connection):
        """Test adding position with minimal data."""
        position_ops = PositionOperations(db_connection)

        minimal_data = {
            'latitude': 40.7128,
            'longitude': -74.0060
        }

        success = position_ops.add_position('!minimal1', minimal_data)
        assert success is True

        # Verify data was stored with defaults
        with db_connection.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM positions WHERE node_id = ?", ('!minimal1',))
            result = cursor.fetchone()

            assert result['latitude'] == 40.7128
            assert result['longitude'] == -74.0060
            assert result['source'] == 'unknown'  # Default value

    def test_get_last_position_basic(self, db_connection, sample_position_data):
        """Test getting last position for a node."""
        position_ops = PositionOperations(db_connection)

        # Add position
        position_ops.add_position('!lastpos1', sample_position_data)

        # Get last position
        last_pos = position_ops.get_last_position('!lastpos1')

        assert last_pos is not None
        assert last_pos['latitude'] == sample_position_data['latitude']
        assert last_pos['longitude'] == sample_position_data['longitude']
        assert last_pos['altitude'] == sample_position_data['altitude']
        assert 'timestamp' in last_pos

    def test_get_last_position_multiple_entries(self, db_connection, sample_position_data):
        """Test that get_last_position returns most recent entry."""
        position_ops = PositionOperations(db_connection)

        # Add first position
        position_ops.add_position('!multi1', sample_position_data)

        # Add second position with different coordinates
        second_data = sample_position_data.copy()
        second_data['latitude'] = 41.0000
        second_data['longitude'] = -75.0000
        position_ops.add_position('!multi1', second_data)

        # Get last position should return the most recent
        last_pos = position_ops.get_last_position('!multi1')

        assert last_pos is not None
        assert last_pos['latitude'] == 41.0000
        assert last_pos['longitude'] == -75.0000

    def test_get_last_position_nonexistent_node(self, db_connection):
        """Test getting position for non-existent node."""
        position_ops = PositionOperations(db_connection)

        last_pos = position_ops.get_last_position('!nonexistent')
        assert last_pos is None

    def test_position_timestamp_handling(self, db_connection, sample_position_data):
        """Test that position timestamps are handled correctly."""
        position_ops = PositionOperations(db_connection)

        success = position_ops.add_position('!timestamptest', sample_position_data)
        assert success is True

        # Verify timestamp was set
        with db_connection.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT timestamp FROM positions WHERE node_id = ?",
                          ('!timestamptest',))
            result = cursor.fetchone()

            assert result['timestamp'] is not None
            # Should be a reasonable timestamp string
            assert len(result['timestamp']) > 10

    def test_position_error_handling(self, db_connection):
        """Test error handling in position operations."""
        position_ops = PositionOperations(db_connection)

        # Test with None node_id
        success = position_ops.add_position(None, {'latitude': 40.0, 'longitude': -74.0})
        assert success is False

        # Test get_last_position for non-existent node doesn't crash
        result = position_ops.get_last_position('!nonexistent')
        assert result is None

    def test_position_coordinates_precision(self, db_connection):
        """Test that position coordinates maintain precision."""
        position_ops = PositionOperations(db_connection)

        precise_data = {
            'latitude': 40.712345678,
            'longitude': -74.006789012,
            'altitude': 123.456789,
            'accuracy': 2.5
        }

        success = position_ops.add_position('!precision1', precise_data)
        assert success is True

        # Verify precision is maintained
        last_pos = position_ops.get_last_position('!precision1')

        assert abs(last_pos['latitude'] - 40.712345678) < 0.000001
        assert abs(last_pos['longitude'] - (-74.006789012)) < 0.000001
        assert abs(last_pos['altitude'] - 123.456789) < 0.000001
        assert abs(last_pos['accuracy'] - 2.5) < 0.01

