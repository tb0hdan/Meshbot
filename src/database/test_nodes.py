"""Tests for node database operations."""
from datetime import datetime, timezone, timedelta

import pytest

from src.database.nodes import NodeOperations


class TestNodeOperations:
    """Test cases for NodeOperations class."""

    def test_add_new_node(self, db_connection, sample_node_data):
        """Test adding a new node."""
        node_ops = NodeOperations(db_connection)

        success, is_new = node_ops.add_or_update_node(sample_node_data)

        assert success is True
        assert is_new is True

        # Verify node was added
        with db_connection.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM nodes WHERE node_id = ?", (sample_node_data['node_id'],))
            result = cursor.fetchone()

            assert result is not None
            assert result['node_id'] == sample_node_data['node_id']
            assert result['long_name'] == sample_node_data['long_name']
            assert result['short_name'] == sample_node_data['short_name']

    def test_update_existing_node(self, db_connection, sample_node_data):
        """Test updating an existing node."""
        node_ops = NodeOperations(db_connection)

        # Add node first
        node_ops.add_or_update_node(sample_node_data)

        # Update the node with new data
        updated_data = sample_node_data.copy()
        updated_data['long_name'] = 'Updated Node Name'
        updated_data['firmware_version'] = '2.4.0.def456'

        success, is_new = node_ops.add_or_update_node(updated_data)

        assert success is True
        assert is_new is False

        # Verify node was updated
        with db_connection.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM nodes WHERE node_id = ?", (sample_node_data['node_id'],))
            result = cursor.fetchone()

            assert result['long_name'] == 'Updated Node Name'
            assert result['firmware_version'] == '2.4.0.def456'

    def test_add_node_minimal_data(self, db_connection):
        """Test adding a node with minimal required data."""
        node_ops = NodeOperations(db_connection)

        minimal_data = {
            'node_id': '!minimal1',
            'long_name': 'Minimal Node'
        }

        success, is_new = node_ops.add_or_update_node(minimal_data)

        assert success is True
        assert is_new is True

        # Verify defaults were applied
        with db_connection.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM nodes WHERE node_id = ?", (minimal_data['node_id'],))
            result = cursor.fetchone()

            assert result['long_name'] == 'Minimal Node'
            assert result['hops_away'] == 0
            assert result['is_router'] == 0  # SQLite stores booleans as integers
            assert result['is_client'] == 1   # SQLite stores booleans as integers

    def test_get_active_nodes(self, db_connection, multiple_nodes_data):
        """Test getting active nodes within time window."""
        node_ops = NodeOperations(db_connection)

        # Add multiple nodes with different last_heard times
        for node_data in multiple_nodes_data:
            node_ops.add_or_update_node(node_data)

        # Get nodes active in last 30 minutes
        active_nodes = node_ops.get_active_nodes(minutes=30)

        # Should return only the first two nodes (5 and 10 minutes ago)
        assert len(active_nodes) == 2

        node_ids = {node['node_id'] for node in active_nodes}
        assert '!12345678' in node_ids  # 5 minutes ago
        assert '!87654321' in node_ids  # 10 minutes ago
        assert '!11223344' not in node_ids  # 2 hours ago

        # Verify ordering (most recent first)
        assert active_nodes[0]['node_id'] == '!12345678'
        assert active_nodes[1]['node_id'] == '!87654321'

    def test_get_active_nodes_with_telemetry(self, db_connection, sample_node_data, sample_telemetry_data):
        """Test getting active nodes includes latest telemetry data."""
        node_ops = NodeOperations(db_connection)

        # Add node
        node_ops.add_or_update_node(sample_node_data)

        # Add telemetry
        with db_connection.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO telemetry (node_id, battery_level, voltage, temperature, humidity)
                VALUES (?, ?, ?, ?, ?)
            """, (
                sample_node_data['node_id'],
                sample_telemetry_data['battery_level'],
                sample_telemetry_data['voltage'],
                sample_telemetry_data['temperature'],
                sample_telemetry_data['humidity']
            ))

        active_nodes = node_ops.get_active_nodes()

        assert len(active_nodes) == 1
        node = active_nodes[0]
        assert node['battery_level'] == sample_telemetry_data['battery_level']
        assert node['voltage'] == sample_telemetry_data['voltage']
        assert node['temperature'] == sample_telemetry_data['temperature']

    def test_get_all_nodes(self, db_connection, multiple_nodes_data):
        """Test getting all nodes regardless of activity."""
        node_ops = NodeOperations(db_connection)

        # Add multiple nodes
        for node_data in multiple_nodes_data:
            node_ops.add_or_update_node(node_data)

        all_nodes = node_ops.get_all_nodes()

        # Should return all nodes
        assert len(all_nodes) == 3

        node_ids = {node['node_id'] for node in all_nodes}
        assert '!12345678' in node_ids
        assert '!87654321' in node_ids
        assert '!11223344' in node_ids

        # Verify ordering (most recent first)
        assert all_nodes[0]['node_id'] == '!12345678'

    def test_find_node_by_name_exact_match(self, db_connection, sample_node_data):
        """Test finding node by exact name match."""
        node_ops = NodeOperations(db_connection)
        node_ops.add_or_update_node(sample_node_data)

        # Test exact long name match
        result = node_ops.find_node_by_name('Test Node Alpha')
        assert result is not None
        assert result['node_id'] == sample_node_data['node_id']

        # Test exact short name match
        result = node_ops.find_node_by_name('ALPHA')
        assert result is not None
        assert result['node_id'] == sample_node_data['node_id']

    def test_find_node_by_name_partial_match(self, db_connection, sample_node_data):
        """Test finding node by partial name match."""
        node_ops = NodeOperations(db_connection)
        node_ops.add_or_update_node(sample_node_data)

        # Test partial long name match
        result = node_ops.find_node_by_name('Alpha')
        assert result is not None
        assert result['node_id'] == sample_node_data['node_id']

        # Test partial short name match
        result = node_ops.find_node_by_name('ALP')
        assert result is not None
        assert result['node_id'] == sample_node_data['node_id']

    def test_find_node_by_name_no_match(self, db_connection, sample_node_data):
        """Test finding node when no match exists."""
        node_ops = NodeOperations(db_connection)
        node_ops.add_or_update_node(sample_node_data)

        result = node_ops.find_node_by_name('NonExistentNode')
        assert result is None

    def test_find_node_by_name_prioritization(self, db_connection):
        """Test that exact matches are prioritized over partial matches."""
        node_ops = NodeOperations(db_connection)

        # Add nodes with overlapping names
        nodes = [
            {
                'node_id': '!exact1',
                'long_name': 'Test',
                'short_name': 'TST1'
            },
            {
                'node_id': '!partial1',
                'long_name': 'TestNode',
                'short_name': 'TST2'
            }
        ]

        for node in nodes:
            node_ops.add_or_update_node(node)

        # Search for "Test" should return exact match first
        result = node_ops.find_node_by_name('Test')
        assert result['node_id'] == '!exact1'

    def test_get_node_display_name(self, db_connection):
        """Test getting display name for nodes."""
        node_ops = NodeOperations(db_connection)

        # Node with both long and short name
        full_node = {
            'node_id': '!full1',
            'long_name': 'Full Node Name',
            'short_name': 'FULL'
        }
        node_ops.add_or_update_node(full_node)

        # Node with only short name
        short_only_node = {
            'node_id': '!short1',
            'long_name': '',
            'short_name': 'SHORT'
        }
        node_ops.add_or_update_node(short_only_node)

        # Node with no names
        no_name_node = {
            'node_id': '!noname1',
            'long_name': '',
            'short_name': ''
        }
        node_ops.add_or_update_node(no_name_node)

        # Test display name preferences
        assert node_ops.get_node_display_name('!full1') == 'Full Node Name'
        assert node_ops.get_node_display_name('!short1') == 'SHORT'
        assert node_ops.get_node_display_name('!noname1') == '!noname1'

        # Test non-existent node
        assert node_ops.get_node_display_name('!nonexistent') == '!nonexistent'

    def test_node_operations_error_handling(self, db_connection):
        """Test error handling in node operations."""
        node_ops = NodeOperations(db_connection)

        # Test with invalid data
        invalid_data = {'invalid_field': 'value'}

        success, is_new = node_ops.add_or_update_node(invalid_data)
        assert success is False
        assert is_new is False

    def test_node_with_null_values(self, db_connection):
        """Test handling nodes with null/None values."""
        node_ops = NodeOperations(db_connection)

        node_data = {
            'node_id': '!nulltest',
            'long_name': 'Null Test Node',
            'short_name': None,
            'macaddr': None,
            'hw_model': None,
            'firmware_version': None,
            'last_heard': None,
            'hops_away': None
        }

        success, is_new = node_ops.add_or_update_node(node_data)
        assert success is True
        assert is_new is True

        # Verify node was added with defaults
        with db_connection.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM nodes WHERE node_id = ?", (node_data['node_id'],))
            result = cursor.fetchone()

            assert result['long_name'] == 'Null Test Node'
            assert result['hops_away'] == 0  # Default value

    def test_node_timestamps(self, db_connection, sample_node_data):
        """Test that node timestamps are handled correctly."""
        node_ops = NodeOperations(db_connection)

        # Add node
        success, is_new = node_ops.add_or_update_node(sample_node_data)
        assert success is True

        # Verify timestamps
        with db_connection.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT first_seen, last_seen, last_heard FROM nodes WHERE node_id = ?",
                          (sample_node_data['node_id'],))
            result = cursor.fetchone()

            assert result['first_seen'] is not None
            assert result['last_seen'] is not None
            assert result['last_heard'] == sample_node_data['last_heard']
