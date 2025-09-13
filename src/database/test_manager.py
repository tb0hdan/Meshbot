"""Tests for database manager."""
from unittest.mock import patch, Mock

import pytest

from src.database.manager import MeshtasticDatabase


class TestMeshtasticDatabase:
    """Test cases for MeshtasticDatabase class."""

    def test_database_initialization(self, temp_db_path):
        """Test database initialization."""
        with patch('src.database.maintenance.DatabaseMaintenance.start_maintenance_task'):
            db = MeshtasticDatabase(temp_db_path)

            # Verify components are initialized
            assert db.connection_manager is not None
            assert db.nodes is not None
            assert db.telemetry is not None
            assert db.positions is not None
            assert db.messages is not None
            assert db.maintenance is not None

            db.close()

    def test_context_manager(self, temp_db_path):
        """Test database as context manager."""
        with patch('src.database.maintenance.DatabaseMaintenance.start_maintenance_task'):
            with MeshtasticDatabase(temp_db_path) as db:
                assert db is not None
                # Database should be functional within context
                success, is_new = db.add_or_update_node({
                    'node_id': '!test1',
                    'long_name': 'Test Node'
                })
                assert success is True

    def test_node_operations_delegation(self, test_database, sample_node_data):
        """Test that node operations are properly delegated."""
        success, is_new = test_database.add_or_update_node(sample_node_data)
        assert success is True
        assert is_new is True

        nodes = test_database.get_all_nodes()
        assert len(nodes) == 1
        assert nodes[0]['node_id'] == sample_node_data['node_id']

        active_nodes = test_database.get_active_nodes()
        assert len(active_nodes) <= 1  # Depends on last_heard time

        found_node = test_database.find_node_by_name('Test Node Alpha')
        assert found_node is not None
        assert found_node['node_id'] == sample_node_data['node_id']

        display_name = test_database.get_node_display_name(sample_node_data['node_id'])
        assert display_name == sample_node_data['long_name']

    def test_telemetry_operations_delegation(self, test_database, sample_telemetry_data):
        """Test that telemetry operations are properly delegated."""
        success = test_database.add_telemetry('!test1', sample_telemetry_data)
        assert success is True

        summary = test_database.get_telemetry_summary()
        assert isinstance(summary, dict)

        history = test_database.get_telemetry_history('!test1')
        assert isinstance(history, list)

    def test_position_operations_delegation(self, test_database, sample_position_data):
        """Test that position operations are properly delegated."""
        success = test_database.add_position('!test1', sample_position_data)
        assert success is True

        last_pos = test_database.get_last_position('!test1')
        assert last_pos is not None
        assert last_pos['latitude'] == sample_position_data['latitude']

    def test_message_operations_delegation(self, test_database, sample_message_data):
        """Test that message operations are properly delegated."""
        success = test_database.add_message(sample_message_data)
        assert success is True

        topology = test_database.get_network_topology()
        assert isinstance(topology, dict)
        assert 'connections' in topology

        stats = test_database.get_message_statistics()
        assert isinstance(stats, dict)

    def test_maintenance_operations_delegation(self, test_database):
        """Test that maintenance operations are properly delegated."""
        # Test cleanup doesn't crash
        test_database.cleanup_old_data(days=1)

        # Test connection closing
        test_database.close_connections()

    def test_database_shutdown(self, temp_db_path):
        """Test proper database shutdown."""
        with patch('src.database.maintenance.DatabaseMaintenance.start_maintenance_task'), \
             patch('src.database.maintenance.DatabaseMaintenance.stop_maintenance') as mock_stop:

            db = MeshtasticDatabase(temp_db_path)
            db.close()

            # Verify maintenance was stopped
            mock_stop.assert_called_once()

    def test_initialization_error_handling(self, temp_db_path):
        """Test initialization error handling."""
        with patch('src.database.schema.DatabaseSchema.create_tables', side_effect=Exception('Test error')):
            with pytest.raises(Exception):
                MeshtasticDatabase(temp_db_path)
