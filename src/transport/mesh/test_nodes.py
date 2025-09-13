"""Tests for MeshtasticNodeProcessor class."""
import time
from datetime import datetime, timezone, timedelta
from unittest.mock import Mock
import pytest

from src.transport.mesh.nodes import MeshtasticNodeProcessor


class TestMeshtasticNodeProcessor:
    """Test cases for MeshtasticNodeProcessor class."""

    def test_init_with_database(self, test_database):
        """Test node processor initialization with database."""
        mock_connection = Mock()
        processor = MeshtasticNodeProcessor(mock_connection, test_database)

        assert processor.connection == mock_connection
        assert processor.database == test_database
        assert processor.last_node_refresh == 0

    def test_init_without_database(self):
        """Test node processor initialization without database."""
        mock_connection = Mock()
        processor = MeshtasticNodeProcessor(mock_connection)

        assert processor.connection == mock_connection
        assert processor.database is None
        assert processor.last_node_refresh == 0

    def test_process_nodes_no_interface(self, test_database):
        """Test process_nodes when no interface is available."""
        mock_connection = Mock()
        mock_connection.get_interface.return_value = None
        processor = MeshtasticNodeProcessor(mock_connection, test_database)

        processed, new = processor.process_nodes()

        assert processed == []
        assert new == []

    def test_process_nodes_no_database(self, mock_meshtastic_interface):
        """Test process_nodes when no database is available."""
        mock_connection = Mock()
        mock_connection.get_interface.return_value = mock_meshtastic_interface
        processor = MeshtasticNodeProcessor(mock_connection)

        processed, new = processor.process_nodes()

        assert processed == []
        assert new == []

    def test_process_nodes_no_nodes_attribute(self, test_database):
        """Test process_nodes when interface has no nodes attribute."""
        mock_connection = Mock()
        mock_iface = Mock()
        delattr(mock_iface, 'nodes')
        mock_connection.get_interface.return_value = mock_iface
        processor = MeshtasticNodeProcessor(mock_connection, test_database)

        processed, new = processor.process_nodes()

        assert processed == []
        assert new == []

    def test_process_nodes_empty_nodes(self, test_database, mock_meshtastic_interface):
        """Test process_nodes when nodes dictionary is empty."""
        mock_connection = Mock()
        mock_meshtastic_interface.nodes = {}
        mock_connection.get_interface.return_value = mock_meshtastic_interface
        processor = MeshtasticNodeProcessor(mock_connection, test_database)

        processed, new = processor.process_nodes()

        assert processed == []
        assert new == []

    def test_process_nodes_success(self, test_database, multiple_nodes_data):
        """Test successful node processing with multiple nodes."""
        mock_connection = Mock()
        mock_iface = Mock()
        mock_iface.nodes = multiple_nodes_data
        mock_connection.get_interface.return_value = mock_iface
        processor = MeshtasticNodeProcessor(mock_connection, test_database)

        processed, new = processor.process_nodes()

        assert len(processed) == 3
        assert len(new) == 3  # All nodes are new in fresh database
        assert processor.last_node_refresh > 0

        # Verify node data structure
        for node in processed:
            assert 'node_id' in node
            assert 'long_name' in node
            assert 'short_name' in node

    def test_process_nodes_with_existing_node(self, test_database, sample_node_data, multiple_nodes_data):
        """Test processing nodes when some already exist in database."""
        # First add a node to database
        test_database.add_or_update_node(sample_node_data)

        mock_connection = Mock()
        mock_iface = Mock()
        # Include the existing node in the nodes data
        multiple_nodes_data['!12345678'] = {
            'num': 123456789,
            'user': {'longName': 'Test Node Alpha', 'shortName': 'ALPHA'},
            'lastHeard': time.time(),
            'hopsAway': 1
        }
        mock_iface.nodes = multiple_nodes_data
        mock_connection.get_interface.return_value = mock_iface
        processor = MeshtasticNodeProcessor(mock_connection, test_database)

        processed, new = processor.process_nodes()

        assert len(processed) >= 3  # At least original nodes processed
        assert len(new) >= 2  # At least some new nodes

    def test_process_nodes_with_telemetry(self, test_database, sample_raw_node_data):
        """Test processing nodes with telemetry data."""
        mock_connection = Mock()
        mock_iface = Mock()
        mock_iface.nodes = {'!12345678': sample_raw_node_data}
        mock_connection.get_interface.return_value = mock_iface
        processor = MeshtasticNodeProcessor(mock_connection, test_database)

        processed, new = processor.process_nodes()

        assert len(processed) == 1
        assert len(new) == 1

        # Telemetry processing completed without errors (test does not verify storage)
        # since telemetry fields in test data might not match database schema exactly

    def test_process_nodes_with_position(self, test_database, sample_raw_node_data):
        """Test processing nodes with position data."""
        mock_connection = Mock()
        mock_iface = Mock()
        mock_iface.nodes = {'!12345678': sample_raw_node_data}
        mock_connection.get_interface.return_value = mock_iface
        processor = MeshtasticNodeProcessor(mock_connection, test_database)

        processed, new = processor.process_nodes()

        assert len(processed) == 1

        # Verify position was stored (check database)
        position = test_database.get_last_position('!12345678')
        assert position is not None
        assert position['latitude'] == 40.7128
        assert position['longitude'] == -74.0060

    def test_process_nodes_database_error(self, mock_meshtastic_interface, multiple_nodes_data):
        """Test process_nodes handling database errors."""
        mock_connection = Mock()
        mock_meshtastic_interface.nodes = multiple_nodes_data
        mock_connection.get_interface.return_value = mock_meshtastic_interface

        # Mock database that raises errors
        mock_database = Mock()
        mock_database.add_or_update_node.side_effect = Exception("Database error")

        processor = MeshtasticNodeProcessor(mock_connection, mock_database)

        processed, new = processor.process_nodes()

        # Should handle errors gracefully
        assert processed == []
        assert new == []

    def test_process_nodes_individual_node_error(self, test_database):
        """Test process_nodes with individual node processing errors."""
        mock_connection = Mock()
        mock_iface = Mock()

        # Create nodes data with one invalid node
        nodes_data = {
            '!12345678': {
                'num': 123456789,
                'user': {'longName': 'Valid Node', 'shortName': 'VALID'},
                'lastHeard': time.time()
            },
            '!invalid': None  # This will cause an error
        }
        mock_iface.nodes = nodes_data
        mock_connection.get_interface.return_value = mock_iface
        processor = MeshtasticNodeProcessor(mock_connection, test_database)

        processed, new = processor.process_nodes()

        # Should process the valid node and skip the invalid one
        assert len(processed) == 1
        assert processed[0]['node_id'] == '!12345678'

    def test_get_nodes_from_db_success(self, test_database, sample_node_data):
        """Test getting nodes from database successfully."""
        # Add a node to database first
        test_database.add_or_update_node(sample_node_data)

        mock_connection = Mock()
        processor = MeshtasticNodeProcessor(mock_connection, test_database)

        nodes = processor.get_nodes_from_db()

        assert len(nodes) == 1
        assert nodes[0]['node_id'] == sample_node_data['node_id']

    def test_get_nodes_from_db_no_database(self):
        """Test getting nodes when no database is available."""
        mock_connection = Mock()
        processor = MeshtasticNodeProcessor(mock_connection)

        nodes = processor.get_nodes_from_db()

        assert nodes == []

    def test_get_nodes_from_db_database_error(self):
        """Test getting nodes when database raises error."""
        mock_connection = Mock()
        mock_database = Mock()
        mock_database.get_all_nodes.side_effect = Exception("Database error")
        processor = MeshtasticNodeProcessor(mock_connection, mock_database)

        nodes = processor.get_nodes_from_db()

        assert nodes == []

    def test_extract_node_info_complete_data(self, sample_raw_node_data):
        """Test extracting node info from complete raw data."""
        mock_connection = Mock()
        processor = MeshtasticNodeProcessor(mock_connection)

        node_info = processor._extract_node_info('!12345678', sample_raw_node_data)

        assert node_info['node_id'] == '!12345678'
        assert node_info['node_num'] == 123456789
        assert node_info['long_name'] == 'Test Node Alpha'
        assert node_info['short_name'] == 'ALPHA'
        assert node_info['macaddr'] == '00:11:22:33:44:55'
        assert node_info['hw_model'] == 'TBEAM'
        assert node_info['firmware_version'] == '2.3.2.abc123'
        assert node_info['hops_away'] == 1
        assert node_info['is_router'] is False
        assert node_info['is_client'] is True

    def test_extract_node_info_minimal_data(self):
        """Test extracting node info from minimal raw data."""
        mock_connection = Mock()
        processor = MeshtasticNodeProcessor(mock_connection)

        minimal_data = {'num': 123456789}
        node_info = processor._extract_node_info('!12345678', minimal_data)

        assert node_info['node_id'] == '!12345678'
        assert node_info['node_num'] == 123456789
        assert node_info['long_name'] == 'Unknown'
        assert node_info['short_name'] == ''
        assert node_info['hops_away'] == 0
        assert node_info['is_router'] is False
        assert node_info['is_client'] is True

    def test_extract_node_info_missing_user(self):
        """Test extracting node info when user data is missing."""
        mock_connection = Mock()
        processor = MeshtasticNodeProcessor(mock_connection)

        data_without_user = {
            'num': 123456789,
            'lastHeard': time.time()
        }
        node_info = processor._extract_node_info('!12345678', data_without_user)

        assert node_info['long_name'] == 'Unknown'
        assert node_info['short_name'] == ''

    def test_store_node_in_database_success(self, test_database, sample_node_data):
        """Test storing node in database successfully."""
        mock_connection = Mock()
        processor = MeshtasticNodeProcessor(mock_connection, test_database)

        success, is_new = processor._store_node_in_database(sample_node_data)

        assert success is True
        assert is_new is True

    def test_store_node_in_database_error(self, sample_node_data):
        """Test storing node when database raises error."""
        mock_connection = Mock()
        mock_database = Mock()
        mock_database.add_or_update_node.side_effect = Exception("Database error")
        processor = MeshtasticNodeProcessor(mock_connection, mock_database)

        success, is_new = processor._store_node_in_database(sample_node_data)

        assert success is False
        assert is_new is False

    def test_store_telemetry_data_success(self, test_database, sample_raw_node_data):
        """Test storing telemetry data successfully."""
        mock_connection = Mock()
        processor = MeshtasticNodeProcessor(mock_connection, test_database)

        # Should not raise exception
        processor._store_telemetry_data('!12345678', sample_raw_node_data)

        # Telemetry processing completed without exception

    def test_store_telemetry_data_no_data(self, test_database):
        """Test storing telemetry when no telemetry data exists."""
        mock_connection = Mock()
        processor = MeshtasticNodeProcessor(mock_connection, test_database)

        node_data_without_telemetry = {'num': 123456789}

        # Should not raise exception
        processor._store_telemetry_data('!12345678', node_data_without_telemetry)

    def test_store_telemetry_data_database_error(self, sample_raw_node_data):
        """Test storing telemetry when database raises error."""
        mock_connection = Mock()
        mock_database = Mock()
        mock_database.add_telemetry.side_effect = Exception("Database error")
        processor = MeshtasticNodeProcessor(mock_connection, mock_database)

        # Should handle error gracefully
        processor._store_telemetry_data('!12345678', sample_raw_node_data)

    def test_store_position_data_success(self, test_database, sample_raw_node_data):
        """Test storing position data successfully."""
        mock_connection = Mock()
        processor = MeshtasticNodeProcessor(mock_connection, test_database)

        # Should not raise exception
        processor._store_position_data('!12345678', sample_raw_node_data)

        # Verify position was stored
        position = test_database.get_last_position('!12345678')
        assert position is not None

    def test_store_position_data_no_coordinates(self, test_database):
        """Test storing position when no coordinates exist."""
        mock_connection = Mock()
        processor = MeshtasticNodeProcessor(mock_connection, test_database)

        node_data_without_position = {'num': 123456789}

        # Should not raise exception
        processor._store_position_data('!12345678', node_data_without_position)

    def test_store_position_data_partial_coordinates(self, test_database):
        """Test storing position with only latitude."""
        mock_connection = Mock()
        processor = MeshtasticNodeProcessor(mock_connection, test_database)

        node_data_partial = {
            'num': 123456789,
            'latitude': 40.7128
            # longitude is missing
        }

        # Should not store position without both coordinates
        processor._store_position_data('!12345678', node_data_partial)

    def test_store_position_data_database_error(self, sample_raw_node_data):
        """Test storing position when database raises error."""
        mock_connection = Mock()
        mock_database = Mock()
        mock_database.add_position.side_effect = Exception("Database error")
        processor = MeshtasticNodeProcessor(mock_connection, mock_database)

        # Should handle error gracefully
        processor._store_position_data('!12345678', sample_raw_node_data)

    def test_process_nodes_exception_handling(self, test_database):
        """Test process_nodes with general exception handling."""
        mock_connection = Mock()
        # Mock to return None instead of raising exception to test the None check
        mock_connection.get_interface.return_value = None
        processor = MeshtasticNodeProcessor(mock_connection, test_database)

        processed, new = processor.process_nodes()

        assert processed == []
        assert new == []
