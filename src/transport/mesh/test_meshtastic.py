"""Tests for MeshtasticInterface unified class."""
from unittest.mock import Mock, AsyncMock, patch
import pytest

from src.transport.mesh.meshtastic import MeshtasticInterface


class TestMeshtasticInterface:
    """Test cases for MeshtasticInterface unified class."""

    def test_init_with_hostname_and_database(self, test_database):
        """Test interface initialization with hostname and database."""
        hostname = "192.168.1.100"
        interface = MeshtasticInterface(hostname, test_database)

        assert interface.hostname == hostname
        assert interface.database == test_database
        assert interface.connection.hostname == hostname
        assert interface.messaging.connection == interface.connection
        assert interface.node_processor.connection == interface.connection
        assert interface.node_processor.database == test_database

    def test_init_defaults(self):
        """Test interface initialization with defaults."""
        interface = MeshtasticInterface()

        assert interface.hostname is None
        assert interface.database is None
        assert interface.connection.hostname is None
        assert interface.node_processor.database is None

    def test_init_only_hostname(self):
        """Test interface initialization with only hostname."""
        hostname = "192.168.1.100"
        interface = MeshtasticInterface(hostname)

        assert interface.hostname == hostname
        assert interface.database is None
        assert interface.connection.hostname == hostname

    def test_init_only_database(self, test_database):
        """Test interface initialization with only database."""
        interface = MeshtasticInterface(database=test_database)

        assert interface.hostname is None
        assert interface.database == test_database
        assert interface.connection.hostname is None
        assert interface.node_processor.database == test_database

    @pytest.mark.asyncio
    async def test_connect_success(self):
        """Test successful connection."""
        interface = MeshtasticInterface()

        # Mock the connection's connect method as async
        interface.connection.connect = AsyncMock(return_value=True)

        result = await interface.connect()

        assert result is True
        interface.connection.connect.assert_called_once()

    @pytest.mark.asyncio
    async def test_connect_failure(self):
        """Test connection failure."""
        interface = MeshtasticInterface()

        # Mock the connection's connect method to return False
        interface.connection.connect = AsyncMock(return_value=False)

        result = await interface.connect()

        assert result is False
        interface.connection.connect.assert_called_once()

    def test_send_text_success(self):
        """Test successful text sending."""
        interface = MeshtasticInterface()

        # Mock the messaging's send_text method
        interface.messaging.send_text = Mock(return_value=True)

        message = "Hello mesh!"
        result = interface.send_text(message)

        assert result is True
        interface.messaging.send_text.assert_called_once_with(message, None)

    def test_send_text_with_destination(self):
        """Test text sending with destination."""
        interface = MeshtasticInterface()

        # Mock the messaging's send_text method
        interface.messaging.send_text = Mock(return_value=True)

        message = "Hello specific node!"
        destination = "!12345678"
        result = interface.send_text(message, destination)

        assert result is True
        interface.messaging.send_text.assert_called_once_with(message, destination)

    def test_send_text_failure(self):
        """Test text sending failure."""
        interface = MeshtasticInterface()

        # Mock the messaging's send_text method to return False
        interface.messaging.send_text = Mock(return_value=False)

        message = "Hello mesh!"
        result = interface.send_text(message)

        assert result is False
        interface.messaging.send_text.assert_called_once_with(message, None)

    def test_process_nodes_success(self, test_database):
        """Test successful node processing."""
        interface = MeshtasticInterface(database=test_database)

        # Mock the node processor's process_nodes method
        expected_processed = [{'node_id': '!12345678', 'long_name': 'Test Node'}]
        expected_new = [{'node_id': '!12345678', 'long_name': 'Test Node'}]
        interface.node_processor.process_nodes = Mock(return_value=(expected_processed, expected_new))

        processed, new = interface.process_nodes()

        assert processed == expected_processed
        assert new == expected_new
        interface.node_processor.process_nodes.assert_called_once()

    def test_process_nodes_empty_result(self, test_database):
        """Test node processing with empty result."""
        interface = MeshtasticInterface(database=test_database)

        # Mock the node processor's process_nodes method to return empty lists
        interface.node_processor.process_nodes = Mock(return_value=([], []))

        processed, new = interface.process_nodes()

        assert processed == []
        assert new == []

    def test_get_nodes_from_db_success(self, test_database):
        """Test getting nodes from database."""
        interface = MeshtasticInterface(database=test_database)

        # Mock the node processor's get_nodes_from_db method
        expected_nodes = [
            {'node_id': '!12345678', 'long_name': 'Node A'},
            {'node_id': '!87654321', 'long_name': 'Node B'}
        ]
        interface.node_processor.get_nodes_from_db = Mock(return_value=expected_nodes)

        nodes = interface.get_nodes_from_db()

        assert nodes == expected_nodes
        interface.node_processor.get_nodes_from_db.assert_called_once()

    def test_get_nodes_from_db_empty(self, test_database):
        """Test getting nodes from database when empty."""
        interface = MeshtasticInterface(database=test_database)

        # Mock the node processor's get_nodes_from_db method to return empty list
        interface.node_processor.get_nodes_from_db = Mock(return_value=[])

        nodes = interface.get_nodes_from_db()

        assert nodes == []

    def test_iface_property(self, mock_meshtastic_interface):
        """Test iface property for backward compatibility."""
        interface = MeshtasticInterface()

        # Mock the connection's get_interface method
        interface.connection.get_interface = Mock(return_value=mock_meshtastic_interface)

        result = interface.iface

        assert result == mock_meshtastic_interface
        interface.connection.get_interface.assert_called_once()

    def test_iface_property_none(self):
        """Test iface property when no interface exists."""
        interface = MeshtasticInterface()

        # Mock the connection's get_interface method to return None
        interface.connection.get_interface = Mock(return_value=None)

        result = interface.iface

        assert result is None

    def test_last_node_refresh_property(self, test_database):
        """Test last_node_refresh property for backward compatibility."""
        interface = MeshtasticInterface(database=test_database)

        # Set a test value
        interface.node_processor.last_node_refresh = 1234567890

        result = interface.last_node_refresh

        assert result == 1234567890

    def test_is_connected_true(self):
        """Test is_connected when connection is available."""
        interface = MeshtasticInterface()

        # Mock the connection's is_connected method
        interface.connection.is_connected = Mock(return_value=True)

        result = interface.is_connected()

        assert result is True
        interface.connection.is_connected.assert_called_once()

    def test_is_connected_false(self):
        """Test is_connected when connection is not available."""
        interface = MeshtasticInterface()

        # Mock the connection's is_connected method
        interface.connection.is_connected = Mock(return_value=False)

        result = interface.is_connected()

        assert result is False
        interface.connection.is_connected.assert_called_once()

    def test_disconnect(self):
        """Test disconnection."""
        interface = MeshtasticInterface()

        # Mock the connection's disconnect method
        interface.connection.disconnect = Mock()

        interface.disconnect()

        interface.connection.disconnect.assert_called_once()

    def test_component_integration(self, test_database):
        """Test that all components are properly integrated."""
        hostname = "192.168.1.100"
        interface = MeshtasticInterface(hostname, test_database)

        # Verify that all components share the same connection
        assert interface.messaging.connection is interface.connection
        assert interface.node_processor.connection is interface.connection

        # Verify that node processor has the database
        assert interface.node_processor.database is test_database

        # Verify connection has the hostname
        assert interface.connection.hostname == hostname

    def test_backward_compatibility_properties(self, test_database):
        """Test that backward compatibility properties work correctly."""
        interface = MeshtasticInterface(database=test_database)

        # Test that accessing properties through the interface works
        assert hasattr(interface, 'iface')
        assert hasattr(interface, 'last_node_refresh')
        assert hasattr(interface, 'hostname')
        assert hasattr(interface, 'database')

    def test_method_delegation(self, test_database):
        """Test that methods are properly delegated to components."""
        interface = MeshtasticInterface(database=test_database)

        # Mock all component methods
        interface.connection.connect = AsyncMock(return_value=True)
        interface.connection.is_connected = Mock(return_value=True)
        interface.connection.disconnect = Mock()
        interface.messaging.send_text = Mock(return_value=True)
        interface.node_processor.process_nodes = Mock(return_value=([], []))
        interface.node_processor.get_nodes_from_db = Mock(return_value=[])

        # Call methods through the interface
        interface.send_text("test")
        interface.process_nodes()
        interface.get_nodes_from_db()
        interface.is_connected()
        interface.disconnect()

        # Verify all methods were called
        interface.messaging.send_text.assert_called_once()
        interface.node_processor.process_nodes.assert_called_once()
        interface.node_processor.get_nodes_from_db.assert_called_once()
        interface.connection.is_connected.assert_called_once()
        interface.connection.disconnect.assert_called_once()

    @pytest.mark.asyncio
    async def test_async_method_delegation(self):
        """Test that async methods are properly delegated."""
        interface = MeshtasticInterface()

        # Mock the async connect method
        interface.connection.connect = AsyncMock(return_value=True)

        result = await interface.connect()

        assert result is True
        interface.connection.connect.assert_called_once()

    def test_initialization_order(self, test_database):
        """Test that components are initialized in the correct order."""
        hostname = "192.168.1.100"
        interface = MeshtasticInterface(hostname, test_database)

        # Connection should be initialized first
        assert interface.connection is not None
        assert interface.connection.hostname == hostname

        # Messaging should be initialized with connection
        assert interface.messaging is not None
        assert interface.messaging.connection is interface.connection

        # Node processor should be initialized with connection and database
        assert interface.node_processor is not None
        assert interface.node_processor.connection is interface.connection
        assert interface.node_processor.database is test_database

    def test_interface_encapsulation(self, test_database):
        """Test that the interface properly encapsulates its components."""
        interface = MeshtasticInterface(database=test_database)

        # Components should not be accessible directly through public interface
        # but should work through the unified interface
        assert hasattr(interface, 'connection')
        assert hasattr(interface, 'messaging')
        assert hasattr(interface, 'node_processor')

        # Public methods should work
        assert callable(interface.connect)
        assert callable(interface.send_text)
        assert callable(interface.process_nodes)
        assert callable(interface.get_nodes_from_db)
        assert callable(interface.is_connected)
        assert callable(interface.disconnect)
