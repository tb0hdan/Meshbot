"""Tests for MeshtasticConnection class."""
import asyncio
from unittest.mock import Mock, patch, AsyncMock
import pytest

from src.transport.mesh.connection import MeshtasticConnection


class TestMeshtasticConnection:
    """Test cases for MeshtasticConnection class."""

    def test_init_with_hostname(self):
        """Test connection initialization with hostname."""
        hostname = "192.168.1.100"
        connection = MeshtasticConnection(hostname)

        assert connection.hostname == hostname
        assert connection.iface is None

    def test_init_without_hostname(self):
        """Test connection initialization without hostname."""
        connection = MeshtasticConnection()

        assert connection.hostname is None
        assert connection.iface is None

    @pytest.mark.asyncio
    @patch('meshtastic.tcp_interface.TCPInterface')
    async def test_connect_tcp_success(self, mock_tcp_interface):
        """Test successful TCP connection."""
        hostname = "192.168.1.100"
        connection = MeshtasticConnection(hostname)

        # Setup mock
        mock_iface = Mock()
        mock_iface.isConnected = Mock(return_value=True)
        mock_tcp_interface.return_value = mock_iface

        result = await connection.connect()

        assert result is True
        assert connection.iface == mock_iface
        mock_tcp_interface.assert_called_once_with(hostname)
        mock_iface.isConnected.assert_called_once()

    @pytest.mark.asyncio
    @patch('meshtastic.serial_interface.SerialInterface')
    async def test_connect_serial_success(self, mock_serial_interface):
        """Test successful Serial connection."""
        connection = MeshtasticConnection()

        # Setup mock
        mock_iface = Mock()
        mock_iface.isConnected = Mock(return_value=True)
        mock_serial_interface.return_value = mock_iface

        result = await connection.connect()

        assert result is True
        assert connection.iface == mock_iface
        mock_serial_interface.assert_called_once()
        mock_iface.isConnected.assert_called_once()

    @pytest.mark.asyncio
    @patch('meshtastic.tcp_interface.TCPInterface')
    async def test_connect_tcp_failure(self, mock_tcp_interface):
        """Test TCP connection failure."""
        hostname = "192.168.1.100"
        connection = MeshtasticConnection(hostname)

        # Setup mock to raise exception
        mock_tcp_interface.side_effect = Exception("Connection failed")

        result = await connection.connect()

        assert result is False
        assert connection.iface is None

    @pytest.mark.asyncio
    @patch('meshtastic.tcp_interface.TCPInterface')
    async def test_connect_tcp_not_connected(self, mock_tcp_interface):
        """Test TCP connection when isConnected returns False."""
        hostname = "192.168.1.100"
        connection = MeshtasticConnection(hostname)

        # Setup mock
        mock_iface = Mock()
        mock_iface.isConnected = Mock(return_value=False)
        mock_tcp_interface.return_value = mock_iface

        result = await connection.connect()

        assert result is False
        assert connection.iface == mock_iface

    @pytest.mark.asyncio
    @patch('meshtastic.tcp_interface.TCPInterface')
    async def test_connect_tcp_no_isconnected_method(self, mock_tcp_interface):
        """Test TCP connection when interface has no isConnected method."""
        hostname = "192.168.1.100"
        connection = MeshtasticConnection(hostname)

        # Setup mock without isConnected method
        mock_iface = Mock()
        delattr(mock_iface, 'isConnected')
        mock_tcp_interface.return_value = mock_iface

        result = await connection.connect()

        assert result is True
        assert connection.iface == mock_iface

    @pytest.mark.asyncio
    @patch('meshtastic.tcp_interface.TCPInterface')
    async def test_connect_tcp_isconnected_exception(self, mock_tcp_interface):
        """Test TCP connection when isConnected raises exception."""
        hostname = "192.168.1.100"
        connection = MeshtasticConnection(hostname)

        # Setup mock
        mock_iface = Mock()
        mock_iface.isConnected = Mock(side_effect=Exception("Check failed"))
        mock_tcp_interface.return_value = mock_iface

        result = await connection.connect()

        assert result is True
        assert connection.iface == mock_iface

    def test_is_connected_true(self, mock_meshtastic_interface):
        """Test is_connected when interface is connected."""
        connection = MeshtasticConnection()
        connection.iface = mock_meshtastic_interface

        result = connection.is_connected()

        assert result is True
        mock_meshtastic_interface.isConnected.assert_called_once()

    def test_is_connected_false(self, mock_meshtastic_interface):
        """Test is_connected when interface is not connected."""
        connection = MeshtasticConnection()
        connection.iface = mock_meshtastic_interface
        mock_meshtastic_interface.isConnected.return_value = False

        result = connection.is_connected()

        assert result is False

    def test_is_connected_no_interface(self):
        """Test is_connected when no interface exists."""
        connection = MeshtasticConnection()

        result = connection.is_connected()

        assert result is False

    def test_is_connected_no_method(self):
        """Test is_connected when interface has no isConnected method."""
        connection = MeshtasticConnection()
        mock_iface = Mock()
        delattr(mock_iface, 'isConnected')
        connection.iface = mock_iface

        result = connection.is_connected()

        assert result is True

    def test_is_connected_exception(self):
        """Test is_connected when isConnected raises exception."""
        connection = MeshtasticConnection()
        mock_iface = Mock()
        mock_iface.isConnected = Mock(side_effect=Exception("Check failed"))
        connection.iface = mock_iface

        result = connection.is_connected()

        assert result is False

    def test_get_interface(self, mock_meshtastic_interface):
        """Test get_interface method."""
        connection = MeshtasticConnection()
        connection.iface = mock_meshtastic_interface

        result = connection.get_interface()

        assert result == mock_meshtastic_interface

    def test_get_interface_none(self):
        """Test get_interface when no interface exists."""
        connection = MeshtasticConnection()

        result = connection.get_interface()

        assert result is None

    def test_disconnect_success(self, mock_meshtastic_interface):
        """Test successful disconnect."""
        connection = MeshtasticConnection()
        connection.iface = mock_meshtastic_interface
        mock_meshtastic_interface.close = Mock()

        connection.disconnect()

        mock_meshtastic_interface.close.assert_called_once()
        assert connection.iface is None

    def test_disconnect_no_interface(self):
        """Test disconnect when no interface exists."""
        connection = MeshtasticConnection()

        # Should not raise exception
        connection.disconnect()

        assert connection.iface is None

    def test_disconnect_no_close_method(self):
        """Test disconnect when interface has no close method."""
        connection = MeshtasticConnection()
        mock_iface = Mock()
        delattr(mock_iface, 'close')
        connection.iface = mock_iface

        # Should not raise exception
        connection.disconnect()

        assert connection.iface is None

    def test_disconnect_exception(self):
        """Test disconnect when close raises exception."""
        connection = MeshtasticConnection()
        mock_iface = Mock()
        mock_iface.close = Mock(side_effect=Exception("Close failed"))
        connection.iface = mock_iface

        # Should not raise exception
        connection.disconnect()

        assert connection.iface is None

    @pytest.mark.asyncio
    @patch('meshtastic.serial_interface.SerialInterface')
    async def test_connect_empty_hostname_uses_serial(self, mock_serial_interface):
        """Test that empty hostname uses serial connection."""
        connection = MeshtasticConnection("")

        # Setup mock
        mock_iface = Mock()
        mock_iface.isConnected = Mock(return_value=True)
        mock_serial_interface.return_value = mock_iface

        result = await connection.connect()

        assert result is True
        mock_serial_interface.assert_called_once()

    @pytest.mark.asyncio
    @patch('meshtastic.serial_interface.SerialInterface')
    async def test_connect_single_char_hostname_uses_serial(self, mock_serial_interface):
        """Test that single character hostname uses serial connection."""
        connection = MeshtasticConnection("x")

        # Setup mock
        mock_iface = Mock()
        mock_iface.isConnected = Mock(return_value=True)
        mock_serial_interface.return_value = mock_iface

        result = await connection.connect()

        assert result is True
        mock_serial_interface.assert_called_once()
