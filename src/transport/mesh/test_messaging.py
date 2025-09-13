"""Tests for MeshtasticMessaging class."""
from unittest.mock import Mock
import pytest

from src.transport.mesh.messaging import MeshtasticMessaging


class TestMeshtasticMessaging:
    """Test cases for MeshtasticMessaging class."""

    def test_init(self):
        """Test messaging initialization."""
        mock_connection = Mock()
        messaging = MeshtasticMessaging(mock_connection)

        assert messaging.connection == mock_connection

    def test_send_text_success_primary_channel(self, mock_meshtastic_interface):
        """Test successful text sending to primary channel."""
        mock_connection = Mock()
        mock_connection.get_interface.return_value = mock_meshtastic_interface
        messaging = MeshtasticMessaging(mock_connection)

        message = "Hello mesh network!"
        result = messaging.send_text(message)

        assert result is True
        mock_connection.get_interface.assert_called_once()
        mock_meshtastic_interface.sendText.assert_called_once_with(message)

    def test_send_text_success_specific_node(self, mock_meshtastic_interface):
        """Test successful text sending to specific node."""
        mock_connection = Mock()
        mock_connection.get_interface.return_value = mock_meshtastic_interface
        messaging = MeshtasticMessaging(mock_connection)

        message = "Hello specific node!"
        destination_id = "!12345678"
        result = messaging.send_text(message, destination_id)

        assert result is True
        mock_connection.get_interface.assert_called_once()
        mock_meshtastic_interface.sendText.assert_called_once_with(
            message, destinationId=destination_id
        )

    def test_send_text_no_interface(self):
        """Test text sending when no interface is available."""
        mock_connection = Mock()
        mock_connection.get_interface.return_value = None
        messaging = MeshtasticMessaging(mock_connection)

        message = "Hello mesh network!"
        result = messaging.send_text(message)

        assert result is False
        mock_connection.get_interface.assert_called_once()

    def test_send_text_specific_node_fallback_to_primary(self, mock_meshtastic_interface):
        """Test fallback to primary channel when specific node sending fails."""
        mock_connection = Mock()
        mock_connection.get_interface.return_value = mock_meshtastic_interface
        messaging = MeshtasticMessaging(mock_connection)

        # Mock sendText to fail on first call (with destinationId) and succeed on second
        def side_effect(*args, **kwargs):
            if 'destinationId' in kwargs:
                raise Exception("Failed to send to specific node")
            return None

        mock_meshtastic_interface.sendText.side_effect = side_effect

        message = "Hello specific node!"
        destination_id = "!12345678"
        result = messaging.send_text(message, destination_id)

        assert result is True
        assert mock_meshtastic_interface.sendText.call_count == 2
        # First call with destinationId, second call without
        calls = mock_meshtastic_interface.sendText.call_args_list
        assert calls[0][0] == (message,) and calls[0][1] == {'destinationId': destination_id}
        assert calls[1][0] == (message,) and calls[1][1] == {}

    def test_send_text_exception_primary_channel(self, mock_meshtastic_interface):
        """Test exception handling when sending to primary channel."""
        mock_connection = Mock()
        mock_connection.get_interface.return_value = mock_meshtastic_interface
        messaging = MeshtasticMessaging(mock_connection)

        # Mock sendText to raise exception
        mock_meshtastic_interface.sendText.side_effect = Exception("Send failed")

        message = "Hello mesh network!"
        result = messaging.send_text(message)

        assert result is False
        mock_connection.get_interface.assert_called_once()
        mock_meshtastic_interface.sendText.assert_called_once_with(message)

    def test_send_text_exception_specific_node_and_fallback(self, mock_meshtastic_interface):
        """Test exception handling when both specific node and fallback fail."""
        mock_connection = Mock()
        mock_connection.get_interface.return_value = mock_meshtastic_interface
        messaging = MeshtasticMessaging(mock_connection)

        # Mock sendText to always raise exception
        mock_meshtastic_interface.sendText.side_effect = Exception("Send failed")

        message = "Hello specific node!"
        destination_id = "!12345678"
        result = messaging.send_text(message, destination_id)

        assert result is False
        assert mock_meshtastic_interface.sendText.call_count == 2

    def test_send_text_empty_message(self, mock_meshtastic_interface):
        """Test sending empty message."""
        mock_connection = Mock()
        mock_connection.get_interface.return_value = mock_meshtastic_interface
        messaging = MeshtasticMessaging(mock_connection)

        message = ""
        result = messaging.send_text(message)

        assert result is True
        mock_meshtastic_interface.sendText.assert_called_once_with(message)

    def test_send_text_long_message(self, mock_meshtastic_interface):
        """Test sending long message."""
        mock_connection = Mock()
        mock_connection.get_interface.return_value = mock_meshtastic_interface
        messaging = MeshtasticMessaging(mock_connection)

        message = "A" * 500  # Very long message
        result = messaging.send_text(message)

        assert result is True
        mock_meshtastic_interface.sendText.assert_called_once_with(message)

    def test_send_text_unicode_message(self, mock_meshtastic_interface):
        """Test sending unicode message."""
        mock_connection = Mock()
        mock_connection.get_interface.return_value = mock_meshtastic_interface
        messaging = MeshtasticMessaging(mock_connection)

        message = "Hello 🌐 mesh! 中文测试"
        result = messaging.send_text(message)

        assert result is True
        mock_meshtastic_interface.sendText.assert_called_once_with(message)

    def test_send_text_none_destination(self, mock_meshtastic_interface):
        """Test sending text with None destination (should use primary channel)."""
        mock_connection = Mock()
        mock_connection.get_interface.return_value = mock_meshtastic_interface
        messaging = MeshtasticMessaging(mock_connection)

        message = "Hello mesh network!"
        result = messaging.send_text(message, None)

        assert result is True
        mock_meshtastic_interface.sendText.assert_called_once_with(message)

    def test_send_text_empty_destination(self, mock_meshtastic_interface):
        """Test sending text with empty destination (should use primary channel)."""
        mock_connection = Mock()
        mock_connection.get_interface.return_value = mock_meshtastic_interface
        messaging = MeshtasticMessaging(mock_connection)

        message = "Hello mesh network!"
        result = messaging.send_text(message, "")

        assert result is True
        mock_meshtastic_interface.sendText.assert_called_once_with(message)

    def test_is_ready_connected(self):
        """Test is_ready when connection is available."""
        mock_connection = Mock()
        mock_connection.is_connected.return_value = True
        messaging = MeshtasticMessaging(mock_connection)

        result = messaging.is_ready()

        assert result is True
        mock_connection.is_connected.assert_called_once()

    def test_is_ready_not_connected(self):
        """Test is_ready when connection is not available."""
        mock_connection = Mock()
        mock_connection.is_connected.return_value = False
        messaging = MeshtasticMessaging(mock_connection)

        result = messaging.is_ready()

        assert result is False
        mock_connection.is_connected.assert_called_once()

    def test_send_text_numeric_destination(self, mock_meshtastic_interface):
        """Test sending text with numeric destination ID."""
        mock_connection = Mock()
        mock_connection.get_interface.return_value = mock_meshtastic_interface
        messaging = MeshtasticMessaging(mock_connection)

        message = "Hello numeric node!"
        destination_id = 123456789
        result = messaging.send_text(message, destination_id)

        assert result is True
        mock_meshtastic_interface.sendText.assert_called_once_with(
            message, destinationId=destination_id
        )

    def test_send_text_interface_exception_on_get(self):
        """Test exception handling when getting interface fails."""
        mock_connection = Mock()
        mock_connection.get_interface.side_effect = Exception("Interface error")
        messaging = MeshtasticMessaging(mock_connection)

        message = "Hello mesh network!"
        result = messaging.send_text(message)

        assert result is False
        mock_connection.get_interface.assert_called_once()
