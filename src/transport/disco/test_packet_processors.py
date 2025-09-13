"""Tests for Discord packet processors."""
import queue
import math
from datetime import datetime
from unittest.mock import Mock, patch

import pytest

from .packet_processors import PacketProcessor


class TestPacketProcessor:
    """Tests for PacketProcessor class."""

    @pytest.fixture
    def packet_processor(self, mock_database_for_processors, mock_meshtastic, mock_command_handler):
        """Create a PacketProcessor instance for testing."""
        mesh_queue = queue.Queue()
        return PacketProcessor(mock_database_for_processors, mesh_queue, mock_meshtastic, mock_command_handler)

    def test_process_text_packet_basic(self, packet_processor, sample_mesh_packet):
        """Test processing basic text packet."""
        packet_processor.database.get_node_display_name.return_value = "TestNode"

        packet_processor.process_text_packet(sample_mesh_packet)

        # Should queue message for Discord
        assert not packet_processor.mesh_to_discord_queue.empty()
        queued_item = packet_processor.mesh_to_discord_queue.get()

        assert queued_item['type'] == 'text'
        assert queued_item['from_name'] == 'TestNode'
        assert queued_item['text'] == 'Hello from test node!'
        assert queued_item['hops_away'] == 1

    def test_process_text_packet_ping_message(self, packet_processor):
        """Test processing ping text packet triggers pong response."""
        ping_packet = {
            'fromId': '!12345678',
            'toId': 'Primary',
            'hopsAway': 0,
            'decoded': {
                'portnum': 'TEXT_MESSAGE_APP',
                'text': 'ping'
            }
        }

        packet_processor.database.get_node_display_name.return_value = "PingNode"

        packet_processor.process_text_packet(ping_packet)

        # Should send pong response
        packet_processor.meshtastic.send_text.assert_called_once()
        call_args = packet_processor.meshtastic.send_text.call_args[0][0]
        assert "Pong!" in call_args
        assert "PingNode" in call_args

    def test_process_text_packet_ping_no_meshtastic(self, packet_processor):
        """Test ping handling when Meshtastic interface is unavailable."""
        ping_packet = {
            'fromId': '!12345678',
            'toId': 'Primary',
            'hopsAway': 0,
            'decoded': {
                'portnum': 'TEXT_MESSAGE_APP',
                'text': 'ping'
            }
        }

        packet_processor.meshtastic = None
        packet_processor.database.get_node_display_name.return_value = "PingNode"

        # Should not raise exception
        packet_processor.process_text_packet(ping_packet)

    def test_process_text_packet_adds_to_monitor(self, packet_processor, sample_mesh_packet):
        """Test that text packets are added to live monitor buffer."""
        packet_processor.database.get_node_display_name.return_value = "TestNode"

        packet_processor.process_text_packet(sample_mesh_packet)

        # Should add to command handler buffer
        packet_processor.command_handler.add_packet_to_buffer.assert_called_once()
        buffer_item = packet_processor.command_handler.add_packet_to_buffer.call_args[0][0]

        assert buffer_item['type'] == 'text'
        assert buffer_item['from_name'] == 'TestNode'
        assert buffer_item['text'] == 'Hello from test node!'

    def test_process_text_packet_stores_in_database(self, packet_processor, sample_mesh_packet):
        """Test that text packets are stored in database."""
        packet_processor.database.get_node_display_name.return_value = "TestNode"

        packet_processor.process_text_packet(sample_mesh_packet)

        # Should store message in database
        packet_processor.database.add_message.assert_called_once()
        message_data = packet_processor.database.add_message.call_args[0][0]

        assert message_data['from_node_id'] == '!12345678'
        assert message_data['to_node_id'] == '!87654321'
        assert message_data['message_text'] == 'Hello from test node!'
        assert message_data['port_num'] == 'TEXT_MESSAGE_APP'

    def test_process_telemetry_packet_device_metrics(self, packet_processor, sample_telemetry_packet):
        """Test processing telemetry packet with device metrics."""
        packet_processor.process_telemetry_packet(sample_telemetry_packet)

        # Should store telemetry data
        packet_processor.database.add_telemetry.assert_called_once()
        node_id, telemetry_data = packet_processor.database.add_telemetry.call_args[0]

        assert node_id == '!12345678'
        assert telemetry_data['battery_level'] == 85
        assert telemetry_data['voltage'] == 4.1
        assert telemetry_data['channel_utilization'] == 12.5
        assert telemetry_data['temperature'] == 23.5

    def test_process_telemetry_packet_invalid_node_id(self, packet_processor):
        """Test processing telemetry packet with invalid node ID."""
        invalid_packet = {
            'fromId': None,  # Invalid node ID
            'decoded': {
                'portnum': 'TELEMETRY_APP',
                'telemetry': {'deviceMetrics': {'batteryLevel': 85}}
            }
        }

        packet_processor.process_telemetry_packet(invalid_packet)

        # Should not store data for invalid node ID
        packet_processor.database.add_telemetry.assert_not_called()

    def test_process_telemetry_packet_no_data(self, packet_processor):
        """Test processing telemetry packet with no telemetry data."""
        empty_packet = {
            'fromId': '!12345678',
            'decoded': {
                'portnum': 'TELEMETRY_APP',
                'telemetry': {}  # No telemetry data
            }
        }

        packet_processor.process_telemetry_packet(empty_packet)

        # Should not store empty data
        packet_processor.database.add_telemetry.assert_not_called()

    def test_extract_telemetry_data_all_metrics(self, packet_processor):
        """Test extracting all types of telemetry metrics."""
        telemetry_data = {
            'deviceMetrics': {
                'batteryLevel': 85,
                'voltage': 4.1,
                'channelUtilization': 12.5,
                'airUtilTx': 8.2,
                'uptimeSeconds': 86400
            },
            'environmentMetrics': {
                'temperature': 23.5,
                'relativeHumidity': 65.0,
                'barometricPressure': 1013.25,
                'gasResistance': 150000
            },
            'airQualityMetrics': {
                'pm10Environmental': 15.2,
                'pm25Environmental': 8.9,
                'pm100Environmental': 25.1,
                'aqi': 45
            },
            'powerMetrics': {
                'ch1Voltage': 3.3,
                'ch2Voltage': 5.0,
                'ch3Voltage': 12.0
            }
        }

        packet = {'snr': 10.5, 'rssi': -75, 'frequency': 915.0}

        extracted = packet_processor._extract_telemetry_data(telemetry_data, packet)

        # Device metrics
        assert extracted['battery_level'] == 85
        assert extracted['voltage'] == 4.1
        assert extracted['channel_utilization'] == 12.5
        assert extracted['air_util_tx'] == 8.2
        assert extracted['uptime_seconds'] == 86400

        # Environment metrics
        assert extracted['temperature'] == 23.5
        assert extracted['humidity'] == 65.0
        assert extracted['pressure'] == 1013.25
        assert extracted['gas_resistance'] == 150000

        # Air quality metrics
        assert extracted['pm10'] == 15.2
        assert extracted['pm25'] == 8.9
        assert extracted['pm100'] == 25.1
        assert extracted['iaq'] == 45

        # Power metrics
        assert extracted['ch1_voltage'] == 3.3
        assert extracted['ch2_voltage'] == 5.0
        assert extracted['ch3_voltage'] == 12.0

        # Radio metrics
        assert extracted['snr'] == 10.5
        assert extracted['rssi'] == -75
        assert extracted['frequency'] == 915.0

    def test_process_position_packet_basic(self, packet_processor, sample_position_packet):
        """Test processing basic position packet."""
        # Mock no previous position
        packet_processor.database.get_last_position.return_value = None

        packet_processor.process_position_packet(sample_position_packet)

        # Should store position
        packet_processor.database.add_position.assert_called_once()
        node_id, position_data = packet_processor.database.add_position.call_args[0]

        assert node_id == '!12345678'
        assert position_data['latitude'] == 40.7128
        assert position_data['longitude'] == -74.0060
        assert position_data['altitude'] == 10
        assert position_data['source'] == 'meshtastic'

    def test_process_position_packet_invalid_coordinates(self, packet_processor):
        """Test processing position packet with invalid coordinates."""
        invalid_packet = {
            'fromId': '!12345678',
            'decoded': {
                'portnum': 'POSITION_APP',
                'position': {
                    'latitude_i': 0,  # Invalid (0,0) coordinates
                    'longitude_i': 0,
                    'altitude': 10
                }
            }
        }

        packet_processor.process_position_packet(invalid_packet)

        # Should not store invalid coordinates
        packet_processor.database.add_position.assert_not_called()

    def test_process_position_packet_movement_detection(self, packet_processor, sample_position_packet):
        """Test movement detection in position packet processing."""
        # Mock previous position
        packet_processor.database.get_last_position.return_value = {
            'latitude': 40.7120,  # Different from new position
            'longitude': -74.0070,
            'altitude': 5
        }

        packet_processor.process_position_packet(sample_position_packet)

        # Should detect movement and queue notification
        assert not packet_processor.mesh_to_discord_queue.empty()
        movement_item = packet_processor.mesh_to_discord_queue.get()
        assert movement_item['type'] == 'movement'

    def test_calculate_distance_basic(self):
        """Test distance calculation between two points."""
        # New York to Los Angeles (approximately 3944 km)
        lat1, lon1 = 40.7128, -74.0060  # New York
        lat2, lon2 = 34.0522, -118.2437  # Los Angeles

        distance = PacketProcessor.calculate_distance(lat1, lon1, lat2, lon2)

        # Should be approximately 3,944,000 meters (allow 10% tolerance)
        expected = 3944000
        assert abs(distance - expected) < expected * 0.1

    def test_calculate_distance_same_point(self):
        """Test distance calculation for same point."""
        lat, lon = 40.7128, -74.0060

        distance = PacketProcessor.calculate_distance(lat, lon, lat, lon)

        assert distance == 0.0

    def test_calculate_distance_error_handling(self):
        """Test distance calculation error handling."""
        with patch('math.sin', side_effect=Exception("Math error")):
            distance = PacketProcessor.calculate_distance(40.7, -74.0, 40.8, -74.1)
            assert distance == 0.0  # Should return 0 on error

    def test_check_for_movement_threshold(self, packet_processor):
        """Test movement detection threshold."""
        # Mock previous position 50 meters away (below threshold)
        packet_processor.database.get_last_position.return_value = {
            'latitude': 40.7128,
            'longitude': -74.0060
        }

        # New position ~50 meters away
        new_lat, new_lon = 40.7132, -74.0060

        packet_processor._check_for_movement('!12345678', new_lat, new_lon, 10)

        # Should not create movement notification (below 100m threshold)
        assert packet_processor.mesh_to_discord_queue.empty()

    def test_check_for_movement_above_threshold(self, packet_processor):
        """Test movement detection above threshold."""
        # Mock previous position 200 meters away (above threshold)
        packet_processor.database.get_last_position.return_value = {
            'latitude': 40.7128,
            'longitude': -74.0060
        }

        # New position ~200 meters away
        new_lat, new_lon = 40.7146, -74.0060

        packet_processor._check_for_movement('!12345678', new_lat, new_lon, 10)

        # Should create movement notification
        assert not packet_processor.mesh_to_discord_queue.empty()
        movement_item = packet_processor.mesh_to_discord_queue.get()
        assert movement_item['type'] == 'movement'
        assert movement_item['distance_moved'] > 100

    def test_process_routing_packet_basic(self, packet_processor, sample_routing_packet):
        """Test processing basic routing packet."""
        packet_processor.database.get_node_display_name.side_effect = lambda x: f"Node{x[-8:]}"

        packet_processor.process_routing_packet(sample_routing_packet)

        # Should queue traceroute for Discord
        assert not packet_processor.mesh_to_discord_queue.empty()
        traceroute_item = packet_processor.mesh_to_discord_queue.get()

        assert traceroute_item['type'] == 'traceroute'
        assert traceroute_item['from_name'] == 'Node12345678'
        assert traceroute_item['to_name'] == 'Node87654321'
        assert traceroute_item['hops_count'] > 0

    def test_process_routing_packet_no_route_discovery(self, packet_processor):
        """Test processing routing packet without route discovery data."""
        invalid_packet = {
            'fromId': '!12345678',
            'toId': '!87654321',
            'decoded': {
                'portnum': 'ROUTING_APP',
                'routing': {}  # No routeDiscovery data
            }
        }

        packet_processor.process_routing_packet(invalid_packet)

        # Should not queue anything
        assert packet_processor.mesh_to_discord_queue.empty()

    def test_build_route_string_bidirectional(self, packet_processor):
        """Test building route string with bidirectional route."""
        packet_processor.database.get_node_display_name.side_effect = lambda x: f"Node{x[-8:]}"

        route = [111111111, 222222222]
        route_back = [333333333, 444444444]
        snr_towards = [32, 28, 24]  # SNR values * 4
        snr_back = [20, 16, 12]

        route_parts = packet_processor._build_route_string(
            "SourceNode", "DestNode", route, route_back, snr_towards, snr_back
        )

        assert len(route_parts) == 4  # 2 headers + 2 route lines
        assert "Towards DestNode" in route_parts[0]
        assert "Back from DestNode" in route_parts[2]
        assert "8.0dB" in route_parts[1]  # 32/4 = 8.0
        assert "5.0dB" in route_parts[3]  # 20/4 = 5.0

    def test_build_route_string_unknown_snr(self, packet_processor):
        """Test building route string with unknown SNR values."""
        packet_processor.database.get_node_display_name.side_effect = lambda x: f"Node{x[-8:]}"

        route = [111111111]
        route_back = []
        snr_towards = [-128]  # UNK_SNR value
        snr_back = []

        route_parts = packet_processor._build_route_string(
            "SourceNode", "DestNode", route, route_back, snr_towards, snr_back
        )

        # Should not include SNR for unknown values
        assert "dB" not in route_parts[1]

    def test_add_telemetry_to_monitor(self, packet_processor):
        """Test adding telemetry data to monitor buffer."""
        packet_processor.database.get_node_display_name.return_value = "TestNode"

        extracted_data = {
            'battery_level': 85,
            'temperature': 23.5,
            'snr': 10.5
        }

        packet_processor._add_telemetry_to_monitor('!12345678', extracted_data)

        # Should add to command handler buffer
        packet_processor.command_handler.add_packet_to_buffer.assert_called_once()
        buffer_item = packet_processor.command_handler.add_packet_to_buffer.call_args[0][0]

        assert buffer_item['type'] == 'telemetry'
        assert buffer_item['from_name'] == 'TestNode'
        assert buffer_item['sensor_data'] == ['battery_level', 'temperature', 'snr']

    def test_add_telemetry_to_monitor_no_handler(self, packet_processor):
        """Test adding telemetry to monitor when no command handler."""
        packet_processor.command_handler = None

        extracted_data = {'battery_level': 85}

        # Should not raise exception
        packet_processor._add_telemetry_to_monitor('!12345678', extracted_data)

    def test_store_telemetry_data_success(self, packet_processor):
        """Test successful telemetry data storage."""
        packet_processor.database.add_telemetry.return_value = True

        extracted_data = {'battery_level': 85, 'temperature': 23.5}

        packet_processor._store_telemetry_data('!12345678', extracted_data)

        packet_processor.database.add_telemetry.assert_called_once_with('!12345678', extracted_data)

    def test_store_telemetry_data_failure(self, packet_processor):
        """Test handling telemetry data storage failure."""
        packet_processor.database.add_telemetry.return_value = False

        extracted_data = {'battery_level': 85}

        # Should not raise exception on storage failure
        packet_processor._store_telemetry_data('!12345678', extracted_data)

    def test_store_telemetry_data_exception(self, packet_processor):
        """Test handling telemetry data storage exception."""
        packet_processor.database.add_telemetry.side_effect = Exception("DB Error")

        extracted_data = {'battery_level': 85}

        # Should not raise exception
        packet_processor._store_telemetry_data('!12345678', extracted_data)

    def test_process_text_packet_database_error(self, packet_processor, sample_mesh_packet):
        """Test text packet processing with database storage error."""
        packet_processor.database.get_node_display_name.return_value = "TestNode"
        packet_processor.database.add_message.side_effect = Exception("DB Error")

        # Should not raise exception
        packet_processor.process_text_packet(sample_mesh_packet)

        # Should still queue for Discord despite database error
        assert not packet_processor.mesh_to_discord_queue.empty()

    def test_process_position_packet_database_error(self, packet_processor, sample_position_packet):
        """Test position packet processing with database storage error."""
        packet_processor.database.get_last_position.return_value = None
        packet_processor.database.add_position.side_effect = Exception("DB Error")

        # Should not raise exception
        packet_processor.process_position_packet(sample_position_packet)

    def test_create_movement_notification_details(self, packet_processor):
        """Test movement notification creation with detailed validation."""
        packet_processor.database.get_node_display_name.return_value = "MobileNode"

        packet_processor._create_movement_notification(
            '!12345678', 250.5, 40.7128, -74.0060, 40.7130, -74.0058, 15.0
        )

        assert not packet_processor.mesh_to_discord_queue.empty()
        movement_payload = packet_processor.mesh_to_discord_queue.get()

        assert movement_payload['type'] == 'movement'
        assert movement_payload['from_name'] == 'MobileNode'
        assert movement_payload['distance_moved'] == 250.5
        assert movement_payload['new_alt'] == 15.0
        assert 'timestamp' in movement_payload

        # Should also add to monitor buffer
        packet_processor.command_handler.add_packet_to_buffer.assert_called_once()

    def test_process_telemetry_packet_adds_radio_metrics(self, packet_processor):
        """Test that radio metrics are included in telemetry processing."""
        telemetry_packet = {
            'fromId': '!12345678',
            'hopsAway': 0,
            'snr': 12.5,
            'rssi': -68,
            'frequency': 915.0,
            'decoded': {
                'portnum': 'TELEMETRY_APP',
                'telemetry': {
                    'deviceMetrics': {'batteryLevel': 90}
                }
            }
        }

        packet_processor.process_telemetry_packet(telemetry_packet)

        # Should include radio metrics in stored data
        packet_processor.database.add_telemetry.assert_called_once()
        node_id, telemetry_data = packet_processor.database.add_telemetry.call_args[0]

        assert telemetry_data['snr'] == 12.5
        assert telemetry_data['rssi'] == -68
        assert telemetry_data['frequency'] == 915.0
        assert telemetry_data['battery_level'] == 90
