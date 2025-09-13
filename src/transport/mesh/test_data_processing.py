"""Tests for MeshtasticDataProcessor class."""
import pytest

from src.transport.mesh.data_processing import MeshtasticDataProcessor


class TestMeshtasticDataProcessor:
    """Test cases for MeshtasticDataProcessor class."""

    def test_extract_telemetry_data_complete(self, sample_telemetry_data):
        """Test extracting complete telemetry data."""
        result = MeshtasticDataProcessor.extract_telemetry_data(sample_telemetry_data)

        assert result['snr'] == 10.5
        assert result['rssi'] == -75
        assert result['frequency'] == 915.0
        assert result['latitude'] == 40.7128
        assert result['longitude'] == -74.0060
        assert result['altitude'] == 10
        assert result['speed'] == 2.5
        assert result['heading'] == 180.0
        assert result['accuracy'] == 3.0

    def test_extract_telemetry_data_partial(self):
        """Test extracting partial telemetry data."""
        partial_data = {
            'snr': 8.5,
            'temperature': 23.0,
            'other_field': 'should_not_be_included'
        }

        result = MeshtasticDataProcessor.extract_telemetry_data(partial_data)

        assert result['snr'] == 8.5
        assert 'temperature' not in result  # temperature not in telemetry_fields
        assert 'other_field' not in result
        assert len(result) == 1

    def test_extract_telemetry_data_empty(self):
        """Test extracting telemetry from empty data."""
        result = MeshtasticDataProcessor.extract_telemetry_data({})

        assert result == {}

    def test_extract_telemetry_data_none_values(self):
        """Test extracting telemetry with None values."""
        data_with_none = {
            'snr': 10.5,
            'rssi': None,
            'frequency': 915.0,
            'latitude': None
        }

        result = MeshtasticDataProcessor.extract_telemetry_data(data_with_none)

        assert result['snr'] == 10.5
        assert result['frequency'] == 915.0
        assert 'rssi' not in result
        assert 'latitude' not in result

    def test_extract_position_data_complete(self, sample_position_data):
        """Test extracting complete position data."""
        node_data = {
            'latitude': sample_position_data['latitude'],
            'longitude': sample_position_data['longitude'],
            'altitude': sample_position_data['altitude'],
            'speed': sample_position_data['speed'],
            'heading': sample_position_data['heading'],
            'accuracy': sample_position_data['accuracy']
        }

        result = MeshtasticDataProcessor.extract_position_data(node_data)

        assert result is not None
        assert result['latitude'] == 40.7128
        assert result['longitude'] == -74.0060
        assert result['altitude'] == 10
        assert result['speed'] == 2.5
        assert result['heading'] == 180.0
        assert result['accuracy'] == 3.0
        assert result['source'] == 'meshtastic'

    def test_extract_position_data_minimal(self):
        """Test extracting position data with only required fields."""
        node_data = {
            'latitude': 40.7128,
            'longitude': -74.0060
        }

        result = MeshtasticDataProcessor.extract_position_data(node_data)

        assert result is not None
        assert result['latitude'] == 40.7128
        assert result['longitude'] == -74.0060
        assert result['altitude'] is None
        assert result['speed'] is None
        assert result['heading'] is None
        assert result['accuracy'] is None
        assert result['source'] == 'meshtastic'

    def test_extract_position_data_missing_latitude(self):
        """Test extracting position data without latitude."""
        node_data = {
            'longitude': -74.0060,
            'altitude': 10
        }

        result = MeshtasticDataProcessor.extract_position_data(node_data)

        assert result is None

    def test_extract_position_data_missing_longitude(self):
        """Test extracting position data without longitude."""
        node_data = {
            'latitude': 40.7128,
            'altitude': 10
        }

        result = MeshtasticDataProcessor.extract_position_data(node_data)

        assert result is None

    def test_extract_position_data_none_coordinates(self):
        """Test extracting position data with None coordinates."""
        node_data = {
            'latitude': None,
            'longitude': -74.0060
        }

        result = MeshtasticDataProcessor.extract_position_data(node_data)

        assert result is None

    def test_extract_environmental_data_complete(self):
        """Test extracting complete environmental data."""
        telemetry_packet = {
            'temperature': 23.5,
            'humidity': 65.0,
            'pressure': 1013.25,
            'gas_resistance': 150000,
            'voltage': 4.12,
            'current': 0.5,
            'battery_level': 85.5
        }

        result = MeshtasticDataProcessor.extract_environmental_data(telemetry_packet)

        assert result['temperature'] == 23.5
        assert result['humidity'] == 65.0
        assert result['pressure'] == 1013.25
        assert result['gas_resistance'] == 150000
        assert result['voltage'] == 4.12
        assert result['current'] == 0.5
        assert result['battery_level'] == 85.5

    def test_extract_environmental_data_partial(self):
        """Test extracting partial environmental data."""
        telemetry_packet = {
            'temperature': 23.5,
            'non_env_field': 'should_not_be_included'
        }

        result = MeshtasticDataProcessor.extract_environmental_data(telemetry_packet)

        assert result['temperature'] == 23.5
        assert 'non_env_field' not in result
        assert len(result) == 1

    def test_extract_environmental_data_empty(self):
        """Test extracting environmental data from empty packet."""
        result = MeshtasticDataProcessor.extract_environmental_data({})

        assert result == {}

    def test_extract_device_metrics_complete(self):
        """Test extracting complete device metrics."""
        telemetry_packet = {
            'channel_utilization': 12.3,
            'air_util_tx': 8.7,
            'uptime_seconds': 86400
        }

        result = MeshtasticDataProcessor.extract_device_metrics(telemetry_packet)

        assert result['channel_utilization'] == 12.3
        assert result['air_util_tx'] == 8.7
        assert result['uptime_seconds'] == 86400

    def test_extract_device_metrics_partial(self):
        """Test extracting partial device metrics."""
        telemetry_packet = {
            'channel_utilization': 12.3,
            'other_field': 'should_not_be_included'
        }

        result = MeshtasticDataProcessor.extract_device_metrics(telemetry_packet)

        assert result['channel_utilization'] == 12.3
        assert 'other_field' not in result
        assert len(result) == 1

    def test_normalize_node_data_complete(self, sample_raw_node_data):
        """Test normalizing complete node data."""
        result = MeshtasticDataProcessor.normalize_node_data(sample_raw_node_data)

        assert result['long_name'] == 'Test Node Alpha'
        assert result['short_name'] == 'ALPHA'
        assert result['hw_model'] == 'TBEAM'
        assert result['firmware_version'] == '2.3.2.abc123'
        assert result['macaddr'] == '00:11:22:33:44:55'
        assert result['hops_away'] == 1
        assert result['is_router'] is False
        assert result['is_client'] is True

    def test_normalize_node_data_missing_user(self):
        """Test normalizing node data without user information."""
        raw_data = {
            'hwModel': 'TBEAM',
            'hopsAway': 2
        }

        result = MeshtasticDataProcessor.normalize_node_data(raw_data)

        assert result['long_name'] == 'Unknown'
        assert result['short_name'] == ''
        assert result['hw_model'] == 'TBEAM'
        assert result['hops_away'] == 2
        assert result['is_router'] is False
        assert result['is_client'] is True

    def test_normalize_node_data_empty_user(self):
        """Test normalizing node data with empty user information."""
        raw_data = {
            'user': {},
            'hwModel': 'TBEAM'
        }

        result = MeshtasticDataProcessor.normalize_node_data(raw_data)

        assert result['long_name'] == 'Unknown'
        assert result['short_name'] == ''

    def test_validate_telemetry_data_valid(self, sample_telemetry_data):
        """Test validating valid telemetry data."""
        valid_data = {
            'snr': 10.5,
            'rssi': -75,
            'temperature': 23.5,
            'humidity': 65.0,
            'pressure': 1013.25,
            'battery_level': 85.5,
            'voltage': 4.12,
            'latitude': 40.7128,
            'longitude': -74.0060,
            'altitude': 100,
            'speed': 5.0
        }

        result = MeshtasticDataProcessor.validate_telemetry_data(valid_data)

        assert result is True

    def test_validate_telemetry_data_empty(self):
        """Test validating empty telemetry data."""
        result = MeshtasticDataProcessor.validate_telemetry_data({})

        assert result is False

    def test_validate_telemetry_data_invalid_snr(self):
        """Test validating telemetry with invalid SNR."""
        invalid_data = {'snr': 50}  # Too high

        result = MeshtasticDataProcessor.validate_telemetry_data(invalid_data)

        assert result is False

    def test_validate_telemetry_data_invalid_rssi(self):
        """Test validating telemetry with invalid RSSI."""
        invalid_data = {'rssi': 10}  # Should be negative

        result = MeshtasticDataProcessor.validate_telemetry_data(invalid_data)

        assert result is False

    def test_validate_telemetry_data_invalid_temperature(self):
        """Test validating telemetry with invalid temperature."""
        invalid_data = {'temperature': 150}  # Too high

        result = MeshtasticDataProcessor.validate_telemetry_data(invalid_data)

        assert result is False

    def test_validate_telemetry_data_invalid_humidity(self):
        """Test validating telemetry with invalid humidity."""
        invalid_data = {'humidity': 150}  # Over 100%

        result = MeshtasticDataProcessor.validate_telemetry_data(invalid_data)

        assert result is False

    def test_validate_telemetry_data_invalid_latitude(self):
        """Test validating telemetry with invalid latitude."""
        invalid_data = {'latitude': 200}  # Invalid latitude

        result = MeshtasticDataProcessor.validate_telemetry_data(invalid_data)

        assert result is False

    def test_validate_telemetry_data_invalid_type(self):
        """Test validating telemetry with invalid data types."""
        invalid_data = {'snr': 'not_a_number'}

        result = MeshtasticDataProcessor.validate_telemetry_data(invalid_data)

        assert result is False

    def test_validate_telemetry_data_mixed_valid_invalid(self):
        """Test validating telemetry with mix of valid and invalid data."""
        mixed_data = {
            'snr': 10.5,  # Valid
            'rssi': 50    # Invalid (should be negative)
        }

        result = MeshtasticDataProcessor.validate_telemetry_data(mixed_data)

        assert result is False

    def test_validate_telemetry_data_unknown_fields(self):
        """Test validating telemetry with unknown fields (should be valid)."""
        data_with_unknown = {
            'snr': 10.5,
            'unknown_field': 999  # Not in validation rules
        }

        result = MeshtasticDataProcessor.validate_telemetry_data(data_with_unknown)

        assert result is True  # Unknown fields don't affect validation

    def test_format_packet_for_storage_complete(self, sample_packet_data):
        """Test formatting complete packet data for storage."""
        result = MeshtasticDataProcessor.format_packet_for_storage(sample_packet_data)

        assert result['from_node'] == '123456789'
        assert result['to_node'] == '987654321'
        assert 'timestamp' in result
        assert result['payload'] == 'test_payload'
        assert result['message_text'] == 'Hello from test node!'
        assert 'telemetry' in result
        assert 'position' in result

    def test_format_packet_for_storage_minimal(self):
        """Test formatting minimal packet data."""
        minimal_packet = {
            'from': 123456789,
            'payload': 'test'
        }

        result = MeshtasticDataProcessor.format_packet_for_storage(minimal_packet)

        assert result['from_node'] == '123456789'
        assert result['payload'] == 'test'
        assert 'to_node' not in result
        assert 'message_text' not in result

    def test_format_packet_for_storage_no_decoded(self):
        """Test formatting packet without decoded payload."""
        packet_without_decoded = {
            'from': 123456789,
            'to': 987654321,
            'payload': 'raw_payload'
        }

        result = MeshtasticDataProcessor.format_packet_for_storage(packet_without_decoded)

        assert result['from_node'] == '123456789'
        assert result['to_node'] == '987654321'
        assert result['payload'] == 'raw_payload'
        assert 'message_text' not in result
        assert 'telemetry' not in result
        assert 'position' not in result

    def test_format_packet_for_storage_partial_decoded(self):
        """Test formatting packet with partial decoded payload."""
        packet_partial = {
            'from': 123456789,
            'decoded': {
                'text': 'Hello!'
                # No telemetry or position
            }
        }

        result = MeshtasticDataProcessor.format_packet_for_storage(packet_partial)

        assert result['from_node'] == '123456789'
        assert result['message_text'] == 'Hello!'
        assert 'telemetry' not in result
        assert 'position' not in result

    def test_format_packet_for_storage_empty(self):
        """Test formatting empty packet data."""
        result = MeshtasticDataProcessor.format_packet_for_storage({})

        assert result == {}

    def test_format_packet_for_storage_string_conversion(self):
        """Test that from/to fields are converted to strings."""
        packet_with_numbers = {
            'from': 123456789,
            'to': 987654321
        }

        result = MeshtasticDataProcessor.format_packet_for_storage(packet_with_numbers)

        assert isinstance(result['from_node'], str)
        assert isinstance(result['to_node'], str)
        assert result['from_node'] == '123456789'
        assert result['to_node'] == '987654321'
